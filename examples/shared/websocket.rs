//! A WebSocket channel, as a server holds its connections (RFC Appendix
//! A.8): each connection is subscribed to a room, and a delivery to the
//! room goes out on each of its connections.

use std::collections::HashMap;
use std::future::{Future, ready};
use std::sync::{Arc, Mutex, PoisonError};

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventContent, RoomContext, RoomEvent, RoomId,
};
use roomkit::{Channel, ChannelDeclaration, ChannelError, text_of};
use serde_json::{Map, Value, json};
use tokio::sync::mpsc;

/// How many frames a connection may fall behind before it is dropped.
const BACKLOG: usize = 64;

/// The id the channel gives a connection: the server's, never the
/// client's.
pub type ConnectionId = u64;

/// A WebSocket channel: a transport whose connections each follow one room.
/// A delivery to a room goes out, as one JSON text frame, on each of the
/// room's connections, through the queue [`register_connection`] returns,
/// which the server's socket writer empties. A server registers a
/// connection before it answers the upgrade, so that no delivery made
/// once the client is connected misses it. A delivery never waits for a
/// socket: a connection whose queue is full, a client that stopped
/// reading, is dropped, and its queue ends. Cloning it shares its
/// connections.
///
/// The frame of an event: `{"room_id": "...", "index": 3, "type":
/// "message", "from": "assistant", "text": "..."}`, `from` naming the
/// channel the event came from. A tool call's row carries the call as
/// `tool` (`name`, `id`, `status`, and `result` on its end row), and the
/// message that ends an agent's turn its end as `ended` (the record's
/// `loop_end_reason`).
///
/// [`register_connection`]: Self::register_connection
#[derive(Clone, Debug)]
pub struct WebSocketChannel {
    declaration: ChannelDeclaration,
    connections: Arc<Mutex<Connections>>,
}

/// The open connections, and the id the next one takes.
#[derive(Debug, Default)]
struct Connections {
    next: ConnectionId,
    open: HashMap<ConnectionId, Connection>,
}

/// An open connection: the room it follows, and its queue of frames.
#[derive(Debug)]
struct Connection {
    room_id: RoomId,
    frames: mpsc::Sender<String>,
}

impl WebSocketChannel {
    /// A WebSocket channel registered as `channel_id`.
    pub fn new(channel_id: ChannelId) -> Self {
        Self {
            declaration: ChannelDeclaration::new(
                channel_id,
                ChannelType::WEBSOCKET,
                ChannelCategory::Transport,
                ChannelDirection::Bidirectional,
            ),
            connections: Arc::default(),
        }
    }

    /// The channel's id.
    pub fn id(&self) -> &ChannelId {
        &self.declaration.channel_id
    }

    /// Registers a connection following `room_id`: its registration, which
    /// unregisters it when dropped, and the frames the room's deliveries
    /// send it, which end once it is unregistered or dropped.
    pub fn register_connection(&self, room_id: RoomId) -> (Registration, mpsc::Receiver<String>) {
        let (frames, queue) = mpsc::channel(BACKLOG);
        let mut connections = self.lock();
        connections.next += 1;
        let id = connections.next;
        connections.open.insert(id, Connection { room_id, frames });
        let registration = Registration {
            channel: self.clone(),
            id,
        };
        (registration, queue)
    }

    /// How many connections are registered.
    pub fn connections(&self) -> usize {
        self.lock().open.len()
    }

    /// Unregisters the connection `id`, which then receives nothing more.
    fn unregister_connection(&self, id: ConnectionId) {
        self.lock().open.remove(&id);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Connections> {
        self.connections
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// A connection registered on a [`WebSocketChannel`], until it is dropped.
#[derive(Debug)]
pub struct Registration {
    channel: WebSocketChannel,
    id: ConnectionId,
}

impl Registration {
    /// The connection's id.
    pub fn id(&self) -> ConnectionId {
        self.id
    }

    /// Queues `frame` for this connection alone, as a delivery is queued:
    /// `false` when it cannot take it now or is gone.
    pub fn send(&self, frame: String) -> bool {
        self.channel
            .lock()
            .open
            .get(&self.id)
            .is_some_and(|connection| connection.frames.try_send(frame).is_ok())
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.channel.unregister_connection(self.id);
    }
}

impl Channel for WebSocketChannel {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let mut frame = Map::new();
        let fields = json!({
            "room_id": event.room_id,
            "index": event.index,
            "type": event.event_type,
            "from": event.source.channel_id,
            "text": text_of(&event.content),
        });
        frame.extend(fields.as_object().cloned().unwrap_or_default());
        if let EventContent::ToolCall(call) = &event.content {
            let tool = json!({
                "name": call.tool_name,
                "id": call.tool_id,
                "status": call.status,
                "result": call.result,
            });
            frame.insert("tool".to_owned(), tool);
        }
        if let Some(ended) = event.metadata.get("loop_end_reason") {
            frame.insert("ended".to_owned(), ended.clone());
        }
        let frame = Value::Object(frame).to_string();
        // Only the connections of the room delivered to; one that cannot
        // take the frame now is dropped rather than waited for.
        self.lock().open.retain(|_, connection| {
            connection.room_id != binding.room_id
                || connection.frames.try_send(frame.clone()).is_ok()
        });
        ready(Ok(ChannelOutput::default()))
    }
}
