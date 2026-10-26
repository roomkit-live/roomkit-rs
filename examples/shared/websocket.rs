//! A WebSocket channel, as a server holds its connections (RFC Appendix
//! A.8): each connection is subscribed to a room, and a delivery to the
//! room goes out on each of its connections. It streams: an agent's answer
//! reaches the connections that asked for it as it is written.

use std::collections::HashMap;
use std::future::{Future, ready};
use std::sync::{Arc, Mutex, PoisonError};

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventContent, RoomContext, RoomEvent, RoomId,
};
use roomkit::{Channel, ChannelDeclaration, ChannelError, DeliveryStream, StreamItem, text_of};
use serde_json::{Map, Value, json};
use tokio::sync::{mpsc, watch};

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
/// The frame of an event: `{"room_id": "...", "id": "...", "index": 3,
/// "type": "message", "from": "assistant", "text": "..."}`, `from` naming
/// the channel the event came from. A tool call's row carries the call as
/// `tool` (`name`, `id`, `status`, and `result` on its end row), and the
/// message that ends an agent's turn its end as `ended` (the record's
/// `loop_end_reason`).
///
/// The channel streams (RFC §6.1 `deliver_stream`): an agent's answer comes
/// as it is written. A connection that asked to stream receives each piece
/// of text as `{"room_id": "...", "type": "delta", "from": "assistant",
/// "text": "..."}`, then the row the pieces join to, as the store holds it,
/// with its id and index; every other connection receives the rows only.
/// The text is shown before `BEFORE_BROADCAST` judges the row it ends in,
/// so a room whose hooks rewrite answers keeps its connections from
/// streaming. The answer stops, the text sent so far stored marked
/// `cancelled`, once the last connection of the room that was there when
/// it began has left; a room with none reads it through, stored whole.
///
/// [`register_connection`]: Self::register_connection
#[derive(Clone, Debug)]
pub struct WebSocketChannel {
    declaration: ChannelDeclaration,
    connections: Arc<Mutex<Connections>>,
    /// Counts the connections that left, so that a stream sees one leave.
    left: watch::Sender<u64>,
}

/// The open connections, and the id the next one takes.
#[derive(Debug, Default)]
struct Connections {
    next: ConnectionId,
    open: HashMap<ConnectionId, Connection>,
}

/// An open connection: the room it follows, its queue of frames, and
/// whether it reads answers as they are written.
#[derive(Debug)]
struct Connection {
    room_id: RoomId,
    frames: mpsc::Sender<String>,
    streams: bool,
}

impl WebSocketChannel {
    /// A WebSocket channel registered as `channel_id`.
    pub fn new(channel_id: ChannelId) -> Self {
        let mut declaration = ChannelDeclaration::new(
            channel_id,
            ChannelType::WEBSOCKET,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        );
        declaration.supports_streaming_delivery = true;
        Self {
            declaration,
            connections: Arc::default(),
            left: watch::Sender::new(0),
        }
    }

    /// The channel's id.
    pub fn id(&self) -> &ChannelId {
        &self.declaration.channel_id
    }

    /// Registers a connection following `room_id`: its registration, which
    /// unregisters it when dropped, and the frames the room's deliveries
    /// send it, which end once it is unregistered or dropped.
    /// A connection that `streams` also receives an answer's text as it is
    /// written.
    pub fn register_connection(
        &self,
        room_id: RoomId,
        streams: bool,
    ) -> (Registration, mpsc::Receiver<String>) {
        let (frames, queue) = mpsc::channel(BACKLOG);
        let mut connections = self.lock();
        connections.next += 1;
        let id = connections.next;
        let connection = Connection {
            room_id,
            frames,
            streams,
        };
        connections.open.insert(id, connection);
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
        self.left.send_modify(|left| *left += 1);
    }

    /// Whether `room_id` has a connection.
    fn watched(&self, room_id: &RoomId) -> bool {
        self.lock()
            .open
            .values()
            .any(|connection| &connection.room_id == room_id)
    }

    /// Sends `frame` to the connections of `room_id` that `take` it; one
    /// that cannot take it now is dropped rather than waited for.
    fn send(&self, room_id: &RoomId, frame: &str, take: impl Fn(&Connection) -> bool) {
        let mut dropped = false;
        self.lock().open.retain(|_, connection| {
            let kept = &connection.room_id != room_id
                || !take(connection)
                || connection.frames.try_send(frame.to_owned()).is_ok();
            dropped |= !kept;
            kept
        });
        if dropped {
            self.left.send_modify(|left| *left += 1);
        }
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
        // Only the connections of the room delivered to.
        self.send(&binding.room_id, &frame_of(&event), |_| true);
        ready(Ok(ChannelOutput::default()))
    }

    /// An agent's answer as it is written: each piece of text to the
    /// connections that stream, each row to every connection of the room;
    /// stopped once the last connection there when it began has left.
    async fn deliver_stream(
        &self,
        mut stream: DeliveryStream,
        binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let room_id = binding.room_id;
        let watched = self.watched(&room_id);
        let mut left = self.left.subscribe();
        loop {
            if watched && !self.watched(&room_id) {
                // Nobody reads it any more: the answer stops here.
                return Ok(ChannelOutput::default());
            }
            let item = tokio::select! {
                item = stream.next() => item,
                // A connection left: look again.
                _ = left.changed() => continue,
            };
            match item {
                Some(Ok(StreamItem::Text(text))) => {
                    let frame = json!({
                        "room_id": room_id,
                        "type": "delta",
                        "from": stream.responder(),
                        "text": text,
                    });
                    self.send(&room_id, &frame.to_string(), |connection| {
                        connection.streams
                    });
                }
                Some(Ok(StreamItem::Row(row))) => self.send(&room_id, &frame_of(&row), |_| true),
                Some(Ok(_)) => {}
                // The answer failed: what was written went out already.
                Some(Err(failure)) => return Err(failure),
                None => return Ok(ChannelOutput::default()),
            }
        }
    }
}

/// The frame of `event`.
fn frame_of(event: &RoomEvent) -> String {
    let mut frame = Map::new();
    let fields = json!({
        "room_id": event.room_id,
        "id": event.id,
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
    Value::Object(frame).to_string()
}
