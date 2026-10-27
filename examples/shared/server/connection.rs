//! One connection of the server: what it names, its join, then its reading
//! and its writing, until it closes.

use std::collections::HashMap;
use std::pin::pin;
use std::time::Duration;

use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, close_code};
use axum::extract::{self, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use futures_util::stream::{FuturesUnordered, SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use roomkit::model::{EventContent, InboundMessage, ParticipantId, RoomId};
use roomkit::{DeliveryHandle, InboundResult, KitError, ProcessInboundOptions};
use serde_json::json;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite;

use super::{App, Counted};
use crate::Registration;

/// What a client is told when the agent's turn on its message failed,
/// when the room refused its message, and when the server stopped before
/// the answer: none says more, so that no provider's error or room's state
/// reaches it.
pub const TURN_FAILED: &str = "the agent could not answer";
/// See [`TURN_FAILED`].
pub const REFUSED: &str = "the room refused the message";
/// See [`TURN_FAILED`].
pub const STOPPED: &str = "the server stopped before the answer";

/// What a message came to: refused or decided at once, or once its
/// delivery ran.
type Told = Result<InboundResult, KitError>;

/// The longest conversation id, user id and name a connection may give, in
/// characters, and what the server answers a connection that names one
/// wrong.
const ID_LIMIT: usize = 64;
const NAMING: &str = "a conversation and a user of 1 to 64 letters, digits, _ or -, and a \
                      name of 64 characters at most with no control character, are needed";

/// The largest message, and frame, a connection may send, in bytes.
pub const MESSAGE_LIMIT: usize = 64 * 1024;

/// A connection asking to join a conversation: the conversation's id names
/// its room, the `user` parameter its participant, `name` their name.
pub(super) async fn connect(
    State(app): State<App>,
    extract::Path(conversation): extract::Path<String>,
    Query(query): Query<HashMap<String, String>>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let counted = app.live.enter();
    let Some((room_id, user, name)) = named(&conversation, &query) else {
        return (StatusCode::BAD_REQUEST, NAMING).into_response();
    };
    let streams = query.get("stream").is_some_and(|stream| stream == "1");
    // A task of its own: a client that leaves drops this handler and what
    // it awaits, which must not leave a new room half made, without its
    // agent or its binding.
    let joining = tokio::spawn({
        let (app, room_id, user) = (app.clone(), room_id.clone(), user.clone());
        async move { app.join(&room_id, &user, name).await }
    });
    if let Err(error) = joining.await.map_err(Into::into).and_then(|joined| joined) {
        // What the client named is shown escaped, never as it came.
        println!(
            "  {:?} could not join {:?}: {error}",
            user.as_str(),
            room_id.as_str()
        );
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    // Registered before the upgrade is answered, so that no delivery made
    // once the client is connected misses it; an upgrade that fails drops
    // the registration with the callback.
    let connection = app.websocket.register_connection(room_id.clone(), streams);
    // Read once it is registered: an event committed meanwhile reaches it
    // live, in its history, or both, which a client tells apart by index.
    if app.history > 0 {
        match app.history_frame(&room_id).await {
            Ok(frame) => {
                // A queue just made has room for it.
                let _queued = connection.0.send(frame);
            }
            Err(error) => println!(
                "  {:?}'s history could not be read: {error}",
                room_id.as_str()
            ),
        }
    }
    // The frame's limit too: a frame is refused at its header, before the
    // library reads its body, up to 16 MiB by default.
    upgrade
        .max_frame_size(MESSAGE_LIMIT)
        .max_message_size(MESSAGE_LIMIT)
        .on_upgrade(move |socket| app.converse(socket, counted, connection, room_id, user))
}

/// The room, the participant and the name a connection to `conversation`
/// with `query` names, when each is one the server takes: a conversation
/// and a user of 1 to 64 ASCII letters, digits, `_` or `-`, and a name, if
/// any, of 64 characters at most, none a control character. An empty name
/// is none.
fn named(
    conversation: &str,
    query: &HashMap<String, String>,
) -> Option<(RoomId, ParticipantId, Option<String>)> {
    let plain = |id: &str| {
        (1..=ID_LIMIT).contains(&id.len())
            && id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    };
    let user = query.get("user").filter(|user| plain(user))?;
    if !plain(conversation) {
        return None;
    }
    let name = query.get("name").filter(|name| !name.is_empty());
    if name
        .is_some_and(|name| name.chars().count() > ID_LIMIT || name.chars().any(char::is_control))
    {
        return None;
    }
    Some((
        RoomId::new(conversation).ok()?,
        ParticipantId::new(user.as_str()).ok()?,
        name.cloned(),
    ))
}

impl App {
    /// Runs a connection of `user` in `room_id`: each text frame it sends is
    /// their message, and the room's deliveries go out on it, until it
    /// closes or the server stops.
    ///
    /// A connection whose writer stops, its client gone or not reading,
    /// ends at once, reader included: what it would send then never enters
    /// the room.
    async fn converse(
        self,
        socket: WebSocket,
        _counted: Counted,
        (registration, frames): (Registration, mpsc::Receiver<String>),
        room_id: RoomId,
        user: ParticipantId,
    ) {
        let (outgoing, incoming) = socket.split();
        let patience = self.patience.send;
        let mut writer = pin!(write(outgoing, frames, patience));
        let ending = tokio::select! {
            _ = &mut writer => return,
            ending = self.read(incoming, &registration, &room_id, &user) => ending,
        };
        drop(registration);
        // What was queued goes out, then the close that answers the ending.
        let Some(mut outgoing) = writer.await else {
            return;
        };
        // A socket gone meanwhile has nothing more to close.
        let _closed = match ending {
            Ending::Broken => return,
            Ending::Closed => tokio::time::timeout(patience, outgoing.close()).await,
            Ending::Stopping => {
                let close = close_frame(close_code::AWAY, "the server is stopping");
                tokio::time::timeout(patience, outgoing.send(close)).await
            }
            Ending::TooLarge => {
                let close = close_frame(close_code::SIZE, "the message is too large");
                tokio::time::timeout(patience, outgoing.send(close)).await
            }
        };
    }

    /// Reads `user`'s messages on `incoming` into `room_id`, until the
    /// client closes the connection, breaks it or sends a message over the
    /// limit, or the server stops. Each message's delivery runs on while the
    /// next is read; a stop waits for them, to tell each client whose answer
    /// it cut.
    async fn read(
        &self,
        mut incoming: SplitStream<WebSocket>,
        registration: &Registration,
        room_id: &RoomId,
        user: &ParticipantId,
    ) -> Ending {
        let mut stopping = self.stopping.clone();
        let mut answering = FuturesUnordered::new();
        let ending = loop {
            let received = tokio::select! {
                received = incoming.next() => received,
                Some(outcome) = answering.next() => {
                    tell(registration, user, outcome);
                    continue;
                }
                _ = stopping.wait_for(|stop| *stop) => break Ending::Stopping,
            };
            match received {
                Some(Ok(Message::Text(text))) => {
                    let said = self.say(registration, room_id, user, text.as_str()).await;
                    answering.extend(said.map(DeliveryHandle::wait));
                }
                Some(Ok(Message::Binary(_) | Message::Ping(_) | Message::Pong(_))) => {}
                Some(Ok(Message::Close(_))) => break Ending::Closed,
                Some(Err(error)) => break Ending::of(error),
                None => break Ending::Broken,
            }
        };
        // A client gone is told nothing; its deliveries run on, unwatched.
        if matches!(ending, Ending::Stopping) {
            while let Some(outcome) = answering.next().await {
                tell(registration, user, outcome);
            }
        }
        ending
    }

    /// `user`'s message `text`, into the room `room_id`, its delivery
    /// deferred: the call returns at the commit, so that the connection
    /// reads on while the agent answers. A message the room refused is told
    /// at once; the handle returned waits for the rest.
    async fn say(
        &self,
        registration: &Registration,
        room_id: &RoomId,
        user: &ParticipantId,
        text: &str,
    ) -> Option<DeliveryHandle> {
        let message = InboundMessage::new(
            self.websocket.id().clone(),
            user.as_str(),
            EventContent::text(text),
        );
        let options = ProcessInboundOptions {
            room_id: Some(room_id.clone()),
            defer_delivery: true,
            ..ProcessInboundOptions::default()
        };
        match self.kit.process_inbound(message, options).await {
            Ok(InboundResult {
                blocked: false,
                delivery: Some(delivery),
                ..
            }) => Some(delivery),
            outcome => {
                tell(registration, user, outcome);
                None
            }
        }
    }
}

/// Tells `user`, on `registration`, what became of their message when the
/// room refused it, its turn failed, or the server stopped before the
/// answer: an error frame, `{"error": "..."}`, its reason one of the
/// server's own.
fn tell(registration: &Registration, user: &ParticipantId, outcome: Told) {
    let told = match outcome {
        Ok(result) if result.blocked => {
            println!(
                "  {:?}'s message was refused: {:?}",
                user.as_str(),
                result.reason
            );
            REFUSED
        }
        Ok(result) => {
            let Some(error) = result.error else {
                return;
            };
            println!(
                "  the turn on {:?}'s message failed: {:?}",
                user.as_str(),
                error.message
            );
            TURN_FAILED
        }
        // Stored, its turn cut by the kit's close past its grace.
        Err(KitError::ShutDown) => {
            println!("  the server stopped before {:?}'s answer", user.as_str());
            STOPPED
        }
        Err(error) => {
            println!("  {:?}'s message was refused: {error}", user.as_str());
            REFUSED
        }
    };
    // A connection that cannot take it now is not waited for.
    let _told = registration.send(json!({ "error": told }).to_string());
}

/// Sends each frame of `frames` on `outgoing` until the queue ends, then
/// hands the socket back for its close. Each send gets `patience`: one that
/// takes longer, a client that stopped reading, or that fails ends it at
/// once, with no socket to close.
async fn write(
    mut outgoing: SplitSink<WebSocket, Message>,
    mut frames: mpsc::Receiver<String>,
    patience: Duration,
) -> Option<SplitSink<WebSocket, Message>> {
    while let Some(frame) = frames.recv().await {
        let sent = tokio::time::timeout(patience, outgoing.send(Message::Text(frame.into()))).await;
        if !matches!(sent, Ok(Ok(()))) {
            return None;
        }
    }
    Some(outgoing)
}

/// How a connection's reading ended, which decides how it is closed.
#[derive(Clone, Copy, Debug)]
enum Ending {
    /// The client closed it: its close is answered.
    Closed,
    /// The server stops: closed with 1001, going away.
    Stopping,
    /// The client sent a message over the limit: closed with 1009, too
    /// large.
    TooLarge,
    /// The connection broke: nothing more is sent.
    Broken,
}

impl Ending {
    /// The ending a read's `error` makes: too large for a message over the
    /// limit, broken for any other.
    fn of(error: axum::Error) -> Self {
        match error.into_inner().downcast::<tungstenite::Error>() {
            Ok(error) if matches!(*error, tungstenite::Error::Capacity(_)) => Self::TooLarge,
            Ok(_) | Err(_) => Self::Broken,
        }
    }
}

/// A close frame of `code`, saying `reason`.
fn close_frame(code: u16, reason: &'static str) -> Message {
    Message::Close(Some(CloseFrame {
        code,
        reason: reason.into(),
    }))
}
