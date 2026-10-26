//! An AI agent served over WebSocket, its rooms kept in SQLite: an axum
//! server where each WebSocket connection is a person taking part in a room
//! through the `websocket` channel, an agent answering each message through
//! an OpenAI-compatible endpoint (RFC §6.4), and the rooms in a SQLite file
//! (RFC §14), so that a restarted server goes on with each conversation.
//!
//! - A connection to `/conversations/{conversation}?user=alice&name=Alice`
//!   is the participant `alice`, named Alice, of the room whose id is the
//!   conversation's (RFC §5.5). A new conversation's room is created with
//!   the agent and the `websocket` channel attached.
//! - Each text frame it sends is a message from `alice` into that room,
//!   through the inbound pipeline (RFC §10.1). A message the room refuses,
//!   or whose turn fails, gets `{"error": "..."}` back, a reason of the
//!   server's own. A message or a frame over 64 KiB closes the connection
//!   with 1009, a frame at its header, and a stop closes them all with
//!   1001.
//! - The room's deliveries to `websocket` go out on each of the room's
//!   connections, one JSON frame per event: the agent's answers. A room
//!   delivers an event to every channel but its source (RFC §10.1), so the
//!   people on `websocket` see the agent's answers, not each other's
//!   messages. A client that stops reading is dropped, reader included,
//!   once one send waits 5 seconds; a stop gives the connections and the
//!   kit 10 seconds.
//! - The conversation and the `user` are 1 to 64 letters, digits, `_` or
//!   `-`, and the name 64 characters at most, none a control character;
//!   anything else is refused with 400.
//!
//! What it leaves out, which a service built from it adds:
//! - TLS: it serves plain `ws://`, to be put behind a TLS proxy.
//! - Authentication: `user` is whoever the client says it is; a service
//!   takes the participant from its session.
//! - The `Origin` check: none is made, so with a session held in a cookie
//!   any web page could open a socket as its visitor (cross-site WebSocket
//!   hijacking). A service checks it in `connect`, given the request's
//!   `headers: HeaderMap`:
//!
//!   ```text
//!   if headers.get(ORIGIN).is_some_and(|origin| origin != "https://chat.example.com") {
//!       return StatusCode::FORBIDDEN.into_response();
//!   }
//!   ```
//!
//! - Who may create and join a room: any client creates one by naming a
//!   new conversation, and joins any it names.
//! - A rate limit: nothing bounds how often a client writes, and each
//!   message costs a model call.
//!
//! Its checks run offline, against a fake of the OpenAI wire and a
//! temporary file: Alice and Bob talk with the agent in one room, the
//! server restarts on the same file, and Alice goes on.
//!
//! With `--serve`, it stays up on 127.0.0.1:8080, or the address given
//! after the flag, its rooms in `server_agent.db`, until Ctrl+C. The agent
//! calls the endpoint `OPENAI_BASE_URL`, `OPENAI_API_KEY` and
//! `OPENAI_MODEL` name, or, without them, the fake, which answers from the
//! checks' script.
//!
//! ```sh
//! cargo run --example server_agent
//! OPENAI_BASE_URL=https://api.openai.com/v1 OPENAI_API_KEY=sk-... \
//!     OPENAI_MODEL=gpt-6-luna cargo run --example server_agent -- --serve
//! websocat 'ws://127.0.0.1:8080/conversations/bakery?user=alice&name=Alice'
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::pin;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use std::{env, io};

use axum::Router;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, close_code};
use axum::extract::{self, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use roomkit::model::{
    BindingOptions, ChannelId, EventContent, EventType, InboundMessage, Participant, ParticipantId,
    Post, RoomEvent, RoomId,
};
use roomkit::{ConversationStore, Environment, RoomKit, SystemEnvironment, text_of};
use roomkit_openai::{OpenAiAgent, OpenAiConfig};
use roomkit_sqlite::SqliteStore;
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::{WebSocketStream, tungstenite};

use shared::{Checks, FakeOpenAi, OpenAiEndpoint, Registration, TempDatabase, WebSocketChannel};

const SYSTEM_PROMPT: &str = "You answer the customers of a bakery, in one short sentence.";

/// Where `--serve` listens unless told another address, and the file it
/// keeps its rooms in.
const SERVE_ADDRESS: &str = "127.0.0.1:8080";
const SERVE_DATABASE: &str = "server_agent.db";

/// What a client is told when the agent's turn on its message failed, and
/// when the room refused its message: neither says more, so that no
/// provider's error or room's state reaches it.
const TURN_FAILED: &str = "the agent could not answer";
const REFUSED: &str = "the room refused the message";

/// The longest conversation id, user id and name a connection may give, in
/// characters, and what the server answers a connection that names one
/// wrong.
const ID_LIMIT: usize = 64;
const NAMING: &str = "a conversation and a user of 1 to 64 letters, digits, _ or -, and a \
                      name of 64 characters at most with no control character, are needed";

/// The largest message, and frame, a connection may send, in bytes.
const MESSAGE_LIMIT: usize = 64 * 1024;

/// The conversation of the checks, and what is said in it.
const CONVERSATION: &str = "bakery";
const ALICE_ASKS: &str = "Hi! Do you have sourdough today?";
const BOB_ASKS: &str = "Can I pick up two loaves at 5?";
const ALICE_AGAIN: &str = "Are the loaves still warm?";

/// What the fake answers, turn after turn.
const REPLIES: &[&str] = &[
    "Yes, we have sourdough until noon.",
    "Yes Bob, two loaves will be ready at 5.",
    "They came out of the oven an hour ago, Alice.",
];

/// Where the checks' servers listen: a free port of the loopback.
const ANY_PORT: &str = "127.0.0.1:0";

/// The kitchen's conversation, where Dave stops reading, what he sends
/// once he fell behind, and the posts that make him fall behind: large
/// ones, and as many as it takes, up to a bound.
const KITCHEN: &str = "kitchen";
const DAVE_LATE: &str = "Sorry, I was away. Is there any rye left?";
const LARGE_POST: usize = 60 * 1024;
const POSTS_AT_MOST: usize = 400;

/// The conversation where the model is unreachable, and how long a client
/// there waits to be told.
const CELLAR: &str = "cellar";

/// The length of the frame a client announces and never sends: far over
/// the limit, far under what the WebSocket library takes by default
/// (16 MiB).
const ANNOUNCED: u64 = 1 << 20;
const TOLD_PATIENCE: Duration = Duration::from_secs(2);

/// The receive buffer of a client that falls behind, in bytes.
const SMALL_BUFFER: u32 = 4096;

/// How long a check waits for a connection to end or a server to stop.
const STOP_PATIENCE: Duration = Duration::from_secs(5);

/// How long a client waits for an answer: a real model's included.
const ANSWER_PATIENCE: Duration = Duration::from_secs(60);

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let mut args = env::args().skip(1);
    if args.next().as_deref() == Some("--serve") {
        serve(args.next().as_deref().unwrap_or(SERVE_ADDRESS)).await?;
        return Ok(ExitCode::SUCCESS);
    }
    check().await
}

/// A running server: its address, its kit over the SQLite file, and what
/// stops it.
struct Server {
    address: SocketAddr,
    kit: RoomKit<SqliteStore>,
    stop: watch::Sender<bool>,
    serving: JoinHandle<io::Result<()>>,
    websocket: WebSocketChannel,
    live: Live,
    patience: Patience,
}

/// How long the server waits on a client and on its own work.
#[derive(Clone, Copy, Debug)]
struct Patience {
    /// How long one frame's send may take before its client is taken for
    /// gone: one that stopped reading (the Python implementation's
    /// `send_timeout`, 5 seconds by default).
    send: Duration,
    /// How long a stop waits for the connections and the kit's work to
    /// end.
    grace: Duration,
}

/// The server's patience when it serves; the checks' is shorter, so that
/// they run in milliseconds.
const SERVING: Patience = Patience {
    send: Duration::from_secs(5),
    grace: Duration::from_secs(10),
};
const CHECKING: Patience = Patience {
    send: Duration::from_millis(100),
    grace: Duration::from_secs(2),
};

/// What each request and connection of the server works with.
#[derive(Clone)]
struct App {
    kit: RoomKit<SqliteStore>,
    websocket: WebSocketChannel,
    agent: ChannelId,
    /// One join at a time, so that a conversation's room is created once.
    joining: Arc<Mutex<()>>,
    stopping: watch::Receiver<bool>,
    live: Live,
    patience: Patience,
}

/// How many connections the server runs: each one's task counts itself
/// while it runs, so that a stop sees the last one end.
#[derive(Clone)]
struct Live(Arc<watch::Sender<usize>>);

/// One connection counted, until it is dropped.
struct Counted(Live);

impl Live {
    fn new() -> Self {
        Self(Arc::new(watch::Sender::new(0)))
    }

    /// Counts one more connection while the guard it returns is held.
    fn enter(&self) -> Counted {
        self.0.send_modify(|count| *count += 1);
        Counted(self.clone())
    }

    /// Waits until `count` connections or fewer run.
    async fn down_to(&self, count: usize) {
        let mut counted = self.0.subscribe();
        // The sender is this counter's own: it outlives the wait.
        let _reached = counted.wait_for(|live| *live <= count).await;
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.0.send_modify(|count| *count -= 1);
    }
}

impl Server {
    /// A server on `address` keeping its rooms in the file `database`, its
    /// agent calling `endpoint`, waiting on its clients with `patience`.
    async fn start(
        database: &Path,
        endpoint: &OpenAiEndpoint,
        address: SocketAddr,
        patience: Patience,
    ) -> Result<Self, Box<dyn Error>> {
        let kit = RoomKit::with_store(SqliteStore::open(database).await?);
        let websocket = WebSocketChannel::new(ChannelId::new("websocket")?);
        let config = OpenAiConfig::new(endpoint.base_url(), endpoint.api_key(), endpoint.model())
            .with_system_prompt(SYSTEM_PROMPT)
            .with_history(20);
        let agent = OpenAiAgent::new(ChannelId::new("assistant")?, config)?;
        let agent_id = agent.id().clone();
        kit.register_channel(websocket.clone())?;
        kit.register_channel(agent)?;

        let (stop, stopping) = watch::channel(false);
        let live = Live::new();
        let app = App {
            kit: kit.clone(),
            websocket: websocket.clone(),
            agent: agent_id,
            joining: Arc::default(),
            stopping: stopping.clone(),
            live: live.clone(),
            patience,
        };
        let listener = TcpListener::bind(address).await?;
        let address = listener.local_addr()?;
        let serving = tokio::spawn(listen(listener, app, stopping));
        Ok(Self {
            address,
            kit,
            stop,
            serving,
            websocket,
            live,
            patience,
        })
    }

    /// Stops the server: it accepts no more connections and closes the
    /// ones open. Their work and the kit's get the grace of its patience to
    /// end; the kit is then shut down (`RoomKit::shutdown`), which ends
    /// what still runs, and the file is closed once the last connection
    /// let go of it.
    ///
    /// # Errors
    ///
    /// Returns the server's error, or the grace running out.
    async fn stop(self) -> Result<(), Box<dyn Error>> {
        let Self {
            kit,
            stop,
            mut serving,
            live,
            patience,
            ..
        } = self;
        stop.send_replace(true);
        let ended = async {
            let served = (&mut serving).await;
            live.down_to(0).await;
            kit.idle().await;
            served
        };
        let ended = tokio::time::timeout(patience.grace, ended).await;
        kit.shutdown().await;
        let Ok(served) = ended else {
            serving.abort();
            return Err("the server's work outlived its grace".into());
        };
        Ok(served??)
    }
}

/// Serves `app` on `listener` until `stopping` says to stop, then waits for
/// the requests under way.
async fn listen(
    listener: TcpListener,
    app: App,
    mut stopping: watch::Receiver<bool>,
) -> io::Result<()> {
    let router = Router::new()
        .route("/conversations/{conversation}", get(connect))
        .with_state(app);
    let stop = async move {
        // A sender dropped stops the server as well.
        let _stopped = stopping.wait_for(|stop| *stop).await;
    };
    axum::serve(listener, router)
        .with_graceful_shutdown(stop)
        .await
}

/// A connection asking to join a conversation: the conversation's id names
/// its room, the `user` parameter its participant, `name` their name.
async fn connect(
    State(app): State<App>,
    extract::Path(conversation): extract::Path<String>,
    Query(query): Query<HashMap<String, String>>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let Some((room_id, user, name)) = named(&conversation, &query) else {
        return (StatusCode::BAD_REQUEST, NAMING).into_response();
    };
    if let Err(error) = app.join(&room_id, &user, name).await {
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
    let connection = app.websocket.register_connection(room_id.clone());
    // The frame's limit too: a frame is refused at its header, before the
    // library reads its body, up to 16 MiB by default.
    upgrade
        .max_frame_size(MESSAGE_LIMIT)
        .max_message_size(MESSAGE_LIMIT)
        .on_upgrade(move |socket| app.converse(socket, connection, room_id, user))
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
    /// Makes `user` a participant of the room `room_id`, named `name`,
    /// creating the room with the agent and the `websocket` channel
    /// attached when it is new. A participant is written through the store
    /// (RFC §14.1); one the room has already is kept as it is.
    async fn join(
        &self,
        room_id: &RoomId,
        user: &ParticipantId,
        name: Option<String>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let _one_at_a_time = self.joining.lock().await;
        let store = self.kit.store();
        if store.get_room(room_id).await?.is_none() {
            self.kit.create_room(room_id).await?;
            self.kit.attach(room_id, &self.agent).await?;
            // Several people share the channel's conversation (RFC §10.4).
            let group = BindingOptions {
                group: true,
                ..BindingOptions::default()
            };
            self.kit
                .attach_with(room_id, self.websocket.id(), group)
                .await?;
        }
        if store.get_participant(room_id, user).await?.is_none() {
            let mut person = Participant::new(
                user.clone(),
                room_id.clone(),
                self.websocket.id().clone(),
                SystemEnvironment.now(),
            );
            person.display_name = name;
            store.create_participant(person).await?;
        }
        Ok(())
    }

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
        (registration, frames): (Registration, mpsc::Receiver<String>),
        room_id: RoomId,
        user: ParticipantId,
    ) {
        let _counted = self.live.enter();
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
    /// limit, or the server stops.
    async fn read(
        &self,
        mut incoming: SplitStream<WebSocket>,
        registration: &Registration,
        room_id: &RoomId,
        user: &ParticipantId,
    ) -> Ending {
        let mut stopping = self.stopping.clone();
        loop {
            let received = tokio::select! {
                received = incoming.next() => received,
                _ = stopping.wait_for(|stop| *stop) => return Ending::Stopping,
            };
            match received {
                Some(Ok(Message::Text(text))) => {
                    self.say(registration, room_id, user, text.as_str()).await;
                }
                Some(Ok(Message::Binary(_) | Message::Ping(_) | Message::Pong(_))) => {}
                Some(Ok(Message::Close(_))) => return Ending::Closed,
                Some(Err(error)) => return Ending::of(error),
                None => return Ending::Broken,
            }
        }
    }

    /// `user`'s message `text`, into the room `room_id`. A message the room
    /// refused, or whose turn failed, gets an error frame on the
    /// connection, `{"error": "..."}`, its reason one of the server's own.
    async fn say(
        &self,
        registration: &Registration,
        room_id: &RoomId,
        user: &ParticipantId,
        text: &str,
    ) {
        let message = InboundMessage::new(
            self.websocket.id().clone(),
            user.as_str(),
            EventContent::text(text),
        );
        let told = match self.kit.process_inbound_in(room_id, message).await {
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
            Err(error) => {
                println!("  {:?}'s message was refused: {error}", user.as_str());
                REFUSED
            }
        };
        // A connection that cannot take it now is not waited for.
        let _told = registration.send(json!({ "error": told }).to_string());
    }
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

/// `--serve`: the server on `address`, until Ctrl+C.
async fn serve(address: &str) -> Result<(), Box<dyn Error>> {
    let endpoint = OpenAiEndpoint::from_env(REPLIES).await?;
    let server = Server::start(
        Path::new(SERVE_DATABASE),
        &endpoint,
        address.parse()?,
        SERVING,
    )
    .await?;
    println!("The agent calls {}.", endpoint.shown_base_url());
    println!(
        "Serving ws://{}/conversations/{{conversation}}?user=...&name=..., \
         the rooms in {SERVE_DATABASE}. Ctrl+C stops.",
        server.address
    );
    tokio::signal::ctrl_c().await?;
    server.stop().await
}

/// The checks: two people in one room, then a restart on the same file,
/// and a client that stops reading.
async fn check() -> Result<ExitCode, Box<dyn Error>> {
    let mut checks = Checks::default();
    the_conversation_survives_a_restart(&mut checks).await?;
    a_client_stops_reading(&mut checks).await?;
    what_a_client_is_told(&mut checks).await?;
    what_a_connection_must_name(&mut checks).await?;
    Ok(checks.finish())
}

/// Alice and Bob talk with the agent, the server restarts on the same file,
/// and Alice goes on.
async fn the_conversation_survives_a_restart(checks: &mut Checks) -> Result<(), Box<dyn Error>> {
    let endpoint = OpenAiEndpoint::from_env(REPLIES).await?;
    println!("The agent calls {}.", endpoint.shown_base_url());
    let database = TempDatabase::new("server_agent");
    let [first, second] = two_people_talk(database.path(), &endpoint).await?;
    println!("The server restarts on the same file.");
    let third = alice_goes_on(database.path(), &endpoint).await?;
    let timeline = timeline_in(database.path(), CONVERSATION).await?;
    shared::print_timeline(&timeline);
    let said = messages_of(&timeline);
    let answers = [&first[0], &second[0], &third];

    checks.expect(
        "the agent answers each message, and its answer reaches both people's sockets",
        answers.iter().all(|answer| answer.is_some())
            && first[0] == first[1]
            && second[0] == second[1],
    );
    checks.expect(
        "after the restart, the conversation goes on: Alice's next message is answered",
        third.is_some(),
    );
    // Each question, then the answer the sockets received.
    let conversation: Vec<Option<&str>> = [ALICE_ASKS, BOB_ASKS, ALICE_AGAIN]
        .into_iter()
        .zip(answers)
        .flat_map(|(asked, answer)| [Some(asked), answer.as_deref()])
        .collect();
    checks.expect(
        "the file holds the whole conversation, both servers' turns, in order",
        said.iter().map(|text| Some(text.as_str())).eq(conversation),
    );
    if let Some(fake) = endpoint.fake() {
        check_the_model_s_turns(checks, &fake.requests(), &answers);
    }
    Ok(())
}

/// The first server: Alice and Bob join the conversation, Alice asks, then
/// Bob. For each question, the answer each socket received, Alice's then
/// Bob's.
async fn two_people_talk(
    database: &Path,
    endpoint: &OpenAiEndpoint,
) -> Result<[[Option<String>; 2]; 2], Box<dyn Error>> {
    let server = Server::start(database, endpoint, ANY_PORT.parse()?, CHECKING).await?;
    let mut alice = Client::connect(server.address, CONVERSATION, "alice", "Alice").await?;
    let mut bob = Client::connect(server.address, CONVERSATION, "bob", "Bob").await?;
    alice.say(ALICE_ASKS).await?;
    let first = [alice.answer().await, bob.answer().await];
    bob.say(BOB_ASKS).await?;
    let second = [alice.answer().await, bob.answer().await];
    alice.close().await;
    bob.close().await;
    server.stop().await?;
    Ok([first, second])
}

/// The second server, on the same file: Alice comes back and asks again.
/// The answer she received.
async fn alice_goes_on(
    database: &Path,
    endpoint: &OpenAiEndpoint,
) -> Result<Option<String>, Box<dyn Error>> {
    let server = Server::start(database, endpoint, ANY_PORT.parse()?, CHECKING).await?;
    let mut alice = Client::connect(server.address, CONVERSATION, "alice", "Alice").await?;
    alice.say(ALICE_AGAIN).await?;
    let answer = alice.answer().await;
    alice.close().await;
    server.stop().await?;
    Ok(answer)
}

/// Dave joins the kitchen's conversation and stops reading, while the host
/// posts one large menu after another to the room. Once his connection
/// cannot keep up, the server drops it, reader included, so that what he
/// sends then never enters the room, and a stop does not wait for him.
async fn a_client_stops_reading(checks: &mut Checks) -> Result<(), Box<dyn Error>> {
    println!("Dave stops reading while the host posts large menus to the kitchen.");
    let endpoint = OpenAiEndpoint::from_env(REPLIES).await?;
    let database = TempDatabase::new("server_agent_kitchen");
    let server = Server::start(database.path(), &endpoint, ANY_PORT.parse()?, CHECKING).await?;
    let mut dave = Client::connect_reading_little(server.address, KITCHEN, "dave", "Dave").await?;
    let room = RoomId::new(KITCHEN)?;
    let menu = format!("Today's menu{}", ".".repeat(LARGE_POST));
    let mut posts = 0;
    while server.websocket.connections() > 0 && posts < POSTS_AT_MOST {
        server
            .kit
            .post(&room, Post::new(EventContent::text(menu.as_str())))
            .await?;
        posts += 1;
    }
    let dropped = tokio::time::timeout(STOP_PATIENCE, server.live.down_to(0))
        .await
        .is_ok();
    println!("  after {posts} menus, Dave's connection ended: {dropped}");
    // Sent to a connection the server closed, it fails or goes nowhere.
    let _sent = dave.say(DAVE_LATE).await;
    let stopped = tokio::time::timeout(STOP_PATIENCE, server.stop()).await;
    let late = messages_of(&timeline_in(database.path(), KITCHEN).await?)
        .iter()
        .any(|text| text == DAVE_LATE);
    checks.expect(
        "a client that stops reading is dropped, reader included: what it sends then \
         never enters the room",
        dropped && !late,
    );
    checks.expect(
        "with a client that reads nothing, the server stops within its grace",
        matches!(stopped, Ok(Ok(()))),
    );
    Ok(())
}

/// What a client is told. The model unreachable, Frank's message gets an
/// error frame; Grace's message over the limit closes her connection with
/// 1009 (too large), and so does Henry's frame announced over it, before
/// its body; the server's stop closes Frank's with 1001 (going away).
async fn what_a_client_is_told(checks: &mut Checks) -> Result<(), Box<dyn Error>> {
    println!("The model is unreachable.");
    let endpoint = unreachable_model().await?;
    let database = TempDatabase::new("server_agent_cellar");
    let server = Server::start(database.path(), &endpoint, ANY_PORT.parse()?, CHECKING).await?;
    let mut frank = Client::connect(server.address, CELLAR, "frank", "Frank").await?;
    frank.say("Is anyone there?").await?;
    let told = frank.next_frame(TOLD_PATIENCE).await;
    println!("  Frank receives {told:?}");
    let mut grace = Client::connect(server.address, CELLAR, "grace", "Grace").await?;
    println!("Grace sends a message one byte over the limit.");
    grace.send("a".repeat(MESSAGE_LIMIT + 1)).await?;
    let too_large = grace.close_code(TOLD_PATIENCE).await;
    let mut henry = Client::connect(server.address, CELLAR, "henry", "Henry").await?;
    println!("Henry announces a frame of {ANNOUNCED} bytes and sends its first KiB.");
    henry.announce(ANNOUNCED).await?;
    let refused_at_header = henry.close_code(TOLD_PATIENCE).await;
    tokio::time::timeout(STOP_PATIENCE, server.stop()).await??;
    let going_away = frank.close_code(TOLD_PATIENCE).await;

    checks.expect(
        "a turn that fails tells its client, with a reason that holds no secret",
        told.as_ref()
            .and_then(|frame| frame.get("error"))
            .and_then(Value::as_str)
            == Some(TURN_FAILED),
    );
    checks.expect(
        "a message over the limit closes its connection with 1009, too large",
        too_large == Some(1009),
    );
    checks.expect(
        "a frame announced over the limit is refused at its header, before its body",
        refused_at_header == Some(1009),
    );
    checks.expect(
        "a stop closes the connections with 1001, going away",
        going_away == Some(1001),
    );
    Ok(())
}

/// What a connection must name: its conversation and user plainly, its
/// name without control characters, each within 64 characters. Each
/// connection here names one thing wrong, and is refused with 400.
async fn what_a_connection_must_name(checks: &mut Checks) -> Result<(), Box<dyn Error>> {
    println!("Connections that name something wrong.");
    let endpoint = OpenAiEndpoint::Fake(FakeOpenAi::start(REPLIES).await?);
    let database = TempDatabase::new("server_agent_names");
    let server = Server::start(database.path(), &endpoint, ANY_PORT.parse()?, CHECKING).await?;
    let long = "a".repeat(65);
    let mut refused = Vec::new();
    for (what, path) in [
        (
            "a user with a space",
            "/conversations/bakery?user=al%20ice".to_owned(),
        ),
        (
            "a user with a slash",
            "/conversations/bakery?user=al%2Fice".to_owned(),
        ),
        ("no user", "/conversations/bakery".to_owned()),
        (
            "a conversation of 65 characters",
            format!("/conversations/{long}?user=alice"),
        ),
        (
            "a conversation with a dot",
            "/conversations/bak.ery?user=alice".to_owned(),
        ),
        (
            "a name with a bell",
            "/conversations/bakery?user=alice&name=Al%07ice".to_owned(),
        ),
        (
            "a name with a line break",
            "/conversations/bakery?user=alice&name=Al%0Aice".to_owned(),
        ),
        (
            "a name of 65 characters",
            format!("/conversations/bakery?user=alice&name={long}"),
        ),
    ] {
        let status = Client::refused(server.address, &path).await?;
        println!("  {what}: {status:?}");
        refused.push(status);
    }
    tokio::time::timeout(STOP_PATIENCE, server.stop()).await??;
    checks.expect(
        "a conversation or user other than 1 to 64 letters, digits, _ or -, or a name over \
         64 characters or with a control character, is refused with 400",
        refused.iter().all(|status| *status == Some(400)),
    );
    Ok(())
}

/// An endpoint where nothing listens: a port the system gave and took
/// back.
async fn unreachable_model() -> Result<OpenAiEndpoint, Box<dyn Error>> {
    let address = TcpListener::bind(ANY_PORT).await?.local_addr()?;
    Ok(OpenAiEndpoint::Real {
        base_url: format!("http://{address}/v1"),
        api_key: String::new(),
        model: "unreachable".to_owned(),
    })
}

/// The timeline of the room `conversation`, as the file holds it once the
/// servers are gone.
async fn timeline_in(
    database: &Path,
    conversation: &str,
) -> Result<Vec<RoomEvent>, Box<dyn Error>> {
    let store = SqliteStore::open(database).await?;
    Ok(store.list_events(&RoomId::new(conversation)?, true).await?)
}

/// The texts of a timeline's messages.
fn messages_of(timeline: &[RoomEvent]) -> Vec<String> {
    timeline
        .iter()
        .filter(|event| event.event_type == EventType::Message)
        .map(|event| text_of(&event.content))
        .collect()
}

/// The checks of what the agent sent the fake, turn after turn, and of the
/// answers the sockets received from it.
fn check_the_model_s_turns(checks: &mut Checks, requests: &[Value], answers: &[&Option<String>]) {
    println!("The agent's turn after the restart sent:");
    for message in requests.get(2).map(messages).unwrap_or_default() {
        println!("  {message}");
    }
    checks.expect(
        "the answers are the model's",
        answers.len() == REPLIES.len()
            && answers
                .iter()
                .zip(REPLIES)
                .all(|(answer, reply)| answer.as_deref() == Some(*reply)),
    );
    checks.expect(
        "the first turn labels Alice's message: two people take part, Bob silent",
        requests.first().is_some_and(|request| {
            matches!(sent(request).as_slice(), [system, alice]
                if is_system(system) && is_labelled(alice, "Alice", ALICE_ASKS))
        }),
    );
    checks.expect(
        "the turn after the restart reads the conversation from the file, each person \
         labelled as before",
        requests.get(2).is_some_and(goes_on),
    );
}

/// A person's WebSocket client in one of the checks' conversations.
struct Client {
    name: &'static str,
    socket: WebSocketStream<TcpStream>,
}

impl Client {
    /// `name`'s connection to `conversation` on the server at `address`, as
    /// the participant `user`.
    async fn connect(
        address: SocketAddr,
        conversation: &str,
        user: &str,
        name: &'static str,
    ) -> Result<Self, Box<dyn Error>> {
        Self::open(address, conversation, user, name, TcpSocket::new_v4()?).await
    }

    /// The same connection, whose socket takes in a few KiB at most before
    /// the server must wait for it to read: a client that falls behind
    /// quickly.
    async fn connect_reading_little(
        address: SocketAddr,
        conversation: &str,
        user: &str,
        name: &'static str,
    ) -> Result<Self, Box<dyn Error>> {
        let socket = TcpSocket::new_v4()?;
        socket.set_recv_buffer_size(SMALL_BUFFER)?;
        Self::open(address, conversation, user, name, socket).await
    }

    async fn open(
        address: SocketAddr,
        conversation: &str,
        user: &str,
        name: &'static str,
        socket: TcpSocket,
    ) -> Result<Self, Box<dyn Error>> {
        let stream = socket.connect(address).await?;
        let url = format!("ws://{address}/conversations/{conversation}?user={user}&name={name}");
        let (socket, _response) = tokio_tungstenite::client_async(url, stream).await?;
        println!("{name} connects to {conversation}.");
        Ok(Self { name, socket })
    }

    /// Sends `text`.
    async fn say(&mut self, text: &str) -> Result<(), Box<dyn Error>> {
        println!("{} says {text:?}", self.name);
        self.socket.send(tungstenite::Message::text(text)).await?;
        Ok(())
    }

    /// The HTTP status the server at `address` refuses the upgrade to
    /// `path` with, `None` when it accepts it.
    async fn refused(address: SocketAddr, path: &str) -> Result<Option<u16>, Box<dyn Error>> {
        let stream = TcpStream::connect(address).await?;
        match tokio_tungstenite::client_async(format!("ws://{address}{path}"), stream).await {
            Ok(_accepted) => Ok(None),
            Err(tungstenite::Error::Http(response)) => Ok(Some(response.status().as_u16())),
            Err(error) => Err(error.into()),
        }
    }

    /// Sends `text` without printing it: a large one.
    async fn send(&mut self, text: String) -> Result<(), Box<dyn Error>> {
        self.socket.send(tungstenite::Message::text(text)).await?;
        Ok(())
    }

    /// Writes, under the WebSocket, the header of a text frame of `length`
    /// bytes, then its first KiB, and nothing more.
    async fn announce(&mut self, length: u64) -> Result<(), Box<dyn Error>> {
        // FIN and text; masked, a 64-bit length, the mask 0 leaving the
        // payload as it is.
        let mut frame = vec![0x81, 0x80 | 127];
        frame.extend_from_slice(&length.to_be_bytes());
        frame.extend_from_slice(&[0; 4]);
        frame.extend_from_slice(&[b'a'; 1024]);
        self.socket.get_mut().write_all(&frame).await?;
        Ok(())
    }

    /// The next JSON frame the server sends, if one comes within
    /// `patience`.
    async fn next_frame(&mut self, patience: Duration) -> Option<Value> {
        let next = async {
            while let Some(Ok(received)) = self.socket.next().await {
                if let tungstenite::Message::Text(frame) = received {
                    return serde_json::from_str(frame.as_str()).ok();
                }
            }
            None
        };
        tokio::time::timeout(patience, next).await.ok().flatten()
    }

    /// The code of the close frame the server sends, past any other frame;
    /// `None` when the connection ends without one, or none comes within
    /// `patience`.
    async fn close_code(&mut self, patience: Duration) -> Option<u16> {
        let closed = async {
            while let Some(Ok(received)) = self.socket.next().await {
                if let tungstenite::Message::Close(frame) = received {
                    return frame.map(|frame| u16::from(frame.code));
                }
            }
            None
        };
        let code = tokio::time::timeout(patience, closed).await.ok().flatten();
        println!("  {} is closed with the code {code:?}", self.name);
        code
    }

    /// The text of the next message the room delivers, if one comes in
    /// time.
    async fn answer(&mut self) -> Option<String> {
        let next_message = async {
            while let Some(Ok(received)) = self.socket.next().await {
                let tungstenite::Message::Text(frame) = received else {
                    continue;
                };
                let frame: Value = serde_json::from_str(frame.as_str()).unwrap_or_default();
                let field = |key: &str| frame.get(key).and_then(Value::as_str);
                if field("type") == Some("message") {
                    return field("text").map(str::to_owned);
                }
            }
            None
        };
        let answer = tokio::time::timeout(ANSWER_PATIENCE, next_message)
            .await
            .ok()
            .flatten();
        println!("  {} receives {answer:?}", self.name);
        answer
    }

    /// Closes the connection, and waits for the server to close its side.
    async fn close(mut self) {
        // A connection the server closed first is closed already.
        let _closed = self.socket.close(None).await;
        let drained = async { while self.socket.next().await.is_some() {} };
        let _drained = tokio::time::timeout(ANSWER_PATIENCE, drained).await;
    }
}

/// The role and text of each message a Chat Completions request sent.
fn sent(request: &Value) -> Vec<(String, String)> {
    messages(request)
        .iter()
        .map(|message| {
            let field = |key: &str| message[key].as_str().unwrap_or_default().to_owned();
            (field("role"), field("content"))
        })
        .collect()
}

/// The messages of a Chat Completions request.
fn messages(request: &Value) -> Vec<Value> {
    request["messages"].as_array().cloned().unwrap_or_default()
}

fn is_system(message: &(String, String)) -> bool {
    message == &("system".to_owned(), SYSTEM_PROMPT.to_owned())
}

/// Whether `message` is the user's `text`, labelled `label` (RFC §6.4).
fn is_labelled(message: &(String, String), label: &str, text: &str) -> bool {
    message.0 == "user" && message.1.starts_with(&format!("{label}: {text:?}"))
}

/// Whether the turn after the restart sent the system prompt, then the
/// conversation before it: Alice's question, the first answer, Bob's, the
/// second answer, and Alice's new question, labelled `Alice` as before
/// rather than as a second Alice.
fn goes_on(request: &Value) -> bool {
    let sent = sent(request);
    let [system, alice, first, bob, second, again] = sent.as_slice() else {
        return false;
    };
    let answered = |message: &(String, String), reply: &str| {
        message == &("assistant".to_owned(), reply.to_owned())
    };
    is_system(system)
        && is_labelled(alice, "Alice", ALICE_ASKS)
        && REPLIES.first().is_some_and(|reply| answered(first, reply))
        && is_labelled(bob, "Bob", BOB_ASKS)
        && REPLIES.get(1).is_some_and(|reply| answered(second, reply))
        && is_labelled(again, "Alice", ALICE_AGAIN)
}
