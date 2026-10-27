//! The bakery's agent served over WebSocket, its rooms kept in SQLite: the
//! server `server_agent` shows and `web_chat` puts a page in front of. Each
//! connection to `/conversations/{conversation}` is a person taking part in
//! the conversation's room through the `websocket` channel, and the agent
//! answers each message through an OpenAI-compatible endpoint (RFC §6.4).
//! `server_agent`'s header says what a connection names, sends and
//! receives.

mod connection;
mod history;

use std::error::Error;
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::routing::get;
use roomkit::model::{ChannelId, Participant, ParticipantId, RoomId, Timestamp};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, RoomKit, RoomKitOptions,
};
use roomkit_openai::{OpenAiAgent, OpenAiConfig};
use roomkit_sqlite::SqliteStore;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;

use crate::bakery::{SYSTEM_PROMPT, orders};
use crate::{OpenAiEndpoint, WebSocketChannel};

pub use self::connection::{MESSAGE_LIMIT, REFUSED, STOPPED, TURN_FAILED};

/// The path of the WebSocket endpoint, the conversation's id its last
/// segment.
const CONVERSATIONS: &str = "/conversations/{conversation}";

/// How many messages of the conversation the agent reads before each
/// answer.
const HISTORY: usize = 20;

/// How long the server waits on a client and on its own work.
#[derive(Clone, Copy, Debug)]
pub struct Patience {
    /// How long one frame's send may take before its client is taken for
    /// gone: one that stopped reading (the Python implementation's
    /// `send_timeout`, 5 seconds by default).
    pub send: Duration,
    /// How long a stop waits for the connections and the kit's work to
    /// end.
    pub grace: Duration,
}

/// The server's patience when it serves.
pub const SERVING: Patience = Patience {
    send: Duration::from_secs(5),
    grace: Duration::from_secs(10),
};

/// The checks' patience, shorter, so that they run in milliseconds.
pub const CHECKING: Patience = Patience {
    send: Duration::from_millis(100),
    grace: Duration::from_secs(2),
};

/// What a server example serves around the agent's socket.
#[derive(Clone, Copy, Debug)]
pub struct Site {
    /// The routes it makes of the WebSocket endpoint's: a page served
    /// beside it, or a layer in front of it.
    pub routes: fn(Router) -> Router,
    /// How many of the conversation's newest events a connection is sent
    /// as it opens, in one `history` frame; none with 0.
    pub history: u64,
}

impl Site {
    /// The WebSocket endpoint alone, with no history: `server_agent`'s.
    pub const SOCKET_ONLY: Self = Self {
        routes: |conversations| conversations,
        history: 0,
    };
}

/// A running server: its address, its kit over the SQLite file, and what
/// stops it.
#[derive(Debug)]
pub struct AgentServer {
    address: SocketAddr,
    kit: RoomKit<SqliteStore>,
    stop: watch::Sender<bool>,
    serving: JoinHandle<io::Result<()>>,
    websocket: WebSocketChannel,
    live: Live,
    patience: Patience,
}

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
    /// How many past events a connection is sent as it opens.
    history: u64,
}

/// How many connections the server runs, each counted from its upgrade
/// request to its end, so that a stop sees the last one end.
#[derive(Clone, Debug)]
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

impl AgentServer {
    /// A server on `address` keeping its rooms in the file `database`, its
    /// agent calling `endpoint`, waiting on its clients with `patience`.
    ///
    /// # Errors
    ///
    /// Returns the error opening the file, making the agent or binding the
    /// address failed with.
    pub async fn start(
        database: &Path,
        endpoint: &OpenAiEndpoint,
        address: SocketAddr,
        patience: Patience,
    ) -> Result<Self, Box<dyn Error>> {
        Self::start_with(database, endpoint, address, patience, Site::SOCKET_ONLY).await
    }

    /// The same server, serving `site` around its socket.
    ///
    /// # Errors
    ///
    /// Returns the error opening the file, making the agent or binding the
    /// address failed with.
    pub async fn start_with(
        database: &Path,
        endpoint: &OpenAiEndpoint,
        address: SocketAddr,
        patience: Patience,
        site: Site,
    ) -> Result<Self, Box<dyn Error>> {
        let kit = RoomKit::new(RoomKitOptions::with_store(
            SqliteStore::open(database).await?,
        ));
        let websocket = WebSocketChannel::new(ChannelId::new("websocket")?);
        let config = OpenAiConfig::new(endpoint.base_url(), endpoint.api_key(), endpoint.model())
            .with_system_prompt(SYSTEM_PROMPT)
            .with_history(HISTORY);
        let agent = OpenAiAgent::new(ChannelId::new("assistant")?, config)?.with_tools(orders())?;
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
            history: site.history,
        };
        let conversations = Router::new()
            .route(CONVERSATIONS, get(connection::connect))
            .with_state(app);
        let listener = TcpListener::bind(address).await?;
        let address = listener.local_addr()?;
        let serving = tokio::spawn(listen(listener, (site.routes)(conversations), stopping));
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

    /// The address it listens on.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Its kit.
    pub fn kit(&self) -> &RoomKit<SqliteStore> {
        &self.kit
    }

    /// Its WebSocket channel.
    pub fn websocket(&self) -> &WebSocketChannel {
        &self.websocket
    }

    /// Waits until `count` connections or fewer run, each counted from its
    /// upgrade request to its end.
    pub async fn connections_down_to(&self, count: usize) {
        self.live.down_to(count).await;
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
    pub async fn stop(self) -> Result<(), Box<dyn Error>> {
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

/// Serves `router` on `listener` until `stopping` says to stop, then waits
/// for the requests under way.
async fn listen(
    listener: TcpListener,
    router: Router,
    mut stopping: watch::Receiver<bool>,
) -> io::Result<()> {
    let stop = async move {
        // A sender dropped stops the server as well.
        let _stopped = stopping.wait_for(|stop| *stop).await;
    };
    axum::serve(listener, router)
        .with_graceful_shutdown(stop)
        .await
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
            let room = CreateRoomOptions {
                room_id: Some(room_id.clone()),
                ..Default::default()
            };
            self.kit.create_room(room).await?;
            self.kit
                .attach_channel(room_id, &self.agent, AttachChannelOptions::default())
                .await?;
            // Several people share the channel's conversation (RFC §10.4).
            let group = AttachChannelOptions {
                group: true,
                ..Default::default()
            };
            self.kit
                .attach_channel(room_id, self.websocket.id(), group)
                .await?;
        }
        if store.get_participant(room_id, user).await?.is_none() {
            let mut person = Participant::new(
                user.clone(),
                room_id.clone(),
                self.websocket.id().clone(),
                wall_clock(),
            );
            person.display_name = name;
            store.create_participant(person).await?;
        }
        Ok(())
    }
}

/// The instant a person joins at: the wall clock's, as the kit stamps its
/// own records on a system clock.
#[expect(
    clippy::disallowed_methods,
    reason = "a participant the server stores joins at the wall clock's instant"
)]
fn wall_clock() -> Timestamp {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let millis = i64::try_from(elapsed.as_millis()).unwrap_or_default();
    Timestamp::from_unix_millis(millis).unwrap_or(Timestamp::UNIX_EPOCH)
}
