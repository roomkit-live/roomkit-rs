//! Reply references and the two directions of each active remote turn.
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot, watch};
use uuid::Uuid;

use super::stream::Message;
use super::{Node, RedisTurnBackend};
use crate::RoomKitError;
use crate::turns::lock;

#[derive(Default)]
pub(super) struct Routes {
    answers: HashMap<String, (String, oneshot::Sender<String>)>,
    turns: HashMap<String, TurnEndpoint>,
    handled: HashMap<(String, String), Option<String>>,
    expires: VecDeque<(Instant, (String, String))>,
}

struct TurnEndpoint {
    peer: String,
    tx: mpsc::UnboundedSender<Message>,
    worker: bool,
}

pub(super) struct TurnRoute {
    pub(super) inbox: mpsc::UnboundedReceiver<Message>,
    node: Weak<Node>,
    id: String,
}

impl Drop for TurnRoute {
    fn drop(&mut self) {
        if let Some(node) = self.node.upgrade() {
            lock(&node.routes).turns.remove(&self.id);
        }
    }
}

struct Question<'a>(&'a Mutex<Routes>, String);
impl Drop for Question<'_> {
    fn drop(&mut self) {
        lock(self.0).answers.remove(&self.1);
    }
}

impl Routes {
    pub(super) fn running(&self) -> Vec<String> {
        self.turns
            .iter()
            .filter(|(_, e)| e.worker)
            .map(|(id, _)| id.clone())
            .collect()
    }

    fn first(&mut self, msg: &Message) -> Result<(), Option<String>> {
        let now = Instant::now();
        while self.expires.front().is_some_and(|(at, _)| {
            now.duration_since(*at) >= Duration::from_secs(600) || self.expires.len() >= 100_000
        }) {
            if let Some((_, key)) = self.expires.pop_front() {
                self.handled.remove(&key);
            }
        }
        let key = (msg.sender.clone(), msg.reference.clone());
        if let Some(answer) = self.handled.get(&key) {
            return Err(answer.clone());
        }
        self.expires.push_back((now, key.clone()));
        self.handled.insert(key, None);
        Ok(())
    }

    pub(super) fn close(&mut self) {
        self.answers.clear();
        self.turns.clear();
    }
}

impl RedisTurnBackend {
    pub(super) fn route_turn(&self, id: &str, peer: &str, worker: bool) -> Option<TurnRoute> {
        use std::collections::hash_map::Entry;
        let mut routes = lock(&self.0.routes);
        let Entry::Vacant(slot) = routes.turns.entry(id.into()) else {
            return None;
        };
        let (tx, inbox) = mpsc::unbounded_channel();
        slot.insert(TurnEndpoint {
            peer: peer.into(),
            tx,
            worker,
        });
        Some(TurnRoute {
            inbox,
            node: Arc::downgrade(&self.0),
            id: id.into(),
        })
    }

    pub(super) async fn ask(
        &self,
        peer: &str,
        kind: &str,
        body: String,
        target: &str,
        bound: Duration,
    ) -> Result<String, RoomKitError> {
        tokio::time::timeout(bound, self.exchange(peer, kind, body, target))
            .await
            .map_err(|_| RoomKitError::TurnLink {
                reason: "executor_lost".into(),
                message: Some(format!("Executor {peer}:{target} did not answer the turn")),
            })?
    }

    pub(super) async fn ask_living(
        &self,
        peer: &str,
        body: String,
        target: &str,
    ) -> Result<String, RoomKitError> {
        let question = self.exchange(peer, "channel", body, target);
        tokio::pin!(question);
        loop {
            tokio::select! {
                result = &mut question => return result,
                () = tokio::time::sleep(self.timeouts().heartbeat_interval.max(Duration::from_millis(1))) => {}
            }
            let alive = self.executors().await?.iter().any(|info| {
                info.node.as_deref() == Some(peer) && info.executor_id == target && info.online
            });
            if !alive {
                return Err(RoomKitError::TurnLink {
                    reason: "executor_lost".into(),
                    message: None,
                });
            }
        }
    }

    async fn exchange(
        &self,
        peer: &str,
        kind: &str,
        body: String,
        target: &str,
    ) -> Result<String, RoomKitError> {
        if *self.0.stop.borrow() {
            return Err(closed());
        }
        self.listen().await;
        let id = Uuid::new_v4().simple().to_string();
        let (tx, rx) = oneshot::channel();
        lock(&self.0.routes)
            .answers
            .insert(id.clone(), (peer.into(), tx));
        let _registration = Question(&self.0.routes, id.clone());
        let question = Message::new(kind, self.node_id(), body, target, &id);
        self.post(peer, &question).await?;
        rx.await.map_err(|_| closed())
    }

    pub(super) async fn answer(&self, msg: &Message, body: String) -> Result<(), RoomKitError> {
        let body = crate::turns::wire::peer_error_payload(body).map_err(RoomKitError::Value)?;
        let key = (msg.sender.clone(), msg.reference.clone());
        if let Some(slot) = lock(&self.0.routes).handled.get_mut(&key) {
            *slot = Some(body.clone());
        }
        self.post(
            &msg.sender,
            &Message::new("answer", self.node_id(), body, "", &msg.reference),
        )
        .await
    }

    async fn route(&self, msg: Message) -> Result<(), RoomKitError> {
        match msg.kind.as_str() {
            "answer" => {
                let mut routes = lock(&self.0.routes);
                if routes
                    .answers
                    .get(&msg.reference)
                    .is_some_and(|(peer, _)| peer == &msg.sender)
                    && let Some((_, tx)) = routes.answers.remove(&msg.reference)
                {
                    let _ = tx.send(msg.body);
                }
            }
            "frame" | "control" | "released" => {
                let routes = lock(&self.0.routes);
                if let Some(route) = routes.turns.get(&msg.target)
                    && route.peer == msg.sender
                    && route.worker == (msg.kind != "frame")
                {
                    let _ = route.tx.send(msg);
                }
            }
            "request" | "channel" | "owner" | "work" => {
                let first = lock(&self.0.routes).first(&msg);
                match first {
                    Ok(()) => self.handle(&msg).await?,
                    Err(Some(previous)) => self.answer(&msg, previous).await?,
                    Err(None) => {}
                }
            }
            kind => tracing::warn!(kind, "Unknown turn inbox entry"),
        }
        Ok(())
    }

    pub(super) async fn listen(&self) {
        self.0
            .reader
            .get_or_init(|| async {
                let node = Arc::downgrade(&self.0);
                let stop = self.0.stop.subscribe();
                tokio::spawn(receive(node, stop));
            })
            .await;
    }
}

pub(super) fn closed() -> RoomKitError {
    RoomKitError::TurnLink {
        reason: "backend_closed".into(),
        message: None,
    }
}

async fn receive(node: Weak<Node>, mut stop: watch::Receiver<bool>) {
    let mut last = "0-0".to_owned();
    let mut connection = None;
    while !*stop.borrow() {
        let Some(owner) = node.upgrade() else { break };
        let backend = RedisTurnBackend(owner);
        let read = async {
            if connection.is_none() {
                connection = Some(backend.inbox_connection().await?);
            }
            backend
                .read(connection.as_mut().expect("connected"), &last)
                .await
        };
        let entries = tokio::select! {
            () = async { let _ = stop.changed().await; } => break,
            result = read => result,
        };
        match entries {
            Ok(entries) => {
                let mut ids = Vec::with_capacity(entries.len());
                for entry in entries {
                    if let Some(msg) = Message::read(&entry) {
                        if let Err(error) = backend.route(msg).await {
                            tracing::warn!(%error, "Turn inbox routing failed");
                        }
                    } else {
                        tracing::warn!("A malformed turn inbox entry was dropped");
                    }
                    last.clone_from(&entry.id);
                    ids.push(entry.id);
                }
                if !ids.is_empty()
                    && let Err(error) = backend.acknowledge(&ids).await
                {
                    tracing::warn!(%error, "Could not delete consumed turn inbox entries");
                }
            }
            Err(error) => {
                tracing::warn!(%error, "Turn inbox read failed");
                connection = None;
                let delay = backend.read_block().min(Duration::from_secs(1));
                drop(backend);
                tokio::select! { _ = stop.changed() => break, () = tokio::time::sleep(delay) => {} }
            }
        }
    }
}
