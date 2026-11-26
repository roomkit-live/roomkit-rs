//! Reply references and the two directions of each active remote turn.
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
mod history;

use tokio::sync::{mpsc, oneshot, watch};
use uuid::Uuid;

use super::liveness::{Observation, Observed};
use super::stream::Message;
use super::{DistributedTurnBackend, Node};
use crate::RoomKitError;
use crate::turns::{MemoryTurn, lock};

#[derive(Default)]
pub(super) struct Routes {
    pub(super) turn_owners: super::after_turn::TurnOwners,
    pub(super) closing: bool,
    pub(super) jobs: super::jobs::Jobs,
    answers: HashMap<String, (String, oneshot::Sender<String>)>,
    owner_turns: HashMap<String, TurnEndpoint>,
    worker_turns: HashMap<String, TurnEndpoint>,
    observing: bool,
    history: history::Histories,
}

struct TurnEndpoint {
    peer: String,
    tx: mpsc::UnboundedSender<Message>,
    watch: Option<(String, Weak<MemoryTurn>)>,
}

pub(super) struct TurnRoute {
    pub(super) inbox: mpsc::UnboundedReceiver<Message>,
    node: Weak<Node>,
    worker: bool,
    id: String,
}

impl Drop for TurnRoute {
    fn drop(&mut self) {
        if let Some(node) = self.node.upgrade() {
            lock(&node.routes).turns_mut(self.worker).remove(&self.id);
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
    fn turns_mut(&mut self, worker: bool) -> &mut HashMap<String, TurnEndpoint> {
        if worker {
            &mut self.worker_turns
        } else {
            &mut self.owner_turns
        }
    }

    pub(super) fn running(&self) -> Vec<String> {
        self.worker_turns.keys().cloned().collect()
    }

    /// Subscribe this accepted handover. Only the first subscription starts a reader.
    pub(super) fn watch(&mut self, turn: &Arc<MemoryTurn>, member: String) -> bool {
        let Some(route) = self.owner_turns.get_mut(&*turn.turn_id) else {
            return false;
        };
        route.watch = Some((member, Arc::downgrade(turn)));
        !std::mem::replace(&mut self.observing, true)
    }

    pub(super) fn observed(&mut self) -> Observed {
        let mut nodes = BTreeMap::<_, Vec<_>>::new();
        for route in self.owner_turns.values() {
            if let Some((member, weak)) = &route.watch
                && let Some(turn) = weak.upgrade()
            {
                nodes
                    .entry(route.peer.clone())
                    .or_default()
                    .push(Observation {
                        member: member.clone(),
                        turn,
                    });
            }
        }
        if nodes.is_empty() {
            self.observing = false;
        }
        nodes
    }

    pub(super) fn begin_close(&mut self) {
        self.closing = true;
        self.answers.clear();
        self.owner_turns.clear();
    }
}

impl DistributedTurnBackend {
    pub(super) fn route_turn(&self, id: &str, peer: &str, worker: bool) -> Option<TurnRoute> {
        use std::collections::hash_map::Entry;
        let mut routes = lock(&self.0.routes);
        if routes.closing {
            return None;
        }
        if let Some(other) = routes.turns_mut(!worker).get(id)
            && !(self.0.loop_back && peer == self.node_id() && other.peer == peer)
        {
            return None;
        }
        let Entry::Vacant(slot) = routes.turns_mut(worker).entry(id.into()) else {
            return None;
        };
        let (tx, inbox) = mpsc::unbounded_channel();
        slot.insert(TurnEndpoint {
            peer: peer.into(),
            tx,
            watch: None,
        });
        Some(TurnRoute {
            inbox,
            node: Arc::downgrade(&self.0),
            worker,
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
        let member = format!("{peer}:{target}");
        let question = self.exchange(peer, "channel", body, target);
        tokio::pin!(question);
        loop {
            tokio::select! {
                result = &mut question => return result,
                () = tokio::time::sleep(self.timeouts().heartbeat_interval.max(Duration::from_millis(1))) => {}
            }
            // An answer and the final withdrawal can land in the same heartbeat.
            let live = tokio::select! {
                biased;
                answer = &mut question => return answer,
                live = super::registry::member_alive(self, &member) => live?,
            };
            if !live {
                return Err(RoomKitError::TurnLink {
                    reason: "executor_lost".into(),
                    message: None,
                });
            }
        }
    }

    pub(super) async fn exchange(
        &self,
        peer: &str,
        kind: &str,
        body: String,
        target: &str,
    ) -> Result<String, RoomKitError> {
        if *self.0.draining.borrow() {
            return Err(closed());
        }
        self.listen().await;
        let id = Uuid::new_v4().simple().to_string();
        let (tx, mut rx) = oneshot::channel();
        {
            let mut routes = lock(&self.0.routes);
            if routes.closing {
                return Err(closed());
            }
            routes.answers.insert(id.clone(), (peer.into(), tx));
        }
        let _registration = Question(&self.0.routes, id.clone());
        let question = Message::addressed(kind, self.node_id(), body, target, &id);
        tokio::select! {
            biased;
            answer = &mut rx => return answer.map_err(|_| closed()),
            result = self.post(peer, &question) => result?,
        }
        rx.await.map_err(|_| closed())
    }

    pub(super) fn answer(&self, msg: &Message, body: String) -> Result<(), RoomKitError> {
        let body = crate::turns::wire::peer_error_payload(body).map_err(RoomKitError::Value)?;
        lock(&self.0.routes).answered(msg, &body);
        self.post_unconfirmed(
            &msg.sender,
            &Message::addressed("answer", self.node_id(), body, "", &msg.reference),
        )
    }

    fn route(&self, msg: Message) -> Result<(), RoomKitError> {
        match msg.kind.as_str() {
            "answer" => {
                let mut routes = lock(&self.0.routes);
                if routes
                    .answers
                    .get(&msg.reference)
                    .is_some_and(|(peer, _)| peer == &msg.sender)
                    && let Some((_, tx)) = routes.answers.remove(&msg.reference)
                {
                    drop(tx.send(msg.body));
                }
            }
            "frame" | "control" | "released" => {
                let routes = lock(&self.0.routes);
                let targets = if msg.kind == "frame" {
                    &routes.owner_turns
                } else {
                    &routes.worker_turns
                };
                if let Some(route) = targets.get(&msg.target)
                    && route.peer == msg.sender
                {
                    drop(route.tx.send(msg));
                }
            }
            "request" | "channel" | "owner" | "work" => {
                let first = lock(&self.0.routes).first(&msg);
                match first {
                    Ok(()) => self.handle(&msg)?,
                    Err(Some(previous)) => self.answer(&msg, previous)?,
                    Err(None) => {}
                }
            }
            kind => tracing::warn!(kind, "Unknown turn inbox entry"),
        }
        Ok(())
    }

    /// The test runner posts this marker behind the sender's publications on the same FIFO.
    pub(crate) async fn publications_handled_from(
        &self,
        sender: &Self,
    ) -> Result<(), RoomKitError> {
        let id = Uuid::new_v4().simple().to_string();
        let (tx, rx) = oneshot::channel();
        self.listen().await;
        lock(&self.0.routes)
            .answers
            .insert(id.clone(), (sender.node_id().to_owned(), tx));
        let _registration = Question(&self.0.routes, id.clone());
        let marker = Message::addressed("answer", sender.node_id(), String::new(), "", &id);
        let wait = async {
            sender.post(self.node_id(), &marker).await?;
            rx.await.map_err(|_| closed())?;
            self.finish_received_publications().await;
            Ok(())
        };
        tokio::time::timeout(self.timeouts().accept, wait)
            .await
            .map_err(|_| RoomKitError::Timeout)?
    }

    pub(super) async fn listen(&self) {
        self.0
            .reader
            .get_or_init(|| async {
                let node = Arc::downgrade(&self.0);
                let stop = self.0.stop.subscribe();
                tokio::spawn(receive(node, stop)).abort_handle()
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
    let mut last = String::new();
    let mut inbox = None;
    while !*stop.borrow() {
        let Some(owner) = node.upgrade() else { break };
        let backend = DistributedTurnBackend(owner);
        let read = async {
            let inbox = match &mut inbox {
                Some(inbox) => inbox,
                slot @ None => {
                    let block = backend.read_block();
                    slot.insert(backend.broker().inbox(backend.node_id(), block).await?)
                }
            };
            inbox.read(&last).await
        };
        let entries = tokio::select! {
            () = async { let _ = stop.changed().await; } => break,
            result = read => result,
        };
        match entries {
            Ok(entries) => {
                let mut ids = Vec::with_capacity(entries.len());
                for entry in entries {
                    if let Some(msg) = entry.message {
                        if let Err(error) = backend.route(msg) {
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
                inbox = None;
                let delay = backend.read_block().min(Duration::from_secs(1));
                drop(backend);
                tokio::select! { _ = stop.changed() => break, () = tokio::time::sleep(delay) => {} }
            }
        }
    }
}

#[cfg(test)]
mod history_tests;
