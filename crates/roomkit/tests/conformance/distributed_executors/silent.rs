//! A peer that acknowledges receipt of the request but emits no turn frames,
//! like Python's `answers: false` executor. The owner and every other worker
//! use the normal transport; the mute peer still applies real channel controls.
//! It is written on the backend's two ports: it beats in the registry and
//! reads its node's inbox as any service gives them.
use std::collections::BTreeMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use roomkit::turns::{
    Beat, BrokerMessage, ChannelResult, TurnBroker, TurnRegistry, TurnRequest,
    wire::ChannelControlEnvelope,
};
use roomkit::{RoomKitError, TurnExecutor};
use tokio::sync::oneshot;

use super::Worker;
use crate::run::Run;
use crate::scenarios::Node;

pub(super) struct Silent {
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<(), RoomKitError>>,
}

struct Peer {
    broker: Arc<dyn TurnBroker>,
    registry: Arc<dyn TurnRegistry>,
    node: String,
    member: String,
    declaration: String,
    run: Weak<Run>,
    gate: String,
    worker: TurnExecutor,
}

impl Silent {
    /// A mute peer under `node`'s name, on the services `services` runs on.
    pub(super) async fn start(
        services: &Node,
        node: &str,
        worker: TurnExecutor,
        spec: &Worker,
        run: Arc<Run>,
    ) -> Result<Self, RoomKitError> {
        let member = format!("{node}:{}", spec.id);
        let declaration = serde_json::json!({"node": node, "executor_id": spec.id,
            "channels": spec.hosts, "organizations":spec.organizations,
            "room_hooks":worker.hooks().defined_room_hooks()})
        .to_string();
        let peer = Peer {
            broker: Arc::clone(&services.broker),
            registry: Arc::clone(&services.registry),
            node: node.to_owned(),
            member,
            declaration,
            run: Arc::downgrade(&run),
            gate: spec.gate.clone().unwrap_or_default(),
            worker,
        };
        peer.beat().await?;
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let result =
                tokio::select! { _ = stopped => Ok(()), received = peer.listen() => received };
            let cleanup = peer.withdraw().await;
            result.and(cleanup)
        });
        Ok(Self { stop, task })
    }

    pub(super) async fn close(self) -> Result<(), RoomKitError> {
        let _ = self.stop.send(());
        self.task.await.map_err(error)?
    }
}

impl Peer {
    /// Its beat: alive, with its declaration, at the registry's time.
    async fn beat(&self) -> Result<(), RoomKitError> {
        self.registry
            .beat(&Beat {
                declared: BTreeMap::from([(self.member.clone(), self.declaration.clone())]),
                ..Beat::new(&self.node, 1)
            })
            .await
    }

    async fn withdraw(&self) -> Result<(), RoomKitError> {
        self.registry
            .beat(&Beat {
                node: self.node.clone(),
                generation: 2,
                gone: vec![self.member.clone()],
                ..Beat::default()
            })
            .await
    }

    async fn listen(&self) -> Result<(), RoomKitError> {
        let tick = self
            .worker
            .timeouts()
            .heartbeat_interval
            .max(Duration::from_millis(1));
        let mut inbox = self.broker.inbox(&self.node, tick).await?;
        let mut last = String::new();
        loop {
            self.beat().await?;
            let entries = inbox.read(&last).await?;
            let mut read = Vec::with_capacity(entries.len());
            for entry in entries {
                let message = entry
                    .message
                    .ok_or_else(|| error("mute peer received a malformed entry"))?;
                self.receive(&message).await?;
                last.clone_from(&entry.id);
                read.push(entry.id);
            }
            if !read.is_empty() {
                self.broker.acknowledge(&self.node, &read).await?;
            }
        }
    }

    async fn receive(&self, message: &BrokerMessage) -> Result<(), RoomKitError> {
        let response = match message.kind.as_str() {
            "request" => {
                let turn: TurnRequest = serde_json::from_str(&message.body).map_err(error)?;
                let needs = roomkit::turns::TakesOptions {
                    room_hooks: turn.room_hooks,
                    organization_id: turn.context.room.organization_id,
                };
                if self.worker.takes(&turn.channel_id, needs) {
                    "taken"
                } else {
                    "refused"
                }
                .to_owned()
            }
            "channel" => {
                let control: ChannelControlEnvelope =
                    serde_json::from_str(&message.body).map_err(error)?;
                let answer = self
                    .worker
                    .control(control.message)
                    .await?
                    .unwrap_or(ChannelResult {
                        result: serde_json::Value::Null,
                        error: None,
                        room_state: None,
                        executor_id: None,
                    });
                serde_json::to_string(&answer).map_err(error)?
            }
            "control" | "released" => return Ok(()),
            other => return Err(error(format!("unexpected mute peer entry: {other}"))),
        };
        let taken = response == "taken";
        let answer = BrokerMessage {
            reference: message.reference.clone(),
            ..BrokerMessage::new("answer", &self.node, response)
        };
        self.broker.post(&message.sender, &[answer]).await?;
        if taken
            && !self.gate.is_empty()
            && let Some(run) = self.run.upgrade()
        {
            run.gates.reach(&self.gate);
        }
        Ok(())
    }
}

fn error(cause: impl std::fmt::Display) -> RoomKitError {
    RoomKitError::Runtime(cause.to_string())
}
