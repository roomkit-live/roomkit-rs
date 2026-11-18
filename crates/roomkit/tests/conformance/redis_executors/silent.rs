//! A peer that acknowledges receipt of the request but emits no turn frames,
//! like Python's `answers: false` executor. The owner and every other worker
//! use the normal transport; the mute peer still applies real channel controls.
use std::sync::{Arc, Weak};
use std::time::Duration;

use redis::aio::MultiplexedConnection;
use redis::streams::{StreamId, StreamReadReply};
use roomkit::turns::{ChannelResult, TurnRequest, wire::ChannelControlEnvelope};
use roomkit::{RoomKitError, TurnExecutor};
use tokio::sync::oneshot;

use super::Worker;
use crate::run::Run;

const BEAT: &str = "
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
redis.call('ZADD', KEYS[1], now, ARGV[1])
return redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])";

pub(super) struct Silent {
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<(), RoomKitError>>,
}

struct Peer {
    connection: MultiplexedConnection,
    prefix: String,
    node: String,
    member: String,
    declaration: String,
    run: Weak<Run>,
    gate: String,
    worker: TurnExecutor,
}

impl Silent {
    pub(super) async fn start(
        url: &str,
        prefix: &str,
        node: &str,
        worker: TurnExecutor,
        spec: &Worker,
        run: Arc<Run>,
    ) -> Result<Self, RoomKitError> {
        let client = redis::Client::open(url).map_err(error)?;
        let member = format!("{node}:{}", spec.id);
        let declaration = serde_json::json!({"node": node, "executor_id": spec.id,
            "channels": spec.hosts, "organizations":spec.organizations,
            "room_hooks":worker.hooks().defined_room_hooks()})
        .to_string();
        let mut peer = Peer {
            connection: client
                .get_multiplexed_async_connection()
                .await
                .map_err(error)?,
            prefix: prefix.to_owned(),
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
    fn key(&self, suffix: &str) -> String {
        format!("{}:{suffix}", self.prefix)
    }

    async fn beat(&mut self) -> Result<(), RoomKitError> {
        // Registry scores use the Redis clock, including when CI runs remotely.
        redis::cmd("EVAL")
            .arg(BEAT)
            .arg(2)
            .arg(self.key("alive"))
            .arg(self.key("executors"))
            .arg(&self.member)
            .arg(&self.declaration)
            .exec_async(&mut self.connection)
            .await
            .map_err(error)
    }

    async fn withdraw(&mut self) -> Result<(), RoomKitError> {
        redis::pipe()
            .atomic()
            .cmd("ZREM")
            .arg(self.key("alive"))
            .arg(&self.member)
            .ignore()
            .cmd("HDEL")
            .arg(self.key("executors"))
            .arg(&self.member)
            .ignore()
            .cmd("DEL")
            .arg(self.key(&format!("inbox:{}", self.node)))
            .ignore()
            .query_async(&mut self.connection)
            .await
            .map_err(error)
    }

    async fn listen(&mut self) -> Result<(), RoomKitError> {
        let inbox = self.key(&format!("inbox:{}", self.node));
        let tick = self
            .worker
            .timeouts()
            .heartbeat_interval
            .max(Duration::from_millis(1));
        let mut last = "0-0".to_owned();
        loop {
            self.beat().await?;
            let batch: Option<StreamReadReply> = redis::cmd("XREAD")
                .arg("STREAMS")
                .arg(&inbox)
                .arg(&last)
                .query_async(&mut self.connection)
                .await
                .map_err(error)?;
            for stream in batch.into_iter().flat_map(|batch| batch.keys) {
                for entry in stream.ids {
                    self.receive(&entry).await?;
                    last = entry.id;
                }
            }
            tokio::time::sleep(tick).await;
        }
    }

    async fn receive(&mut self, entry: &StreamId) -> Result<(), RoomKitError> {
        let field = |name| {
            entry
                .get::<String>(name)
                .ok_or_else(|| error(format!("mute peer received no {name}")))
        };
        let request = field("kind")?;
        let response = match request.as_str() {
            "request" => {
                let turn: TurnRequest = serde_json::from_str(&field("body")?).map_err(error)?;
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
                    serde_json::from_str(&field("body")?).map_err(error)?;
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
        let destination = self.key(&format!("inbox:{}", field("sender")?));
        redis::cmd("XADD")
            .arg(destination)
            .arg("*")
            .arg("kind")
            .arg("answer")
            .arg("sender")
            .arg(&self.node)
            .arg("body")
            .arg(response)
            .arg("target")
            .arg("")
            .arg("ref")
            .arg(field("ref")?)
            .exec_async(&mut self.connection)
            .await
            .map_err(error)?;
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
