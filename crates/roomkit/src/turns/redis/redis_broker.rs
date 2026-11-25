//! The backend's service on Redis: a stream per node's inbox, the beats
//! and the registry in sorted sets and hashes written by scripts, at Redis
//! server time in milliseconds.
use std::time::Duration;

use redis::aio::{ConnectionManager, ConnectionManagerConfig, MultiplexedConnection};
use redis::streams::{StreamId, StreamReadReply};
use tokio::sync::OnceCell;

use super::broker::{Beater, Entry, Inbox, TurnBroker};
use super::registry::{Beat, View};
use super::stream::Message;
use crate::RoomKitError;
use crate::turns::{TurnFuture, TurnTimeouts};

pub(super) struct RedisBroker {
    pub(super) prefix: String,
    timeouts: TurnTimeouts,
    pub(super) address: redis::Client,
    pub(super) data: OnceCell<ConnectionManager>,
}

impl RedisBroker {
    pub(super) fn new(
        address: redis::Client,
        client: Option<ConnectionManager>,
        prefix: String,
        timeouts: TurnTimeouts,
    ) -> Self {
        Self {
            prefix,
            timeouts,
            address,
            data: OnceCell::new_with(client),
        }
    }

    pub(super) async fn connection(&self) -> Result<ConnectionManager, RoomKitError> {
        let bound = (self.timeouts.heartbeat_timeout / 2).max(Duration::from_millis(1));
        self.data
            .get_or_try_init(|| async {
                let config = ConnectionManagerConfig::new()
                    .set_connection_timeout(Some(bound))
                    .set_response_timeout(Some(bound))
                    .set_number_of_retries(0);
                ConnectionManager::new_with_config(self.address.clone(), config)
                    .await
                    .map_err(|error| redis_error(&error))
            })
            .await
            .cloned()
    }

    pub(super) fn key(&self, suffix: &str) -> String {
        format!("{}:{suffix}", self.prefix)
    }

    pub(super) fn ttl_ms(&self) -> u64 {
        u64::try_from(
            self.timeouts
                .heartbeat_timeout
                .as_millis()
                .saturating_mul(3),
        )
        .unwrap_or(u64::MAX)
        .max(1)
    }

    pub(super) async fn inbox_connection(
        &self,
        block: Duration,
    ) -> Result<MultiplexedConnection, RoomKitError> {
        let bound = (self.timeouts.heartbeat_timeout / 2).max(Duration::from_millis(1));
        let config = redis::AsyncConnectionConfig::new()
            .set_connection_timeout(Some(bound))
            .set_response_timeout(Some(bound + block));
        self.address
            .get_multiplexed_async_connection_with_config(&config)
            .await
            .map_err(|error| redis_error(&error))
    }

    pub(super) async fn beat_connection(&self) -> Result<MultiplexedConnection, RoomKitError> {
        let bound = (self.timeouts.heartbeat_timeout / 2).max(Duration::from_millis(1));
        let cfg = redis::AsyncConnectionConfig::new()
            .set_connection_timeout(Some(bound))
            .set_response_timeout(Some(bound));
        self.address
            .get_multiplexed_async_connection_with_config(&cfg)
            .await
            .map_err(|error| redis_error(&error))
    }

    fn holder_key(&self, channel: &str, room: &str) -> String {
        self.key(&format!("holder:{}:{}", quoted(channel), quoted(room)))
    }
}

async fn write_beat(
    prefix: &str,
    ttl_ms: u64,
    connection: &mut impl redis::aio::ConnectionLike,
    beat: &Beat,
) -> Result<(), RoomKitError> {
    let key = |suffix: &str| format!("{prefix}:{suffix}");
    let suffix = |name: &str| key(&format!("{name}:{}", beat.node));
    let payload = serde_json::to_string(beat).map_err(|e| RoomKitError::Runtime(e.to_string()))?;
    redis::cmd("EVAL")
        .arg(include_str!("beat.lua"))
        .arg(6)
        .arg(key("alive"))
        .arg(key("executors"))
        .arg(key("owners"))
        .arg(suffix("inbox"))
        .arg(suffix("running"))
        .arg(suffix("generation"))
        .arg(ttl_ms)
        .arg(payload)
        .exec_async(connection)
        .await
        .map_err(|error| redis_error(&error))
}

impl TurnBroker for RedisBroker {
    fn connect(&self) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async move { self.connection().await.map(drop) })
    }

    fn connected(&self) -> bool {
        self.data.get().is_some()
    }

    fn post<'a>(
        &'a self,
        node: &'a str,
        message: &'a Message,
    ) -> TurnFuture<'a, Result<(), RoomKitError>> {
        Box::pin(async move {
            let key = self.key(&format!("inbox:{node}"));
            let mut connection = self.connection().await?;
            // A crash between adding a message and setting the TTL would leave an orphaned inbox.
            redis::pipe()
                .atomic()
                .cmd("XADD")
                .arg(&key)
                .arg("*")
                .arg("kind")
                .arg(&message.kind)
                .arg("sender")
                .arg(&message.sender)
                .arg("body")
                .arg(&message.body)
                .arg("target")
                .arg(&message.target)
                .arg("ref")
                .arg(&message.reference)
                .ignore()
                .cmd("PEXPIRE")
                .arg(key)
                .arg(self.ttl_ms())
                .ignore()
                .query_async(&mut connection)
                .await
                .map_err(|error| redis_error(&error))
        })
    }

    fn inbox<'a>(
        &'a self,
        node: &'a str,
        block: Duration,
    ) -> TurnFuture<'a, Result<Box<dyn Inbox>, RoomKitError>> {
        Box::pin(async move {
            let inbox: Box<dyn Inbox> = Box::new(RedisInbox {
                connection: self.inbox_connection(block).await?,
                key: self.key(&format!("inbox:{node}")),
                block,
            });
            Ok(inbox)
        })
    }

    fn acknowledge<'a>(
        &'a self,
        node: &'a str,
        ids: &'a [String],
    ) -> TurnFuture<'a, Result<(), RoomKitError>> {
        Box::pin(async move {
            let mut connection = self.connection().await?;
            let key = self.key(&format!("inbox:{node}"));
            redis::cmd("XDEL")
                .arg(key)
                .arg(ids)
                .exec_async(&mut connection)
                .await
                .map_err(|error| redis_error(&error))
        })
    }

    fn beat<'a>(&'a self, beat: &'a Beat) -> TurnFuture<'a, Result<(), RoomKitError>> {
        Box::pin(async move {
            let mut connection = self.connection().await?;
            write_beat(&self.prefix, self.ttl_ms(), &mut connection, beat).await
        })
    }

    fn beater(&self) -> TurnFuture<'_, Result<Box<dyn Beater>, RoomKitError>> {
        Box::pin(async move {
            let beater: Box<dyn Beater> = Box::new(RedisBeater {
                connection: self.beat_connection().await?,
                prefix: self.prefix.clone(),
                ttl_ms: self.ttl_ms(),
            });
            Ok(beater)
        })
    }

    fn view(&self, forget_after_ms: i64) -> TurnFuture<'_, Result<View, RoomKitError>> {
        Box::pin(async move {
            let mut connection = self.connection().await?;
            let body: String = redis::cmd("EVAL")
                .arg(include_str!("view.lua"))
                .arg(3)
                .arg(self.key("alive"))
                .arg(self.key("executors"))
                .arg(self.key("owners"))
                .arg(forget_after_ms)
                .query_async(&mut connection)
                .await
                .map_err(|error| redis_error(&error))?;
            serde_json::from_str(&body).map_err(|e| RoomKitError::Runtime(e.to_string()))
        })
    }

    fn forget_owner<'a>(&'a self, node: &'a str) -> TurnFuture<'a, Result<(), RoomKitError>> {
        Box::pin(async move {
            let mut connection = self.connection().await?;
            redis::cmd("ZREM")
                .arg(self.key("owners"))
                .arg(node)
                .exec_async(&mut connection)
                .await
                .map_err(|error| redis_error(&error))
        })
    }

    fn beats<'a>(
        &'a self,
        members: &'a [&'a str],
        running: &'a [(&'a str, Vec<&'a str>)],
    ) -> TurnFuture<'a, Result<Vec<Vec<Option<i64>>>, RoomKitError>> {
        Box::pin(async move {
            let mut pipe = redis::pipe();
            pipe.atomic()
                .cmd("ZMSCORE")
                .arg(self.key("alive"))
                .arg(members);
            for (node, ids) in running {
                pipe.cmd("SMISMEMBER")
                    .arg(self.key(&format!("running:{node}")))
                    .arg(ids);
            }
            let mut client = self.connection().await?;
            pipe.query_async(&mut client)
                .await
                .map_err(|error| redis_error(&error))
        })
    }

    fn holder<'a>(
        &'a self,
        channel: &'a str,
        room: &'a str,
    ) -> TurnFuture<'a, Result<Option<String>, RoomKitError>> {
        Box::pin(async move {
            let mut conn = self.connection().await?;
            redis::cmd("GET")
                .arg(self.holder_key(channel, room))
                .query_async(&mut conn)
                .await
                .map_err(|error| redis_error(&error))
        })
    }

    fn hold<'a>(
        &'a self,
        channel: &'a str,
        room: &'a str,
        member: &'a str,
    ) -> TurnFuture<'a, Result<(), RoomKitError>> {
        Box::pin(async move {
            let mut conn = self.connection().await?;
            redis::cmd("SET")
                .arg(self.holder_key(channel, room))
                .arg(member)
                .arg("EX")
                .arg(7 * 24 * 3600)
                .exec_async(&mut conn)
                .await
                .map_err(|error| redis_error(&error))
        })
    }

    fn forget_holder<'a>(
        &'a self,
        channel: &'a str,
        room: &'a str,
    ) -> TurnFuture<'a, Result<(), RoomKitError>> {
        Box::pin(async move {
            let mut conn = self.connection().await?;
            redis::cmd("DEL")
                .arg(self.holder_key(channel, room))
                .exec_async(&mut conn)
                .await
                .map_err(|error| redis_error(&error))
        })
    }

    fn claim_presence<'a>(
        &'a self,
        channel_id: &'a str,
        room_id: &'a str,
        online: Option<bool>,
    ) -> TurnFuture<'a, Result<bool, RoomKitError>> {
        Box::pin(async move {
            let key = self.key(&format!(
                "presence:{}:{}",
                quoted(room_id),
                quoted(channel_id)
            ));
            let state = online.map_or("", |value| if value { "online" } else { "offline" });
            let mut connection = self.connection().await?;
            redis::cmd("EVAL")
                .arg(include_str!("claim_presence.lua"))
                .arg(1)
                .arg(key)
                .arg(state)
                .arg(7 * 24 * 60 * 60 * 1000)
                .query_async(&mut connection)
                .await
                .map_err(|error| redis_error(&error))
        })
    }
}

struct RedisInbox {
    connection: MultiplexedConnection,
    key: String,
    block: Duration,
}

impl Inbox for RedisInbox {
    fn read<'a>(&'a mut self, last: &'a str) -> TurnFuture<'a, Result<Vec<Entry>, RoomKitError>> {
        Box::pin(async move {
            let block = u64::try_from(self.block.as_millis()).unwrap_or(1000);
            let batch: Option<StreamReadReply> = redis::cmd("XREAD")
                .arg("COUNT")
                .arg(256)
                .arg("BLOCK")
                .arg(block)
                .arg("STREAMS")
                .arg(&self.key)
                .arg(last)
                .query_async(&mut self.connection)
                .await
                .map_err(|error| redis_error(&error))?;
            Ok(batch
                .into_iter()
                .flat_map(|r| r.keys)
                .flat_map(|k| k.ids)
                .map(|entry| Entry {
                    message: message(&entry),
                    id: entry.id,
                })
                .collect())
        })
    }
}

pub(super) fn message(entry: &StreamId) -> Option<Message> {
    Some(Message {
        kind: entry.get("kind")?,
        sender: entry.get("sender")?,
        body: entry.get("body")?,
        target: entry.get("target").unwrap_or_default(),
        reference: entry.get("ref").unwrap_or_default(),
    })
}

struct RedisBeater {
    connection: MultiplexedConnection,
    prefix: String,
    ttl_ms: u64,
}

impl Beater for RedisBeater {
    fn beat<'a>(&'a mut self, beat: &'a Beat) -> TurnFuture<'a, Result<(), RoomKitError>> {
        Box::pin(write_beat(
            &self.prefix,
            self.ttl_ms,
            &mut self.connection,
            beat,
        ))
    }
}

/// Python's urllib quote(safe="") applied to one key component.
fn quoted(input: &str) -> String {
    use std::fmt::Write;
    let mut encoded = String::new();
    for byte in input.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.~".contains(&byte) {
            encoded.push(char::from(byte));
        } else {
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
}

fn redis_error(error: &redis::RedisError) -> RoomKitError {
    RoomKitError::Runtime(error.to_string())
}
