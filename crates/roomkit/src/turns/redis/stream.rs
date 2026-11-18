//! Each node reads its own stream and acknowledges entries after routing them.
use std::time::Duration;

use redis::aio::MultiplexedConnection;
use redis::streams::{StreamId, StreamReadReply};

use super::{RedisTurnBackend, redis_error};
use crate::RoomKitError;

#[derive(Clone, Debug)]
pub(super) struct Message {
    pub(super) kind: String,
    pub(super) sender: String,
    pub(super) body: String,
    pub(super) target: String,
    pub(super) reference: String,
}

impl Message {
    pub(super) fn new(
        kind: &str,
        sender: &str,
        body: String,
        target: &str,
        reference: &str,
    ) -> Self {
        Self {
            kind: kind.into(),
            sender: sender.into(),
            body,
            target: target.into(),
            reference: reference.into(),
        }
    }

    pub(super) fn read(entry: &StreamId) -> Option<Self> {
        Some(Self {
            kind: entry.get("kind")?,
            sender: entry.get("sender")?,
            body: entry.get("body")?,
            target: entry.get("target").unwrap_or_default(),
            reference: entry.get("ref").unwrap_or_default(),
        })
    }
}

impl RedisTurnBackend {
    pub(super) async fn write(&self, destination: &str, msg: &Message) -> Result<(), RoomKitError> {
        let key = self.key(&format!("inbox:{destination}"));
        let mut connection = self.connection().await?;
        // A crash between adding a message and setting the TTL would leave an orphaned inbox.
        redis::pipe()
            .atomic()
            .cmd("XADD")
            .arg(&key)
            .arg("*")
            .arg("kind")
            .arg(&msg.kind)
            .arg("sender")
            .arg(&msg.sender)
            .arg("body")
            .arg(&msg.body)
            .arg("target")
            .arg(&msg.target)
            .arg("ref")
            .arg(&msg.reference)
            .ignore()
            .cmd("PEXPIRE")
            .arg(key)
            .arg(self.ttl_ms())
            .ignore()
            .query_async(&mut connection)
            .await
            .map_err(|error| redis_error(&error))
    }

    pub(super) fn ttl_ms(&self) -> u64 {
        u64::try_from(
            self.timeouts()
                .heartbeat_timeout
                .as_millis()
                .saturating_mul(3),
        )
        .unwrap_or(u64::MAX)
        .max(1)
    }

    pub(super) async fn inbox_connection(&self) -> Result<MultiplexedConnection, RoomKitError> {
        let bound = (self.timeouts().heartbeat_timeout / 2).max(Duration::from_millis(1));
        let config = redis::AsyncConnectionConfig::new()
            .set_connection_timeout(Some(bound))
            .set_response_timeout(Some(bound + self.read_block()));
        self.0
            .address
            .get_multiplexed_async_connection_with_config(&config)
            .await
            .map_err(|error| redis_error(&error))
    }

    pub(super) fn read_block(&self) -> Duration {
        self.timeouts()
            .heartbeat_interval
            .min(Duration::from_secs(1))
            .max(Duration::from_millis(1))
    }

    pub(super) async fn read(
        &self,
        connection: &mut MultiplexedConnection,
        last: &str,
    ) -> Result<Vec<StreamId>, RoomKitError> {
        let key = self.key(&format!("inbox:{}", self.node_id()));
        let block = u64::try_from(self.read_block().as_millis()).unwrap_or(1000);
        let batch: Option<StreamReadReply> = redis::cmd("XREAD")
            .arg("COUNT")
            .arg(256)
            .arg("BLOCK")
            .arg(block)
            .arg("STREAMS")
            .arg(key)
            .arg(last)
            .query_async(connection)
            .await
            .map_err(|error| redis_error(&error))?;
        Ok(batch
            .into_iter()
            .flat_map(|r| r.keys)
            .flat_map(|k| k.ids)
            .collect())
    }

    pub(super) async fn acknowledge(&self, ids: &[String]) -> Result<(), RoomKitError> {
        let mut connection = self.connection().await?;
        let key = self.key(&format!("inbox:{}", self.node_id()));
        redis::cmd("XDEL")
            .arg(key)
            .arg(ids)
            .exec_async(&mut connection)
            .await
            .map_err(|error| redis_error(&error))
    }
}
