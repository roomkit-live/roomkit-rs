//! Each node reads its own stream and acknowledges entries after routing them.
use std::time::Duration;

use super::RedisTurnBackend;
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
}

impl RedisTurnBackend {
    pub(super) async fn write(&self, destination: &str, msg: &Message) -> Result<(), RoomKitError> {
        self.broker().post(destination, msg).await
    }

    pub(super) fn read_block(&self) -> Duration {
        self.timeouts()
            .heartbeat_interval
            .min(Duration::from_secs(1))
            .max(Duration::from_millis(1))
    }

    pub(super) async fn acknowledge(&self, ids: &[String]) -> Result<(), RoomKitError> {
        self.broker().acknowledge(self.node_id(), ids).await
    }
}
