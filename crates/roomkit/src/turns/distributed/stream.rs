//! Each node reads its own stream and acknowledges entries after routing them.
use std::time::Duration;

use super::DistributedTurnBackend;
use crate::RoomKitError;

pub(super) use super::broker::BrokerMessage as Message;

impl DistributedTurnBackend {
    pub(super) async fn write(&self, destination: &str, msg: &Message) -> Result<(), RoomKitError> {
        self.broker()
            .post(destination, std::slice::from_ref(msg))
            .await
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
