//! The broker and the registry of this process keep the ports' contract:
//! the checks are the ones a service's own tests call (`crate::testing`).
use std::sync::Arc;

use super::{InMemoryTurnBroker, InMemoryTurnRegistry};
use crate::testing::{check_turn_broker, check_turn_registry};

#[tokio::test]
async fn the_services_of_this_process_keep_the_ports() {
    check_turn_broker(Arc::new(InMemoryTurnBroker::new())).await;
    check_turn_registry(Arc::new(InMemoryTurnRegistry::new())).await;
}
