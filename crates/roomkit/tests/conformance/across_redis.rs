//! A node of the distributed backend across a Redis server, for the runners
//! built with the Redis assembly.

use roomkit::{RedisTurnBackend, RedisTurnBackendOptions, RoomKitError, TurnTimeouts};

use crate::scenarios::Node;

/// A node of its own connections under `key_prefix`, with the broker and
/// the registry it runs on.
pub(crate) fn node(
    url: &str,
    key_prefix: &str,
    timeouts: TurnTimeouts,
) -> Result<Node, RoomKitError> {
    let backend = RedisTurnBackend::new(
        Some(url),
        RedisTurnBackendOptions {
            key_prefix: key_prefix.to_owned(),
            timeouts,
            ..RedisTurnBackendOptions::default()
        },
    )?;
    let (broker, registry) = backend.services();
    Ok(Node {
        backend: backend.node().clone(),
        broker,
        registry,
    })
}
