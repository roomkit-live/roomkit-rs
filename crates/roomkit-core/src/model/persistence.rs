//! Which events a kit stores (RFC §14.3), the Python implementation's
//! `PersistencePolicy`.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::EventType;

/// Which event types a kit's commits store, configured on the kit
/// (`RoomKitOptions::persistence_policy`).
///
/// An event of a type it keeps out is delivered all the same, and consumes
/// no index: the room's counters do not move (RFC §14.3). An edit or a
/// delete kept out still writes its target, alone. A hook's injected
/// event, a record stored `BLOCKED` and a record committed outside the
/// pipeline are stored whatever the policy.
///
/// With `persist_types` `None`, the default, every type is stored;
/// `exclude_types` wins over `persist_types`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PersistencePolicy {
    /// Store only these event types; `None` stores every type.
    pub persist_types: Option<BTreeSet<EventType>>,
    /// Never store these event types, whatever `persist_types` names.
    pub exclude_types: BTreeSet<EventType>,
}

impl PersistencePolicy {
    /// Whether an event of `event_type` is stored: not excluded, and named
    /// by `persist_types` when it names any.
    pub fn should_persist(&self, event_type: EventType) -> bool {
        if self.exclude_types.contains(&event_type) {
            return false;
        }
        self.persist_types
            .as_ref()
            .is_none_or(|types| types.contains(&event_type))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn types(types: &[EventType]) -> BTreeSet<EventType> {
        types.iter().copied().collect()
    }

    // Python: tests/test_store_activity.py::TestPersistencePolicy::test_default_persists_all
    #[test]
    fn default_persists_all() {
        let policy = PersistencePolicy::default();
        assert!(policy.should_persist(EventType::Message));
        assert!(policy.should_persist(EventType::ToolCallStart));
        assert!(policy.should_persist(EventType::Typing));
    }

    // Python: tests/test_store_activity.py::TestPersistencePolicy::test_persist_types_whitelist
    #[test]
    fn persist_types_whitelist() {
        let policy = PersistencePolicy {
            persist_types: Some(types(&[
                EventType::Message,
                EventType::ToolCallStart,
                EventType::ToolCallEnd,
            ])),
            ..PersistencePolicy::default()
        };
        assert!(policy.should_persist(EventType::Message));
        assert!(policy.should_persist(EventType::ToolCallStart));
        assert!(!policy.should_persist(EventType::Typing));
        assert!(!policy.should_persist(EventType::Presence));
    }

    // Python: tests/test_store_activity.py::TestPersistencePolicy::test_exclude_types
    #[test]
    fn exclude_types() {
        let policy = PersistencePolicy {
            exclude_types: types(&[EventType::Typing, EventType::Presence]),
            ..PersistencePolicy::default()
        };
        assert!(policy.should_persist(EventType::Message));
        assert!(!policy.should_persist(EventType::Typing));
        assert!(!policy.should_persist(EventType::Presence));
    }

    // Python: tests/test_store_activity.py::TestPersistencePolicy::test_exclude_overrides_persist
    #[test]
    fn exclude_overrides_persist() {
        let policy = PersistencePolicy {
            persist_types: Some(types(&[EventType::Message, EventType::Typing])),
            exclude_types: types(&[EventType::Typing]),
        };
        assert!(policy.should_persist(EventType::Message));
        assert!(!policy.should_persist(EventType::Typing));
    }
}
