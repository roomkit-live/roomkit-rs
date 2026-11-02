//! The tools the kit sets up for its channels, which the room's owner
//! serves their turns (RFC §10.6.2, §10.6.5): Python's
//! `turns._registry_tools.OwnerToolStore`.
//!
//! The owner keeps them by channel, beside the channel, never on it: each
//! turn of the channel lists them in its request, its executor declares
//! them beside the channel's own tools, and a call to one crosses back to
//! the owner, whose handler runs in the room of the call.

use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};

use roomkit_core::model::ChannelId;
use serde_json::Value;

use crate::RoomKit;
use crate::channel::BoxFuture;
use crate::turn::{OwnerCall, OwnerToolAnswer};
use crate::turns::wire::OwnerTool;

/// What serves the calls of one tool the owner serves: the kit and the
/// call to the handler's answer.
pub(crate) type OwnerServe<S> =
    Arc<dyn Fn(RoomKit<S>, OwnerCall) -> BoxFuture<'static, OwnerToolAnswer> + Send + Sync>;

/// One tool the owner serves a channel: its definition, and its handler.
pub(crate) struct OwnerToolEntry<S> {
    /// `{name, description, parameters}`, as a turn's request carries it.
    pub(crate) definition: Value,
    pub(crate) serve: OwnerServe<S>,
}

impl<S> OwnerToolEntry<S> {
    pub(crate) fn name(&self) -> &str {
        self.definition
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
    }
}

/// The tools the kit set up for each of its channels, by channel id.
pub(crate) struct OwnerToolStore<S> {
    by_channel: RwLock<HashMap<ChannelId, Vec<OwnerToolEntry<S>>>>,
}

impl<S> Default for OwnerToolStore<S> {
    fn default() -> Self {
        Self {
            by_channel: RwLock::new(HashMap::new()),
        }
    }
}

impl<S> std::fmt::Debug for OwnerToolStore<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let by_channel = self
            .by_channel
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        let names: HashMap<&ChannelId, Vec<&str>> = by_channel
            .iter()
            .map(|(channel, entries)| (channel, entries.iter().map(OwnerToolEntry::name).collect()))
            .collect();
        formatter
            .debug_struct("OwnerToolStore")
            .field("by_channel", &names)
            .finish()
    }
}

impl<S> OwnerToolStore<S> {
    /// Whether the owner serves `channel_id` a tool named `name`.
    pub(crate) fn serves(&self, channel_id: &ChannelId, name: &str) -> bool {
        self.read()
            .get(channel_id)
            .is_some_and(|entries| entries.iter().any(|entry| entry.name() == name))
    }

    /// Sets up `entries` for `channel_id`, all of them or none: a name the
    /// owner serves the channel already is refused, with the first such
    /// name.
    pub(crate) fn register_all(
        &self,
        channel_id: &ChannelId,
        entries: Vec<OwnerToolEntry<S>>,
    ) -> Result<(), String> {
        let mut by_channel = self
            .by_channel
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let held = by_channel.entry(channel_id.clone()).or_default();
        let mut names: Vec<&str> = held.iter().map(OwnerToolEntry::name).collect();
        for entry in &entries {
            if names.contains(&entry.name()) {
                return Err(entry.name().to_owned());
            }
            names.push(entry.name());
        }
        held.extend(entries);
        Ok(())
    }

    /// The tools the owner serves the turns of `channel_id`, as a turn's
    /// request lists them, in the order they were set up.
    pub(crate) fn declared(&self, channel_id: &ChannelId) -> Vec<OwnerTool> {
        self.read()
            .get(channel_id)
            .map(|entries| {
                entries
                    .iter()
                    .map(|entry| OwnerTool {
                        tool: entry.definition.clone(),
                        waits_by_design: false,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// What serves `channel_id`'s calls to `name`, if the owner serves it.
    pub(crate) fn lookup(&self, channel_id: &ChannelId, name: &str) -> Option<OwnerServe<S>> {
        self.read().get(channel_id).and_then(|entries| {
            entries
                .iter()
                .find(|entry| entry.name() == name)
                .map(|entry| Arc::clone(&entry.serve))
        })
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<ChannelId, Vec<OwnerToolEntry<S>>>> {
        self.by_channel
            .read()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;
    use crate::InMemoryStore;

    fn entry(name: &str) -> OwnerToolEntry<InMemoryStore> {
        OwnerToolEntry {
            definition: json!({ "name": name }),
            serve: Arc::new(|_, _| Box::pin(async { OwnerToolAnswer::Served(json!("ok")) })),
        }
    }

    // Python: tests/tasks/test_cancel_task.py::test_setup_task_tools_sets_up_both_tools_or_neither
    #[test]
    fn tools_are_set_up_all_or_none() {
        let store = OwnerToolStore::default();
        let agent = ChannelId::new("speaker").unwrap();
        store
            .register_all(&agent, vec![entry("cancel_task")])
            .unwrap();
        let refused = store.register_all(&agent, vec![entry("task_status"), entry("cancel_task")]);
        assert_eq!(
            (refused, store.serves(&agent, "task_status")),
            (Err("cancel_task".to_owned()), false)
        );
    }
}
