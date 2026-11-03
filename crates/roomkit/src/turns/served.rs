//! The tools the kit sets up for its channels, which the room's owner
//! serves their turns (RFC §10.6.2, §10.6.5): Python's
//! `turns._registry_tools.OwnerToolStore`.
//!
//! The owner keeps them by channel, beside the channel, never on it: each
//! turn of the channel lists them in its request, its executor declares
//! them beside the channel's own tools, and a call to one crosses back to
//! the owner, whose handler runs in the room of the call. A tool set up for
//! one room alone (a delegation's result tool, RFC §23.3) is listed and
//! served in that room's turns only.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, PoisonError, RwLock};

use roomkit_core::model::{ChannelId, RoomId};
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
    /// Whether its call waits by design, the channel's bound per call not
    /// applying to it (RFC §21.6): a strategy's tool, Python's
    /// `orchestration_tool(..., waits=True)`.
    pub(crate) waits: bool,
    /// The install that set it up for a room, by its address: the same
    /// install setting it up again replaces its own (Python's registry's
    /// `owner`).
    pub(crate) owner: Option<usize>,
}

impl<S> OwnerToolEntry<S> {
    pub(crate) fn name(&self) -> &str {
        self.definition
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
    }
}

/// The tools set up for one channel in one room, by the channel and the
/// room.
type InRooms<S> = HashMap<(ChannelId, RoomId), Vec<OwnerToolEntry<S>>>;

/// The tools the kit set up for each of its channels, by channel id, in
/// every room or in one.
pub(crate) struct OwnerToolStore<S> {
    by_channel: RwLock<HashMap<ChannelId, Vec<OwnerToolEntry<S>>>>,
    in_rooms: RwLock<InRooms<S>>,
}

impl<S> Default for OwnerToolStore<S> {
    fn default() -> Self {
        Self {
            by_channel: RwLock::new(HashMap::new()),
            in_rooms: RwLock::new(HashMap::new()),
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
        let in_rooms = self.in_rooms.read().unwrap_or_else(PoisonError::into_inner);
        let in_rooms: HashMap<&(ChannelId, RoomId), Vec<&str>> = in_rooms
            .iter()
            .map(|(scope, entries)| (scope, entries.iter().map(OwnerToolEntry::name).collect()))
            .collect();
        formatter
            .debug_struct("OwnerToolStore")
            .field("by_channel", &names)
            .field("in_rooms", &in_rooms)
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
    ) -> Result<(), (String, String)> {
        let mut by_channel = self
            .by_channel
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let held = by_channel.entry(channel_id.clone()).or_default();
        let mut names: Vec<&str> = held.iter().map(OwnerToolEntry::name).collect();
        for entry in &entries {
            if names.contains(&entry.name()) {
                return Err((entry.name().to_owned(), "served by the channel".to_owned()));
            }
            if let Some(room_id) = self.room_serving(channel_id, entry.name()) {
                let clash = format!("served in room '{room_id}'");
                return Err((entry.name().to_owned(), clash));
            }
            names.push(entry.name());
        }
        held.extend(entries);
        Ok(())
    }

    /// The first room, by its id, whose tools for `channel_id` hold `name`.
    fn room_serving(&self, channel_id: &ChannelId, name: &str) -> Option<RoomId> {
        let in_rooms = self.in_rooms.read().unwrap_or_else(PoisonError::into_inner);
        in_rooms
            .iter()
            .filter(|((channel, _), entries)| {
                channel == channel_id && entries.iter().any(|entry| entry.name() == name)
            })
            .map(|((_, room_id), _)| room_id.clone())
            .min()
    }

    /// Why `name` cannot be set up for `channel_id` in `room_id` for the
    /// install `owner`: what [`register_all_in_room`](Self::register_all_in_room)
    /// would refuse it with, nothing set up. Python's registry's `check`.
    pub(crate) fn clash_in_room(
        &self,
        channel_id: &ChannelId,
        room_id: &RoomId,
        name: &str,
        owner: usize,
    ) -> Option<String> {
        if self.serves(channel_id, name) {
            return Some("served by the channel".to_owned());
        }
        let in_rooms = self.in_rooms.read().unwrap_or_else(PoisonError::into_inner);
        let clashes = in_rooms
            .get(&(channel_id.clone(), room_id.clone()))
            .is_some_and(|held| {
                held.iter()
                    .any(|served| served.name() == name && served.owner != Some(owner))
            });
        clashes.then(|| format!("served in room '{}'", room_id.as_str()))
    }

    /// Sets up `entry` for `channel_id` in `room_id` alone, as Python's
    /// registry's `register(entry, room_id=...)`: refused, with what serves
    /// its name already, when the owner serves the channel that name in
    /// every room or in that room.
    pub(crate) fn register_in_room(
        &self,
        channel_id: &ChannelId,
        room_id: &RoomId,
        entry: OwnerToolEntry<S>,
    ) -> Result<(), String> {
        if self.serves(channel_id, entry.name()) {
            return Err("served by the channel".to_owned());
        }
        let mut in_rooms = self
            .in_rooms
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let held = in_rooms
            .entry((channel_id.clone(), room_id.clone()))
            .or_default();
        if held.iter().any(|served| served.name() == entry.name()) {
            return Err(format!("served in room '{}'", room_id.as_str()));
        }
        held.push(entry);
        Ok(())
    }

    /// Sets up `entries` for `channel_id` in `room_id` alone, all of them or
    /// none, as Python's registry's `register_all(entries, room_id=...,
    /// owner=...)`: a name the owner serves the channel in every room is
    /// refused, and one served in the room unless `owner` set it up, which
    /// it then replaces; the first refused name's clash is the error.
    pub(crate) fn register_all_in_room(
        &self,
        channel_id: &ChannelId,
        room_id: &RoomId,
        entries: Vec<OwnerToolEntry<S>>,
        owner: usize,
    ) -> Result<(), (String, String)> {
        let mut in_rooms = self
            .in_rooms
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let scope = (channel_id.clone(), room_id.clone());
        let held = in_rooms.get(&scope).map(Vec::as_slice).unwrap_or_default();
        for entry in &entries {
            let name = entry.name();
            if self.serves(channel_id, name) {
                return Err((name.to_owned(), "served by the channel".to_owned()));
            }
            let clashes = held
                .iter()
                .any(|served| served.name() == name && served.owner != Some(owner));
            if clashes {
                let clash = format!("served in room '{}'", room_id.as_str());
                return Err((name.to_owned(), clash));
            }
        }
        let held = in_rooms.entry(scope).or_default();
        for entry in entries {
            held.retain(|served| served.name() != entry.name());
            held.push(OwnerToolEntry {
                owner: Some(owner),
                ..entry
            });
        }
        Ok(())
    }

    /// The names of the tools set up for one room alone, by channel: what
    /// an install added there, which an uninstall takes back.
    pub(crate) fn room_names(&self, room_id: &RoomId) -> BTreeMap<ChannelId, BTreeSet<String>> {
        let in_rooms = self.in_rooms.read().unwrap_or_else(PoisonError::into_inner);
        in_rooms
            .iter()
            .filter(|((_, room), _)| room == room_id)
            .map(|((channel, _), entries)| {
                let names = entries.iter().map(|entry| entry.name().to_owned());
                (channel.clone(), names.collect())
            })
            .collect()
    }

    /// Drops every tool set up for `channel_id`, in every room and in one:
    /// the channel left the kit, and one registered again under its id
    /// gets its own setup (Python's `OwnerToolStore.forget`).
    pub(crate) fn forget(&self, channel_id: &ChannelId) {
        self.by_channel
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(channel_id);
        self.in_rooms
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|(channel, _), _| channel != channel_id);
    }

    /// Stops serving `channel_id` the tool `name` set up for `room_id`.
    pub(crate) fn unregister_in_room(&self, channel_id: &ChannelId, room_id: &RoomId, name: &str) {
        let mut in_rooms = self
            .in_rooms
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let key = (channel_id.clone(), room_id.clone());
        if let Some(held) = in_rooms.get_mut(&key) {
            held.retain(|entry| entry.name() != name);
            if held.is_empty() {
                in_rooms.remove(&key);
            }
        }
    }

    /// The tools the owner serves the turns of `channel_id` in `room_id`,
    /// as a turn's request lists them: those of every room in the order they
    /// were set up, then the room's own.
    pub(crate) fn declared(&self, channel_id: &ChannelId, room_id: &RoomId) -> Vec<OwnerTool> {
        let listed = |entries: &[OwnerToolEntry<S>]| -> Vec<OwnerTool> {
            entries
                .iter()
                .map(|entry| OwnerTool {
                    tool: entry.definition.clone(),
                    waits_by_design: entry.waits,
                })
                .collect()
        };
        let mut declared = self
            .read()
            .get(channel_id)
            .map(|entries| listed(entries))
            .unwrap_or_default();
        let in_rooms = self.in_rooms.read().unwrap_or_else(PoisonError::into_inner);
        if let Some(entries) = in_rooms.get(&(channel_id.clone(), room_id.clone())) {
            declared.extend(listed(entries));
        }
        declared
    }

    /// What serves `channel_id`'s calls to `name` in `room_id`, if the
    /// owner serves it there.
    pub(crate) fn lookup(
        &self,
        channel_id: &ChannelId,
        room_id: &RoomId,
        name: &str,
    ) -> Option<OwnerServe<S>> {
        let find = |entries: &Vec<OwnerToolEntry<S>>| {
            entries
                .iter()
                .find(|entry| entry.name() == name)
                .map(|entry| Arc::clone(&entry.serve))
        };
        let in_room = {
            let in_rooms = self.in_rooms.read().unwrap_or_else(PoisonError::into_inner);
            in_rooms
                .get(&(channel_id.clone(), room_id.clone()))
                .and_then(find)
        };
        in_room.or_else(|| self.read().get(channel_id).and_then(find))
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
            waits: false,
            owner: None,
        }
    }

    // Python: tests/tasks/test_structured_result.py::TestStructuredResultGuard::test_returns_structured_payload_when_worker_submits
    // (the result tool declared in the child room only, and removed afterwards)
    #[test]
    fn a_room_s_tool_is_declared_and_served_in_that_room_alone() {
        let store = OwnerToolStore::default();
        let agent = ChannelId::new("worker").unwrap();
        let (child, other) = (RoomId::new("r::task-1").unwrap(), RoomId::new("r").unwrap());
        store
            .register_in_room(&agent, &child, entry("submit_result"))
            .unwrap();
        let again = store.register_in_room(&agent, &child, entry("submit_result"));
        let names = |room: &RoomId| -> Vec<Value> {
            store
                .declared(&agent, room)
                .into_iter()
                .map(|tool| tool.tool["name"].clone())
                .collect()
        };
        let declared = (names(&child), names(&other));
        let served = (
            store.lookup(&agent, &child, "submit_result").is_some(),
            store.lookup(&agent, &other, "submit_result").is_some(),
        );
        store.unregister_in_room(&agent, &child, "submit_result");
        assert_eq!(
            (declared, served, again, names(&child)),
            (
                (vec![json!("submit_result")], vec![]),
                (true, false),
                Err("served in room 'r::task-1'".to_owned()),
                vec![]
            )
        );
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
            (
                Err(("cancel_task".to_owned(), "served by the channel".to_owned())),
                false
            )
        );
    }
}
