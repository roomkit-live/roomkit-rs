//! Named executor hooks and owner selections (RFC §10.6.6).
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::{
    HookId, HookRegistry, HookSpec, HookTrigger, Registered, RegistrationError, Removed,
    check_alone,
};
use crate::model::RoomId;

#[derive(Clone, Debug, Default)]
pub(super) struct NamedHooks {
    pub(super) definitions: Vec<Registered>,
    pub(super) selected: BTreeMap<RoomId, Vec<String>>,
    enabled: BTreeMap<RoomId, Vec<String>>,
    pub(super) direct: BTreeSet<HookId>,
}

impl HookRegistry {
    /// Define a named hook, replacing only that name's same trigger.
    /// The runtime validates the executor side and the nonempty name first.
    #[doc(hidden)]
    pub fn define_room_hook(
        &mut self,
        spec: HookSpec,
    ) -> Result<(HookId, Option<Removed>), RegistrationError> {
        check_alone(&spec)?;
        let position = self
            .named
            .definitions
            .iter()
            .position(|hook| hook.spec.name == spec.name && hook.spec.trigger == spec.trigger);
        let replaced = position.map(|i| Removed::of(self.named.definitions.remove(i)));
        let id = HookId(self.next);
        self.next += 1;
        self.named.definitions.push(Registered {
            id,
            room: None,
            spec,
            held: Arc::new(()),
        });
        Ok((id, replaced))
    }

    /// Remove all definitions under a name, handing their handlers back.
    #[doc(hidden)]
    pub fn undefine_room_hook(&mut self, name: &str) -> Vec<Removed> {
        let mut removed = Vec::new();
        let mut kept = Vec::new();
        for hook in self.named.definitions.drain(..) {
            if hook.spec.name == name {
                removed.push(Removed::of(hook));
            } else {
                kept.push(hook);
            }
        }
        self.named.definitions = kept;
        removed
    }

    /// The names currently defined.
    #[doc(hidden)]
    pub fn defined_room_hooks(&self) -> BTreeSet<String> {
        self.named
            .definitions
            .iter()
            .map(|hook| hook.spec.name.clone())
            .collect()
    }

    /// Whether every requested name has a definition.
    #[doc(hidden)]
    pub fn defines_room_hooks(&self, names: &[String]) -> bool {
        names.iter().all(|name| {
            self.named
                .definitions
                .iter()
                .any(|hook| hook.spec.name == *name)
        })
    }

    /// Replace the engine's current selection, preserving name order and duplicates.
    #[doc(hidden)]
    pub fn use_room_hooks(&mut self, room: &RoomId, names: Vec<String>) {
        if names.is_empty() {
            self.named.selected.remove(room);
        } else {
            self.named.selected.insert(room.clone(), names);
        }
    }

    /// The selected names that are defined now, as the executor engine lists them.
    #[doc(hidden)]
    pub fn selected_room_hooks(&self, room: &RoomId) -> Vec<String> {
        self.named
            .selected
            .get(room)
            .into_iter()
            .flatten()
            .filter(|name| {
                self.named
                    .definitions
                    .iter()
                    .any(|hook| hook.spec.name == **name)
            })
            .cloned()
            .collect()
    }

    /// Stop selecting a defined name, independently of the owner's next requests.
    #[doc(hidden)]
    pub fn unuse_room_hook(&mut self, room: &RoomId, name: &str) -> bool {
        if !self.selected_room_hooks(room).iter().any(|n| n == name) {
            return false;
        }
        if let Some(names) = self.named.selected.get_mut(room) {
            names.retain(|n| n != name);
        }
        true
    }

    /// The owner's names for the room's next turn, including undefined names.
    #[doc(hidden)]
    pub fn enabled_room_hooks(&self, room: &RoomId) -> Vec<String> {
        self.named.enabled.get(room).cloned().unwrap_or_default()
    }

    /// Enable a name once, applying the owner's whole selection to its executor.
    #[doc(hidden)]
    pub fn enable_room_hook(&mut self, room: &RoomId, name: &str) -> Result<(), RegistrationError> {
        if self.owner_room_hook_names(room).iter().any(|n| n == name) {
            return Err(RegistrationError::DuplicateName {
                name: name.to_owned(),
            });
        }
        let names = self.named.enabled.entry(room.clone()).or_default();
        if !names.iter().any(|n| n == name) {
            names.push(name.to_owned());
        }
        let selected = names.clone();
        self.use_room_hooks(room, selected);
        Ok(())
    }

    /// Disable a name on the owner and its executor.
    #[doc(hidden)]
    pub fn disable_room_hook(&mut self, room: &RoomId, name: &str) -> bool {
        let Some(names) = self.named.enabled.get_mut(room) else {
            return false;
        };
        let Some(i) = names.iter().position(|n| n == name) else {
            return false;
        };
        names.remove(i);
        let selected = names.clone();
        self.use_room_hooks(room, selected);
        true
    }

    /// Mark a registration given directly to the executor, not through the kit.
    #[doc(hidden)]
    pub fn executor_registration(&mut self, id: HookId) {
        self.named.direct.insert(id);
    }

    /// Register directly on the executor, whose names exclude owner triggers.
    #[doc(hidden)]
    pub fn register_executor_hook(&mut self, spec: HookSpec) -> Result<HookId, RegistrationError> {
        self.add_from(None, spec, true)
    }

    /// Give the executor a room hook independently of named activation.
    #[doc(hidden)]
    pub fn add_owner_room_hook(
        &mut self,
        room: RoomId,
        spec: HookSpec,
    ) -> Result<HookId, RegistrationError> {
        self.add_from(Some(room), spec, true)
    }

    pub(super) fn registration_name_taken(
        &self,
        room: Option<&RoomId>,
        name: &str,
        trigger: HookTrigger,
        executor: bool,
    ) -> bool {
        let enabled =
            !executor && room.is_some_and(|r| self.enabled_room_hooks(r).iter().any(|n| n == name));
        enabled
            || self.in_scope(room).any(|hook| {
                hook.spec.name == name
                    && if executor {
                        hook.spec.trigger.is_turn_trigger()
                    } else {
                        !self.named.direct.contains(&hook.id)
                            || (trigger.is_turn_trigger() && hook.spec.trigger.is_turn_trigger())
                    }
            })
    }

    /// Remove a registration only from the engine's own scope.
    #[doc(hidden)]
    pub fn remove_engine_hook(
        &mut self,
        room: Option<&RoomId>,
        name: &str,
        executor: bool,
    ) -> Option<Removed> {
        let position = self.hooks.iter().position(|hook| {
            hook.room.as_ref() == room
                && hook.spec.name == name
                && if executor {
                    hook.spec.trigger.is_turn_trigger()
                } else {
                    !self.named.direct.contains(&hook.id)
                }
        })?;
        let hook = self.hooks.remove(position);
        self.named.direct.remove(&hook.id);
        Some(Removed::of(hook))
    }

    /// The room hooks the owner itself gave, excluding executor-owned registrations.
    #[doc(hidden)]
    pub fn owner_room_hook_names(&self, room: &RoomId) -> Vec<String> {
        self.in_scope(Some(room))
            .filter(|h| !self.named.direct.contains(&h.id))
            .map(|h| h.spec.name.clone())
            .collect()
    }
}
