//! The kit's hook engine (RFC §9.1, §10.6.6): Python's `HookEngine`, as
//! `RoomKit.hook_engine` and `RoomKit.hooks_for(trigger)` give it, its
//! registrations and its reads. The chains themselves run in the core's
//! operations.

use roomkit_core::model::RoomId;
use roomkit_core::rules::hook_registry::{
    HookId, HookRegistry, HookSpec, HookTrigger, RegistrationError, Removed,
};

use super::{Inner, RoomKit, read, write};
use crate::error::RoomKitError;
use crate::hooks::{HookRegistration, Kept};
use crate::store::{ConversationStore, InMemoryStore};

/// The kit's hooks, the kit's own and each room's (RFC §9.1): Python's
/// `HookEngine`. The kit holds the hooks of every trigger in one registry,
/// a turn's included, running its turns on its own executor (RFC §10.6.6);
/// an engine reads it from one side of the turn backend:
///
/// - the kit's, [`RoomKit::hook_engine`], takes and reads every hook;
/// - its executor's, [`RoomKit::hooks_for`] a trigger a turn fires, takes
///   and reads the hooks of those triggers alone, refuses another trigger's
///   and any room's hook, and lists no room hook, as Python's turn
///   executor's engine does.
///
/// Its calls read no room and are taken during the kit's close and after
/// it, as Python's engine takes them. Cloning it is cheap. It holds the
/// kit: a hook that registers hooks keeps a
/// [`WeakRoomKit`](crate::WeakRoomKit) and takes the engine from it, or the
/// kit and its hooks hold each other (decision 18).
pub struct HookEngine<S = InMemoryStore> {
    kit: RoomKit<S>,
    /// Whether it is the turn executor's side: the triggers a turn fires.
    turns: bool,
}

impl<S> Clone for HookEngine<S> {
    fn clone(&self) -> Self {
        Self {
            kit: self.kit.clone(),
            turns: self.turns,
        }
    }
}

impl<S> std::fmt::Debug for HookEngine<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HookEngine")
            .field("turns", &self.turns)
            .finish_non_exhaustive()
    }
}

/// What [`HookEngine::has_hooks`] takes: Python's `has_hooks(trigger=None)`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HasHooksOptions {
    /// Python `trigger=None`: the trigger asked about; `None` asks about
    /// every trigger.
    pub trigger: Option<HookTrigger>,
}

impl<S: ConversationStore> RoomKit<S> {
    /// The kit's hook engine (RFC §10.6.6): Python's `RoomKit.hook_engine`.
    /// A hook on a trigger a turn fires registered here is the kit's own
    /// executor's.
    pub fn hook_engine(&self) -> HookEngine<S> {
        HookEngine {
            kit: self.clone(),
            turns: false,
        }
    }

    /// The engine `trigger`'s hooks register on (RFC §10.6.6), for code
    /// that registers hooks on triggers of both sides: Python's
    /// `RoomKit.hooks_for(trigger)`, the kit's turn executor's for a trigger
    /// a turn fires (`BEFORE_TOOL_USE`, `ON_TOOL_CALL`), the kit's own
    /// otherwise. It answers during the kit's close and after it.
    pub fn hooks_for(&self, trigger: HookTrigger) -> HookEngine<S> {
        HookEngine {
            kit: self.clone(),
            turns: trigger.is_turn_trigger(),
        }
    }
}

impl<S: ConversationStore> HookEngine<S> {
    /// Registers `hook` as a hook of every room (RFC §9.1) and returns the
    /// registration held, under the name it was given or made unique among
    /// the kit's hooks: Python's `HookEngine.register(hook)`.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::Value`], Python's `ValueError`, when the
    /// registration breaks a rule of RFC §9.1, its handler takes another
    /// payload than its trigger's, or the turn executor's engine is given a
    /// trigger the room owner fires (RFC §10.6.6).
    pub fn register(&self, hook: HookRegistration) -> Result<HookRegistration, RoomKitError> {
        if !self.sees(hook.trigger) {
            return Err(RoomKitError::Value(format!(
                "Hook '{}': {} fires on the room owner (RFC §10.6.6); register it with \
                 kit.hook(...)",
                hook.name,
                trigger_name(hook.trigger)
            )));
        }
        self.given(hook, |registry, spec| {
            if spec.trigger.is_tool_trigger() {
                registry.register_tool_hook(spec)
            } else {
                registry.register(spec)
            }
            .map(|id| (id, None))
        })
    }

    /// Registers `hook` for the room `room_id` alone (RFC §9.1) and returns
    /// the registration held, under the name it was given or made unique
    /// among the room's hooks: Python's `HookEngine.add_room_hook(room_id,
    /// hook)`. It reads no room: a room id no room holds yet takes the
    /// hook, which waits for it. [`RoomKit::add_room_hook`] reads the room
    /// with the caller's organization first (RFC §17.2).
    ///
    /// # Errors
    ///
    /// As [`register`](Self::register); the turn executor's engine refuses
    /// every room hook (RFC §10.6.6).
    pub fn add_room_hook(
        &self,
        room_id: &RoomId,
        hook: HookRegistration,
    ) -> Result<HookRegistration, RoomKitError> {
        self.takes_room_hooks(&hook)?;
        self.given(hook, |registry, spec| {
            registry
                .add_room_hook(room_id.clone(), spec)
                .map(|id| (id, None))
        })
    }

    /// Registers `hook` for the room `room_id` in place of the room's hook
    /// of the same name, and returns the registration held: Python's
    /// `HookEngine.replace_room_hook(room_id, hook)`, a strategy installed
    /// again in its room replacing what it set up there. A registration
    /// refused leaves the hook it would replace in place.
    ///
    /// # Errors
    ///
    /// As [`add_room_hook`](Self::add_room_hook).
    pub fn replace_room_hook(
        &self,
        room_id: &RoomId,
        hook: HookRegistration,
    ) -> Result<HookRegistration, RoomKitError> {
        self.takes_room_hooks(&hook)?;
        self.given(hook, |registry, spec| {
            registry.replace_room_hook(room_id.clone(), spec)
        })
    }

    /// Removes the hook of every room named `name`: whether the engine held
    /// one, Python's `HookEngine.remove_global_hook(name)`. A room's hook of
    /// that name stays.
    pub fn remove_global_hook(&self, name: &str) -> bool {
        self.removed(|registry| {
            let held = registry.global_hook(name)?;
            if !self.sees(held.trigger) {
                return None;
            }
            registry.remove_global_hook(name)
        })
    }

    /// Removes the hook named `name` the room `room_id` was given: whether
    /// the engine held one, Python's `HookEngine.remove_room_hook(room_id,
    /// name)`. It reads no room; the hook of every room of that name stays.
    pub fn remove_room_hook(&self, room_id: &RoomId, name: &str) -> bool {
        self.removed(|registry| {
            let held = registry.room_hook(room_id, name)?;
            if !self.sees(held.trigger) {
                return None;
            }
            registry.remove_room_hook(room_id, name)
        })
    }

    /// Whether a conversation router's hook applies to `room_id`, the kit's
    /// or the room's own: Python's `HookEngine.has_router_hook(room_id)`.
    #[must_use]
    pub fn has_router_hook(&self, room_id: &RoomId) -> bool {
        let inner = &self.kit.inner;
        let config = read(&inner.config);
        let handlers = read(&inner.hooks);
        config
            .hooks
            .ids_applying_to(room_id)
            .any(|id| matches!(handlers.get(&id), Some(Kept::Router(_))))
    }

    /// The names of the hooks the room `room_id` was given, in their
    /// registration order: Python's `HookEngine.room_hook_names(room_id)`.
    /// The turn executor's engine lists none: the kit gave it the room's
    /// hooks, which it holds as the kit's.
    pub fn room_hook_names(&self, room_id: &RoomId) -> Vec<String> {
        if self.turns {
            return Vec::new();
        }
        read(&self.kit.inner.config).hooks.room_hook_names(room_id)
    }

    /// Whether the engine holds a hook, of every room or of one, on the
    /// trigger `options` names or on any: Python's
    /// `HookEngine.has_hooks(trigger)`.
    pub fn has_hooks(&self, options: HasHooksOptions) -> bool {
        read(&self.kit.inner.config).hooks.has_hooks_where(|spec| {
            self.sees(spec.trigger)
                && options
                    .trigger
                    .is_none_or(|trigger| spec.trigger == trigger)
        })
    }

    /// The kit whose registry the engine reads.
    pub(crate) fn kit(&self) -> &RoomKit<S> {
        &self.kit
    }

    /// Whether the engine takes and reads the hooks of `trigger`: every
    /// trigger's on the kit's side, a turn's on its executor's.
    fn sees(&self, trigger: HookTrigger) -> bool {
        !self.turns || trigger.is_turn_trigger()
    }

    /// Refuses `hook` for a room on the turn executor's engine, as Python's
    /// executor refuses it: its room hooks are defined by name (RFC
    /// §10.6.6).
    fn takes_room_hooks(&self, hook: &HookRegistration) -> Result<(), RoomKitError> {
        if !self.turns {
            return Ok(());
        }
        Err(RoomKitError::Value(format!(
            "Hook '{}': a turn executor's room hook is defined by name, \
             executor.room_hook(name, ...), and enabled for a room on its owner, \
             kit.enable_room_hook(room_id, name) (RFC §10.6.6)",
            hook.name
        )))
    }

    /// Registers `hook` through `register`, which hands back the id the
    /// handler is kept under and the id of the hook it replaced, then gives
    /// the registration held.
    fn given(
        &self,
        hook: HookRegistration,
        register: impl FnOnce(
            &mut HookRegistry,
            HookSpec,
        ) -> Result<(HookId, Option<Removed>), RegistrationError>,
    ) -> Result<HookRegistration, RoomKitError> {
        let spec = hook.spec();
        if !hook.takes_its_payload() {
            return Err(RegistrationError::PayloadMismatch {
                hook: spec.name,
                trigger: spec.trigger,
            }
            .into());
        }
        let kept = hook.r#fn;
        let inner = &self.kit.inner;
        let (held, replaced) = inner.try_change_config(|config| {
            let (id, replaced) = register(&mut config.hooks, spec.clone())?;
            // The handler is in place before an operation can see the hook.
            write(&inner.hooks).insert(id, kept.clone());
            let held = config.hooks.get(id).cloned();
            Ok::<_, RoomKitError>((held, replaced))
        })?;
        inner.retire(replaced);
        Ok(HookRegistration::held(held.unwrap_or(spec), kept))
    }

    /// Removes the hook `remove` takes out of the registry; whether there
    /// was one. Its handler is retired: an operation that read the
    /// registry before may still run it.
    fn removed(&self, remove: impl FnOnce(&mut HookRegistry) -> Option<Removed>) -> bool {
        let inner = &self.kit.inner;
        let Ok(removed) = inner.try_change_config(|config| {
            Ok::<_, std::convert::Infallible>(remove(&mut config.hooks))
        });
        let was_held = removed.is_some();
        inner.retire(removed);
        was_held
    }
}

impl<S> Inner<S> {
    /// Keeps the handler of `removed`, and its registration, for the
    /// operations that read the registry before its removal: each runs the
    /// hooks it read, as the Python implementation's chain runs the list it
    /// took. Lets go of each retired hook no copy of the registry holds any
    /// more, which no operation can ask for.
    fn retire(&self, removed: Option<Removed>) {
        let mut retired = write(&self.retired_hooks);
        retired.extend(removed);
        let mut handlers = write(&self.hooks);
        retired.retain(|hook| {
            let held = hook.held.strong_count() > 0;
            if !held {
                handlers.remove(&hook.id);
            }
            held
        });
    }

    /// The registration of the retired hook `hook`.
    pub(super) fn retired_spec(&self, hook: HookId) -> Option<HookSpec> {
        read(&self.retired_hooks)
            .iter()
            .find(|retired| retired.id == hook)
            .map(|retired| retired.spec.clone())
    }
}

/// `trigger` as Python prints it, its serialized name.
fn trigger_name(trigger: HookTrigger) -> String {
    serde_json::to_value(trigger)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_default()
}
