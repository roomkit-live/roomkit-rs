//! Named hooks on the kit's executor, and the owner's per-room activation.
use std::borrow::Cow;
use std::collections::BTreeSet;

use roomkit_core::model::RoomId;
use roomkit_core::rules::hook_registry::HookRegistry;

use super::{HookEngine, RoomKit, read};
use crate::{ConversationStore, HookRegistration, RoomKitError};

/// Options for enabling a named hook on one room (RFC §17.2).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EnableRoomHookOptions {
    /// Python `organization_id=None`: no scope check when absent.
    pub organization_id: Option<String>,
}

/// Options for disabling a named hook on one room (RFC §17.2).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DisableRoomHookOptions {
    /// Python `organization_id=None`: no scope check when absent.
    pub organization_id: Option<String>,
}

impl<S: ConversationStore> RoomKit<S> {
    /// Enables a named executor hook for the room's subsequent turns (RFC §10.6.6).
    /// Repeated activation preserves its place and does not duplicate it.
    ///
    /// # Errors
    /// Refuses a closed kit, a foreign organization, or a name the kit's room hooks hold.
    pub async fn enable_room_hook(
        &self,
        room_id: &RoomId,
        name: &str,
        options: EnableRoomHookOptions,
    ) -> Result<(), RoomKitError> {
        self.inner.open()?;
        self.reached(room_id, options.organization_id.as_deref())
            .await?;
        self.inner
            .try_change_config(|config| config.hooks.enable_room_hook(room_id, name))?;
        Ok(())
    }

    /// Disables a named hook for subsequent turns, returning whether it was enabled.
    /// A turn already taken keeps the names of its request (RFC §10.6.6).
    ///
    /// # Errors
    /// Refuses a closed kit or a room outside the supplied organization.
    pub async fn disable_room_hook(
        &self,
        room_id: &RoomId,
        name: &str,
        options: DisableRoomHookOptions,
    ) -> Result<bool, RoomKitError> {
        self.inner.open()?;
        self.reached(room_id, options.organization_id.as_deref())
            .await?;
        let Ok(removed) = self.inner.try_change_config(|config| {
            Ok::<_, std::convert::Infallible>(config.hooks.disable_room_hook(room_id, name))
        });
        Ok(removed)
    }
}

impl<S: ConversationStore> HookEngine<S> {
    /// Defines a room hook under its name on a turn executor (RFC §10.6.6).
    /// Redefining that name on the same trigger replaces only that trigger.
    ///
    /// # Errors
    /// Refuses the owner's engine, an empty name, an owner trigger or an invalid registration.
    pub fn define_room_hook(&self, hook: HookRegistration) -> Result<(), RoomKitError> {
        self.named_side(&hook)?;
        if hook.name.is_empty() {
            return Err(RoomKitError::Value(
                "A room hook defined on a turn executor needs a name".to_owned(),
            ));
        }
        self.given(hook, HookRegistry::define_room_hook).map(drop)
    }

    /// Removes every definition under `name`, returning whether it existed.
    pub fn undefine_room_hook(&self, name: &str) -> bool {
        if !self.turns {
            return false;
        }
        let inner = &self.kit().inner;
        let Ok(removed) = inner.try_change_config(|config| {
            Ok::<_, std::convert::Infallible>(config.hooks.undefine_room_hook(name))
        });
        let found = !removed.is_empty();
        for hook in removed {
            inner.retire(Some(hook));
        }
        found
    }

    /// The names defined on this engine (RFC §10.6.6).
    pub fn defined_room_hooks(&self) -> BTreeSet<String> {
        if !self.turns {
            return BTreeSet::new();
        }
        read(&self.kit().inner.config).hooks.defined_room_hooks()
    }

    /// Whether every requested name has a definition on this engine.
    pub fn defines_room_hooks(&self, names: &[String]) -> bool {
        names
            .iter()
            .all(|name| self.defined_room_hooks().contains(name))
    }

    /// Replaces the executor engine's current selection for `room_id`.
    /// Request names captured by a running turn take precedence (RFC §10.6.6).
    pub fn use_room_hooks(&self, room_id: &RoomId, names: Vec<String>) {
        if !self.turns {
            return;
        }
        let Ok(()) = self.kit().inner.try_change_config(|config| {
            config.hooks.use_room_hooks(room_id, names);
            Ok::<_, std::convert::Infallible>(())
        });
    }

    /// Gives the executor a hook for one room, as the owner does (RFC §10.6.6).
    ///
    /// # Errors
    /// Refuses the owner's engine, an owner trigger or an invalid registration.
    pub fn add_owner_room_hook(
        &self,
        room_id: &RoomId,
        hook: HookRegistration,
    ) -> Result<HookRegistration, RoomKitError> {
        self.named_side(&hook)?;
        self.given(hook, |registry, spec| {
            let id = registry.add_owner_room_hook(room_id.clone(), spec)?;
            Ok((id, None))
        })
    }

    fn named_side(&self, hook: &HookRegistration) -> Result<(), RoomKitError> {
        if !self.turns {
            return Err(RoomKitError::Value(
                "Room hooks are defined by name on a turn executor only".to_owned(),
            ));
        }
        if !hook.trigger.is_turn_trigger() {
            return Err(RoomKitError::Value(format!(
                "Hook {:?}: this trigger fires on the room owner; register it with kit.add_room_hook(room_id, ...)",
                hook.name
            )));
        }
        Ok(())
    }
}

/// Read current definitions under the names carried by the current turn.
/// Outside a turn, the engine's current room selection applies.
pub(super) fn registry_for_turn<'a>(
    registry: &'a HookRegistry,
    room: &RoomId,
) -> Cow<'a, HookRegistry> {
    let Some(host) = crate::turn::turn_in_scope().filter(|host| host.room_id() == room) else {
        return Cow::Borrowed(registry);
    };
    let mut copy = registry.clone();
    copy.use_room_hooks(room, host.room_hook_names().to_vec());
    Cow::Owned(copy)
}
