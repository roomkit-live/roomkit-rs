//! A room's hooks end with the room (RFC §9.1, §17.2).
use std::collections::BTreeSet;

use super::{HookRegistry, Removed};
use crate::model::RoomId;

impl HookRegistry {
    /// Forgets what the room `room` holds under a name not in `keep`: its
    /// hooks, those the kit gave its executor included, and the names it
    /// enables, its executor's selection following. The kit creating a
    /// room under an id an earlier room left them under keeps only those
    /// registered while no room held the id: Python's
    /// `RoomHookLifetime.forget_earlier_room`. Returns the registrations
    /// taken out, whose handlers an operation that read the registry before
    /// may still run.
    #[doc(hidden)]
    pub fn forget_room(&mut self, room: &RoomId, keep: &BTreeSet<String>) -> Vec<Removed> {
        let (forgotten, kept) = self
            .hooks
            .drain(..)
            .partition(|hook| hook.room.as_ref() == Some(room) && !keep.contains(&hook.spec.name));
        self.hooks = kept;
        let removed: Vec<Removed> = forgotten
            .into_iter()
            .map(|hook| {
                self.named.direct.remove(&hook.id);
                Removed::of(hook)
            })
            .collect();
        self.named.forget_enabled(room, keep);
        removed
    }

    /// Whether the room `room` holds a hook, or enables a name, that is not
    /// in `keep`: what [`forget_room`](Self::forget_room) would forget.
    #[doc(hidden)]
    #[must_use]
    pub fn holds_room_hooks_beyond(&self, room: &RoomId, keep: &BTreeSet<String>) -> bool {
        self.in_scope(Some(room))
            .any(|hook| !keep.contains(&hook.spec.name))
            || self
                .enabled_room_hooks(room)
                .iter()
                .any(|name| !keep.contains(name))
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::rules::hook_registry::{HookExecution, HookSpec, HookTrigger};

    fn room(id: &str) -> RoomId {
        RoomId::new(id).unwrap()
    }

    fn hook(name: &str) -> HookSpec {
        HookSpec::new(name, HookTrigger::AfterBroadcast, HookExecution::Async)
    }

    #[test]
    fn a_room_forgets_what_it_holds_but_the_names_kept() {
        let mut registry = HookRegistry::new();
        let (r1, r2) = (room("r1"), room("r2"));
        for name in ["old", "waiting"] {
            registry.add_room_hook(r1.clone(), hook(name)).unwrap();
        }
        registry.add_room_hook(r2.clone(), hook("old")).unwrap();
        for name in ["tools", "kept"] {
            registry.enable_room_hook(&r1, name).unwrap();
        }
        let keep = BTreeSet::from(["waiting".to_owned(), "kept".to_owned()]);
        assert!(registry.holds_room_hooks_beyond(&r1, &keep));

        let removed = registry.forget_room(&r1, &keep);

        assert_eq!(
            removed
                .iter()
                .map(|r| r.spec.name.as_str())
                .collect::<Vec<_>>(),
            ["old"]
        );
        assert_eq!(registry.room_hook_names(&r1), ["waiting"]);
        assert_eq!(registry.room_hook_names(&r2), ["old"]);
        assert_eq!(registry.enabled_room_hooks(&r1), ["kept"]);
        assert!(!registry.holds_room_hooks_beyond(&r1, &keep));
    }
}
