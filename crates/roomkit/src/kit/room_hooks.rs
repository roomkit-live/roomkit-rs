//! Hooks added for one room (RFC §9.1): a registration's scope is the kit
//! or one room, whose hooks fire for its payloads alone, beside the kit's.

use std::future::Future;
use std::sync::Arc;

use roomkit_core::model::{RoomContext, RoomId};
use roomkit_core::rules::hook_registry::{HookExecution, HookSpec, HookTrigger, RegistrationError};

use super::{RoomKit, write};
use crate::error::RoomKitError;
use crate::hooks::{Closure, HookHandler, HookPayload, HookRegistration, IntoHookReturn};
use crate::store::ConversationStore;

/// What [`RoomKit::add_room_hook`] takes beside the room, the trigger, the
/// execution and the handler: Python's `add_room_hook(room_id, trigger,
/// execution, fn, priority=0, name="", *, fail_closed=False,
/// needs_lock=True, organization_id=None)`. A room's hook declares no
/// filter and runs within the default timeout, 30 seconds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AddRoomHookOptions {
    /// Python `priority=0`: its place in its chain, lower first; at equal
    /// priority the kit's hooks run before the room's.
    pub priority: i32,
    /// Python `name=""`: unique among the room's hooks, and may be one of
    /// the kit's; a hook given none takes `hook`, then `hook#2`, ...
    pub name: String,
    /// Python `fail_closed=False`: a SYNC hook whose outcome is unusable
    /// blocks instead of allowing (RFC §9.3).
    pub fail_closed: bool,
    /// Python `needs_lock=True`: `false` runs a SYNC `BEFORE_BROADCAST`
    /// hook off the room lock (RFC §9.5.1).
    pub needs_lock: bool,
    /// Python `organization_id=None`: the organization the call acts for
    /// (RFC §17.2). The room is read with it first, and a room of another
    /// organization is not found, no hook registered; `None` reads no room,
    /// and a room id no room holds yet takes the hook, which waits for it.
    pub organization_id: Option<String>,
}

impl Default for AddRoomHookOptions {
    fn default() -> Self {
        let defaults = HookSpec::new("", HookTrigger::BeforeBroadcast, HookExecution::Sync);
        Self {
            priority: defaults.priority,
            name: String::new(),
            fail_closed: defaults.fail_closed,
            needs_lock: defaults.needs_lock,
            organization_id: None,
        }
    }
}

/// What [`RoomKit::remove_room_hook`] takes beside the room and the name:
/// Python's `remove_room_hook(room_id, name, *, organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RemoveRoomHookOptions {
    /// Python `organization_id=None`: the organization the call acts for
    /// (RFC §17.2). A room of another organization is not found, no hook
    /// removed; `None` reads no room.
    pub organization_id: Option<String>,
}

impl<S: ConversationStore> RoomKit<S> {
    /// Adds a hook on `trigger` for the room `room_id` alone (RFC §9.1),
    /// run as `execution`, and returns the registration the kit holds,
    /// under the name it was given or made unique: Python's
    /// `add_room_hook(room_id, trigger, execution, fn, ...)`, its keywords
    /// in `options`. The closure takes the trigger's payload, as
    /// [`hook`](Self::hook)'s does. A hook on `BEFORE_TOOL_USE` or
    /// `ON_TOOL_CALL` runs on the calls of the room's turns.
    ///
    /// The room's hooks join the kit's in each chain of the room, by
    /// priority, the kit's first at equal priority. A name is unique among
    /// the room's hooks; it may be one of the kit's.
    ///
    /// ```
    /// # async fn add(kit: &roomkit::RoomKit) -> Result<(), roomkit::RoomKitError> {
    /// use roomkit::model::hook::BlockOptions;
    /// use roomkit::model::{HookResult, RoomContext, RoomEvent, RoomId};
    /// use roomkit::{AddRoomHookOptions, HookExecution, HookTrigger};
    ///
    /// let room = RoomId::new("vip-room")?;
    /// let options = AddRoomHookOptions { name: "hold".to_owned(), ..Default::default() };
    /// let held = kit
    ///     .add_room_hook(
    ///         &room,
    ///         HookTrigger::BeforeBroadcast,
    ///         HookExecution::Sync,
    ///         |_event: RoomEvent, _context: RoomContext| async {
    ///             HookResult::block("on hold", BlockOptions::default())
    ///         },
    ///         options,
    ///     )
    ///     .await?;
    /// assert_eq!(held.name, "hold");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::KitClosed`] once the kit's close started (RFC
    /// §12.10.4); [`RoomKitError::RoomNotFound`] when `options` names an
    /// organization the room is not of, nothing registered; and
    /// [`RoomKitError::Value`], Python's `ValueError`, when the
    /// registration breaks a rule of RFC §9.1 or its handler takes another
    /// payload than its trigger's.
    pub async fn add_room_hook<P, F, Fut, R>(
        &self,
        room_id: &RoomId,
        trigger: HookTrigger,
        execution: HookExecution,
        r#fn: F,
        options: AddRoomHookOptions,
    ) -> Result<HookRegistration, RoomKitError>
    where
        P: HookPayload,
        F: Fn(P, RoomContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoHookReturn<P>,
    {
        self.inner.open()?;
        self.reached(room_id, options.organization_id.as_deref())
            .await?;
        let mut spec = HookSpec::new(options.name, trigger, execution);
        spec.priority = options.priority;
        spec.fail_closed = options.fail_closed;
        spec.needs_lock = options.needs_lock;
        if !P::accepts(trigger) {
            return Err(RegistrationError::PayloadMismatch {
                hook: spec.name,
                trigger,
            }
            .into());
        }
        let handler: Arc<dyn HookHandler<P>> = Arc::new(Closure(r#fn));
        let kept = P::kept(handler);
        let held = self.inner.try_change_config(|config| {
            let id = config.hooks.add_room_hook(room_id.clone(), spec)?;
            // The handler is in place before an operation can see the hook.
            write(&self.inner.hooks).insert(id, kept.clone());
            Ok::<Option<HookSpec>, RoomKitError>(config.hooks.get(id).cloned())
        })?;
        // The registry holds what it just registered, under the name it
        // gave it.
        let spec = held.unwrap_or_else(|| HookSpec::new("", trigger, execution));
        Ok(HookRegistration::held(spec, kept))
    }

    /// Removes the hook named `name` added for the room `room_id`: whether
    /// the room held one, Python's `remove_room_hook(room_id, name, *,
    /// organization_id=None)`. The kit's hook of that name stays (RFC
    /// §9.1); an operation that already read the room's hooks may still run
    /// it.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::KitClosed`] once the kit's close started (RFC
    /// §12.10.4), and [`RoomKitError::RoomNotFound`] when `options` names
    /// an organization the room is not of, nothing removed.
    pub async fn remove_room_hook(
        &self,
        room_id: &RoomId,
        name: &str,
        options: RemoveRoomHookOptions,
    ) -> Result<bool, RoomKitError> {
        self.inner.open()?;
        self.reached(room_id, options.organization_id.as_deref())
            .await?;
        let Ok(removed) = self.inner.try_change_config(|config| {
            Ok::<_, std::convert::Infallible>(config.hooks.remove_room_hook(room_id, name))
        });
        let Some(id) = removed else {
            return Ok(false);
        };
        write(&self.inner.hooks).remove(&id);
        Ok(true)
    }
}
