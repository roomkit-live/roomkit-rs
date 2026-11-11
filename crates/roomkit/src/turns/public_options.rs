//! Options of the public local executor (RFC §10.6.6).
use crate::HookExecution;
use crate::hooks::HookOptions as KitHookOptions;
use roomkit_core::model::{ChannelId, ChannelType, EventDirection, EventType};
use std::collections::BTreeSet;
use std::time::Duration;

/// Python `TurnExecutor.hook`'s optional arguments.
#[derive(Clone, Debug)]
pub struct HookOptions {
    /// Python `execution=SYNC`.
    pub execution: HookExecution,
    /// Python `priority=0`: lower runs first.
    pub priority: i32,
    /// Python `name=""`: a generated unique name when empty.
    pub name: String,
    /// Python `timeout=30.0`: the longest the hook may run.
    pub timeout: Duration,
    /// Python `channel_types=None`.
    pub channel_types: Option<BTreeSet<ChannelType>>,
    /// Python `channel_ids=None`.
    pub channel_ids: Option<BTreeSet<ChannelId>>,
    /// Python `directions=None`.
    pub directions: Option<BTreeSet<EventDirection>>,
    /// Python `event_types=None`.
    pub event_types: Option<BTreeSet<EventType>>,
    /// Python `fail_closed=False`.
    pub fail_closed: bool,
}

impl Default for HookOptions {
    fn default() -> Self {
        let defaults = KitHookOptions::default();
        Self {
            execution: defaults.execution,
            priority: defaults.priority,
            name: defaults.name,
            timeout: defaults.timeout,
            channel_types: None,
            channel_ids: None,
            directions: None,
            event_types: None,
            fail_closed: defaults.fail_closed,
        }
    }
}

impl HookOptions {
    pub(crate) fn options(self) -> KitHookOptions {
        KitHookOptions {
            execution: self.execution,
            priority: self.priority,
            name: self.name,
            timeout: self.timeout,
            channel_types: self.channel_types,
            channel_ids: self.channel_ids,
            directions: self.directions,
            event_types: self.event_types,
            fail_closed: self.fail_closed,
            needs_lock: true,
        }
    }
}

/// Python `TurnExecutor.room_hook`'s optional arguments.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RoomHookOptions {
    /// Python `execution=SYNC`.
    pub execution: HookExecution,
    /// Python `priority=0`.
    pub priority: i32,
    /// Python `timeout=30.0`.
    pub timeout: Duration,
    /// Python `fail_closed=False`.
    pub fail_closed: bool,
}

impl Default for RoomHookOptions {
    fn default() -> Self {
        let defaults = KitHookOptions::default();
        Self {
            execution: defaults.execution,
            priority: defaults.priority,
            timeout: defaults.timeout,
            fail_closed: false,
        }
    }
}

/// Python `TurnExecutor.takes(channel_id, room_hooks=(), organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TakesOptions {
    /// Names required by the room's turn.
    pub room_hooks: Vec<String>,
    /// Organization served; the kit's own executor serves every organization.
    pub organization_id: Option<String>,
}
