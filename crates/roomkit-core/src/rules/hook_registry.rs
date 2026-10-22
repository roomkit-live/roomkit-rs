//! Hook registrations, the checks they pass, and which hooks an event fires
//! (RFC §9.1, §9.2).
//!
//! The core holds hooks by their registration only. The handler behind a
//! registration is the driver's, found by the [`HookId`] the registry hands
//! back.

use std::collections::BTreeSet;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::model::{ChannelId, ChannelType, EventDirection, EventSource};

/// When a hook fires (RFC §9.2).
///
/// The triggers the implemented steps fire, and the three that fail closed
/// (RFC §9.3), which the voice and AI steps fire. The RFC's list grows here
/// step by step (gap G-003).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookTrigger {
    /// Before an event reaches the room's channels; can block or modify it.
    BeforeBroadcast,
    /// After every channel has processed an event.
    AfterBroadcast,
    /// A room was created.
    OnRoomCreated,
    /// A room was closed.
    OnRoomClosed,
    /// A channel was attached to a room.
    OnChannelAttached,
    /// A turn failed.
    OnError,
    /// After a speech-to-text transcription; can modify it.
    OnTranscription,
    /// Before text-to-speech synthesis; can block or modify the text.
    BeforeTts,
    /// Before a tool executes; can block or override the call.
    BeforeToolUse,
}

impl HookTrigger {
    /// How the framework runs the trigger (RFC §9.2): its SYNC hooks as a
    /// chain, or every hook as a concurrent observer.
    pub fn execution(self) -> HookExecution {
        match self {
            Self::BeforeBroadcast
            | Self::OnTranscription
            | Self::BeforeTts
            | Self::BeforeToolUse => HookExecution::Sync,
            Self::AfterBroadcast
            | Self::OnRoomCreated
            | Self::OnRoomClosed
            | Self::OnChannelAttached
            | Self::OnError => HookExecution::Async,
        }
    }

    /// Whether an unusable outcome of any of its hooks blocks rather than
    /// allows (RFC §9.3).
    pub fn fails_closed(self) -> bool {
        match self {
            Self::OnTranscription | Self::BeforeTts | Self::BeforeToolUse => true,
            Self::BeforeBroadcast
            | Self::AfterBroadcast
            | Self::OnRoomCreated
            | Self::OnRoomClosed
            | Self::OnChannelAttached
            | Self::OnError => false,
        }
    }
}

/// How a hook runs (RFC §9.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookExecution {
    /// In priority order, able to block or modify.
    Sync,
    /// Concurrently, as an observer; its result is discarded.
    Async,
}

/// Identifies a registered hook; the driver keeps the hook's handler under
/// it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HookId(usize);

/// Which events a hook fires for (RFC §9.1). An absent filter lets every
/// event through.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HookFilter {
    /// Only events from these channel types.
    pub channel_types: Option<BTreeSet<ChannelType>>,
    /// Only events from these channels.
    pub channel_ids: Option<BTreeSet<ChannelId>>,
    /// Only events that travelled in these directions (gap G-013).
    pub directions: Option<BTreeSet<EventDirection>>,
}

impl HookFilter {
    /// Whether an event from `source` passes the filter.
    pub fn admits(&self, source: &EventSource) -> bool {
        self.channel_types
            .as_ref()
            .is_none_or(|types| types.contains(&source.channel_type))
            && self
                .channel_ids
                .as_ref()
                .is_none_or(|ids| ids.contains(&source.channel_id))
            && self
                .directions
                .as_ref()
                .is_none_or(|directions| directions.contains(&source.direction))
    }
}

/// A hook's registration, without its handler (RFC §9.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HookSpec {
    /// The hook's name, unique in its registry; a block names it (gap
    /// G-018).
    pub name: String,
    /// When it fires.
    pub trigger: HookTrigger,
    /// How it runs.
    pub execution: HookExecution,
    /// Its place in the chain: lower first; equal priorities keep their
    /// registration order (gap G-018).
    pub priority: i32,
    /// The longest it may run; the driver enforces it.
    pub timeout: Duration,
    /// Which events it fires for.
    pub filter: HookFilter,
    /// SYNC only: an unusable outcome blocks instead of allowing (RFC §9.3).
    pub fail_closed: bool,
    /// SYNC `BEFORE_BROADCAST` only: `false` runs the hook off the room lock
    /// (RFC §9.5.1).
    pub needs_lock: bool,
}

/// A hook's timeout when its registration names none (RFC §9.1).
pub const DEFAULT_HOOK_TIMEOUT: Duration = Duration::from_secs(30);

impl HookSpec {
    /// A hook named `name` on `trigger`, run as `execution`, with the RFC
    /// defaults: priority 0, a 30-second timeout, no filter, fail open, under
    /// the room lock.
    pub fn new(name: impl Into<String>, trigger: HookTrigger, execution: HookExecution) -> Self {
        Self {
            name: name.into(),
            trigger,
            execution,
            priority: 0,
            timeout: DEFAULT_HOOK_TIMEOUT,
            filter: HookFilter::default(),
            fail_closed: false,
            needs_lock: true,
        }
    }
}

/// Why a registration was refused (RFC §9.1).
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum RegistrationError {
    /// Another hook already has the name.
    #[error("a hook named {name:?} is already registered")]
    DuplicateName {
        /// The name.
        name: String,
    },
    /// `needs_lock = false` outside a SYNC `BEFORE_BROADCAST` hook.
    #[error("hook {hook:?}: only a SYNC BEFORE_BROADCAST hook may run off the room lock")]
    OffLockOutsideBeforeBroadcast {
        /// The refused hook.
        hook: String,
    },
    /// `fail_closed = true` on an ASYNC hook, which cannot block.
    #[error("hook {hook:?}: an ASYNC hook cannot fail closed")]
    FailClosedAsync {
        /// The refused hook.
        hook: String,
    },
    /// A hook that needs the lock would run before one that does not.
    #[error(
        "hook {locked:?} needs the room lock but is ordered before {off_lock:?}, which runs off it"
    )]
    LockOrdering {
        /// The hook that needs the lock.
        locked: String,
        /// The off-lock hook it would precede.
        off_lock: String,
    },
}

/// The hooks of a kit (RFC §9.1).
#[derive(Clone, Debug, Default)]
pub struct HookRegistry {
    hooks: Vec<(HookId, HookSpec)>,
}

impl HookRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `spec`, returning the id the driver keeps its handler
    /// under.
    ///
    /// # Errors
    ///
    /// Returns [`RegistrationError`] when the registration breaks a rule of
    /// RFC §9.1; nothing is registered then.
    pub fn register(&mut self, spec: HookSpec) -> Result<HookId, RegistrationError> {
        check_alone(&spec)?;
        if self.hooks.iter().any(|(_, hook)| hook.name == spec.name) {
            return Err(RegistrationError::DuplicateName { name: spec.name });
        }
        let id = HookId(self.hooks.len());
        self.hooks.push((id, spec));
        if let Err(error) = self.check_lock_ordering() {
            self.hooks.pop();
            return Err(error);
        }
        Ok(id)
    }

    /// The registration named `name`.
    pub fn find(&self, name: &str) -> Option<&HookSpec> {
        self.hooks
            .iter()
            .find(|(_, spec)| spec.name == name)
            .map(|(_, spec)| spec)
    }

    /// The registration of `id`.
    pub fn get(&self, id: HookId) -> Option<&HookSpec> {
        self.hooks
            .iter()
            .find(|(hook_id, _)| *hook_id == id)
            .map(|(_, spec)| spec)
    }

    /// The SYNC hooks `trigger` runs as a chain for an event from `source`,
    /// in chain order. An ASYNC trigger runs no chain.
    pub fn chain(&self, trigger: HookTrigger, source: &EventSource) -> Vec<HookId> {
        if trigger.execution() == HookExecution::Async {
            return Vec::new();
        }
        let mut chain: Vec<(&HookId, &HookSpec)> = self
            .matching(trigger, source)
            .filter(|(_, spec)| spec.execution == HookExecution::Sync)
            .collect();
        chain.sort_by_key(|(id, spec)| (spec.priority, **id));
        chain.into_iter().map(|(id, _)| *id).collect()
    }

    /// The hooks `trigger` runs as observers for an event from `source`:
    /// the ASYNC hooks of a SYNC trigger, every hook of an ASYNC trigger
    /// (RFC §9.2).
    pub fn observers(&self, trigger: HookTrigger, source: &EventSource) -> Vec<HookId> {
        let observes_all = trigger.execution() == HookExecution::Async;
        self.matching(trigger, source)
            .filter(|(_, spec)| observes_all || spec.execution == HookExecution::Async)
            .map(|(id, _)| *id)
            .collect()
    }

    fn matching(
        &self,
        trigger: HookTrigger,
        source: &EventSource,
    ) -> impl Iterator<Item = (&HookId, &HookSpec)> {
        self.hooks
            .iter()
            .filter(move |(_, spec)| spec.trigger == trigger && spec.filter.admits(source))
            .map(|(id, spec)| (id, spec))
    }

    /// No SYNC `BEFORE_BROADCAST` hook that needs the lock may be ordered
    /// before one that does not (RFC §9.1).
    fn check_lock_ordering(&self) -> Result<(), RegistrationError> {
        let mut chain: Vec<&(HookId, HookSpec)> = self
            .hooks
            .iter()
            .filter(|(_, spec)| {
                spec.trigger == HookTrigger::BeforeBroadcast
                    && spec.execution == HookExecution::Sync
            })
            .collect();
        chain.sort_by_key(|(id, spec)| (spec.priority, *id));
        let mut first_locked: Option<&HookSpec> = None;
        for (_, spec) in chain {
            match (spec.needs_lock, first_locked) {
                (true, None) => first_locked = Some(spec),
                (false, Some(locked)) => {
                    return Err(RegistrationError::LockOrdering {
                        locked: locked.name.clone(),
                        off_lock: spec.name.clone(),
                    });
                }
                (true, Some(_)) | (false, None) => {}
            }
        }
        Ok(())
    }
}

/// The rules a registration obeys on its own (RFC §9.1).
fn check_alone(spec: &HookSpec) -> Result<(), RegistrationError> {
    let sync_before_broadcast =
        spec.trigger == HookTrigger::BeforeBroadcast && spec.execution == HookExecution::Sync;
    if !spec.needs_lock && !sync_before_broadcast {
        return Err(RegistrationError::OffLockOutsideBeforeBroadcast {
            hook: spec.name.clone(),
        });
    }
    if spec.fail_closed && spec.execution == HookExecution::Async {
        return Err(RegistrationError::FailClosedAsync {
            hook: spec.name.clone(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn sms_source() -> EventSource {
        EventSource::new(
            ChannelId::new("sms").unwrap(),
            ChannelType::Sms,
            EventDirection::Inbound,
        )
    }

    fn sync_hook(name: &str, priority: i32) -> HookSpec {
        HookSpec {
            priority,
            ..HookSpec::new(name, HookTrigger::BeforeBroadcast, HookExecution::Sync)
        }
    }

    fn off_lock_hook(name: &str, priority: i32) -> HookSpec {
        HookSpec {
            needs_lock: false,
            ..sync_hook(name, priority)
        }
    }

    fn names(registry: &HookRegistry, ids: &[HookId]) -> Vec<String> {
        ids.iter()
            .map(|id| registry.get(*id).unwrap().name.clone())
            .collect()
    }

    #[test]
    fn the_chain_runs_in_priority_order() {
        let mut registry = HookRegistry::new();
        registry.register(sync_hook("audit", 10)).unwrap();
        registry.register(sync_hook("pii_scan", 0)).unwrap();
        let chain = registry.chain(HookTrigger::BeforeBroadcast, &sms_source());
        assert_eq!(names(&registry, &chain), ["pii_scan", "audit"]);
    }

    #[test]
    fn equal_priorities_keep_their_registration_order() {
        let mut registry = HookRegistry::new();
        registry.register(sync_hook("first", 0)).unwrap();
        registry.register(sync_hook("second", 0)).unwrap();
        let chain = registry.chain(HookTrigger::BeforeBroadcast, &sms_source());
        assert_eq!(names(&registry, &chain), ["first", "second"]);
    }

    #[test]
    fn the_chain_leaves_out_async_hooks() {
        let mut registry = HookRegistry::new();
        registry
            .register(HookSpec::new(
                "logger",
                HookTrigger::BeforeBroadcast,
                HookExecution::Async,
            ))
            .unwrap();
        assert_eq!(
            registry.chain(HookTrigger::BeforeBroadcast, &sms_source()),
            Vec::<HookId>::new()
        );
    }

    #[test]
    fn an_async_hook_on_a_sync_trigger_observes() {
        let mut registry = HookRegistry::new();
        let logger = registry
            .register(HookSpec::new(
                "logger",
                HookTrigger::BeforeBroadcast,
                HookExecution::Async,
            ))
            .unwrap();
        assert_eq!(
            registry.observers(HookTrigger::BeforeBroadcast, &sms_source()),
            [logger]
        );
    }

    #[test]
    fn every_hook_of_an_async_trigger_observes_whatever_its_mode() {
        let mut registry = HookRegistry::new();
        let audit = registry
            .register(HookSpec::new(
                "audit",
                HookTrigger::AfterBroadcast,
                HookExecution::Sync,
            ))
            .unwrap();
        assert_eq!(
            registry.observers(HookTrigger::AfterBroadcast, &sms_source()),
            [audit]
        );
    }

    #[test]
    fn a_filtered_hook_does_not_fire_for_another_channel() {
        let mut registry = HookRegistry::new();
        let mut spec = sync_hook("email_only", 0);
        spec.filter.channel_ids = Some(BTreeSet::from([ChannelId::new("email").unwrap()]));
        registry.register(spec).unwrap();
        assert_eq!(
            registry.chain(HookTrigger::BeforeBroadcast, &sms_source()),
            Vec::<HookId>::new()
        );
    }

    #[test]
    fn a_hook_filtered_by_direction_fires_for_that_direction() {
        let mut registry = HookRegistry::new();
        let mut spec = sync_hook("inbound_only", 0);
        spec.filter.directions = Some(BTreeSet::from([EventDirection::Inbound]));
        let id = registry.register(spec).unwrap();
        assert_eq!(
            registry.chain(HookTrigger::BeforeBroadcast, &sms_source()),
            [id]
        );
    }

    #[test]
    fn a_duplicate_name_is_refused() {
        let mut registry = HookRegistry::new();
        registry.register(sync_hook("pii_scan", 0)).unwrap();
        assert_eq!(
            registry.register(sync_hook("pii_scan", 5)),
            Err(RegistrationError::DuplicateName {
                name: "pii_scan".to_owned()
            })
        );
    }

    #[test]
    fn running_off_the_lock_is_refused_outside_before_broadcast() {
        let spec = HookSpec {
            needs_lock: false,
            ..HookSpec::new("tts", HookTrigger::BeforeTts, HookExecution::Sync)
        };
        assert!(matches!(
            HookRegistry::new().register(spec),
            Err(RegistrationError::OffLockOutsideBeforeBroadcast { .. })
        ));
    }

    #[test]
    fn running_off_the_lock_is_refused_on_an_async_hook() {
        let spec = HookSpec {
            needs_lock: false,
            ..HookSpec::new("logger", HookTrigger::BeforeBroadcast, HookExecution::Async)
        };
        assert!(matches!(
            HookRegistry::new().register(spec),
            Err(RegistrationError::OffLockOutsideBeforeBroadcast { .. })
        ));
    }

    #[test]
    fn failing_closed_is_refused_on_an_async_hook() {
        let spec = HookSpec {
            fail_closed: true,
            ..HookSpec::new("logger", HookTrigger::AfterBroadcast, HookExecution::Async)
        };
        assert!(matches!(
            HookRegistry::new().register(spec),
            Err(RegistrationError::FailClosedAsync { .. })
        ));
    }

    #[test]
    fn an_off_lock_hook_after_a_locked_one_is_refused() {
        let mut registry = HookRegistry::new();
        registry.register(sync_hook("router", 0)).unwrap();
        assert_eq!(
            registry.register(off_lock_hook("pii_scan", 5)),
            Err(RegistrationError::LockOrdering {
                locked: "router".to_owned(),
                off_lock: "pii_scan".to_owned()
            })
        );
    }

    #[test]
    fn an_off_lock_hook_sharing_a_locked_hook_s_priority_after_it_is_refused() {
        let mut registry = HookRegistry::new();
        registry.register(sync_hook("router", 0)).unwrap();
        assert!(registry.register(off_lock_hook("pii_scan", 0)).is_err());
    }

    #[test]
    fn a_refused_registration_leaves_the_registry_unchanged() {
        let mut registry = HookRegistry::new();
        registry.register(sync_hook("router", 0)).unwrap();
        let _refused = registry.register(off_lock_hook("pii_scan", 5));
        assert_eq!(
            names(
                &registry,
                &registry.chain(HookTrigger::BeforeBroadcast, &sms_source())
            ),
            ["router"]
        );
    }

    #[test]
    fn an_off_lock_hook_before_the_locked_ones_is_accepted() {
        let mut registry = HookRegistry::new();
        registry.register(sync_hook("router", 10)).unwrap();
        assert!(registry.register(off_lock_hook("pii_scan", 0)).is_ok());
    }
}
