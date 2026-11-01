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

use crate::model::{ChannelId, ChannelType, EventDirection, EventSource, EventType, RoomEvent};
use crate::rules::chain_depth::{CHAIN_DEPTH_LIMIT, REENTRY_LOOP_CAP};
use crate::rules::permissions::WriteRefusal;

/// When a hook fires (RFC §9.2).
///
/// The triggers the implemented operations fire (the Level 0 ones of RFC
/// §26.1 among them), the three that fail closed (RFC §9.3), which the
/// voice and AI steps fire, and `ON_TOOL_CALL`. The list grows with each
/// step.
///
/// The tool triggers' payload is a tool call, no event: their hooks register
/// through [`HookRegistry::register_tool_hook`], never
/// [`HookRegistry::register`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum HookTrigger {
    /// Before an event reaches the room's channels; can block or modify it.
    BeforeBroadcast,
    /// After every channel has processed an event.
    AfterBroadcast,
    /// A stored event was changed: by an accepted edit, or by a host's
    /// direct update (RFC §10.3).
    OnEventUpdated,
    /// A stored event was deleted: marked by an accepted delete, or removed
    /// by a host's direct delete (RFC §10.3).
    OnEventDeleted,
    /// A room was created.
    OnRoomCreated,
    /// A room transitioned to PAUSED, by the integrator or its timer.
    OnRoomPaused,
    /// A room transitioned to CLOSED, by the integrator or its timer.
    OnRoomClosed,
    /// A channel was attached to a room.
    OnChannelAttached,
    /// A channel was detached from a room.
    OnChannelDetached,
    /// A channel was muted in a room (RFC §7.2).
    OnChannelMuted,
    /// A channel was unmuted in a room (RFC §7.2).
    OnChannelUnmuted,
    /// A session started: the inbound pipeline created a room for a
    /// channel other than `VOICE` and `REALTIME_VOICE` (RFC §10.1 step 2).
    OnSessionStarted,
    /// A turn failed.
    OnError,
    /// A task a hook or a channel filed was stored (RFC §10.1 steps 10, 11
    /// and 15).
    OnTaskCreated,
    /// After a speech-to-text transcription; can modify it.
    OnTranscription,
    /// Before text-to-speech synthesis; can block or modify the text.
    BeforeTts,
    /// Before a tool executes; can block or override the call.
    BeforeToolUse,
    /// A tool call's outcome, once per call: its SYNC hooks may replace or
    /// withhold a served call's result, its ASYNC hooks observe every call
    /// (RFC §9.3).
    OnToolCall,
}

impl HookTrigger {
    /// How the framework runs the trigger (RFC §9.2): its SYNC hooks as a
    /// chain, or every hook as a concurrent observer.
    #[doc(hidden)]
    pub fn execution(self) -> HookExecution {
        match self {
            Self::BeforeBroadcast
            | Self::OnTranscription
            | Self::BeforeTts
            | Self::BeforeToolUse
            | Self::OnToolCall => HookExecution::Sync,
            Self::AfterBroadcast
            | Self::OnEventUpdated
            | Self::OnEventDeleted
            | Self::OnRoomCreated
            | Self::OnRoomPaused
            | Self::OnRoomClosed
            | Self::OnChannelAttached
            | Self::OnChannelDetached
            | Self::OnChannelMuted
            | Self::OnChannelUnmuted
            | Self::OnSessionStarted
            | Self::OnError
            | Self::OnTaskCreated => HookExecution::Async,
        }
    }

    /// Whether an unusable outcome of any of its hooks blocks rather than
    /// allows (RFC §9.3).
    #[doc(hidden)]
    pub fn fails_closed(self) -> bool {
        match self {
            Self::OnTranscription | Self::BeforeTts | Self::BeforeToolUse => true,
            Self::OnToolCall
            | Self::BeforeBroadcast
            | Self::AfterBroadcast
            | Self::OnEventUpdated
            | Self::OnEventDeleted
            | Self::OnRoomCreated
            | Self::OnRoomPaused
            | Self::OnRoomClosed
            | Self::OnChannelAttached
            | Self::OnChannelDetached
            | Self::OnChannelMuted
            | Self::OnChannelUnmuted
            | Self::OnSessionStarted
            | Self::OnError
            | Self::OnTaskCreated => false,
        }
    }

    /// Whether the trigger's payload is a tool call rather than an event
    /// (RFC §9.3): `BEFORE_TOOL_USE` and `ON_TOOL_CALL`.
    #[doc(hidden)]
    pub fn is_tool_trigger(self) -> bool {
        matches!(self, Self::BeforeToolUse | Self::OnToolCall)
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

/// Which payloads a hook fires for (RFC §9.1). An absent filter lets every
/// payload through.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct HookFilter {
    /// Only payloads from these channel types.
    pub channel_types: Option<BTreeSet<ChannelType>>,
    /// Only payloads from these channels.
    pub channel_ids: Option<BTreeSet<ChannelId>>,
    /// Only events that travelled in these directions (gap G-013).
    pub directions: Option<BTreeSet<EventDirection>>,
    /// Only events of these types.
    pub event_types: Option<BTreeSet<EventType>>,
}

/// What a hook's filter reads of its payload (RFC §9.1), as the Python
/// implementation's `_hook_matches_event` reads it: an event's channel,
/// channel type, direction and type; for any other payload (a tool call, a
/// session's start) its channel and channel type alone, the direction and
/// the event type letting it through.
#[derive(Clone, Copy, Debug)]
pub struct HookSubject<'payload> {
    channel_id: &'payload ChannelId,
    channel_type: &'payload ChannelType,
    direction: Option<EventDirection>,
    event_type: Option<&'payload EventType>,
}

impl<'payload> HookSubject<'payload> {
    /// The subject of a payload that is no event: a tool call of the agent
    /// `channel_id`, a session started on it.
    pub fn of_channel(
        channel_id: &'payload ChannelId,
        channel_type: &'payload ChannelType,
    ) -> Self {
        Self {
            channel_id,
            channel_type,
            direction: None,
            event_type: None,
        }
    }
}

impl<'payload> From<&'payload RoomEvent> for HookSubject<'payload> {
    fn from(event: &'payload RoomEvent) -> Self {
        Self {
            channel_id: &event.source.channel_id,
            channel_type: &event.source.channel_type,
            direction: Some(event.source.direction),
            event_type: Some(&event.r#type),
        }
    }
}

impl<'payload> From<&'payload EventSource> for HookSubject<'payload> {
    /// The subject of an event from `source` whose type no filter reads.
    fn from(source: &'payload EventSource) -> Self {
        Self {
            channel_id: &source.channel_id,
            channel_type: &source.channel_type,
            direction: Some(source.direction),
            event_type: None,
        }
    }
}

impl HookFilter {
    /// Whether `subject` passes the filter.
    pub fn admits(&self, subject: HookSubject<'_>) -> bool {
        self.channel_types
            .as_ref()
            .is_none_or(|types| types.contains(subject.channel_type))
            && self
                .channel_ids
                .as_ref()
                .is_none_or(|ids| ids.contains(subject.channel_id))
            && subject.direction.is_none_or(|direction| {
                self.directions
                    .as_ref()
                    .is_none_or(|directions| directions.contains(&direction))
            })
            && subject.event_type.is_none_or(|event_type| {
                self.event_types
                    .as_ref()
                    .is_none_or(|types| types.contains(event_type))
            })
    }
}

/// A hook's registration, without its handler (RFC §9.1).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
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
pub(crate) const DEFAULT_HOOK_TIMEOUT: Duration = Duration::from_secs(30);

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
    /// The name is a value the framework writes in `blocked_by`: the hook's
    /// block would read as the framework's refusal.
    #[error("a hook cannot be named {name:?}, which the framework writes in blocked_by")]
    ReservedName {
        /// The name.
        name: String,
    },
    /// `needs_lock = false` outside a SYNC `BEFORE_BROADCAST` hook.
    #[error("hook {hook:?}: only a SYNC BEFORE_BROADCAST hook may run off the room lock")]
    OffLockOutsideBeforeBroadcast {
        /// The refused hook.
        hook: String,
        /// How it would run.
        execution: HookExecution,
        /// Its trigger.
        trigger: HookTrigger,
    },
    /// `fail_closed = true` on an ASYNC hook, which cannot block.
    #[error("hook {hook:?}: an ASYNC hook cannot fail closed")]
    FailClosedAsync {
        /// The refused hook.
        hook: String,
    },
    /// A hook whose handler takes another payload than its trigger hands
    /// (RFC §9.2): a tool trigger hands a tool call, `ON_SESSION_STARTED` a
    /// session's start, every other trigger an event.
    #[error("hook {hook:?}: {trigger:?} hands its hooks {}", payload_of(*.trigger))]
    PayloadMismatch {
        /// The refused hook.
        hook: String,
        /// Its trigger.
        trigger: HookTrigger,
    },
    /// A hook that needs the lock would run before one that does not.
    #[error(
        "hook {locked:?} needs the room lock but is ordered before {off_lock:?}, which runs off it"
    )]
    LockOrdering {
        /// The hook that needs the lock.
        locked: String,
        /// Its priority.
        locked_priority: i32,
        /// The off-lock hook it would precede.
        off_lock: String,
        /// Its priority.
        off_lock_priority: i32,
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

    /// Whether no hook is registered.
    pub fn is_empty(&self) -> bool {
        self.hooks.is_empty()
    }

    /// Registers `spec`, returning the id the driver keeps its handler
    /// under.
    ///
    /// # Errors
    ///
    /// Returns [`RegistrationError`] when the registration breaks a rule of
    /// RFC §9.1; nothing is registered then.
    pub fn register(&mut self, spec: HookSpec) -> Result<HookId, RegistrationError> {
        if spec.trigger.is_tool_trigger() {
            return Err(payload_mismatch(spec));
        }
        self.add(spec)
    }

    /// Registers `spec`, a hook on a tool trigger, whose payload is a tool
    /// call (RFC §9.3), returning the id the driver keeps its handler under.
    ///
    /// # Errors
    ///
    /// Returns [`RegistrationError::PayloadMismatch`] for a trigger that is
    /// no tool trigger, and any other [`RegistrationError`] a registration
    /// breaking a rule of RFC §9.1 gets; nothing is registered then.
    pub fn register_tool_hook(&mut self, spec: HookSpec) -> Result<HookId, RegistrationError> {
        if !spec.trigger.is_tool_trigger() {
            return Err(payload_mismatch(spec));
        }
        self.add(spec)
    }

    fn add(&mut self, mut spec: HookSpec) -> Result<HookId, RegistrationError> {
        if spec.name.is_empty() {
            spec.name = self.default_name();
        }
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

    /// The name a hook registered without one takes, unique in the
    /// registry: `hook`, then `hook#2`, `hook#3`..., skipping a reserved
    /// name, as the Python implementation names a hook from the base it
    /// takes when its function has no name (`_named`, RFC §9.1).
    fn default_name(&self) -> String {
        let taken = |name: &str| is_reserved(name) || self.find(name).is_some();
        let mut name = "hook".to_owned();
        let mut rank = 2;
        while taken(&name) {
            name = format!("hook#{rank}");
            rank += 1;
        }
        name
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
    pub fn chain<'payload>(
        &self,
        trigger: HookTrigger,
        subject: impl Into<HookSubject<'payload>>,
    ) -> Vec<HookId> {
        if trigger.execution() == HookExecution::Async {
            return Vec::new();
        }
        let mut chain: Vec<(&HookId, &HookSpec)> = self
            .matching(trigger, subject.into())
            .filter(|(_, spec)| spec.execution == HookExecution::Sync)
            .collect();
        // At equal priority an off-lock hook runs first (RFC §9.3).
        chain.sort_by_key(|(id, spec)| (spec.priority, spec.needs_lock, **id));
        chain.into_iter().map(|(id, _)| *id).collect()
    }

    /// The off-lock checks of an event from `source`: the SYNC
    /// `BEFORE_BROADCAST` hooks registered with `needs_lock = false` it
    /// fires, in chain order, which run before the room lock (RFC §9.5.1,
    /// §10.1 step 5a). The ordering rule keeps them ahead of every hook that
    /// needs the lock, so they are the chain's first hooks.
    pub fn off_lock_checks<'payload>(
        &self,
        subject: impl Into<HookSubject<'payload>>,
    ) -> Vec<HookId> {
        self.chain_where(subject.into(), |spec| !spec.needs_lock)
    }

    /// The hooks step 9 runs under the lock once an off-lock check ran:
    /// the `BEFORE_BROADCAST` chain of an event from `source` without its
    /// off-lock hooks (RFC §10.1 step 9).
    pub fn locked_chain<'payload>(&self, subject: impl Into<HookSubject<'payload>>) -> Vec<HookId> {
        self.chain_where(subject.into(), |spec| spec.needs_lock)
    }

    /// The `BEFORE_BROADCAST` chain of an event from `source`, kept to the
    /// hooks `keep` admits.
    fn chain_where(
        &self,
        subject: HookSubject<'_>,
        keep: impl Fn(&HookSpec) -> bool,
    ) -> Vec<HookId> {
        self.chain(HookTrigger::BeforeBroadcast, subject)
            .into_iter()
            .filter(|id| self.get(*id).is_some_and(&keep))
            .collect()
    }

    /// The hooks `trigger` runs as observers for an event from `source`:
    /// the ASYNC hooks of a SYNC trigger, every hook of an ASYNC trigger
    /// (RFC §9.2).
    pub fn observers<'payload>(
        &self,
        trigger: HookTrigger,
        subject: impl Into<HookSubject<'payload>>,
    ) -> Vec<HookId> {
        let observes_all = trigger.execution() == HookExecution::Async;
        self.matching(trigger, subject.into())
            .filter(|(_, spec)| observes_all || spec.execution == HookExecution::Async)
            .map(|(id, _)| *id)
            .collect()
    }

    /// The hooks of `trigger` whose filter admits `subject`; a session's
    /// start's hooks read no filter, as the Python implementation fires them
    /// (`skip_event_filter`).
    fn matching<'registry>(
        &'registry self,
        trigger: HookTrigger,
        subject: HookSubject<'registry>,
    ) -> impl Iterator<Item = (&'registry HookId, &'registry HookSpec)> {
        self.hooks
            .iter()
            .filter(move |(_, spec)| {
                spec.trigger == trigger
                    && (trigger == HookTrigger::OnSessionStarted || spec.filter.admits(subject))
            })
            .map(|(id, spec)| (id, spec))
    }

    /// No SYNC `BEFORE_BROADCAST` hook that needs the lock may have a lower
    /// priority than one that does not (RFC §9.1); at equal priority the
    /// off-lock hook runs first.
    fn check_lock_ordering(&self) -> Result<(), RegistrationError> {
        let mut chain: Vec<&(HookId, HookSpec)> = self
            .hooks
            .iter()
            .filter(|(_, spec)| {
                spec.trigger == HookTrigger::BeforeBroadcast
                    && spec.execution == HookExecution::Sync
            })
            .collect();
        chain.sort_by_key(|(id, spec)| (spec.priority, spec.needs_lock, *id));
        let mut first_locked: Option<&HookSpec> = None;
        for (_, spec) in chain {
            match (spec.needs_lock, first_locked) {
                (true, None) => first_locked = Some(spec),
                (false, Some(locked)) => {
                    return Err(RegistrationError::LockOrdering {
                        locked: locked.name.clone(),
                        locked_priority: locked.priority,
                        off_lock: spec.name.clone(),
                        off_lock_priority: spec.priority,
                    });
                }
                (true, Some(_)) | (false, None) => {}
            }
        }
        Ok(())
    }
}

/// The refusal of `spec`, registered with the payload its trigger does not
/// take.
fn payload_mismatch(spec: HookSpec) -> RegistrationError {
    RegistrationError::PayloadMismatch {
        hook: spec.name,
        trigger: spec.trigger,
    }
}

/// The payload `trigger` hands its hooks, as a refusal names it.
fn payload_of(trigger: HookTrigger) -> &'static str {
    if trigger.is_tool_trigger() {
        "a ToolCallEvent"
    } else if trigger == HookTrigger::OnSessionStarted {
        "a SessionStartedEvent"
    } else {
        "a RoomEvent"
    }
}

/// Whether `name` is a value the framework writes in `blocked_by` (RFC
/// §9.1).
fn is_reserved(name: &str) -> bool {
    [
        WriteRefusal::ReadOnly,
        WriteRefusal::Muted,
        WriteRefusal::Unbound,
    ]
    .iter()
    .any(|refusal| refusal.blocked_by() == name)
        || [CHAIN_DEPTH_LIMIT, REENTRY_LOOP_CAP].contains(&name)
}

/// The rules a registration obeys on its own (RFC §9.1).
fn check_alone(spec: &HookSpec) -> Result<(), RegistrationError> {
    if is_reserved(&spec.name) {
        return Err(RegistrationError::ReservedName {
            name: spec.name.clone(),
        });
    }
    let sync_before_broadcast =
        spec.trigger == HookTrigger::BeforeBroadcast && spec.execution == HookExecution::Sync;
    if !spec.needs_lock && !sync_before_broadcast {
        return Err(RegistrationError::OffLockOutsideBeforeBroadcast {
            hook: spec.name.clone(),
            execution: spec.execution,
            trigger: spec.trigger,
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
        EventSource::new(ChannelId::new("sms").unwrap(), ChannelType::SMS)
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
                locked_priority: 0,
                off_lock: "pii_scan".to_owned(),
                off_lock_priority: 5,
            })
        );
    }

    /// The Python implementation's `_named`: a hook registered without a
    /// name takes `hook`, then `hook#2`, the next free in the registry.
    #[test]
    fn an_unnamed_hook_is_named_hook_then_hook_2() {
        let mut registry = HookRegistry::new();
        let ids = [
            sync_hook("", 0),
            sync_hook("", 0),
            sync_hook("hook#3", 0),
            sync_hook("", 0),
        ]
        .map(|spec| registry.register(spec).unwrap());
        assert_eq!(
            names(&registry, &ids),
            ["hook", "hook#2", "hook#3", "hook#4"]
        );
    }

    /// RFC §9.1, as Python's `_hook_matches_event`: `event_types` reads an
    /// event's type; a payload that is no event (a tool call) passes it, and
    /// its direction too.
    #[test]
    fn event_types_filter_events_and_let_other_payloads_through() {
        let mut registry = HookRegistry::new();
        let mut spec = sync_hook("messages_only", 0);
        spec.filter.event_types = Some(BTreeSet::from([EventType::Message]));
        spec.filter.directions = Some(BTreeSet::from([EventDirection::Outbound]));
        registry.register(spec).unwrap();
        let event = |r#type: EventType, direction: EventDirection| {
            RoomEvent::at(
                crate::model::EventId::new("e1").unwrap(),
                crate::model::RoomId::new("r1").unwrap(),
                r#type,
                EventSource {
                    direction,
                    ..sms_source()
                },
                crate::model::TextContent::new("hi"),
                crate::model::Timestamp::UNIX_EPOCH,
            )
        };
        let chained = |subject: HookSubject<'_>| {
            !registry
                .chain(HookTrigger::BeforeBroadcast, subject)
                .is_empty()
        };
        let (sms, kind) = (ChannelId::new("sms").unwrap(), ChannelType::SMS);
        assert_eq!(
            [
                chained((&event(EventType::Message, EventDirection::Outbound)).into()),
                chained((&event(EventType::System, EventDirection::Outbound)).into()),
                chained((&event(EventType::Message, EventDirection::Inbound)).into()),
                chained(HookSubject::of_channel(&sms, &kind)),
            ],
            [true, false, false, true]
        );
    }

    #[test]
    fn an_off_lock_hook_sharing_a_locked_hook_s_priority_runs_first() {
        let mut registry = HookRegistry::new();
        registry.register(sync_hook("router", 0)).unwrap();
        registry.register(off_lock_hook("pii_scan", 0)).unwrap();
        let chain = registry.chain(HookTrigger::BeforeBroadcast, &sms_source());
        assert_eq!(names(&registry, &chain), ["pii_scan", "router"]);
    }

    #[test]
    fn a_hook_named_after_a_framework_refusal_is_refused() {
        for name in [
            "source_read_only",
            "source_muted",
            "source_unbound",
            "event_chain_depth_limit",
            "reentry_loop_cap",
        ] {
            assert_eq!(
                HookRegistry::new().register(sync_hook(name, 0)),
                Err(RegistrationError::ReservedName {
                    name: name.to_owned()
                }),
                "{name}"
            );
        }
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

    /// RFC §9.5.1: step 5a runs the off-lock hooks, step 9 the others.
    mod off_lock {
        use super::*;
        use pretty_assertions::assert_eq;

        fn registry() -> HookRegistry {
            let mut registry = HookRegistry::new();
            registry.register(sync_hook("router", 10)).unwrap();
            registry.register(off_lock_hook("moderation", 5)).unwrap();
            registry.register(off_lock_hook("pii_scan", 0)).unwrap();
            registry
                .register(HookSpec::new(
                    "audit",
                    HookTrigger::BeforeBroadcast,
                    HookExecution::Async,
                ))
                .unwrap();
            registry
        }

        #[test]
        fn the_checks_are_the_off_lock_hooks_in_chain_order() {
            let registry = registry();
            assert_eq!(
                names(&registry, &registry.off_lock_checks(&sms_source())),
                ["pii_scan", "moderation"]
            );
        }

        #[test]
        fn the_locked_chain_leaves_the_off_lock_hooks_out() {
            let registry = registry();
            assert_eq!(
                names(&registry, &registry.locked_chain(&sms_source())),
                ["router"]
            );
        }

        #[test]
        fn a_check_filtered_to_another_channel_does_not_fire() {
            let mut registry = HookRegistry::new();
            let mut scan = off_lock_hook("pii_scan", 0);
            scan.filter.channel_ids = Some([ChannelId::new("email").unwrap()].into());
            registry.register(scan).unwrap();
            assert_eq!(registry.off_lock_checks(&sms_source()), []);
        }
    }

    /// RFC §9.3: a tool trigger's payload is a tool call, no event.
    mod tool_hooks {
        use pretty_assertions::assert_eq;

        use super::*;

        #[test]
        fn a_tool_trigger_s_hook_is_refused_as_an_event_s() {
            let mut registry = HookRegistry::new();
            for trigger in [HookTrigger::BeforeToolUse, HookTrigger::OnToolCall] {
                let spec = HookSpec::new("approve", trigger, HookExecution::Sync);
                assert_eq!(
                    registry.register(spec),
                    Err(RegistrationError::PayloadMismatch {
                        hook: "approve".to_owned(),
                        trigger
                    })
                );
            }
        }

        #[test]
        fn an_event_trigger_s_hook_is_refused_as_a_tool_hook() {
            let mut registry = HookRegistry::new();
            assert!(matches!(
                registry.register_tool_hook(sync_hook("moderation", 0)),
                Err(RegistrationError::PayloadMismatch { .. })
            ));
        }

        #[test]
        fn tool_hooks_run_in_priority_order_and_their_observers_apart() {
            let mut registry = HookRegistry::new();
            let gate = |name: &str, priority| HookSpec {
                priority,
                ..HookSpec::new(name, HookTrigger::BeforeToolUse, HookExecution::Sync)
            };
            registry.register_tool_hook(gate("approve", 5)).unwrap();
            registry.register_tool_hook(gate("audit_args", 1)).unwrap();
            registry
                .register_tool_hook(HookSpec::new(
                    "audit",
                    HookTrigger::OnToolCall,
                    HookExecution::Async,
                ))
                .unwrap();
            let chain = registry.chain(HookTrigger::BeforeToolUse, &sms_source());
            let observers = registry.observers(HookTrigger::OnToolCall, &sms_source());
            assert_eq!(
                (names(&registry, &chain), names(&registry, &observers)),
                (
                    vec!["audit_args".to_owned(), "approve".to_owned()],
                    vec!["audit".to_owned()]
                )
            );
        }
    }
}
