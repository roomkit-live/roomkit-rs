//! Hook registrations, the checks they pass, and which hooks an event fires
//! (RFC §9.1, §9.2).
//!
//! The core holds hooks by their registration only. The handler behind a
//! registration is the driver's, found by the [`HookId`] the registry hands
//! back. A registration's scope is the kit or one room (RFC §9.1): a room's
//! hooks fire for that room's payloads alone, beside the kit's.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Weak};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::model::{
    ChannelId, ChannelType, EventDirection, EventSource, EventType, RoomEvent, RoomId,
};
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
    /// A provider reported where a message the room sent stands (RFC
    /// §10.4, A delivery status follows its message): its payload is the
    /// `DeliveryStatus`, no event, and every hook on it fires in the room,
    /// its filter unread.
    OnDeliveryStatus,
    /// An entry was posted on the kit's status bus naming the room in its
    /// metadata's `room_id` (RFC §19.8); its payload is the entry.
    OnStatusPosted,
    /// `submit_feedback` recorded a rating as an observation of the room
    /// (RFC §15.9); its payload is the observation.
    OnFeedback,
    /// Identity resolution found several candidates for a sender (RFC
    /// §10.1 step 5, §11.2): its identity hooks decide, registered apart;
    /// a hook registered here observes.
    OnIdentityAmbiguous,
    /// Identity resolution found no identity for a sender (RFC §10.1 step
    /// 5, §11.2): its identity hooks decide, registered apart; a hook
    /// registered here observes.
    OnIdentityUnknown,
    /// Before a proactive delivery's strategy runs (RFC §22.3): can block
    /// it or rewrite its content. It fails open.
    BeforeDeliver,
    /// Once a proactive delivery has an outcome, whatever it is (RFC
    /// §22.3).
    AfterDeliver,
    /// A task was delegated to an agent in a child room (RFC §23.5): its
    /// payload is a `task_delegated` event of the parent room, never
    /// stored.
    OnTaskDelegated,
    /// A delegated task ended (RFC §23.5): its payload is a
    /// `task_completed` event of the parent room, never stored.
    OnTaskCompleted,
    /// `resolve_participant` identified a pending participant as an
    /// identity (RFC §5.5, Appendix B.3); its payload is the system event
    /// of the change.
    OnParticipantIdentified,
    /// `add_member` joined a member, or one who left again (RFC §5.5).
    OnParticipantJoined,
    /// `remove_member` set a member's status to LEFT or BANNED (RFC §5.5).
    OnParticipantLeft,
    /// `rename_member` changed a member's display name (RFC §5.5).
    OnParticipantUpdated,
    /// A tool asked a person for input (RFC §9.3, §10.6.5): its payload is
    /// the request, a block rejects it unless it was answered first, and a
    /// hook that fails lets it through. A turn fires it, on the turn
    /// executor hosting the channel.
    OnUserInputRequired,
    /// A verification request completed (RFC §11.7.2): its payload is a
    /// system event of the request's room, its data the identity, the
    /// request, its channel, the method and the expiry.
    OnVerificationCompleted,
    /// A verification or a request ended: expired, revoked, failed (RFC
    /// §11.7.5): its payload is a system event of the room of the request
    /// that ended, or of the request that completed the verification that
    /// ended, its data the reason and what ended.
    OnVerificationEnded,
    /// An AI channel's loop ran a round of calls, before it builds the next
    /// (RFC §6.4, Between two rounds): its payload is the round, whose
    /// hooks may withdraw tools for the rest of the turn and add messages
    /// the next round reads. A turn fires it, on the turn executor hosting
    /// the channel.
    AfterToolRound,
    /// An AI channel's turn is about to generate (RFC §6.4, What the
    /// generation hooks see): its payload is the context the turn starts
    /// from, which its hooks may replace, or block the turn. A turn fires
    /// it, on the turn executor hosting the channel.
    BeforeAiGeneration,
    /// An AI channel's turn completed (RFC §6.4, §9.2): its payload is the
    /// turn's response, observed. A turn fires it, on the turn executor
    /// hosting the channel.
    OnAiResponse,
    /// An identified sender who is not VERIFIED wrote a message (RFC §10.1
    /// step 5b, §11.7.3): its SYNC hooks hold the message (the default),
    /// pass it with [`HookResult::allow_unverified`] or block it. Its
    /// payload is the message's event, labelled with the sender's
    /// verification.
    ///
    /// [`HookResult::allow_unverified`]: crate::model::HookResult::allow_unverified
    OnVerificationRequired,
    /// A handoff went through (RFC §19.6): its payload is an internal
    /// system event of the room, never stored, naming both agents.
    OnHandoff,
    /// A handoff was refused (RFC §19.6): its payload is as `OnHandoff`'s.
    OnHandoffRejected,
    /// A handoff moved the room's phase (RFC §19.6): its payload is as
    /// `OnHandoff`'s; a phase saved by other means fires it not.
    OnPhaseTransition,
    /// An AI channel's model began to reason (RFC §6.7, §9.2): its payload
    /// is a `ThinkingEvent`, observed. A turn fires it, on the turn executor
    /// hosting the channel.
    OnAiThinking,
    /// A room's speak queue changed (RFC §19.7.5): its payload is a
    /// `SpeakQueueEvent`, observed.
    OnSpeakQueue,
    /// A discussion's dispatch policy decided who takes a person's message
    /// (RFC §19.7.5 rule 18): its payload is a `DispatchDecisionEvent`,
    /// observed.
    OnDispatchDecision,
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
            | Self::OnToolCall
            | Self::BeforeDeliver
            | Self::OnUserInputRequired
            | Self::AfterToolRound
            | Self::BeforeAiGeneration
            | Self::OnVerificationRequired => HookExecution::Sync,
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
            | Self::OnTaskCreated
            | Self::OnDeliveryStatus
            | Self::OnStatusPosted
            | Self::OnFeedback
            | Self::OnIdentityAmbiguous
            | Self::OnIdentityUnknown
            | Self::AfterDeliver
            | Self::OnTaskDelegated
            | Self::OnTaskCompleted
            | Self::OnHandoff
            | Self::OnHandoffRejected
            | Self::OnPhaseTransition
            | Self::OnParticipantIdentified
            | Self::OnParticipantJoined
            | Self::OnParticipantLeft
            | Self::OnParticipantUpdated
            | Self::OnVerificationCompleted
            | Self::OnVerificationEnded
            | Self::OnAiResponse
            | Self::OnAiThinking
            | Self::OnSpeakQueue
            | Self::OnDispatchDecision => HookExecution::Async,
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
            | Self::OnTaskCreated
            | Self::OnDeliveryStatus
            | Self::OnStatusPosted
            | Self::OnFeedback
            | Self::OnIdentityAmbiguous
            | Self::OnIdentityUnknown
            | Self::BeforeDeliver
            | Self::AfterDeliver
            | Self::OnTaskDelegated
            | Self::OnTaskCompleted
            | Self::OnHandoff
            | Self::OnHandoffRejected
            | Self::OnPhaseTransition
            | Self::OnParticipantIdentified
            | Self::OnParticipantJoined
            | Self::OnParticipantLeft
            | Self::OnParticipantUpdated
            | Self::OnUserInputRequired
            | Self::OnVerificationCompleted
            | Self::OnVerificationEnded
            | Self::AfterToolRound
            | Self::BeforeAiGeneration
            | Self::OnAiResponse
            | Self::OnAiThinking
            | Self::OnSpeakQueue
            | Self::OnDispatchDecision
            | Self::OnVerificationRequired => false,
        }
    }

    /// Whether a turn trigger's payload carries the agent's channel type: a
    /// tool call, an input request. Only there does a hook's `channel_types`
    /// filter (RFC §9.1; Python's `TYPED_TURN_TRIGGERS`).
    #[doc(hidden)]
    pub fn carries_channel_type(self) -> bool {
        !self.is_turn_trigger()
            || matches!(
                self,
                Self::BeforeToolUse | Self::OnToolCall | Self::OnUserInputRequired
            )
    }

    /// Whether a turn fires the trigger, on the turn executor hosting the
    /// channel, rather than the room owner (RFC §10.6.6), as the Python
    /// implementation's `TURN_TRIGGERS` lists them: `BEFORE_TOOL_USE`,
    /// `ON_TOOL_CALL`, `ON_USER_INPUT_REQUIRED` and an AI channel's loop
    /// triggers among the triggers this list holds.
    #[doc(hidden)]
    pub fn is_turn_trigger(self) -> bool {
        match self {
            Self::BeforeToolUse
            | Self::OnToolCall
            | Self::OnUserInputRequired
            | Self::AfterToolRound
            | Self::BeforeAiGeneration
            | Self::OnAiResponse
            | Self::OnAiThinking => true,
            Self::OnSpeakQueue
            | Self::OnDispatchDecision
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
            | Self::OnTaskCreated
            | Self::OnTranscription
            | Self::BeforeTts
            | Self::OnDeliveryStatus
            | Self::OnStatusPosted
            | Self::OnFeedback
            | Self::OnIdentityAmbiguous
            | Self::OnIdentityUnknown
            | Self::BeforeDeliver
            | Self::AfterDeliver
            | Self::OnTaskDelegated
            | Self::OnTaskCompleted
            | Self::OnHandoff
            | Self::OnHandoffRejected
            | Self::OnPhaseTransition
            | Self::OnParticipantIdentified
            | Self::OnParticipantJoined
            | Self::OnParticipantLeft
            | Self::OnParticipantUpdated
            | Self::OnVerificationCompleted
            | Self::OnVerificationEnded
            | Self::OnVerificationRequired => false,
        }
    }

    /// Whether the trigger's hooks fire whatever their filter, as the Python
    /// implementation fires them (`skip_event_filter`): a session's start, a
    /// delivery status, a status posted, a feedback, whose payload is no
    /// event.
    fn skips_filter(self) -> bool {
        matches!(
            self,
            Self::OnSessionStarted
                | Self::OnDeliveryStatus
                | Self::OnStatusPosted
                | Self::OnFeedback
                | Self::OnSpeakQueue
                | Self::OnDispatchDecision
        )
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
    /// The room the payload is of, whose hooks fire beside the kit's; `None`
    /// fires the kit's alone.
    room_id: Option<&'payload RoomId>,
    /// Whether the payload carries the channel's type: a hook's
    /// `channel_types` filter only then (RFC §9.1).
    typed: bool,
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
            room_id: None,
            typed: true,
        }
    }

    /// The same subject, read with its channel's type only where `trigger`'s
    /// payload carries it (RFC §9.1).
    #[must_use]
    pub fn typed_for(self, trigger: HookTrigger) -> Self {
        Self {
            typed: trigger.carries_channel_type(),
            ..self
        }
    }

    /// The subject as a payload of the room `room_id`, whose hooks fire for
    /// it beside the kit's (RFC §9.1): a tool call of a turn in that room.
    #[must_use]
    pub fn in_room(self, room_id: &'payload RoomId) -> Self {
        Self {
            room_id: Some(room_id),
            ..self
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
            room_id: Some(&event.room_id),
            typed: true,
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
            room_id: None,
            typed: true,
        }
    }
}

impl HookFilter {
    /// Whether `subject` passes the filter.
    pub fn admits(&self, subject: HookSubject<'_>) -> bool {
        (!subject.typed
            || self
                .channel_types
                .as_ref()
                .is_none_or(|types| types.contains(subject.channel_type)))
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

/// The hooks of a kit, its own and each room's (RFC §9.1).
#[derive(Clone, Debug, Default)]
pub struct HookRegistry {
    hooks: Vec<Registered>,
    /// The id the next registration takes: ids are never given out twice,
    /// a removed hook's included.
    next: usize,
}

/// One registration and its scope.
#[derive(Clone, Debug)]
struct Registered {
    id: HookId,
    /// The room it was added for; `None` for the kit's.
    room: Option<RoomId>,
    spec: HookSpec,
    /// Shared by every copy of the registry that holds the registration:
    /// once none does, no operation can ask for the hook any more.
    held: Arc<()>,
}

/// A registration taken out of the registry, as a removal hands it back.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Removed {
    /// The id its handler is kept under.
    pub id: HookId,
    /// The registration, which an operation that read the registry before
    /// the removal may still run.
    pub spec: HookSpec,
    /// Upgrades while a copy of the registry that holds the registration
    /// lives: an operation that started before the removal may still ask
    /// for the hook until it is dead.
    pub held: Weak<()>,
}

impl Removed {
    fn of(hook: Registered) -> Self {
        Self {
            id: hook.id,
            held: Arc::downgrade(&hook.held),
            spec: hook.spec,
        }
    }
}

impl Registered {
    /// Whether it fires for a payload of `room`: the kit's hooks fire for
    /// every room's, a room's for its own alone.
    fn reaches(&self, room: Option<&RoomId>) -> bool {
        self.room.is_none() || self.room.as_ref() == room
    }
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
        self.add(None, spec)
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
        self.add(None, spec)
    }

    /// Adds `spec` for the room `room_id` alone (RFC §9.1), whatever its
    /// trigger's payload, returning the id the driver keeps its handler
    /// under: Python's `HookEngine.add_room_hook`. Its name is unique among
    /// the room's hooks, and may be one of the kit's; it is checked against
    /// the kit's hooks and the room's, a room that does not exist yet
    /// included, whose hooks wait for it.
    ///
    /// # Errors
    ///
    /// Returns [`RegistrationError`] when the registration breaks a rule of
    /// RFC §9.1; nothing is registered then.
    pub fn add_room_hook(
        &mut self,
        room_id: RoomId,
        spec: HookSpec,
    ) -> Result<HookId, RegistrationError> {
        self.add(Some(room_id), spec)
    }

    fn add(
        &mut self,
        room: Option<RoomId>,
        mut spec: HookSpec,
    ) -> Result<HookId, RegistrationError> {
        if spec.name.is_empty() {
            spec.name = self.default_name(room.as_ref());
        }
        check_alone(&spec)?;
        if self
            .in_scope(room.as_ref())
            .any(|hook| hook.spec.name == spec.name)
        {
            return Err(RegistrationError::DuplicateName { name: spec.name });
        }
        let id = HookId(self.next);
        self.hooks.push(Registered {
            id,
            room,
            spec,
            held: Arc::new(()),
        });
        let checked = self
            .hooks
            .last()
            .map_or(Ok(()), |added| self.check_lock_ordering(added));
        if let Err(error) = checked {
            self.hooks.pop();
            return Err(error);
        }
        self.next += 1;
        Ok(id)
    }

    /// Adds `spec` for the room `room_id` in place of the room's hook of the
    /// same name, returning its id and the replaced registration: Python's
    /// `HookEngine.replace_room_hook`, a strategy installed again in its
    /// room replacing what it set up there. A registration refused leaves
    /// the hook it would replace in place.
    ///
    /// # Errors
    ///
    /// Returns [`RegistrationError`] when the registration breaks a rule of
    /// RFC §9.1; nothing is changed then.
    pub fn replace_room_hook(
        &mut self,
        room_id: RoomId,
        spec: HookSpec,
    ) -> Result<(HookId, Option<Removed>), RegistrationError> {
        let position = self.hooks.iter().position(|hook| {
            !spec.name.is_empty()
                && hook.room.as_ref() == Some(&room_id)
                && hook.spec.name == spec.name
        });
        let replaced = position.map(|position| self.hooks.remove(position));
        match self.add(Some(room_id), spec) {
            Ok(id) => Ok((id, replaced.map(Removed::of))),
            Err(error) => {
                if let (Some(position), Some(hook)) = (position, replaced) {
                    self.hooks.insert(position, hook);
                }
                Err(error)
            }
        }
    }

    /// Whether a hook is registered, the kit's or a room's: on `trigger`,
    /// or on any trigger for `None`. Python's `HookEngine.has_hooks`.
    pub fn has_hooks(&self, trigger: Option<HookTrigger>) -> bool {
        self.has_hooks_where(|spec| trigger.is_none_or(|trigger| spec.trigger == trigger))
    }

    /// Whether a hook is registered, the kit's or a room's, whose
    /// registration `keep` admits.
    pub fn has_hooks_where(&self, keep: impl Fn(&HookSpec) -> bool) -> bool {
        self.hooks.iter().any(|hook| keep(&hook.spec))
    }

    /// The ids of the kit's hooks and of those the room `room_id` was given.
    pub fn ids_applying_to<'registry>(
        &'registry self,
        room_id: &'registry RoomId,
    ) -> impl Iterator<Item = HookId> + 'registry {
        self.hooks
            .iter()
            .filter(move |hook| hook.room.as_ref().is_none_or(|room| room == room_id))
            .map(|hook| hook.id)
    }

    /// The kit's hook named `name`.
    pub fn global_hook(&self, name: &str) -> Option<&HookSpec> {
        self.in_scope(None)
            .find(|hook| hook.spec.name == name)
            .map(|hook| &hook.spec)
    }

    /// The hook named `name` the room `room_id` was given.
    pub fn room_hook(&self, room_id: &RoomId, name: &str) -> Option<&HookSpec> {
        self.hooks
            .iter()
            .find(|hook| hook.room.as_ref() == Some(room_id) && hook.spec.name == name)
            .map(|hook| &hook.spec)
    }

    /// Removes the kit's hook named `name`, returning the registration;
    /// `None` when the kit has no hook of that name. A room's hook of that
    /// name stays.
    pub fn remove_global_hook(&mut self, name: &str) -> Option<Removed> {
        self.remove_where(None, name)
    }

    /// Removes the hook named `name` the room `room_id` was given,
    /// returning the registration; `None` when the room has no hook of that
    /// name. The kit's hook of that name stays (RFC §9.1).
    pub fn remove_room_hook(&mut self, room_id: &RoomId, name: &str) -> Option<Removed> {
        self.remove_where(Some(room_id), name)
    }

    fn remove_where(&mut self, room: Option<&RoomId>, name: &str) -> Option<Removed> {
        let position = self
            .hooks
            .iter()
            .position(|hook| hook.room.as_ref() == room && hook.spec.name == name)?;
        Some(Removed::of(self.hooks.remove(position)))
    }

    /// The names of the hooks the room `room_id` was given, in their
    /// registration order: Python's `HookEngine.room_hook_names`.
    pub fn room_hook_names(&self, room_id: &RoomId) -> Vec<String> {
        self.in_scope(Some(room_id))
            .map(|hook| hook.spec.name.clone())
            .collect()
    }

    /// The registrations of one scope: the kit's for `None`, else the
    /// room's.
    fn in_scope<'registry>(
        &'registry self,
        room: Option<&'registry RoomId>,
    ) -> impl Iterator<Item = &'registry Registered> {
        self.hooks
            .iter()
            .filter(move |hook| hook.room.as_ref() == room)
    }

    /// The name a hook registered without one takes, unique in its scope,
    /// the kit's hooks or one room's: `hook`, then `hook#2`, `hook#3`...,
    /// skipping a reserved name, as the Python implementation names a hook
    /// from the base it takes when its function has no name (`_named`, RFC
    /// §9.1).
    fn default_name(&self, room: Option<&RoomId>) -> String {
        let taken = |name: &str| {
            is_reserved(name) || self.in_scope(room).any(|hook| hook.spec.name == name)
        };
        let mut name = "hook".to_owned();
        let mut rank = 2;
        while taken(&name) {
            name = format!("hook#{rank}");
            rank += 1;
        }
        name
    }

    /// The registration of `id`.
    pub fn get(&self, id: HookId) -> Option<&HookSpec> {
        self.hooks
            .iter()
            .find(|hook| hook.id == id)
            .map(|hook| &hook.spec)
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
        let mut chain: Vec<&Registered> = self
            .matching(trigger, subject.into())
            .filter(|hook| hook.spec.execution == HookExecution::Sync)
            .collect();
        chain.sort_by_key(|hook| chain_rank(hook));
        chain.into_iter().map(|hook| hook.id).collect()
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
        let mut observers: Vec<&Registered> = self
            .matching(trigger, subject.into())
            .filter(|hook| observes_all || hook.spec.execution == HookExecution::Async)
            .collect();
        // As the Python implementation starts them: by priority, the kit's
        // before a room's at equal priority, then in registration order.
        observers.sort_by_key(|hook| (hook.spec.priority, hook.room.is_some(), hook.id));
        observers.into_iter().map(|hook| hook.id).collect()
    }

    /// `hooks` split by name, keeping their order: those a name starting
    /// with `_` marks as the framework's own, which a session's start awaits
    /// before its message goes on, and the others, which it runs in the
    /// background, as Python's `_fire_text_session_started` does (RFC §10.1
    /// step 2). A hook no longer held goes with the others.
    pub fn split_own_hooks(&self, hooks: Vec<HookId>) -> (Vec<HookId>, Vec<HookId>) {
        hooks.into_iter().partition(|hook| {
            self.get(*hook)
                .is_some_and(|spec| spec.name.starts_with('_'))
        })
    }

    /// The hooks of `trigger` whose filter admits `subject`, the kit's and
    /// those of the subject's room; the hooks of a session's start, a
    /// delivery status, a status posted and a feedback read no filter, as
    /// the Python implementation fires them (`skip_event_filter`).
    fn matching<'registry>(
        &'registry self,
        trigger: HookTrigger,
        subject: HookSubject<'registry>,
    ) -> impl Iterator<Item = &'registry Registered> {
        self.hooks.iter().filter(move |hook| {
            hook.spec.trigger == trigger
                && hook.reaches(subject.room_id)
                && (trigger.skips_filter() || hook.spec.filter.admits(subject))
        })
    }

    /// No SYNC `BEFORE_BROADCAST` hook that needs the lock may have a lower
    /// priority than one that does not (RFC §9.1), in each chain the hook
    /// `added` joins, as the Python implementation checks it: a room's hook
    /// joins its room's, the kit's with the room's; the kit's joins the
    /// kit's alone and every room's. At equal priority the off-lock hook
    /// runs first. A hook of any other trigger or execution joins none.
    fn check_lock_ordering(&self, added: &Registered) -> Result<(), RegistrationError> {
        if !is_check(&added.spec) {
            return Ok(());
        }
        let mut kit = Vec::new();
        let mut rooms: BTreeMap<&RoomId, Vec<&Registered>> = BTreeMap::new();
        for hook in self.hooks.iter().filter(|hook| is_check(&hook.spec)) {
            match &hook.room {
                None => kit.push(hook),
                Some(room) => rooms.entry(room).or_default().push(hook),
            }
        }
        if let Some(room) = &added.room {
            let own = rooms.get(room).map_or(&[][..], Vec::as_slice);
            return check_chain_order(kit.iter().chain(own).copied());
        }
        check_chain_order(kit.iter().copied())?;
        for own in rooms.values() {
            check_chain_order(kit.iter().chain(own).copied())?;
        }
        Ok(())
    }
}

/// Whether `spec` is a SYNC `BEFORE_BROADCAST` hook, one of the chain the
/// lock ordering rule reads (RFC §9.1).
fn is_check(spec: &HookSpec) -> bool {
    spec.trigger == HookTrigger::BeforeBroadcast && spec.execution == HookExecution::Sync
}

/// Where a SYNC hook runs in its chain (RFC §9.3): by priority, lower
/// first; at equal priority an off-lock hook first, then the kit's hooks
/// before a room's, as the Python implementation merges them, then in
/// registration order (gap G-018).
fn chain_rank(hook: &Registered) -> (i32, bool, bool, HookId) {
    (
        hook.spec.priority,
        hook.spec.needs_lock,
        hook.room.is_some(),
        hook.id,
    )
}

/// The lock ordering rule (RFC §9.1) on one chain of `hooks`.
fn check_chain_order<'registry>(
    hooks: impl Iterator<Item = &'registry Registered>,
) -> Result<(), RegistrationError> {
    let mut chain: Vec<&Registered> = hooks.filter(|hook| is_check(&hook.spec)).collect();
    chain.sort_by_key(|hook| chain_rank(hook));
    let mut first_locked: Option<&HookSpec> = None;
    for spec in chain.into_iter().map(|hook| &hook.spec) {
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
    } else if trigger == HookTrigger::OnUserInputRequired {
        "a PendingInputEvent"
    } else if trigger == HookTrigger::AfterToolRound {
        "a ToolRoundEvent"
    } else if trigger == HookTrigger::BeforeAiGeneration {
        "an AIGenerationEvent"
    } else if trigger == HookTrigger::OnAiResponse {
        "an AIResponseEvent"
    } else if trigger == HookTrigger::OnAiThinking {
        "a ThinkingEvent"
    } else if trigger == HookTrigger::OnSpeakQueue {
        "a SpeakQueueEvent"
    } else if trigger == HookTrigger::OnDispatchDecision {
        "a DispatchDecisionEvent"
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
                RoomId::new("r1").unwrap(),
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

    /// RFC §9.1: a registration's scope is the kit or one room.
    mod room_hooks {
        use pretty_assertions::assert_eq;

        use super::*;

        fn room(id: &str) -> RoomId {
            RoomId::new(id).unwrap()
        }

        fn event_in(room_id: &str) -> RoomEvent {
            RoomEvent::at(
                crate::model::EventId::new("e1").unwrap(),
                room(room_id),
                EventType::Message,
                sms_source(),
                crate::model::TextContent::new("hi"),
                crate::model::Timestamp::UNIX_EPOCH,
            )
        }

        // Python: tests/test_hooks.py::TestSyncHooks::test_room_hooks_merged
        #[test]
        fn a_room_s_hooks_join_the_kit_s_in_its_room_alone() {
            let mut registry = HookRegistry::new();
            registry.register(sync_hook("global", 0)).unwrap();
            registry
                .add_room_hook(room("r1"), sync_hook("room", 1))
                .unwrap();
            let in_r1 = registry.chain(HookTrigger::BeforeBroadcast, &event_in("r1"));
            let in_r2 = registry.chain(HookTrigger::BeforeBroadcast, &event_in("r2"));
            assert_eq!(
                (names(&registry, &in_r1), names(&registry, &in_r2)),
                (
                    vec!["global".to_owned(), "room".to_owned()],
                    vec!["global".to_owned()]
                )
            );
        }

        #[test]
        fn at_equal_priority_the_kit_s_hooks_run_before_the_room_s() {
            let mut registry = HookRegistry::new();
            registry
                .add_room_hook(room("r1"), sync_hook("room", 0))
                .unwrap();
            registry.register(sync_hook("kit", 0)).unwrap();
            let chain = registry.chain(HookTrigger::BeforeBroadcast, &event_in("r1"));
            assert_eq!(names(&registry, &chain), ["kit", "room"]);
        }

        // Python: tests/test_hooks.py::TestRoomHookManagement::test_remove_room_hook
        #[test]
        fn a_room_hook_is_removed_by_its_name_once() {
            let mut registry = HookRegistry::new();
            let id = registry
                .add_room_hook(room("r1"), sync_hook("my_hook", 0))
                .unwrap();
            let removed = |registry: &mut HookRegistry, name| {
                registry
                    .remove_room_hook(&room("r1"), name)
                    .map(|removed| removed.id)
            };
            assert_eq!(
                (
                    removed(&mut registry, "my_hook"),
                    removed(&mut registry, "missing"),
                    removed(&mut registry, "my_hook"),
                ),
                (Some(id), None, None)
            );
        }

        // Python: tests/test_turn_hooks.py::test_a_removal_takes_the_registration_of_its_own_scope
        #[test]
        fn a_removal_takes_the_registration_of_its_own_scope() {
            let mut registry = HookRegistry::new();
            let kit = registry.register(sync_hook("dup", 0)).unwrap();
            let in_room = registry
                .add_room_hook(room("room"), sync_hook("dup", 0))
                .unwrap();
            let room_s = registry.remove_room_hook(&room("room"), "dup");
            let names = registry.room_hook_names(&room("room"));
            let kit_s = registry.remove_global_hook("dup");
            assert_eq!(
                (
                    room_s.map(|removed| removed.id),
                    names,
                    kit_s.map(|removed| removed.id)
                ),
                (Some(in_room), Vec::<String>::new(), Some(kit))
            );
        }

        // Python: tests/test_hook_rules.py::TestHookNames::test_a_room_hook_may_reuse_a_kit_hook_s_name
        // and TestReplacingARoomHook::test_a_duplicate_within_one_room_is_refused
        #[test]
        fn a_name_is_unique_within_its_room_and_may_be_the_kit_s() {
            let mut registry = HookRegistry::new();
            registry.register(sync_hook("audit", 0)).unwrap();
            registry
                .add_room_hook(room("r1"), sync_hook("audit", 0))
                .unwrap();
            registry
                .add_room_hook(room("r2"), sync_hook("audit", 0))
                .unwrap();
            assert_eq!(
                registry.add_room_hook(room("r1"), sync_hook("audit", 0)),
                Err(RegistrationError::DuplicateName {
                    name: "audit".to_owned()
                })
            );
        }

        #[test]
        fn an_unnamed_room_hook_takes_the_first_name_free_in_its_room() {
            let mut registry = HookRegistry::new();
            registry.register(sync_hook("", 0)).unwrap();
            for _ in 0..2 {
                registry
                    .add_room_hook(room("r1"), sync_hook("", 0))
                    .unwrap();
            }
            assert_eq!(registry.room_hook_names(&room("r1")), ["hook", "hook#2"]);
        }

        // Python: tests/test_hook_rules.py::TestReplacingARoomHook::test_replaces_the_room_s_hook_of_the_same_name
        #[test]
        fn a_replacement_takes_the_place_of_the_room_s_hook_of_its_name() {
            let mut registry = HookRegistry::new();
            let first = registry
                .add_room_hook(room("r1"), sync_hook("router", 0))
                .unwrap();
            let (second, replaced) = registry
                .replace_room_hook(room("r1"), sync_hook("router", 3))
                .unwrap();
            let chain = registry.chain(HookTrigger::BeforeBroadcast, &event_in("r1"));
            assert_eq!(
                (replaced.map(|removed| removed.id), chain),
                (Some(first), vec![second])
            );
        }

        #[test]
        fn a_refused_replacement_leaves_the_hook_it_would_replace() {
            let mut registry = HookRegistry::new();
            registry
                .add_room_hook(room("r1"), sync_hook("audit", 0))
                .unwrap();
            let kept = registry
                .add_room_hook(room("r1"), off_lock_hook("scan", 0))
                .unwrap();
            assert!(matches!(
                registry.replace_room_hook(room("r1"), off_lock_hook("scan", 5)),
                Err(RegistrationError::LockOrdering { .. })
            ));
            assert_eq!(registry.off_lock_checks(&event_in("r1")), [kept]);
        }

        // Python: tests/test_turn_hooks.py::test_the_kit_s_engine_reads_the_turn_hooks_registered_through_it
        #[test]
        fn has_hooks_reads_the_kit_s_and_each_room_s() {
            let mut registry = HookRegistry::new();
            let empty = registry.has_hooks(None);
            let gate = HookSpec::new("gate", HookTrigger::BeforeToolUse, HookExecution::Sync);
            registry.add_room_hook(room("r1"), gate).unwrap();
            assert_eq!(
                (
                    empty,
                    registry.has_hooks(None),
                    registry.has_hooks(Some(HookTrigger::BeforeToolUse)),
                    registry.has_hooks(Some(HookTrigger::OnToolCall)),
                ),
                (false, true, true, false)
            );
        }

        // Python: tests/test_hooks.py::TestTriggerIndex::test_has_hooks_after_remove_global,
        // test_has_hooks_after_remove_room and test_trigger_index_survives_partial_removal
        #[test]
        fn has_hooks_follows_each_removal() {
            let mut registry = HookRegistry::new();
            let on = Some;
            registry.register(sync_hook("kept", 0)).unwrap();
            registry.register(sync_hook("removeme", 0)).unwrap();
            let observer = HookSpec::new(
                "removeme",
                HookTrigger::AfterBroadcast,
                HookExecution::Async,
            );
            registry.add_room_hook(room("room1"), observer).unwrap();
            registry.remove_global_hook("removeme");
            let partial = registry.has_hooks(on(HookTrigger::BeforeBroadcast));
            registry.remove_global_hook("kept");
            let emptied = registry.has_hooks(on(HookTrigger::BeforeBroadcast));
            let room_before = registry.has_hooks(on(HookTrigger::AfterBroadcast));
            registry.remove_room_hook(&room("room1"), "removeme");
            assert_eq!(
                (
                    partial,
                    emptied,
                    room_before,
                    registry.has_hooks(on(HookTrigger::AfterBroadcast))
                ),
                (true, false, true, false)
            );
        }

        #[test]
        fn observers_start_by_priority_the_kit_s_first_at_equal_priority() {
            let mut registry = HookRegistry::new();
            let observer = |name: &str, priority| HookSpec {
                priority,
                ..HookSpec::new(name, HookTrigger::AfterBroadcast, HookExecution::Async)
            };
            registry.register(observer("kit-late", 5)).unwrap();
            registry.register(observer("kit-early", 0)).unwrap();
            registry
                .add_room_hook(room("r1"), observer("room-first", -1))
                .unwrap();
            registry
                .add_room_hook(room("r1"), observer("room-tie", 0))
                .unwrap();
            let observers = registry.observers(HookTrigger::AfterBroadcast, &event_in("r1"));
            assert_eq!(
                names(&registry, &observers),
                ["room-first", "kit-early", "room-tie", "kit-late"]
            );
        }

        #[test]
        fn a_removed_registration_is_held_while_a_copy_of_the_registry_holds_it() {
            let mut registry = HookRegistry::new();
            registry.register(sync_hook("guard", 0)).unwrap();
            let read_before = registry.clone();
            let removed = registry.remove_global_hook("guard").unwrap();
            let held = removed.held.upgrade().is_some();
            drop(read_before);
            assert_eq!((held, removed.held.upgrade().is_some()), (true, false));
        }

        #[test]
        fn a_removed_hook_s_id_is_never_given_again() {
            let mut registry = HookRegistry::new();
            let first = registry.register(sync_hook("first", 0)).unwrap();
            registry.remove_global_hook("first");
            let second = registry.register(sync_hook("second", 0)).unwrap();
            assert_ne!(first, second);
        }

        #[test]
        fn the_lock_ordering_holds_in_each_room_s_chain_alone() {
            let mut registry = HookRegistry::new();
            registry
                .add_room_hook(room("r1"), sync_hook("audit", 0))
                .unwrap();
            assert!(matches!(
                registry.add_room_hook(room("r1"), off_lock_hook("scan", 1)),
                Err(RegistrationError::LockOrdering { .. })
            ));
            assert!(matches!(
                registry.register(off_lock_hook("early", 1)),
                Err(RegistrationError::LockOrdering { .. })
            ));
            assert!(
                registry
                    .add_room_hook(room("r2"), off_lock_hook("scan", 1))
                    .is_ok()
            );
        }

        #[test]
        fn a_tool_call_of_a_room_s_turn_fires_that_room_s_tool_hooks() {
            let mut registry = HookRegistry::new();
            let gate = HookSpec::new("approve", HookTrigger::BeforeToolUse, HookExecution::Sync);
            let id = registry.add_room_hook(room("r1"), gate).unwrap();
            let (sms, kind) = (ChannelId::new("sms").unwrap(), ChannelType::SMS);
            let subject = HookSubject::of_channel(&sms, &kind);
            let (r1, r2) = (room("r1"), room("r2"));
            assert_eq!(
                (
                    registry.chain(HookTrigger::BeforeToolUse, subject.in_room(&r1)),
                    registry.chain(HookTrigger::BeforeToolUse, subject.in_room(&r2)),
                ),
                (vec![id], vec![])
            );
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
