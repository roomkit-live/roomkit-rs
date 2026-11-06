//! Step 5 of the inbound pipeline: who sent the message (RFC §10.1 step 5,
//! §11.1 to §11.6), as the Python implementation's `InboundIdentityMixin`.
//!
//! Once its channel parsed the message, the kit's identity resolver is asked
//! who sent it, within the identity timeout, past which the sender reads as
//! UNKNOWN (RFC §11.5), unless nothing is left to resolve: the kit resolves
//! no sender of the channel's type (RFC §11.4), the channel names its own
//! senders, or the room has already identified this one (RFC §11.6). The
//! resolver's answer, its address and channel type filled in where it left
//! them unset, decides:
//!
//! - `IDENTIFIED`: the event is stamped with the identity;
//! - `AMBIGUOUS` or `PENDING`: the identity hooks of `ON_IDENTITY_AMBIGUOUS`
//!   decide, then the hooks registered on it observe; unnamed, the sender
//!   is pending, with the resolver's candidates;
//! - `UNKNOWN` or `REJECTED`: those of `ON_IDENTITY_UNKNOWN` do; a
//!   rejection none of them answers for holds the message (RFC §11.2);
//! - `CHALLENGE_SENT`: the resolver put the question itself, and the
//!   message is held.
//!
//! A hook's challenge is committed as an injected event, in the call's
//! cascade, and the message is held `identity_challenge_sent`; a rejection
//! holds it with its reason. A held message is refused with nothing of it
//! stored. An identity found is announced (`identity_resolved`).
//!
//! What resolution decides of the sender's record is written by the
//! trigger's pass under the room lock, once the status gate let the event
//! through (RFC §10.1 step 6): an identified sender's record created, with
//! the record of its join, or completed; a pending one's created
//! ([`writes`]).

use std::collections::VecDeque;

use super::deadline::Stopped;
use super::{InboundError, InboundResult, Machine, Stage, queues::Lineage};
use crate::model::{
    ChannelType, IdentificationStatus, Identity, IdentityHookResult, IdentityResult,
    InboundMessage, InjectedEvent, Participant, ParticipantId, RoomContext, RoomEvent,
};
use crate::ops::io::Bound;
use crate::ops::lifecycle::record;
use crate::ops::members::channels_reached;
use crate::ops::protocol::{EffectId, IdentityRequest, Outcome, StoreRequest};
use crate::ops::{ChannelConfig, Cx, FrameworkEvent, KitConfig, Notice, Progress};
use crate::rules::authors;
use crate::rules::hook_registry::HookTrigger;
use crate::rules::records::Record;

/// The reason a challenged message is held with (RFC §11.3).
const CHALLENGE_SENT: &str = "identity_challenge_sent";

/// What the trigger's pass writes of the sender, as resolution left them.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum Participation {
    /// The sender is this identity: its record is created identified, or
    /// completed.
    Identified(Box<Identity>),
    /// The sender is left pending, with the resolution's candidates.
    Pending(Box<IdentityResult>),
}

/// A message resolution holds: refused with `reason`, nothing of it stored,
/// once `challenge`, the question a hook put to the sender, is committed.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Held {
    pub(super) reason: String,
    pub(super) challenge: Option<Box<InjectedEvent>>,
}

/// What step 5 comes to.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum Identified {
    /// The event goes on, with what its pass writes of its sender.
    Proceed(Option<Participation>),
    /// The message is held.
    Held(Held),
}

/// The event and the room's context as step 5 left them, and what it
/// decided.
#[derive(Debug)]
pub(super) struct Resolved {
    pub(super) event: RoomEvent,
    pub(super) context: Box<RoomContext>,
    pub(super) identified: Identified,
}

/// Step 5 for one message: the resolver asked, then the identity hooks.
#[derive(Debug)]
pub(super) struct Identification {
    event: RoomEvent,
    context: Box<RoomContext>,
    /// The message's sender, the address an answer that names none is
    /// given.
    sender_id: String,
    channel_type: ChannelType,
    /// The call's deadline (RFC §13.6), which every request of step 5
    /// carries, as the parse's did.
    bound: Bound,
    state: State,
}

#[derive(Debug)]
enum State {
    /// The resolver is asked.
    Resolving(EffectId),
    /// The identity hooks of `trigger` run on the resolver's answer.
    Hooking {
        effect: EffectId,
        trigger: HookTrigger,
        result: Box<IdentityResult>,
    },
    Done,
}

/// Whether step 5 runs for `event`, which `channel` parsed, in the room
/// `context` holds: the kit resolves senders of the channel's type (RFC
/// §11.4), the channel does not name its own senders, and the room has not
/// identified this one on the channel (RFC §11.6).
pub(super) fn wanted(
    config: &KitConfig,
    channel: &ChannelConfig,
    event: &RoomEvent,
    context: &RoomContext,
) -> bool {
    config.identity_enabled_for(&channel.channel_type)
        && !channel.sender_is_participant
        && !authors::sender_identified(event, &context.participants)
}

/// `text` unless it is empty: an empty reason or message is none, as the
/// Python implementation's `or` reads it.
fn given(text: Option<String>) -> Option<String> {
    text.filter(|text| !text.is_empty())
}

/// `event` sent under the sender's own record when that is the record
/// their address names on the channel and the room identified it, as a
/// pending sender's events are sent, so every reader finds the record an
/// advisor identified (RFC §11.2, §11.6 case 2); left as the channel named
/// it otherwise. Step 5 skipped, the kit resolving the channel's senders.
pub(super) fn as_the_record_named(
    config: &KitConfig,
    channel: &ChannelConfig,
    event: &mut RoomEvent,
    context: &RoomContext,
) {
    if !config.identity_enabled_for(&channel.channel_type) || channel.sender_is_participant {
        return;
    }
    if let Some(own) = authors::own_record_identified(event, &context.participants) {
        event.source.participant_id = Some(own);
    }
}

impl Identification {
    /// Asks the kit's resolver who sent `message`, which its channel parsed
    /// to `event` in the room `context` holds, within the call's `bound`.
    pub(super) fn start(
        cx: &mut Cx<'_>,
        event: RoomEvent,
        context: Box<RoomContext>,
        message: &InboundMessage,
        bound: Bound,
    ) -> Self {
        let timeout = cx
            .config
            .identity
            .as_ref()
            .map(|identity| identity.timeout)
            .unwrap_or_default();
        let request = IdentityRequest::Resolve {
            message: Box::new(message.clone()),
            context: context.clone(),
            timeout,
        };
        let effect = cx.io.issue_within(request, bound);
        Self {
            channel_type: event.source.channel_type.clone(),
            sender_id: message.sender_id.clone(),
            event,
            context,
            bound,
            state: State::Resolving(effect),
        }
    }

    /// Whether step 5 waits for `effect`.
    pub(super) fn awaits(&self, effect: EffectId) -> bool {
        match &self.state {
            State::Resolving(awaited)
            | State::Hooking {
                effect: awaited, ..
            } => *awaited == effect,
            State::Done => false,
        }
    }

    /// Takes the driver's answer to step 5's outstanding effect.
    pub(super) fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        effect: EffectId,
        outcome: Outcome,
    ) -> Progress<Result<Resolved, Stopped>> {
        match std::mem::replace(&mut self.state, State::Done) {
            State::Resolving(expected) if expected == effect => self.on_resolved(cx, outcome),
            State::Hooking {
                effect: expected,
                trigger,
                result,
            } if expected == effect => self.on_hooked(cx, trigger, *result, outcome),
            state @ (State::Resolving(_) | State::Hooking { .. } | State::Done) => {
                debug_assert!(false, "step 5 received {effect:?} in {state:?}");
                self.state = state;
                Progress::Waiting
            }
        }
    }

    /// The resolver answered, or its timeout passed, which reads as UNKNOWN
    /// (RFC §11.5); a resolver that raised fails the message, with nothing
    /// written.
    fn on_resolved(
        &mut self,
        cx: &mut Cx<'_>,
        outcome: Outcome,
    ) -> Progress<Result<Resolved, Stopped>> {
        let answered = match outcome.into_sender() {
            Ok(answered) => answered,
            Err(error) => {
                return Progress::Done(Err(Stopped::Failed(InboundError::Identity(error))));
            }
        };
        let result = if let Some(result) = answered {
            *result
        } else {
            self.report_timeout(cx);
            IdentityResult::new(IdentificationStatus::Unknown)
        };
        let result = self.completed(result);
        match result.status {
            IdentificationStatus::Identified => match result.identity.clone() {
                Some(identity) => self.identified(cx, identity, &result),
                None => self.proceed(None),
            },
            IdentificationStatus::Ambiguous | IdentificationStatus::Pending => {
                self.ask_hooks(cx, HookTrigger::OnIdentityAmbiguous, result)
            }
            IdentificationStatus::ChallengeSent => {
                let reason = given(result.message).unwrap_or_else(|| CHALLENGE_SENT.to_owned());
                self.hold(reason, None)
            }
            IdentificationStatus::Unknown | IdentificationStatus::Rejected => {
                self.ask_hooks(cx, HookTrigger::OnIdentityUnknown, result)
            }
        }
    }

    /// Emits `identity_timeout` (RFC §11.5).
    fn report_timeout(&self, cx: &mut Cx<'_>) {
        let timeout = cx
            .config
            .identity
            .as_ref()
            .map(|identity| identity.timeout.as_secs_f64())
            .unwrap_or_default();
        let event = FrameworkEvent::identity_timeout(
            self.event.room_id.clone(),
            self.event.source.channel_id.clone(),
            timeout,
        );
        cx.io.notify(Notice::FrameworkEvent(event));
    }

    /// `result` with the sender's address and its channel's type where the
    /// resolver left them unset: what the identity hooks read (RFC §11.2).
    fn completed(&self, mut result: IdentityResult) -> IdentityResult {
        if result.address.is_none() {
            result.address = Some(self.sender_id.clone());
        }
        if result.channel_type.is_none() {
            result.channel_type = Some(self.channel_type.clone());
        }
        result
    }

    /// Runs the identity hooks of `trigger` on `result`.
    fn ask_hooks(
        &mut self,
        cx: &mut Cx<'_>,
        trigger: HookTrigger,
        result: IdentityResult,
    ) -> Progress<Result<Resolved, Stopped>> {
        let request = IdentityRequest::RunHooks {
            trigger,
            event: Box::new(self.event.clone()),
            context: self.context.clone(),
            result: Box::new(result.clone()),
        };
        let effect = cx.io.issue_within(request, self.bound);
        self.state = State::Hooking {
            effect,
            trigger,
            result: Box::new(result),
        };
        Progress::Waiting
    }

    /// The identity hooks answered: the hooks registered on the trigger
    /// observe, then the first answer decides (RFC §11.2, §11.3).
    fn on_hooked(
        &mut self,
        cx: &mut Cx<'_>,
        trigger: HookTrigger,
        result: IdentityResult,
        outcome: Outcome,
    ) -> Progress<Result<Resolved, Stopped>> {
        let answer = match outcome.into_hooked() {
            Ok(answer) => answer.map(|answer| *answer),
            Err(error) => {
                return Progress::Done(Err(Stopped::Failed(InboundError::Identity(error))));
            }
        };
        cx.fire_observers(trigger, &self.event, &self.context);
        if trigger == HookTrigger::OnIdentityAmbiguous {
            self.on_ambiguous(cx, answer, result)
        } else {
            self.on_unknown(cx, answer, result)
        }
    }

    /// `ON_IDENTITY_AMBIGUOUS`'s answer: an identity names the sender, a
    /// challenge or a rejection holds the message, anything else leaves the
    /// sender pending with the resolver's candidates.
    fn on_ambiguous(
        &mut self,
        cx: &mut Cx<'_>,
        answer: Option<IdentityHookResult>,
        result: IdentityResult,
    ) -> Progress<Result<Resolved, Stopped>> {
        let Some(answer) = answer else {
            return self.pending(result);
        };
        match (answer.status, answer.identity) {
            (IdentificationStatus::Identified, Some(identity)) => {
                self.identified(cx, identity, &result)
            }
            (IdentificationStatus::ChallengeSent, _) => {
                self.hold(CHALLENGE_SENT.to_owned(), answer.inject.map(Box::new))
            }
            (IdentificationStatus::Rejected, _) => {
                let reason = given(answer.reason).unwrap_or_else(|| "identity_rejected".to_owned());
                self.hold(reason, None)
            }
            _ => self.pending(result),
        }
    }

    /// `ON_IDENTITY_UNKNOWN`'s answer: a rejection or a challenge holds the
    /// message, an identity names the sender, a pending answer leaves them
    /// pending with its candidates, else the resolver's; anything else is
    /// [`unanswered`](Self::unanswered).
    fn on_unknown(
        &mut self,
        cx: &mut Cx<'_>,
        answer: Option<IdentityHookResult>,
        mut result: IdentityResult,
    ) -> Progress<Result<Resolved, Stopped>> {
        let Some(answer) = answer else {
            return self.unanswered(result);
        };
        match (answer.status, answer.identity) {
            (IdentificationStatus::Rejected, _) => {
                let reason = given(answer.reason).unwrap_or_else(|| "unknown_sender".to_owned());
                self.hold(reason, None)
            }
            (IdentificationStatus::Identified, Some(identity)) => {
                self.identified(cx, identity, &result)
            }
            (IdentificationStatus::ChallengeSent, _) => {
                self.hold(CHALLENGE_SENT.to_owned(), answer.inject.map(Box::new))
            }
            (IdentificationStatus::Pending, _) => {
                result.status = IdentificationStatus::Pending;
                if let Some(candidates) = answer.candidates.filter(|c| !c.is_empty()) {
                    result.candidates = candidates;
                }
                self.pending(result)
            }
            _ => self.unanswered(result),
        }
    }

    /// No identity hook decided about a sender the resolver did not name: a
    /// rejection, the resolver's refusal, holds the message with its message,
    /// else `identity_rejected`; an unknown sender's message goes on, its
    /// sender unresolved (RFC §11.2).
    fn unanswered(&mut self, result: IdentityResult) -> Progress<Result<Resolved, Stopped>> {
        if result.status == IdentificationStatus::Rejected {
            let reason = given(result.message).unwrap_or_else(|| "identity_rejected".to_owned());
            return self.hold(reason, None);
        }
        self.proceed(None)
    }

    /// The sender is `identity`: the event is stamped with it, and its
    /// resolution announced, `result` its status (RFC §8.2).
    fn identified(
        &mut self,
        cx: &mut Cx<'_>,
        identity: Identity,
        result: &IdentityResult,
    ) -> Progress<Result<Resolved, Stopped>> {
        if let Ok(id) = ParticipantId::new(identity.id.as_str()) {
            self.event.source.participant_id = Some(id);
        }
        let announced = FrameworkEvent::identity_resolved(&self.event, &identity, result.status);
        cx.io.notify(Notice::FrameworkEvent(announced));
        self.proceed(Some(Participation::Identified(Box::new(identity))))
    }

    /// The sender is left pending with `result`, the event sent under the
    /// sender's own record ([`authors::own_record_id`]), or under the room's
    /// record of the id the channel gave when one was reached through the
    /// channel (a member the host added). The pipeline never creates a
    /// record under the id the channel gave (RFC §11.2, RMK-1138): on a
    /// channel whose senders choose it, that id may be an identity's or
    /// another participant's, and a pending record under an identity's id
    /// would be completed when the identity is resolved elsewhere, its
    /// impostor then passing for an identified sender (RFC §11.6).
    fn pending(&mut self, result: IdentityResult) -> Progress<Result<Resolved, Stopped>> {
        if authors::names_its_own_id(&self.event, &self.context.participants) {
            return self.proceed(Some(Participation::Pending(Box::new(result))));
        }
        let source = &mut self.event.source;
        if let Some(sender) = &source.participant_id {
            let own = authors::own_record_id(source.channel_id.as_str(), sender.as_str());
            if let Ok(own) = ParticipantId::new(own) {
                source.participant_id = Some(own);
            }
        }
        self.proceed(Some(Participation::Pending(Box::new(result))))
    }

    fn proceed(
        &mut self,
        participation: Option<Participation>,
    ) -> Progress<Result<Resolved, Stopped>> {
        self.done(Identified::Proceed(participation))
    }

    fn hold(
        &mut self,
        reason: String,
        challenge: Option<Box<InjectedEvent>>,
    ) -> Progress<Result<Resolved, Stopped>> {
        self.done(Identified::Held(Held { reason, challenge }))
    }

    fn done(&mut self, identified: Identified) -> Progress<Result<Resolved, Stopped>> {
        self.state = State::Done;
        let room = self.context.room.clone();
        let context = std::mem::replace(&mut self.context, Box::new(RoomContext::new(room)));
        Progress::Done(Ok(Resolved {
            event: self.event.clone(),
            context,
            identified,
        }))
    }
}

/// The writes that make `event`'s sender a participant of the room as
/// `participation` says, on the room `context` holds under the lock, which
/// then reflects them: an identified sender's record created, with the
/// record of its join, or completed with its identity and the channel it
/// came through; a pending sender's created, with the record of its join,
/// unless the room holds it already (RFC §10.1 step 5, §5.5).
pub(super) fn writes(
    cx: &Cx<'_>,
    participation: Participation,
    event: &RoomEvent,
    context: &mut RoomContext,
) -> VecDeque<StoreRequest> {
    match participation {
        Participation::Identified(identity) => identified_writes(cx, &identity, event, context),
        Participation::Pending(result) => pending_writes(cx, &result, event, context),
    }
}

fn identified_writes(
    cx: &Cx<'_>,
    identity: &Identity,
    event: &RoomEvent,
    context: &mut RoomContext,
) -> VecDeque<StoreRequest> {
    let mut writes = VecDeque::new();
    let Ok(id) = ParticipantId::new(identity.id.as_str()) else {
        return writes;
    };
    let channel_id = &event.source.channel_id;
    if let Some(existing) = context.participants.iter_mut().find(|p| p.id == id) {
        let mut changed = false;
        if existing.identification != IdentificationStatus::Identified {
            existing.identification = IdentificationStatus::Identified;
            existing.identity_id = Some(identity.id.clone());
            if let Some(name) = given(identity.display_name.clone()) {
                existing.display_name = Some(name);
            }
            changed = true;
        }
        if let Some(channels) = channels_reached(existing, channel_id) {
            existing.connected_via = channels;
            changed = true;
        }
        if changed {
            writes.push_back(StoreRequest::UpdateParticipant {
                participant: Box::new(existing.clone()),
            });
        }
        return writes;
    }
    let mut participant = Participant::at(
        id.clone(),
        context.room.id.clone(),
        channel_id.clone(),
        cx.env.now(),
    );
    participant.display_name.clone_from(&identity.display_name);
    participant.identification = IdentificationStatus::Identified;
    participant.identity_id = Some(identity.id.clone());
    joined(
        cx,
        participant,
        &Record::JoinedIdentified(id),
        context,
        &mut writes,
    );
    writes
}

fn pending_writes(
    cx: &Cx<'_>,
    result: &IdentityResult,
    event: &RoomEvent,
    context: &mut RoomContext,
) -> VecDeque<StoreRequest> {
    let mut writes = VecDeque::new();
    let channel_id = &event.source.channel_id;
    let named = event
        .source
        .participant_id
        .as_ref()
        .map_or_else(|| fresh_pending_id(cx), ParticipantId::to_string);
    let mut id = named.clone();
    if let Some(existing) = context.participants.iter().find(|p| p.id.as_str() == id)
        && !existing.connected_via.contains(channel_id)
    {
        // Reached elsewhere since step 5 read the roster.
        id = authors::own_record_id(channel_id.as_str(), &named);
    }
    if context.participants.iter().any(|p| p.id.as_str() == id) {
        return writes;
    }
    let Ok(id) = ParticipantId::new(id) else {
        return writes;
    };
    let mut participant = Participant::at(
        id.clone(),
        context.room.id.clone(),
        channel_id.clone(),
        cx.env.now(),
    );
    participant.identification = IdentificationStatus::Pending;
    participant.candidates = (!result.candidates.is_empty())
        .then(|| result.candidates.iter().map(|c| c.id.clone()).collect());
    joined(
        cx,
        participant,
        &Record::JoinedPending(id),
        context,
        &mut writes,
    );
    writes
}

/// Queues `participant`'s creation and the record of its join, the room
/// `context` holds reflecting both.
fn joined(
    cx: &Cx<'_>,
    participant: Participant,
    joined: &Record,
    context: &mut RoomContext,
    writes: &mut VecDeque<StoreRequest>,
) {
    context.participants.push(participant.clone());
    writes.push_back(StoreRequest::AddParticipant {
        participant: Box::new(participant),
    });
    if let Some(commit) = record::commit(cx, context, joined) {
        writes.push_back(commit);
    }
}

/// The id of a pending sender the event names no one for, as the Python
/// implementation writes it: `pending-` and eight hexadecimal digits.
fn fresh_pending_id(cx: &Cx<'_>) -> String {
    let fresh = cx.env.new_event_id();
    let digits: String = fresh
        .as_str()
        .chars()
        .filter(char::is_ascii_hexdigit)
        .take(8)
        .collect();
    format!("pending-{digits}")
}

impl Machine {
    /// Holds the message step 5 refused: the challenge a hook put to the
    /// sender is committed as an injected event of the call's cascade
    /// (RFC §11.3, §9.4), and the call is refused with `held`'s reason,
    /// nothing of its message stored, once that cascade is over.
    pub(super) fn hold_for_identity(&mut self, cx: &mut Cx<'_>, held: Held, event: &RoomEvent) {
        self.trigger.result = InboundResult {
            blocked: true,
            reason: Some(held.reason),
            refused_before_the_lock: true,
            // Step 2's failed release, the first error the call met (RFC §13.5).
            error: self.trigger.result.error.take(),
            ..InboundResult::default()
        };
        self.trigger.decided = true;
        let challenge: Vec<InjectedEvent> = held.challenge.into_iter().map(|c| *c).collect();
        for job in self.injected(cx, challenge, event, Lineage::Call) {
            self.cascade.queue(job);
        }
        self.stage.enter(Stage::Cascade);
    }
}
