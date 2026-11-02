//! The inbound pipeline (RFC §10.1), with the broadcast of every event it
//! commits (RFC §10.2), for a message and for an event injected directly
//! (RFC §10.5).
//!
//! [`Inbound`] runs one call into a room: a message into the room the
//! caller names or the room the kit's router finds for it (RFC §10.4), or
//! an event the integrator injects into a room ([`Inbound::inject`]), and
//! the whole cascade it starts. A message arrives first (steps 1 to 4): its
//! sender in the channel's form, its room, named or routed, created,
//! attached to or recorded on when it must be, the room's context and the
//! event the channel parses (`arrival`). A direct injection skips the
//! channel's parse and step 2 (`direct`). Then the cascade: the call's
//! pass, its delivery set, the responses that re-enter as passes of their
//! own, the events hooks inject, and the records the room stores in place of
//! responses it refuses (`cascade`). The stages a call goes through, and
//! the order they follow, are written once (`stage`).
//!
//! The room's delivery lane is entered at each commit (RFC §10.1 step 12,
//! §10.2): an event delivered takes its place under the room lock, and its
//! delivery set waits for that place's turn once the lock is released, so
//! one event's set completes before the next one's begins, whichever call
//! committed it. Nothing of the lane comes before the commit: two calls
//! into a room overlap up to the room lock, and commit in lock order. The
//! operation runs its passes and its sets as two queues, one of each at a
//! time (`queues`).
//!
//! A call into the room that the operation's own work makes under the
//! room's lock or in its admission order, a hook or a parse calling
//! `send_event` or `process_inbound` for the room, is adopted (`adoption`):
//! the operation runs it as an entry of its own, beside its work, or nested
//! in the pass whose hook made it, so that it commits ahead of that pass's
//! event; its caller receives its result at its commit, and its delivery
//! set takes its place in the lane at that commit (RFC §13.5, §10.1 step 18,
//! gap G-070). The queues thus carry several cascades: the call's, and one
//! per adopted entry. A turn's or a delivery's call is a new arrival, an
//! operation of its own (RFC §9.5.1 Reentrance), drawing on the reentry
//! budget of the cascade that delivers (gap G-070), whose caller may take
//! the call's decision, published once it is known (`Step::decided`),
//! rather than wait for its delivery.
//!
//! Every pass checks an edit or a delete against its target (RFC §10.1 step
//! 8a, §10.3), updates the target in its commit, and fires `ON_EVENT_UPDATED`
//! or `ON_EVENT_DELETED` with the target as the commit left it, once the lock
//! is released.
//!
//! A turn whose response is a stream is read once the cascade's passes and
//! sets are over, its rows committed as its reader reads past them and
//! delivered to the other targets while it goes on (RFC §6.1, §12.2 steps
//! 13s and 14s, `stream`).
//!
//! An instruction (RFC §10.1.1) takes the same way without being stored:
//! refused before anything is read when it names no agent or carries a key,
//! decided by its pass without a commit, delivered to the agents it names,
//! and fingerprinted on every response it produces (`instructions`).
//!
//! The call's off-lock hooks run before the room lock, ordered by the
//! room's admission ticket, and step 9 applies their outcome (RFC §9.5.1,
//! `off_lock`). The kit's `process_timeout` bounds the call from its start
//! to its decision: past it, nothing of the event is written, what the call
//! holds is released, and the call is refused `process_timeout` (RFC §13.6,
//! `deadline`).

mod adoption;
mod answering;
mod answers;
mod arrival;
mod arrival_change;
mod cascade;
mod chain;
mod charge;
mod commit;
mod deadline;
mod delivery;
mod direct;
mod edits;
mod entry;
mod holds;
mod host_post;
mod identity;
mod instructions;
mod job;
mod named_room;
mod off_lock;
mod pass;
mod persist;
mod queues;
mod record;
mod report;
mod rerun;
mod result;
mod stage;
mod stamp;
mod stream;
mod verification;
mod work_read;

use std::collections::BTreeMap;
use std::sync::Arc;

use self::adoption::Adoptions;
pub use self::answering::TurnResponses;
pub use self::answers::MessageAnswers;
pub use self::arrival::InboundTarget;
use self::cascade::Trigger;
use self::persist::SideEffects;
use self::queues::Cascade;
use self::stage::{CallStage, Stage};
pub use self::stamp::default_inbound_event;
pub(crate) use self::stamp::error_event;
use super::io::Io;
use super::protocol::{Decided, EffectId, Entry, EntryId, Operation, Outcome, ProtocolError, Step};
use super::{Cx, Environment, KitConfig};
use crate::model::{ChannelId, DirectEvent, EventId, InboundMessage, Post, RoomId};
use crate::ops::io::Bound;
pub use crate::ops::protocol::{InboundError, InboundResult};
use crate::rules::chain_depth::{Draw, ReentryBudget};

/// The inbound pipeline for one call (RFC §10.1, §10.5).
///
/// ```
/// # use std::sync::Arc;
/// # use roomkit_core::model::{ChannelId, InboundMessage, RoomId, TextContent};
/// # use roomkit_core::ops::{Inbound, InboundTarget, KitConfig, Next, Operation, SequentialEnvironment};
/// let config = Arc::new(KitConfig::new());
/// let message = InboundMessage::new(ChannelId::new("sms")?, "+15551234567", TextContent::new("Hi"));
/// let room = InboundTarget::Room(RoomId::new("room_1")?);
/// let mut inbound = Inbound::new(config, room, None, message);
/// let step = inbound.poll(&SequentialEnvironment::default());
/// // No channel `sms` is registered: the operation ends at once.
/// assert!(matches!(step.next, Next::Ready(Err(_))));
/// # Ok::<(), roomkit_core::model::ModelError>(())
/// ```
#[derive(Debug)]
pub struct Inbound {
    config: Arc<KitConfig>,
    io: Io,
    /// Boxed: a driver holds the operation across its awaits, and the
    /// call's state inline would make the future of every call that drives
    /// one large.
    machine: Box<Machine>,
}

impl Inbound {
    /// The pipeline for `message`, into the room `target` names or the room
    /// it is routed to, for a caller acting for `organization_id` (RFC
    /// §17.2): a room of another organization fails the message, a room the
    /// call creates is that organization's, and routing reads that
    /// organization's rooms and identities only. A call that names no
    /// organization reads rooms unscoped (gap G-051).
    pub fn new(
        config: Arc<KitConfig>,
        target: InboundTarget,
        organization_id: Option<String>,
        message: InboundMessage,
    ) -> Self {
        let call = Call::Message {
            target,
            organization_id,
            message: Box::new(message),
            answers: None,
        };
        Self::calling(config, call)
    }

    /// [`new`](Self::new) for a message whose channel answered `answers`
    /// of it as it entered (RFC §5.7, §10.4): the message is routed by
    /// their conversation, and its sender recorded where its replies go by
    /// them, where [`new`](Self::new) reads its channel's declaration.
    pub fn answered(
        config: Arc<KitConfig>,
        target: InboundTarget,
        organization_id: Option<String>,
        message: InboundMessage,
        answers: MessageAnswers,
    ) -> Self {
        let call = Call::Message {
            target,
            organization_id,
            message: Box::new(message),
            answers: Some(Box::new(answers)),
        };
        Self::calling(config, call)
    }

    /// The pipeline for `event`, injected directly into `room_id` (RFC
    /// §10.5) by a caller acting for `organization_id` (RFC §17.2): the same
    /// pipeline as a message's from the room lock on, its off-lock check
    /// included, without `handle_inbound()`, routing, the room's creation or
    /// an attach (gap G-069). A call that names no organization reads the
    /// room unscoped.
    ///
    /// Its output is the committed event's result, a record stored
    /// `BLOCKED` included, once its delivery set is over. A refusal before
    /// the commit is an error with nothing written: a room that does not
    /// exist, or of another organization, or of none for a caller that
    /// names one, [`InboundError::RoomNotFound`] (gap G-077); otherwise
    /// [`InboundError::Refused`]: a channel the room does not bind
    /// (`source_unbound`), a room whose status refuses events, the call's
    /// `process_timeout`, a refused edit or delete, a refused instruction.
    pub fn inject(
        config: Arc<KitConfig>,
        room_id: RoomId,
        organization_id: Option<String>,
        event: DirectEvent,
    ) -> Self {
        let call = Call::Direct {
            room_id,
            organization_id,
            event: Box::new(event),
        };
        Self::calling(config, call)
    }

    /// The pipeline for `post`, the host's own words in `room_id` (RFC
    /// §10.5 `post()`), for a caller acting for `organization_id` (RFC
    /// §17.2): injected as [`inject`](Self::inject) injects, from the host
    /// channel `post` names, or the kit's own (`host`), which must be a
    /// registered channel of type `HOST`. The room is read scoped first,
    /// then the speaker checked; a room that never bound the host channel
    /// gets it attached in one section under the room lock, a binding made
    /// by then kept. One `process_timeout` bounds the call from its start
    /// to its commit, the first post's wait for the lock included.
    ///
    /// Its output is a direct injection's. A refusal before its commit is
    /// an error with nothing written: [`InboundError::UnknownChannel`],
    /// [`InboundError::NotAHostChannel`],
    /// [`InboundError::RoomNotFound`] for a room missing or of another
    /// organization, [`InboundError::ParticipantNotFound`] for a speaker
    /// the host channel does not reach, and [`InboundError::Refused`] as an
    /// injection's: `room_closed`, `source_unbound` for a room the host
    /// channel was detached from, `process_timeout`.
    pub fn post(
        config: Arc<KitConfig>,
        room_id: RoomId,
        organization_id: Option<String>,
        post: Post,
    ) -> Self {
        let call = Call::Post {
            room_id,
            organization_id,
            post: Box::new(post),
        };
        Self::calling(config, call)
    }

    /// The pipeline for `responses`, the answers of turns the caller asked
    /// itself on an event it committed outside the pipeline: a delegated
    /// turn's, in a child room a transport is shared into (RFC §23.3 step
    /// 5). Each buffered response re-enters as a room's response does (RFC
    /// §10.1 steps 9 to 14), crossing `BEFORE_BROADCAST` and its source's
    /// right to write (RFC §7.5 rule 2), and rides the room's delivery
    /// lane; each stream is read by the room's reader once they are over
    /// (RFC §12.2 steps 13s and 14s); the cascade they start runs to its
    /// end, as a message's does. The event they answer is committed
    /// already, so nothing refuses the call: a failure is its result's
    /// `error`.
    ///
    /// Its output is the cascade's result: the responses the room kept and
    /// delivered (`response_events`), and the first error met, a stream's
    /// failure among them.
    pub fn answering(config: Arc<KitConfig>, responses: TurnResponses) -> Self {
        let call = Call::Responses {
            responses: Box::new(responses),
        };
        Self::calling(config, call)
    }

    /// The room's agents run again on `event_id`, an event `room_id`
    /// stores, as on a new message of its author (RFC §11.7.6, §10.1 step
    /// 14): Python's `_rerun_agents_on`, which a held message's release
    /// makes. The room's lock is waited for within `process_timeout`, past
    /// which the call is refused `process_timeout`; a room whose status
    /// refuses new events refuses it `room_closed` before any agent runs
    /// (RFC §5.1). An event that is not in the room, is deleted or is a
    /// change itself (RFC §10.3), or whose source binding is gone, is no
    /// transport or cannot write, runs nothing: the result names no event.
    /// Otherwise the event is handed to the room's agents alone, at least
    /// `floor` deep (RFC §8.3), its place in the room's lane behind every
    /// event committed; their answers are committed and delivered as a
    /// message's, and the result names the event and holds them. Nothing of
    /// the event's first pass runs again: no delivery report, no
    /// `AFTER_BROADCAST`, no `event_processed`.
    pub fn rerun(config: Arc<KitConfig>, room_id: RoomId, event_id: EventId, floor: u32) -> Self {
        let call = Call::Rerun {
            room_id,
            event_id,
            floor,
        };
        Self::calling(config, call)
    }

    /// The same call, made by the code a delivery set runs, a turn or a
    /// transport's delivery, drawing on `budget`, the reentry budget of the
    /// cascade that delivers, rather than on one of its own (RFC §8.3, gap
    /// G-070). The call is charged as it starts, as an entry adopted from
    /// that cascade would be: a message first thing, before its channel is
    /// looked up, refused with no event past the budget (RFC §10.1 step 1);
    /// an injected event at its pass, stored `BLOCKED` with
    /// `reentry_loop_cap` past it, its hooks never run. Its own cascade draws
    /// on the same budget, so that a channel that calls into a room on every
    /// delivery stops at the cap.
    /// The call keeps its own admission ticket and `process_timeout`.
    #[must_use]
    pub fn drawing_on(mut self, draw: Draw) -> Self {
        self.machine.cascade.draw_on(draw);
        self
    }

    /// The same message, sent by the code a delivery set of `room_id`
    /// runs into a room it does not name as `room_id`, or that the router
    /// places: it is charged one pass on `budget`, that cascade's, once its
    /// final room is known (RFC §9.5.1, §10.1 step 2, gap G-070). A room
    /// step 2 creates (routed to none, or to a room the store lacks, or
    /// naming one the store lacks) is the delivery's own work: charged
    /// before it is created, its cascade then draws on `budget` as
    /// [`drawing_on`](Self::drawing_on)'s does; when another call created
    /// it first, the charge stays and the cascade has a budget of its own.
    /// A routed room that exists is charged once step 2's locked check kept
    /// it for the message, after any re-route: `room_id` on `budget`, past
    /// which the message is refused, the sender's record kept, and another
    /// room not at all. A message is charged once, wherever routing places
    /// it. A direct injection, which never creates a room, ignores it.
    #[must_use]
    pub fn drawing_on_from_delivery(mut self, room_id: RoomId, draw: Draw) -> Self {
        self.machine.cascade.draw_on_from_delivery(room_id, draw);
        self
    }

    fn calling(config: Arc<KitConfig>, call: Call) -> Self {
        let budget = ReentryBudget::new(config.max_chain_depth);
        let entry = match call {
            Call::Message { .. } | Call::Rerun { .. } => EntryKind::Message,
            Call::Direct { .. } | Call::Post { .. } => EntryKind::Direct,
            Call::Responses { .. } => EntryKind::Responses,
        };
        Self {
            config,
            io: Io::new(),
            machine: Box::new(Machine {
                call: CallFacts::new(entry),
                stage: CallStage::new(call),
                trigger: Trigger::default(),
                cascade: Cascade::new(budget),
                adopted: Adoptions::default(),
                adopted_deliveries: BTreeMap::new(),
                side_effects: SideEffects::default(),
                outcome: None,
            }),
        }
    }
}

impl Operation for Inbound {
    type Output = Result<InboundResult, InboundError>;

    fn poll(&mut self, environment: &dyn Environment) -> Step<Self::Output> {
        let Self {
            config,
            io,
            machine,
        } = self;
        let mut cx = Cx {
            config,
            env: environment,
            io,
        };
        machine.advance(&mut cx);
        let outcome = machine.take_outcome();
        self.io.step(outcome)
    }

    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.io.complete(id, outcome)
    }

    fn adopt(&mut self, entry: Entry, origin: EffectId) -> Result<EntryId, Box<Entry>> {
        self.machine.adopt(&self.io, entry, origin)
    }

    /// Expires the call when it is not decided yet: every request within
    /// its bound is given up on (`Io::expire`), and the call ends as one
    /// past its deadline, with no `process_timeout` event.
    fn expire(&mut self) -> Vec<EffectId> {
        if self.io.has_decided() {
            return Vec::new();
        }
        self.io.expire()
    }

    /// Cuts the turns of the call's own cascade (RFC §10.1 "Explicit
    /// delivery cancellation"), the entries it adopted keeping theirs.
    fn cancel_turns(&mut self) -> Vec<EffectId> {
        self.machine.cascade.cut_turns(&mut self.io)
    }
}

/// What the call brings into the room.
#[derive(Debug)]
enum Call {
    Message {
        target: InboundTarget,
        organization_id: Option<String>,
        message: Box<InboundMessage>,
        answers: Option<Box<MessageAnswers>>,
    },
    Direct {
        room_id: RoomId,
        organization_id: Option<String>,
        event: Box<DirectEvent>,
    },
    Post {
        room_id: RoomId,
        organization_id: Option<String>,
        post: Box<Post>,
    },
    /// The answers of turns the caller asked on an event it committed
    /// (RFC §23.3 step 5).
    Responses { responses: Box<TurnResponses> },
    /// The room's agents run again on a stored event (RFC §11.7.6).
    Rerun {
        room_id: RoomId,
        event_id: EventId,
        floor: u32,
    },
}

/// The kind of the call's own entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EntryKind {
    /// A message (RFC §10.1).
    Message,
    /// An event injected directly (RFC §10.5).
    Direct,
    /// The answers to an event committed outside the pipeline (RFC §23.3
    /// step 5): no entry of the call's own.
    Responses,
}

/// One call's state, grouped by what owns it: the facts the call learns
/// as it goes, its stage, what its trigger decided, its cascade, the
/// entries it adopted, and the side effects its passes and sets collected.
#[derive(Debug)]
struct Machine {
    call: CallFacts,
    stage: CallStage,
    trigger: Trigger,
    cascade: Cascade,
    /// The entries adopted from the operation's own work (gap G-070).
    adopted: Adoptions,
    /// What the delivery sets of adopted entries reported, each held until
    /// its entry's cascade is over, so that it carries the answers that
    /// cascade delivered (Python's `InboundResult.response_events`).
    adopted_deliveries: BTreeMap<EntryId, InboundResult>,
    side_effects: SideEffects,
    outcome: Option<Result<InboundResult, InboundError>>,
}

/// What the call is, learned as it goes: its room, its kind, its channel,
/// what a direct injection is checked for, where its answers may go, and
/// its bound.
#[derive(Debug)]
struct CallFacts {
    /// The call's room, once it arrived there.
    room_id: Option<RoomId>,
    /// What the call brings: a direct injection's refusal before its commit
    /// is an error (RFC §10.5).
    entry: EntryKind,
    /// The call's channel, once resolved (RFC §10.1 step 1).
    channel_id: Option<ChannelId>,
    /// What a direct injection's trigger is checked for before its hooks
    /// (gap G-069); `None` for a message.
    direct: Option<direct::DirectCall>,
    /// The call's bound, its deadline set at its start from
    /// `process_timeout` (RFC §13.6).
    bound: Bound,
    /// What the trigger's pass writes of the message's sender, as step 5
    /// left them (RFC §10.1 step 5).
    participation: Option<identity::Participation>,
}

impl CallFacts {
    /// The facts of a call bringing `entry`, before it learned anything.
    fn new(entry: EntryKind) -> Self {
        Self {
            room_id: None,
            entry,
            channel_id: None,
            direct: None,
            bound: Bound::Unbounded,
            participation: None,
        }
    }
}

impl Machine {
    /// Takes every answer in, starts the entries adopted since, and runs
    /// the queues, until nothing is left to take in: a section nested under
    /// a lock is answered at once, without the driver.
    fn advance(&mut self, cx: &mut Cx<'_>) {
        // Every section of the operation works for its cascade (gap G-087);
        // the cascade's budget is lent again after each answer, which may
        // have set it on its caller's.
        cx.io.work_for(&self.cascade.budget);
        if matches!(self.stage.current(), Stage::Start(_)) {
            self.begin(cx);
        }
        loop {
            while let Some((effect, outcome)) = cx.io.next_outcome() {
                cx.io.work_for(&self.cascade.budget);
                self.dispatch(cx, effect, outcome);
            }
            cx.io.work_for(&self.cascade.budget);
            self.start_adopted(cx);
            self.run_queues(cx);
            self.report_adopted_deliveries(cx);
            self.end_once_settled();
            if !cx.io.has_outcomes() {
                return;
            }
        }
    }

    /// The operation's outcome once it is over, as its caller receives it.
    fn take_outcome(&mut self) -> Option<Result<InboundResult, InboundError>> {
        let outcome = self.outcome.take()?;
        Some(self.for_caller(outcome))
    }

    /// `outcome` as the call's caller receives it: a direct injection's
    /// refusal before its commit raised (RFC §10.5).
    fn for_caller(
        &self,
        outcome: Result<InboundResult, InboundError>,
    ) -> Result<InboundResult, InboundError> {
        if self.call.entry == EntryKind::Direct {
            outcome.and_then(result::committed)
        } else {
            outcome
        }
    }

    /// Publishes the call's decision, once: `outcome`, the trigger's result
    /// at its commit or the call's end before it, with no delivery (RFC
    /// §10.1 step 18). It is published whatever is still to run: the
    /// delivery sets of the entries the call adopted, which may wait in the
    /// room's lane behind the work of the caller that reads it.
    fn publish(&self, cx: &mut Cx<'_>, outcome: &Result<InboundResult, InboundError>) {
        if cx.io.has_decided() {
            return;
        }
        cx.io.decide(Decided {
            room_id: self.call.room_id.clone(),
            result: self.for_caller(outcome.clone()),
        });
    }

    /// Hands an answer to whoever waits for it: an adopted entry, or the
    /// call's own stage.
    fn dispatch(&mut self, cx: &mut Cx<'_>, effect: EffectId, outcome: Outcome) {
        if self.adopted.defers(effect) {
            return self.adopted.defer(effect, outcome);
        }
        if self.adopted.awaits(effect) {
            return self.on_adopted_outcome(cx, effect, outcome);
        }
        if self.cascade.pass_awaits(effect) {
            return self.on_pass_outcome(cx, effect, outcome);
        }
        if self.cascade.set_awaits(effect) {
            return self.on_set_outcome(cx, effect, outcome);
        }
        if self.cascade.stream_awaits(effect) {
            return self.on_stream_outcome(cx, outcome);
        }
        match self.stage.take() {
            Stage::Arriving(arrival) => self.on_arrival(cx, arrival, effect, outcome),
            Stage::ReadingWork(read) => self.on_work_read(cx, read, effect, outcome),
            Stage::Posting(post) => self.on_post(cx, post, effect, outcome),
            Stage::Rerunning(section) => self.on_rerun(cx, section, effect, outcome),
            Stage::Reading(read) => self.on_read(cx, read, effect, outcome),
            Stage::Checking(check) => self.on_check(cx, check, effect, outcome),
            Stage::ReleasingTicket {
                effect: expected,
                then,
            } if expected == effect => self.on_ticket_released(cx, *then, outcome),
            Stage::Persisting {
                side_effects,
                metadata,
            } => self.on_persisting(cx, side_effects, metadata, effect, outcome),
            stage @ (Stage::Start(_)
            | Stage::Cascade
            | Stage::ReleasingTicket { .. }
            | Stage::Ending(_)
            | Stage::Done) => {
                debug_assert!(false, "inbound received {effect:?} in {stage:?}");
                self.stage.put_back(stage);
            }
        }
    }
}
