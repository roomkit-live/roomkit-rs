//! The protocol between an operation and the driver that runs it
//! (docs/architecture.md, "Operations").
//!
//! Each poll of an [`Operation`] returns a [`Step`]: the [`Notice`]s to
//! perform now, which need no answer, and what comes [`Next`], the
//! [`Effect`]s to execute or the operation's output. An effect carries a
//! [`Request`] of one family, the room's lock, its lane and its admission
//! ticket ([`HoldRequest`]), the store ([`StoreRequest`]), a channel
//! ([`ChannelRequest`]), a hook ([`HookRequest`]) or the router; the driver
//! answers it with exactly one [`Outcome`] of the same family, or
//! `Failed`, or `DeadlinePassed` past the deadline it carries
//! ([`Operation::complete`]). An inbound operation adopts the calls into
//! its room that its own work makes ([`Entry`], [`Operation::adopt`]), and
//! reports each one's result once it is decided ([`Settled`]), and its own
//! once it is ([`Decided`]). An operation whose caller is gone before its
//! decision expires ([`Operation::expire`]): the driver drops the requests
//! it gives up on, and runs it to its end.

mod answers;
mod entries;
mod requests;
mod results;

pub(crate) use answers::{KeyHeld, RequestKind};
pub use entries::{Decided, Delivered, Entry, EntryId, Settled};
pub use requests::{
    ChannelOutcome, ChannelRequest, DiscussionOutcome, DiscussionRequest, DiscussionTurn,
    HoldOutcome, HoldRequest, HookAnswer, HookRequest, IdentityAnswer, IdentityRequest, NextTurn,
    Outcome, Raised, Request, StoppedTurn, StoreOutcome, StoreRequest, StreamTarget,
};
pub use results::{InboundError, InboundResult, KIT_CLOSED, LANE_CANCELLED};

use thiserror::Error;

use super::{Environment, FrameworkEvent};
use crate::model::{
    ChannelId, ErrorRecord, EventId, Metadata, RoomContext, RoomEvent, RoomId, Timestamp,
};
use crate::rules::chain_depth::ReentryBudget;
use crate::rules::hook_registry::{HookId, HookTrigger};

/// Identifies an effect within its operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EffectId(u64);

impl EffectId {
    pub(crate) fn first() -> Self {
        Self(0)
    }

    pub(crate) fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// The id of no operation's effect: the one a frame of code the host
    /// runs under a room's lock it took itself carries (a strategy's
    /// install, RFC §13.5, §19.7), which only that code's frame reads.
    #[doc(hidden)]
    #[must_use]
    pub const fn of_host_section() -> Self {
        Self(u64::MAX)
    }
}

/// Something the driver does for an operation.
#[derive(Clone, Debug, PartialEq)]
pub struct Effect {
    /// The id the driver completes the effect with.
    pub id: EffectId,
    /// What to do.
    pub request: Request,
    /// The instant past which the driver gives up on the request and answers
    /// [`Outcome::DeadlinePassed`]: a call's `process_timeout`, which bounds
    /// what precedes its commit (RFC §13.6). `None` for a request nothing
    /// bounds but its own timeouts. An operation never sets one on a write,
    /// a release or a commit: giving up on one of those midway could leave
    /// half of it done.
    pub deadline: Option<Timestamp>,
    /// The room whose lock the operation holds on the request's behalf: the
    /// request runs within that lock's tenure, as a hook under the lock
    /// does, so code it calls may take the lock again, at once (RFC §13.5:
    /// the lock is reentrant within one execution context). `None` for a
    /// request made outside every lock: code it calls waits for a lock as
    /// anyone does.
    pub locked: Option<RoomId>,
    /// The room in whose admission order the request runs (RFC §9.5.1): an
    /// off-lock hook, a message's parse, or any request of an entry the
    /// operation adopted beside its work. Code it runs stands in that
    /// order: an event it injects into the room takes no ticket, so it
    /// never waits for the ticket its own caller holds (RFC §9.5.1
    /// Reentrance). `None` for any other request.
    pub admitted: Option<RoomId>,
    /// The reentry budget of the cascade whose work the request's code is
    /// (RFC §8.3, §10.3, gaps G-070, G-087): an `OnEvent` or a `Deliver`
    /// carries the budget of the cascade whose delivery set made it, so that
    /// the operation a turn's or a delivery's call starts draws one of its
    /// passes, and a channel that calls into a room on every delivery stops
    /// at the cap; a hook, a parse or a route made under a room's lock or
    /// in its admission order (`locked`, `admitted`) carries the budget of
    /// the cascade its section works for, so that a direct change its code
    /// makes is that cascade's work, its triggers' bodies drawing on its hook
    /// calls. `None` for any other request.
    pub budget: Option<ReentryBudget>,
    /// The call's result once this request, the commit of the call's own
    /// event, is stored: the commit at which the call is decided (RFC §10.1
    /// step 18). A driver whose run ends before the decision, the kit's
    /// close aborting it, answers the call's caller with it once that write
    /// ended. `None` for any other request, a commit the call makes for
    /// another event among them: an entry it adopted, a channel's lifecycle
    /// record.
    pub decides: Option<Box<InboundResult>>,
    /// Whose answer this request, a commit, stores, one of Python's
    /// `InboundResult.response_events`: the call's own cascade's, or an
    /// entry's it adopted. A driver whose run the kit's close aborts reports
    /// the answers each stored, as Python's callers read their cascades'.
    /// `None` for any other request.
    pub answers: Option<AnswerOf>,
}

/// The call whose cascade an answer belongs to ([`Effect::answers`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnswerOf {
    /// The operation's own call.
    Call,
    /// An entry the operation adopted.
    Entry(EntryId),
}

/// The error type of a [`ChannelRequest::OnEvent`] or [`ChannelRequest::Deliver`]
/// answered [`Outcome::Failed`] because the driver holds no channel under
/// the binding's id, unregistered since the operation started: a transport
/// target is then skipped, an intelligence target reported
/// `delivery_failed` with this error (gap G-048).
pub const CHANNEL_NOT_REGISTERED: &str = "channel_not_registered";

/// The reason a stream's row ([`Entry::Row`]) is settled with when the
/// stream's target may not read it, and reads nothing more of the response
/// in the stream: the room, as the row's pass read it, no longer lets the
/// target read the response (the agent may not write, the target is no
/// longer bound, or the agent's visibility no longer reaches it), the row
/// then stored, stored `BLOCKED` or refused; or the row is a message whose
/// visibility, as its hooks left it, keeps it from the target. The target
/// is handed the row neither in the stream nor through `deliver()`, and the
/// rows after it are delivered as their plans say.
pub const ROW_WITHHELD: &str = "row_withheld";

/// The reason a stream's tool-call row ([`Entry::Row`]) is settled with,
/// the row committed and delivered, when its visibility, as its hooks left
/// it, keeps it from the stream's target: the target is not handed it, and
/// reads the rest of the response in the stream.
pub const ROW_HIDDEN: &str = "row_hidden";

/// The error type a cut turn is answered with: by the operation itself
/// ([`Operation::cancel_turns`]), or by the driver for a turn the kit's close
/// cut: no failure, nothing of it reported.
pub const TURN_CUT: &str = "turn_cut";

/// Something the driver performs or reports without answering.
#[derive(Clone, Debug, PartialEq)]
pub enum Notice {
    /// Publish a framework event to its subscribers (RFC §8.2).
    FrameworkEvent(FrameworkEvent),
    /// Run these hooks concurrently as observers of `event`; what they return
    /// is discarded and what they raise is logged (RFC §9.3).
    AsyncHooks {
        /// The trigger.
        trigger: HookTrigger,
        /// The hooks to run.
        hooks: Vec<HookId>,
        /// Their payload.
        event: Box<RoomEvent>,
        /// The room's context, whole.
        context: Box<RoomContext>,
        /// For the hooks a cascade fires once an event's delivery set has
        /// run, a direct change's, and the ASYNC hooks of a SYNC trigger,
        /// which observe the event its chain allowed: the work each body is
        /// while it runs (RFC §8.3, §9.2, §9.5.1, §10.1 step 16, §10.3);
        /// `None` for any other.
        after_delivery: Option<AfterDelivery>,
        /// The framework event to publish once every hook has ended, as the
        /// Python implementation's `then`: an inbound event's
        /// `event_processed`, once its `AFTER_BROADCAST` hooks are over
        /// (RFC §8.2).
        then: Option<FrameworkEvent>,
    },
    /// Stop reading the stream the [`ChannelRequest::ReadStream`] effect
    /// `effect` reads, its turn cut from outside (RFC §10.1 "Explicit
    /// delivery cancellation", §12.2 step 13s): its reader is stopped as one
    /// that stopped reading, the text it was handed and the ends of the
    /// calls it opened handed back as rows, the text marked `cancelled`;
    /// the effect is then answered as any read that ended.
    StopReading {
        /// The read to stop.
        effect: EffectId,
    },
    /// The call's turns are cut, the read of its own stream over if one was
    /// under way: what the call's caller reads of them (RFC §6.7, §10.1
    /// "Explicit delivery cancellation"), who does not wait for the delivery
    /// that goes on.
    CutReadOver {
        /// The call's record as the read left it.
        response_metadata: Metadata,
        /// The call's first error as the read left it: a failure of the read
        /// is its caller's error (RFC §12.2 step 13s).
        error: Option<ErrorRecord>,
        /// The rows the read wrote the turn's record on, as stored.
        rewritten: Vec<RoomEvent>,
    },
    /// What the driver's spans read of the operation (Python's
    /// `framework.send_event` and `framework.broadcast`).
    Span(SpanNote),
}

/// What the operation tells the driver's spans: nothing depends on it.
#[derive(Clone, Debug, PartialEq)]
pub enum SpanNote {
    /// A direct injection goes in, its refusals before the room's lock
    /// behind it: from here it is a `framework.send_event`, as Python's
    /// `_inject` opens it.
    Injecting {
        /// The adopted entry it is, `None` for the call's own.
        entry: Option<EntryId>,
    },
    /// A message starts finding its room: `framework.route`, as Python's
    /// `_route_to_room` opens it.
    RouteStarts {
        /// The adopted entry it is, `None` for the call's own.
        entry: Option<EntryId>,
        /// The channel it came on.
        channel_id: ChannelId,
        /// Its sender.
        sender_id: String,
    },
    /// The message's way to its room is over.
    RouteOver {
        /// The adopted entry it is, `None` for the call's own.
        entry: Option<EntryId>,
        /// What it came to.
        end: RouteEnd,
    },
    /// A delivery set starts: `framework.broadcast`, as Python's
    /// `EventRouter.execute_plan` opens it.
    SetStarts {
        /// The event.
        event_id: EventId,
        /// The event's room.
        room_id: RoomId,
        /// The channel the event came from.
        source: ChannelId,
        /// The voice session its metadata names, if any.
        session: Option<String>,
        /// The adopted entry whose cascade it is, `None` for the call's own.
        entry: Option<EntryId>,
    },
    /// The targets of `event_id`'s delivery set are over.
    SetOver {
        /// The event.
        event_id: EventId,
        /// Whom the event reaches, asked or not (Python's
        /// `len(plan.targets)`).
        targets: usize,
        /// The sends that went through (`len(result.delivery_outputs)`).
        delivered: usize,
        /// The targets that failed (`len(result.errors)`).
        failed: usize,
        /// The turns that answered with a stream to read (what Python's
        /// `cascade.add_streams` keeps).
        streams: usize,
        /// The first failure of an agent's turn in the set, when the set is
        /// the trigger's own or a rerun's, never an answer's (what Python's
        /// `cascade.record_error` keeps).
        error: Option<ErrorRecord>,
        /// Whether the set is of the call's own cascade, not an adopted
        /// entry's.
        own: bool,
    },
    /// The call's own cascade failed past its decision: a stream as it was
    /// read, a pass or a record that could not end.
    CascadeFailed {
        /// What failed it.
        error: ErrorRecord,
    },
}

/// What a message's way to its room came to.
#[derive(Clone, Debug, PartialEq)]
pub enum RouteEnd {
    /// Step 2 has its room.
    Room(RoomId),
    /// What failed it, nothing of it written.
    Failed(Box<InboundError>),
    /// It would open a room past the budget of the work that sent it.
    PastTheBudget,
    /// No room opened for it is still its own (RMK-1396).
    RoomTaken,
}

/// The work the body of an ASYNC hook is while it runs, when a cascade
/// fires it once an event's delivery set has run (RFC §10.1 step 16's
/// `AFTER_BROADCAST` hooks, a change's `ON_EVENT_UPDATED` and
/// `ON_EVENT_DELETED` hooks, §10.3), or a direct change does (gap G-087),
/// and when it is an ASYNC hook of a SYNC trigger, `BEFORE_BROADCAST`, which
/// observes the event its chain allowed (RFC §9.2).
///
/// While the body runs, a call it makes into the room is a new arrival
/// that draws one of `budget`'s hook calls as it starts, its cascade on
/// that budget's passes; a direct change it makes there draws one too; and
/// a call it makes into any room is at least `floor` deep (RFC §8.3), so
/// that a hook that asks an agent again after each of its answers ends at
/// the chain-depth limit. Work the body starts that calls once the body
/// has returned is work of its own.
#[derive(Clone, Debug, PartialEq)]
pub struct AfterDelivery {
    /// The budget of the cascade, or of the direct change's work, whose
    /// hook calls the hooks' bodies draw on ([`Draw::HookCall`]).
    ///
    /// [`Draw::HookCall`]: crate::rules::chain_depth::Draw::HookCall
    pub budget: ReentryBudget,
    /// The least chain depth of a call the body makes: one deeper than the
    /// event the hook runs for.
    pub floor: u32,
}

/// What an operation produced on one poll.
#[derive(Clone, Debug, PartialEq)]
pub struct Step<T> {
    /// Notices to perform now, in order.
    pub notices: Vec<Notice>,
    /// The adopted entries decided since the last poll, whose callers the
    /// driver answers now (RFC §10.1 step 18).
    pub settled: Vec<Settled>,
    /// The adopted entries whose delivery set ran since the last poll,
    /// whose callers' completion handles the driver resolves now (RFC
    /// §10.1 step 18).
    pub delivered: Vec<Delivered>,
    /// An inbound call's decision, published once, on the poll that knows
    /// it: what a caller that does not wait for the call's delivery
    /// receives (RFC §10.1 step 18). `None` on every other poll, and for
    /// every other operation.
    pub decided: Option<Decided>,
    /// What comes next.
    pub next: Next<T>,
}

/// What an operation needs next.
#[derive(Clone, Debug, PartialEq)]
pub enum Next<T> {
    /// Execute these effects, in any order or concurrently, and complete
    /// each as it finishes; poll again after each completion.
    Effects(Vec<Effect>),
    /// Effects or adopted entries are still outstanding: complete the next
    /// effect that finishes, or adopt the next call, then poll again.
    Waiting,
    /// The operation is over.
    Ready(T),
}

/// What every operation offers its driver: a step on each poll, and the
/// answer to each effect.
///
/// A driver runs any operation with the same loop: poll, perform the
/// notices, execute the effects and complete each as it finishes, polling
/// between completions, until the operation is ready (see the
/// [`ops`](crate::ops) module).
pub trait Operation {
    /// What the operation produces once it is over.
    type Output;

    /// Advances the operation with the outcomes completed since the last
    /// poll, and says what it needs next.
    ///
    /// A driver hands every poll of an operation the environment it started
    /// with: the operation's clock and its ids never change source midway,
    /// whatever environment the host sets meanwhile.
    fn poll(&mut self, environment: &dyn Environment) -> Step<Self::Output>;

    /// Hands the driver's answer to effect `id`.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] when `id` is not outstanding or `outcome`
    /// does not answer its request; the operation is unchanged then.
    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError>;

    /// Hands the operation a call into its room, made by the code effect
    /// `origin` runs, or by code that code awaits (RFC §13.5, §9.5.1
    /// Reentrance): code under the room's lock or in its admission order.
    /// The operation runs it as an entry of its own and reports its result
    /// in [`Step::settled`]; the next poll starts it.
    ///
    /// # Errors
    ///
    /// Hands `entry` back when the operation does not adopt it: another
    /// room's call, an `origin` no longer outstanding, or one made neither
    /// under the room's lock nor in its admission order. The driver then
    /// runs the call as an operation of its own.
    fn adopt(&mut self, entry: Entry, origin: EffectId) -> Result<EntryId, Box<Entry>> {
        let _ = origin;
        Err(Box::new(entry))
    }

    /// The call's caller stopped waiting before its decision
    /// ([`Step::decided`]): the call expires as one past its
    /// `process_timeout` does (RFC §13.6, gap G-076). Nothing more of its
    /// trigger is written, what it holds is let go, its decision is
    /// published, and every event it committed, its trigger's when the
    /// commit was answered, an adopted entry's, is delivered. A commit not
    /// answered yet, and what completes a room step 2 created, run to their
    /// end first: they are never within the call's bound.
    ///
    /// Returns the effects the operation gives up on, requests within the
    /// call's bound it was waiting for: the driver drops them and never
    /// answers them. The operation goes on to its end through `poll`, the
    /// driver answering its other effects as before. An operation decided
    /// already, or that has no caller to lose, gives up on nothing.
    fn expire(&mut self) -> Vec<EffectId> {
        Vec::new()
    }

    /// Whether it tells what its call's caller reads once its turns are cut,
    /// by [`Notice::CutReadOver`]: at the cut, or when the read of the call's
    /// own stream the cut stopped is over.
    fn tells_its_cut(&self) -> bool {
        false
    }

    /// Cuts the turns of the call's own cascade (RFC §10.1 "Explicit
    /// delivery cancellation", §13.6, gap G-076): a turn its cascade has not
    /// started never starts, a running one is given up on, and no response
    /// of its cascade re-enters. Its committed events are still delivered to
    /// every other target, their `AFTER_BROADCAST` hooks fire, and the
    /// cascades of the entries it adopted keep their turns. A driver calls
    /// it when the caller of a decided call stopped waiting, and may call
    /// it before the decision, when the call's delivery is cancelled by
    /// another way: its event is committed and delivered all the same, and
    /// none of its turns starts.
    ///
    /// Returns the effects the operation gives up on, the turns in flight:
    /// the driver cuts them, joins them, and never answers them. An
    /// operation that runs no turn gives up on nothing.
    fn cancel_turns(&mut self) -> Vec<EffectId> {
        Vec::new()
    }

    /// Cuts the turns of the cascade of `entry`, a call into the room the
    /// operation adopted (RFC §9.5.1 Reentrance), whose delivery is
    /// cancelled (RFC §10.1 "Explicit delivery cancellation"), as
    /// [`cancel_turns`](Self::cancel_turns) cuts the call's own: a turn of
    /// that cascade not started never starts, a running one is given up
    /// on, and no response of it re-enters; the call's own cascade, and the
    /// other entries', keep theirs.
    ///
    /// Returns the effects the operation gives up on, the turns in flight.
    /// An operation that adopts nothing gives up on nothing.
    fn cancel_entry_turns(&mut self, entry: EntryId) -> Vec<EffectId> {
        let _ = entry;
        Vec::new()
    }
}

/// A driver broke the protocol.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ProtocolError {
    /// The effect is unknown, or already completed.
    #[error("effect {0:?} is not outstanding")]
    UnknownEffect(EffectId),
    /// The outcome does not answer the effect's request.
    #[error("effect {effect:?} asked for {expected}, the driver answered {got}")]
    UnexpectedOutcome {
        /// The effect.
        effect: EffectId,
        /// What its request is answered with.
        expected: &'static str,
        /// What the driver answered.
        got: &'static str,
    },
}
