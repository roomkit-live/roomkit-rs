//! A message's way to the room's lock (RFC §10.1 steps 1 to 4): its channel
//! and its sender in the channel's form, its room, the room's context, and
//! the event its channel parses.
//!
//! Step 1 is pure and runs before anything is asked of the driver; steps 2
//! to 4 are a sub-machine of their own, [`Arrival`], which hands the
//! cascade the event of its first pass.
//!
//! A message that names no room is routed first (RFC §10.4): the identity
//! its address resolves to is read once, then the kit's router is asked
//! (`Route`), and step 2 records the sender in the room it answers, under
//! the room lock. A room no longer the message's by then (its binding names
//! another sender, it is CLOSED or ARCHIVED, or PAUSED, neither the sender's
//! conversation by step 1 nor bound to the channel as a group, or step 3 no
//! longer admits the sender, someone else being heard there meanwhile) is
//! left, and the router is asked once more; a second such answer, or none,
//! gives the message a new room (gaps G-045, G-055). A message the code of a
//! delivery set sent is charged on that delivery's reentry budget once its
//! final room is known (`charge`, gap G-070).
//!
//! Every request of the arrival carries the call's deadline (RFC §13.6),
//! but a write, a release, what completes a room step 2 creates, and the
//! reads under step 2's lock, whose wait for the lock alone carries it: a
//! deadline passed stops it with nothing of the event written.

use std::mem;

use super::answers::MessageAnswers;
use super::charge::DeliveryCharge;
use super::deadline::Stopped;
use super::identity::{self, Identification, Identified, Participation};
use super::instructions::{self, Admission};
use super::named_room::{Mode, Named, NamedRoom};
use super::verification::{self, Verifying};
use super::{InboundError, InboundResult, result, stamp};
use crate::model::{
    ChannelType, EventId, EventType, Identity, InboundMessage, RoomContext, RoomEvent, RoomId,
};
use crate::ops::io::Bound;
use crate::ops::protocol::{
    ChannelRequest, EffectId, EntryId, Notice, Outcome, Request, RouteEnd, SpanNote, StoreRequest,
};
use crate::ops::{ChannelConfig, Cx, KitConfig, Progress};
use crate::rules::chain_depth::Draw;
use crate::rules::instructions::InstructionRefusal;
use crate::rules::routing::{self, RouteQuery, Sender};
use crate::rules::{addresses, idempotency, permissions, publications};

/// How many times a message is routed before it gets a new room: once, then
/// once more when the room it was routed to is no longer its own by the
/// lock (gap G-055).
const ROUTES: u8 = 2;

/// Where an inbound message goes (RFC §10.1 step 2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InboundTarget {
    /// The room the caller names: used when it exists, created with the id
    /// when it does not.
    Room(RoomId),
    /// The room the kit's router finds for the message, or a new room when
    /// it finds none (RFC §10.4).
    Routed,
}

/// What a message's admission comes to, before anything is read.
#[derive(Debug)]
pub(super) enum Admitted {
    /// The message arrives, on its channel, its sender in the channel's
    /// form (step 1).
    Message {
        channel: Box<ChannelConfig>,
        message: Box<InboundMessage>,
    },
    /// The message is refused, nothing read or written: the call's result.
    Refused(Box<InboundResult>),
}

/// Admits `message`, the call's own or an entry's, before anything is
/// read: first `draw`, what a message naming its room draws on the reentry
/// budget of the room's work, when it draws on one (RFC §8.3, §10.1 step
/// 1, gaps G-070, G-087): past it, the message is refused
/// `reentry_loop_cap` first thing, with no event, its channel not looked
/// up. Then step 1, its blank idempotency key read as none (RFC §13.4),
/// and the refusals of an instruction that cannot be one (RFC §10.1.1) and
/// of a visibility only the framework writes (gap G-041).
///
/// # Errors
///
/// Returns why the message cannot be one: an unregistered channel, the
/// visibility `internal`, `standalone` on a type other than an
/// instruction.
pub(super) fn admit(
    config: &KitConfig,
    message: InboundMessage,
    draw: Option<&Draw>,
) -> Result<Admitted, InboundError> {
    if draw.is_some_and(|draw| !draw.take()) {
        return Ok(Admitted::Refused(Box::new(result::past_the_budget())));
    }
    let (channel, mut message) = resolve(config, message)?;
    message.idempotency_key = idempotency::key(message.idempotency_key);
    if let Admission::Refused(refusal) = instructions::admit(config, (&message).into())? {
        return Ok(Admitted::Refused(Box::new(instructions::refused(refusal))));
    }
    Ok(Admitted::Message {
        channel: Box::new(channel),
        message: Box::new(message),
    })
}

/// Step 1: the registered channel, and the message with its sender written
/// in the channel's form, before anything reads it (RFC §5.7, §10.1 step 1,
/// gap G-052). The raw payload keeps the provider's own spelling.
///
/// # Errors
///
/// Returns [`InboundError::UnknownChannel`] when no channel has the
/// message's channel id, and [`InboundError::InboundOnHostChannel`] for a
/// host channel, which receives no external payload (RFC §10.5).
fn resolve(
    config: &KitConfig,
    mut message: InboundMessage,
) -> Result<(ChannelConfig, InboundMessage), InboundError> {
    let channel = config
        .channel(&message.channel_id)
        .cloned()
        .ok_or_else(|| InboundError::UnknownChannel(message.channel_id.clone()))?;
    if channel.channel_type == ChannelType::HOST {
        return Err(InboundError::InboundOnHostChannel(channel.channel_id));
    }
    message.sender_id = addresses::normalize(
        &message.sender_id,
        channel.addressing,
        channel.country_code.as_ref(),
    );
    Ok((channel, message))
}

/// A message arrived in its room: its event as the pipeline stamped it, and
/// the room's context as step 2 left it, which the trigger's off-lock check
/// reads (RFC §9.5.1).
#[derive(Debug)]
pub(super) struct Arrived {
    pub(super) event: RoomEvent,
    pub(super) context: Box<RoomContext>,
}

/// Steps 2 to 4 for one message.
#[derive(Debug)]
pub(super) struct Arrival {
    target: InboundTarget,
    /// The call's deadline (RFC §13.6).
    bound: Bound,
    organization_id: Option<String>,
    message: Box<InboundMessage>,
    channel: Box<ChannelConfig>,
    /// Who the message is from, as routing names them. The identity is read
    /// once, for a routed message only.
    sender: Sender,
    /// The identity the sender's address resolved to, which the router is
    /// handed (RFC §10.4).
    identity: Option<Identity>,
    /// The adopted entry the arrival is, `None` for the call's own.
    entry: Option<EntryId>,
    /// How many times the router was asked.
    routes: u8,
    /// Whether the message was given its last new room, once routed as
    /// many times as step 2 asks the router (gap G-055).
    given_last_room: bool,
    /// The room step 2 works on or arrived in; `None` while the message is
    /// routed.
    room: Option<RoomId>,
    /// For a message a delivery's code sent into a room it does not name
    /// as the delivering one, or that routing places, its charge on that
    /// delivery's budget once its final room is known (gap G-070).
    charge: DeliveryCharge,
    /// The depth of the turn the call runs within, which the event takes
    /// at least (RFC §8.3).
    chain_floor: u32,
    /// What step 5 decided of the sender, once the message arrived.
    identified: Option<Identified>,
    /// The key the delivery path publishes the message under, the one key
    /// of the publication namespace its event may keep (RFC §22.1).
    publication: Option<String>,
    state: State,
}

#[derive(Debug)]
enum State {
    Start,
    /// The sender's identity is read (RFC §14.1 `resolve_identity`).
    Resolving(EffectId),
    /// The router is asked (RFC §10.4).
    Routing(EffectId),
    /// Step 2.
    Naming(Box<NamedRoom>),
    /// Step 4.
    Parsing {
        effect: EffectId,
        id: EventId,
        /// The room's context, whole, for the off-lock check.
        context: Box<RoomContext>,
    },
    /// Step 5: who sent the message (RFC §11).
    Identifying(Box<Identification>),
    /// Step 5b: the sender's verification (RFC §11.7).
    Verifying(Box<Verifying>),
    Done,
}

/// What the arrival ends with: the message's event in its room, or why it
/// stopped.
type Arriving = Progress<Result<Arrived, Stopped>>;

impl Arrival {
    /// The arrival of `message`, resolved on `channel` (step 1), into the
    /// room `target` names, for a caller acting for `organization_id`,
    /// within the call's `bound`.
    pub(super) fn new(
        target: InboundTarget,
        organization_id: Option<String>,
        channel: ChannelConfig,
        message: InboundMessage,
        bound: Bound,
    ) -> Self {
        let sender = MessageAnswers::declared(&channel, &message).sender(&message);
        Self {
            target,
            bound,
            organization_id,
            message: Box::new(message),
            channel: Box::new(channel),
            sender,
            identity: None,
            routes: 0,
            given_last_room: false,
            entry: None,
            room: None,
            charge: DeliveryCharge::new(None),
            chain_floor: 0,
            identified: None,
            publication: None,
            state: State::Start,
        }
    }

    /// The same arrival, its sender named by what the message's channel
    /// answered of it as it entered, when it did: its conversation and
    /// where its replies go (RFC §5.7, §10.4).
    pub(super) fn answered(mut self, answers: Option<MessageAnswers>) -> Self {
        if let Some(answers) = answers {
            self.chain_floor = answers.chain_floor;
            self.publication.clone_from(&answers.publication);
            self.sender = answers.sender(&self.message);
        }
        self
    }

    /// The same arrival, for a message the code of a delivery set in
    /// `delivery`'s room sent: charged on `delivery`'s budget once its final
    /// room is known, when step 2 creates that room or it is the delivering
    /// one (gap G-070).
    pub(super) fn charging(mut self, delivery: Option<(RoomId, Draw)>) -> Self {
        self.charge = DeliveryCharge::new(delivery);
        self
    }

    /// The delivering cascade's budget the message's cascade draws on: the
    /// one it was charged on, when step 2 created its room or it came into
    /// the room that cascade delivers in.
    pub(super) fn drawn_on(&self) -> Option<&Draw> {
        self.charge.drawn_on()
    }

    /// The arrival of `message`, resolved on `channel`, into `room_id`,
    /// adopted by the operation whose work called into its room (RFC
    /// §13.5, gap G-070): it carries no deadline.
    pub(super) fn adopted(
        entry: EntryId,
        room_id: RoomId,
        organization_id: Option<String>,
        channel: ChannelConfig,
        message: InboundMessage,
    ) -> Self {
        let mut arrival = Self::new(
            InboundTarget::Room(room_id),
            organization_id,
            channel,
            message,
            Bound::Unbounded,
        );
        arrival.entry = Some(entry);
        arrival
    }

    /// Tells the message's `framework.route` what its way came to.
    fn tell_route(&self, cx: &mut Cx<'_>, end: RouteEnd) {
        let entry = self.entry;
        cx.io
            .notify(Notice::Span(SpanNote::RouteOver { entry, end }));
    }

    /// Whether the message would be routed under the framework's own name:
    /// its sender goes by it, or the address it is routed by, a chat's on a
    /// chat channel, is it (RFC §10.1 step 2, §10.4). The framework names the
    /// room it writes to and is never routed, so such a message is a sender
    /// borrowing the name, on a channel whose senders choose their id.
    pub(super) fn routed_under_the_reserved_name(&self) -> bool {
        matches!(self.target, InboundTarget::Routed)
            && (self.message.sender_id == addresses::SYSTEM_SENDER
                || self.sender.address.as_deref() == Some(addresses::SYSTEM_SENDER))
    }

    /// The room the arrival works on: the room step 2 works on or arrived
    /// in, `None` while the message is routed.
    pub(super) fn room(&self) -> Option<&RoomId> {
        self.room.as_ref()
    }

    /// Whether the arrival waits for `effect`.
    pub(super) fn awaits(&self, effect: EffectId) -> bool {
        match &self.state {
            State::Resolving(awaited)
            | State::Routing(awaited)
            | State::Parsing {
                effect: awaited, ..
            } => *awaited == effect,
            State::Naming(step) => step.awaits(effect),
            State::Identifying(step) => step.awaits(effect),
            State::Verifying(step) => step.awaits(effect),
            State::Start | State::Done => false,
        }
    }

    /// Starts step 2 in the named room, or routes the message.
    pub(super) fn start(&mut self, cx: &mut Cx<'_>) {
        if !matches!(self.state, State::Start) {
            return;
        }
        cx.io.notify(Notice::Span(SpanNote::RouteStarts {
            entry: self.entry,
            channel_id: self.message.channel_id.clone(),
            sender_id: self.message.sender_id.clone(),
        }));
        match self.target.clone() {
            InboundTarget::Room(room_id) => self.enter(cx, room_id, Mode::Named),
            InboundTarget::Routed => self.resolve_identity(cx),
        }
    }

    /// Takes the driver's answer to the arrival's outstanding effect; done
    /// with the stamped event of the message, or why it fails.
    pub(super) fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        effect: EffectId,
        outcome: Outcome,
    ) -> Arriving {
        if outcome == Outcome::DeadlinePassed && !matches!(self.state, State::Naming(_)) {
            // Step 2 answers for its own requests; the arrival's own hold
            // nothing.
            return Progress::Done(Err(Stopped::Expired));
        }
        match mem::replace(&mut self.state, State::Done) {
            State::Resolving(expected) if expected == effect => self.on_resolved(cx, outcome),
            State::Routing(expected) if expected == effect => self.on_routed(cx, outcome),
            State::Naming(mut step) => match step.accept(cx, effect, outcome) {
                Progress::Waiting => {
                    self.state = State::Naming(step);
                    Progress::Waiting
                }
                Progress::Done(named) => {
                    // A room created then left is not the message's: its
                    // cascade does not draw on the delivery's budget there.
                    let kept = step.created() && !matches!(named, Named::Rerouted);
                    self.charge.after_step_two(step.charged(), kept);
                    self.on_named(cx, step.room_id(), named)
                }
            },
            State::Parsing {
                effect: expected,
                id,
                context,
            } if expected == effect => self.after_parse(cx, id, context, outcome),
            State::Identifying(mut step) => match step.accept(cx, effect, outcome) {
                Progress::Waiting => {
                    self.state = State::Identifying(step);
                    Progress::Waiting
                }
                Progress::Done(Ok(resolved)) => {
                    let identity = match &resolved.identified {
                        Identified::Proceed(Some(Participation::Identified(identity))) => {
                            Some((**identity).clone())
                        }
                        Identified::Proceed(_) | Identified::Held(_) => None,
                    };
                    let held = matches!(resolved.identified, Identified::Held(_));
                    self.identified = Some(resolved.identified);
                    let arrived = Arrived {
                        event: resolved.event,
                        context: resolved.context,
                    };
                    if held {
                        return Progress::Done(Ok(arrived));
                    }
                    self.verify(cx, arrived, identity.as_ref())
                }
                Progress::Done(Err(stopped)) => Progress::Done(Err(stopped)),
            },
            State::Verifying(mut step) => match step.accept(cx, effect, outcome) {
                Progress::Waiting => {
                    self.state = State::Verifying(step);
                    Progress::Waiting
                }
                Progress::Done(verified) => {
                    Progress::Done(verified.map(|(event, context)| Arrived { event, context }))
                }
            },
            state @ (State::Start
            | State::Resolving(_)
            | State::Routing(_)
            | State::Parsing { .. }
            | State::Done) => {
                debug_assert!(false, "the arrival received {effect:?} in {state:?}");
                self.state = state;
                Progress::Waiting
            }
        }
    }

    /// Reads the identity the sender's address resolves to in the caller's
    /// organization (RFC §10.4, §14.1, §17.2), then routes; a message with
    /// no address is routed at once.
    fn resolve_identity(&mut self, cx: &mut Cx<'_>) {
        let Some(address) = self.sender.address.clone() else {
            return self.route(cx);
        };
        let request = StoreRequest::ResolveIdentity {
            channel_type: self.channel.channel_type.clone(),
            address,
            organization_id: self.organization_id.clone(),
        };
        self.state = State::Resolving(cx.io.issue_within(request, self.bound));
    }

    /// The identity is read; a store that fails it fails the message, with
    /// nothing written.
    fn on_resolved(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> Arriving {
        match outcome.into_resolved() {
            Ok(identity) => {
                self.sender.identity_id = routing::identity_name(identity.as_ref());
                self.identity = identity;
                self.route(cx);
                Progress::Waiting
            }
            Err(error) => {
                let error = InboundError::Store(error);
                self.tell_route(cx, RouteEnd::Failed(Box::new(error.clone())));
                failed(error)
            }
        }
    }

    /// Asks the kit's router which room the message belongs to (RFC §10.4).
    fn route(&mut self, cx: &mut Cx<'_>) {
        self.room = None;
        self.routes = self.routes.saturating_add(1);
        let query = RouteQuery {
            channel_id: self.message.channel_id.clone(),
            channel_type: self.channel.channel_type.clone(),
            sender_id: self.message.sender_id.clone(),
            metadata: self.message.metadata.clone(),
            organization_id: self.organization_id.clone(),
            sender: self.sender.clone(),
            form: self.channel.form(),
            identity: self.identity.clone(),
        };
        let request = Request::Route {
            query: Box::new(query),
        };
        self.state = State::Routing(cx.io.issue_within(request, self.bound));
    }

    /// The router answered: its room, or a new one when it found none (RFC
    /// §10.1 step 2). A router that fails fails the message, with nothing
    /// written. A routed room is charged once step 2 kept it for the
    /// message (gap G-070).
    fn on_routed(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> Arriving {
        match outcome.into_routed() {
            Ok(Some(room_id)) => {
                self.enter(cx, room_id, Mode::Routed);
                Progress::Waiting
            }
            Ok(None) => self.enter_fresh(cx),
            Err(error) => {
                let error = InboundError::Routing(error);
                self.tell_route(cx, RouteEnd::Failed(Box::new(error.clone())));
                failed(error)
            }
        }
    }

    /// A new room for the message, with a fresh id (RFC §10.1 step 2): the
    /// work of the delivery whose code routed it, charged on its budget
    /// before the room is created.
    fn enter_fresh(&mut self, cx: &mut Cx<'_>) -> Arriving {
        if !self.charge.charge_once() {
            self.tell_route(cx, RouteEnd::PastTheBudget);
            return Progress::Done(Err(Stopped::PastBudget));
        }
        let room_id = cx.env.new_room_id();
        self.enter(cx, room_id, Mode::Fresh);
        Progress::Waiting
    }

    /// Starts step 2 in `room_id`, named as `mode` says. A room the store
    /// lacks, routed or named, is created on the budget of the delivery
    /// whose code sent the message, charged by step 2 before it creates it
    /// unless the message was charged already (a fresh room was charged
    /// before step 2).
    fn enter(&mut self, cx: &mut Cx<'_>, room_id: RoomId, mode: Mode) {
        self.room = Some(room_id.clone());
        let opening = self.charge.opening(mode);
        let mut step = Box::new(
            NamedRoom::new(
                room_id,
                mode,
                self.organization_id.clone(),
                self.channel.clone(),
                self.sender.clone(),
                self.bound,
            )
            .sent_by(self.message.sender_id.clone())
            .opening_on(opening)
            .told_as(self.entry),
        );
        step.start(cx);
        self.state = State::Naming(step);
    }

    /// Step 2 is over: the room's context goes to the channel, or the room
    /// is no longer the message's and the message is routed again.
    fn on_named(&mut self, cx: &mut Cx<'_>, room_id: &RoomId, named: Named) -> Arriving {
        match named {
            Named::Arrived(context) => {
                if !self.charge.arrived_within_budget(room_id) {
                    return Progress::Done(Err(Stopped::PastBudget));
                }
                self.parse(cx, context)
            }
            Named::Failed(error) => {
                self.tell_route(cx, RouteEnd::Failed(Box::new(error.clone())));
                failed(error)
            }
            Named::Expired => Progress::Done(Err(Stopped::Expired)),
            Named::PastBudget => {
                self.tell_route(cx, RouteEnd::PastTheBudget);
                Progress::Done(Err(Stopped::PastBudget))
            }
            Named::Rerouted => {
                tracing::debug!(
                    target: "roomkit::core::inbound",
                    room_id = %room_id,
                    "the routed room is no longer the message's; it is routed once more"
                );
                self.route_again(cx)
            }
        }
    }

    /// Routes the message once more, or gives it a new room once it was
    /// (gap G-055). When that last room is no longer the message's either,
    /// no room opened for it being its own, the message is refused
    /// `room_taken`, nothing of it stored (RFC §10.1 step 2, HITL ef0ef858,
    /// RMK-1396).
    fn route_again(&mut self, cx: &mut Cx<'_>) -> Arriving {
        if self.routes < ROUTES {
            self.route(cx);
            return Progress::Waiting;
        }
        if self.given_last_room {
            tracing::warn!(
                target: "roomkit::core::inbound",
                channel_id = %self.message.channel_id,
                "no room opened for a routed message is still its own: refused"
            );
            self.tell_route(cx, RouteEnd::RoomTaken);
            return Progress::Done(Err(Stopped::Refused(Box::new(result::room_taken()))));
        }
        self.given_last_room = true;
        self.enter_fresh(cx)
    }

    /// Hands the room's context to the channel's `handle_inbound()` (RFC
    /// §10.1 steps 3 and 4). The parse stands in its message's admission
    /// order (gap G-070): an event it injects into the room is adopted
    /// beside the operation's work, and draws on its budget.
    fn parse(&mut self, cx: &mut Cx<'_>, context: Box<RoomContext>) -> Arriving {
        let id = cx.env.new_event_id();
        let request = ChannelRequest::HandleInbound {
            channel_id: self.message.channel_id.clone(),
            message: (*self.message).clone(),
            context: self.receiver_context((*context).clone()),
            id: id.clone(),
        };
        let bound = self.bound;
        let effect =
            cx.in_admission_order(&context.room.id, |cx| cx.io.issue_within(request, bound));
        self.state = State::Parsing {
            effect,
            id,
            context,
        };
        Progress::Waiting
    }

    /// The context the receiving channel parses with: only what it may see
    /// (RFC §7.5 rule 8), and no event when the room does not bind it (RFC
    /// §10.1 step 3).
    fn receiver_context(&self, context: RoomContext) -> Box<RoomContext> {
        Box::new(match context.get_binding(&self.message.channel_id) {
            Some(receiver) => permissions::context_for(receiver, &context),
            None => RoomContext {
                recent_events: Vec::new(),
                ..context
            },
        })
    }

    /// What step 5 decided of the message's sender, once it arrived: the
    /// event goes on, with what its pass writes of them, or is held. A
    /// message no resolution ran for goes on.
    pub(super) fn take_identified(&mut self) -> Identified {
        self.identified.take().unwrap_or(Identified::Proceed(None))
    }

    /// The event the channel parsed is stamped (step 4), then its sender
    /// resolved when step 5 runs for it (RFC §11.4, §11.6).
    fn after_parse(
        &mut self,
        cx: &mut Cx<'_>,
        id: EventId,
        context: Box<RoomContext>,
        outcome: Outcome,
    ) -> Arriving {
        let mut arrived = match self.on_parsed(id, context, outcome) {
            Ok(arrived) => arrived,
            Err(stopped) => return Progress::Done(Err(stopped)),
        };
        if !identity::wanted(cx.config, &self.channel, &arrived.event, &arrived.context) {
            identity::as_the_record_named(
                cx.config,
                &self.channel,
                &mut arrived.event,
                &arrived.context,
            );
            return self.verify(cx, arrived, None);
        }
        let step = Identification::start(
            cx,
            arrived.event,
            arrived.context,
            &self.message,
            self.bound,
        );
        self.state = State::Identifying(Box::new(step));
        Progress::Waiting
    }

    /// Step 5b once step 5 let the message through: the verification of its
    /// sender, the identity step 5 `resolved` or the room's identified
    /// participant the event names, when the kit verifies its senders (RFC
    /// §10.1 step 5b, §11.7.3); the message goes on at once otherwise.
    fn verify(
        &mut self,
        cx: &mut Cx<'_>,
        arrived: Arrived,
        resolved: Option<&Identity>,
    ) -> Arriving {
        let Some(sender) = verification::sender(cx, &arrived.event, &arrived.context, resolved)
        else {
            return Progress::Done(Ok(arrived));
        };
        let step = Verifying::start(cx, arrived.event, arrived.context, sender, self.bound);
        self.state = State::Verifying(Box::new(step));
        Progress::Waiting
    }

    /// Stamps the event the channel parsed (RFC §10.1 step 4), in the room
    /// step 2 arrived in.
    fn on_parsed(
        &self,
        id: EventId,
        context: Box<RoomContext>,
        outcome: Outcome,
    ) -> Result<Arrived, Stopped> {
        let parsed = outcome.into_parsed().map_err(|error| {
            Stopped::Failed(InboundError::ChannelFailed {
                channel_id: self.message.channel_id.clone(),
                error,
            })
        })?;
        let mut event = stamp::stamp_trigger(*parsed, &self.message, id);
        event.room_id.clone_from(&context.room.id);
        // A key in the framework's publication namespace is the delivery
        // path's alone (RFC §22.1): dropped from any other message, whoever
        // wrote it, the parse or its caller.
        let key = event.idempotency_key.take();
        event.idempotency_key = publications::admitted(key, self.publication.as_deref());
        // At least as deep as the turn the call runs within, whatever depth
        // the parse set (RFC §8.3), as Python's `_at_least_as_deep`.
        event.chain_depth = event.chain_depth.max(self.chain_floor);
        // An instruction is never stored: a key its parse set refuses it as
        // the message's would have (RFC §10.1.1 rule 2).
        if event.r#type == EventType::Instruction && event.idempotency_key.is_some() {
            let refused = instructions::refused(InstructionRefusal::NotIdempotent);
            return Err(Stopped::Refused(Box::new(refused)));
        }
        Ok(Arrived { event, context })
    }
}

/// The arrival fails with `error`, nothing of the message written.
fn failed(error: InboundError) -> Arriving {
    Progress::Done(Err(Stopped::Failed(error)))
}
