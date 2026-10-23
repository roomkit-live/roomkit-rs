//! A message's way to the room's lock (RFC §10.1 steps 1 to 4): its channel
//! and its sender in the channel's form, its room, the room's lane, the
//! room's context, and the event its channel parses.
//!
//! Step 1 is pure and runs before anything is asked of the driver; steps 2
//! to 4 are a sub-machine of their own, [`Arrival`], which hands the
//! cascade the event of its first pass with the room's lane held.
//!
//! A message that names no room is routed first (RFC §10.4): the identity
//! its address resolves to is read once, then the kit's router is asked
//! (`Route`). The lane of the room it answers is entered, and step 2 records
//! the sender there under the room lock. A room no longer the message's by
//! then (its binding names another sender, it is no longer ACTIVE, or step 3
//! no longer admits the sender, someone else being heard there meanwhile) has
//! its lane left, and the router is asked once more; a second such answer,
//! or none, gives the message a new room (gap G-055). The arrival never
//! holds two lanes at once.
//!
//! Every request of the arrival carries the call's deadline (RFC §13.6),
//! but a write, a release, what completes a room step 2 creates, and the
//! reads under step 2's lock, whose wait for the lock alone carries it: a
//! deadline passed stops it with nothing of the event written, holding at
//! most the room's lane, which its caller leaves.

use std::mem;

use super::InboundError;
use super::deadline::Stopped;
use super::named_room::{Mode, Named, NamedRoom};
use super::stamp;
use crate::model::{EventId, InboundMessage, RoomContext, RoomEvent, RoomId, Timestamp};
use crate::ops::protocol::{EffectId, Outcome, Request};
use crate::ops::{ChannelDeclaration, Cx, KitConfig, Progress};
use crate::rules::routing::{self, RouteQuery, Sender};
use crate::rules::{addresses, permissions};

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

/// Step 1: the registered channel, and the message with its sender written
/// in the channel's form, before anything reads it (RFC §5.7, §10.1 step 1,
/// gap G-052). The raw payload keeps the provider's own spelling.
///
/// # Errors
///
/// Returns [`InboundError::UnknownChannel`] when no channel has the
/// message's channel id.
pub(super) fn resolve(
    config: &KitConfig,
    mut message: InboundMessage,
) -> Result<(ChannelDeclaration, InboundMessage), InboundError> {
    let channel = config
        .channel(&message.channel_id)
        .cloned()
        .ok_or_else(|| InboundError::UnknownChannel(message.channel_id.clone()))?;
    message.sender_id = addresses::normalize(
        &message.sender_id,
        channel.addressing(),
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
    deadline: Option<Timestamp>,
    organization_id: Option<String>,
    message: Box<InboundMessage>,
    channel: Box<ChannelDeclaration>,
    /// Who the message is from, as routing names them. The identity is read
    /// once, for a routed message only.
    sender: Sender,
    /// How many times the router was asked.
    routes: u8,
    /// The room whose lane the arrival holds.
    lane: Option<RoomId>,
    /// The room of a message the operation adopted, whose lane the
    /// operation holds already (gap G-070).
    within: Option<RoomId>,
    state: State,
}

#[derive(Debug)]
enum State {
    Start,
    /// The sender's identity is read (RFC §14.1 `resolve_identity`).
    Resolving(EffectId),
    /// The router is asked (RFC §10.4).
    Routing(EffectId),
    /// The lane of step 2's room is entered.
    Entering {
        effect: EffectId,
        step: Box<NamedRoom>,
    },
    /// Step 2, with the room's lane held.
    Naming(Box<NamedRoom>),
    /// The lane of a room no longer the message's is left before the
    /// message is routed again.
    Leaving(EffectId),
    /// Step 4.
    Parsing {
        effect: EffectId,
        id: EventId,
        /// The room's context, whole, for the off-lock check.
        context: Box<RoomContext>,
    },
    Done,
}

/// What the arrival ends with: the message's event in its room, or why it
/// stopped.
type Arriving = Progress<Result<Arrived, Stopped>>;

impl Arrival {
    /// The arrival of `message`, resolved on `channel` (step 1), into the
    /// room `target` names, for a caller acting for `organization_id`,
    /// within the call's `deadline`.
    pub(super) fn new(
        target: InboundTarget,
        organization_id: Option<String>,
        channel: ChannelDeclaration,
        message: InboundMessage,
        deadline: Option<Timestamp>,
    ) -> Self {
        let address = routing::routing_address(
            channel.addressing(),
            &message.sender_id,
            &message.metadata,
            channel.chat_key.as_deref(),
        );
        Self {
            target,
            deadline,
            organization_id,
            message: Box::new(message),
            channel: Box::new(channel),
            sender: Sender {
                address,
                identity_id: None,
            },
            routes: 0,
            lane: None,
            within: None,
            state: State::Start,
        }
    }

    /// The arrival of `message`, resolved on `channel`, into `room_id`,
    /// whose lane the operation that adopted the message holds already (RFC
    /// §13.5, gap G-070): step 2 runs at once, with no deadline, and the
    /// arrival enters and leaves no lane.
    pub(super) fn within_lane(
        room_id: RoomId,
        organization_id: Option<String>,
        channel: ChannelDeclaration,
        message: InboundMessage,
    ) -> Self {
        Self {
            within: Some(room_id.clone()),
            ..Self::new(
                InboundTarget::Room(room_id),
                organization_id,
                channel,
                message,
                None,
            )
        }
    }

    /// The room whose lane the arrival holds, which its caller leaves when
    /// the arrival fails.
    pub(super) fn lane(&self) -> Option<&RoomId> {
        self.lane.as_ref()
    }

    /// The room the arrival works on: the room whose lane it enters or
    /// holds, `None` while the message is routed.
    pub(super) fn room(&self) -> Option<&RoomId> {
        match &self.state {
            State::Entering { step, .. } | State::Naming(step) => Some(step.room_id()),
            State::Start
            | State::Resolving(_)
            | State::Routing(_)
            | State::Leaving(_)
            | State::Parsing { .. }
            | State::Done => self.lane.as_ref(),
        }
    }

    /// Whether the arrival waits for `effect`.
    pub(super) fn awaits(&self, effect: EffectId) -> bool {
        match &self.state {
            State::Resolving(awaited)
            | State::Routing(awaited)
            | State::Leaving(awaited)
            | State::Entering {
                effect: awaited, ..
            }
            | State::Parsing {
                effect: awaited, ..
            } => *awaited == effect,
            State::Naming(step) => step.awaits(effect),
            State::Start | State::Done => false,
        }
    }

    /// Starts step 2: the named room's lane is entered, or the message is
    /// routed.
    pub(super) fn start(&mut self, cx: &mut Cx<'_>) {
        if !matches!(self.state, State::Start) {
            return;
        }
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
            // nothing but the lane.
            return Progress::Done(Err(Stopped::Expired));
        }
        match mem::replace(&mut self.state, State::Done) {
            State::Resolving(expected) if expected == effect => self.on_resolved(cx, outcome),
            State::Routing(expected) if expected == effect => self.on_routed(cx, outcome),
            State::Entering {
                effect: expected,
                step,
            } if expected == effect => self.on_entered(cx, step, outcome),
            State::Naming(mut step) => match step.accept(cx, effect, outcome) {
                Progress::Waiting => {
                    self.state = State::Naming(step);
                    Progress::Waiting
                }
                Progress::Done(named) => self.on_named(cx, named),
            },
            State::Leaving(expected) if expected == effect => self.on_left(cx, outcome),
            State::Parsing {
                effect: expected,
                id,
                context,
            } if expected == effect => Progress::Done(self.on_parsed(id, context, outcome)),
            state @ (State::Start
            | State::Resolving(_)
            | State::Routing(_)
            | State::Entering { .. }
            | State::Leaving(_)
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
        let request = Request::ResolveIdentity {
            channel_type: self.channel.channel_type,
            address,
            organization_id: self.organization_id.clone(),
        };
        self.state = State::Resolving(cx.io.issue_within(request, self.deadline));
    }

    /// The identity is read; a store that fails it fails the message, with
    /// nothing written.
    fn on_resolved(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> Arriving {
        match outcome.into_resolved() {
            Ok(identity_id) => {
                self.sender.identity_id = identity_id;
                self.route(cx);
                Progress::Waiting
            }
            Err(error) => failed(InboundError::Store(error)),
        }
    }

    /// Asks the kit's router which room the message belongs to (RFC §10.4).
    fn route(&mut self, cx: &mut Cx<'_>) {
        self.routes = self.routes.saturating_add(1);
        let query = RouteQuery {
            channel_id: self.message.channel_id.clone(),
            channel_type: self.channel.channel_type,
            sender_id: self.message.sender_id.clone(),
            metadata: self.message.metadata.clone(),
            organization_id: self.organization_id.clone(),
            sender: self.sender.clone(),
            form: self.channel.form(),
        };
        let request = Request::Route {
            query: Box::new(query),
        };
        self.state = State::Routing(cx.io.issue_within(request, self.deadline));
    }

    /// The router answered: its room, or a new one when it found none (RFC
    /// §10.1 step 2). A router that fails fails the message, with nothing
    /// written.
    fn on_routed(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> Arriving {
        match outcome.into_routed() {
            Ok(Some(room_id)) => self.enter(cx, room_id, Mode::Routed),
            Ok(None) => self.enter_fresh(cx),
            Err(error) => return failed(InboundError::Routing(error)),
        }
        Progress::Waiting
    }

    /// A new room for the message, with a fresh id (RFC §10.1 step 2).
    fn enter_fresh(&mut self, cx: &mut Cx<'_>) {
        let room_id = cx.env.new_room_id();
        self.enter(cx, room_id, Mode::Fresh);
    }

    /// Enters the lane of the room step 2 works on, before anything is read
    /// of it; within a lane held already, step 2 starts at once.
    fn enter(&mut self, cx: &mut Cx<'_>, room_id: RoomId, mode: Mode) {
        let mut step = Box::new(NamedRoom::new(
            room_id.clone(),
            mode,
            self.organization_id.clone(),
            self.channel.clone(),
            self.sender.clone(),
            self.deadline,
        ));
        if self.within.is_some() {
            step.start(cx);
            self.state = State::Naming(step);
            return;
        }
        let effect = cx
            .io
            .issue_within(Request::EnterLane { room_id }, self.deadline);
        self.state = State::Entering { effect, step };
    }

    /// The room's lane is held: step 2 starts.
    fn on_entered(
        &mut self,
        cx: &mut Cx<'_>,
        mut step: Box<NamedRoom>,
        outcome: Outcome,
    ) -> Arriving {
        if let Err(error) = outcome.into_entered() {
            return failed(InboundError::Lane(error));
        }
        self.lane = Some(step.room_id().clone());
        step.start(cx);
        self.state = State::Naming(step);
        Progress::Waiting
    }

    /// Step 2 is over: the room's context goes to the channel, or the room
    /// is no longer the message's and its lane is left to route it again.
    fn on_named(&mut self, cx: &mut Cx<'_>, named: Named) -> Arriving {
        match named {
            Named::Arrived(context) => self.parse(cx, context),
            Named::Failed(error) => failed(error),
            Named::Expired => Progress::Done(Err(Stopped::Expired)),
            Named::Rerouted => {
                if let Some(room_id) = self.lane.clone() {
                    tracing::debug!(
                        target: "roomkit::core::inbound",
                        room_id = %room_id,
                        "the routed room is no longer the message's; it is routed once more"
                    );
                    self.state = State::Leaving(cx.io.issue(Request::LeaveLane { room_id }));
                } else {
                    self.route_again(cx);
                }
                Progress::Waiting
            }
        }
    }

    /// The lane is left. A failed leave changes nothing the call decided:
    /// it is logged, as at the call's end, and the message is routed all the
    /// same.
    fn on_left(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> Arriving {
        if let (Err(error), Some(room_id)) = (outcome.into_left(), &self.lane) {
            tracing::warn!(
                target: "roomkit::core::inbound",
                room_id = %room_id,
                error_type = %error.error_type,
                message = %error.message,
                "leaving the room's delivery lane failed; the message is routed all the same"
            );
        }
        self.lane = None;
        self.route_again(cx);
        Progress::Waiting
    }

    /// Routes the message once more, or gives it a new room once it was
    /// (gap G-055).
    fn route_again(&mut self, cx: &mut Cx<'_>) {
        if self.routes < ROUTES {
            self.route(cx);
        } else {
            self.enter_fresh(cx);
        }
    }

    /// Hands the room's context to the channel's `handle_inbound()` (RFC
    /// §10.1 steps 3 and 4).
    fn parse(&mut self, cx: &mut Cx<'_>, context: Box<RoomContext>) -> Arriving {
        let id = cx.env.new_event_id();
        let request = Request::HandleInbound {
            channel_id: self.message.channel_id.clone(),
            message: (*self.message).clone(),
            context: self.receiver_context((*context).clone()),
            id: id.clone(),
        };
        let effect = cx.io.issue_within(request, self.deadline);
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
        let receiver = context
            .bindings
            .iter()
            .find(|binding| binding.channel_id == self.message.channel_id);
        Box::new(match receiver {
            Some(receiver) => permissions::context_for(receiver, &context),
            None => RoomContext {
                recent_events: Vec::new(),
                ..context
            },
        })
    }

    /// Stamps the event the channel parsed (RFC §10.1 step 4), in the room
    /// whose lane the arrival holds.
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
        let mut event = stamp::stamp_trigger(*parsed, &self.message, &self.channel, id);
        if let Some(room_id) = self.lane.as_ref().or(self.within.as_ref()) {
            event.room_id = room_id.clone();
        }
        Ok(Arrived { event, context })
    }
}

/// The arrival fails with `error`, nothing of the message written.
fn failed(error: InboundError) -> Arriving {
    Progress::Done(Err(Stopped::Failed(error)))
}
