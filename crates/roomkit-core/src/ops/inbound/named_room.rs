//! Step 2 of the inbound pipeline for a room id in hand (RFC §10.1 step 2,
//! §5.7, §7.5 rule 7, §17.2): the room the caller names, the room a router
//! returned, or a new room for a message no room was found for.
//!
//! The room is read for the organization the caller acts for: a room of
//! another organization fails the message with nothing written (gap G-051),
//! whoever named it, the caller or a router. A missing room is created with
//! the id, in the caller's organization: the `ON_ROOM_CREATED` hooks run,
//! awaited (gap G-050), then `room_created` is emitted; the channel is then
//! attached with a binding naming the message's address, the sender's or on
//! a chat channel the chat (RFC §5.7, gap G-057), as its correspondent and,
//! on a transport, as its recipient; for a channel other than `VOICE` and
//! `REALTIME_VOICE`, `ON_SESSION_STARTED` fires and `session_started` is
//! emitted.
//!
//! A room that exists is handled by who named it:
//!
//! - a room the caller names has an unbound channel attached at the
//!   channel's defaults, unless it was detached from the room (gap G-053): a
//!   detached channel stays unbound, and step 11 refuses its event
//!   `source_unbound`;
//! - a room a router returned has the sender recorded on its binding of the
//!   channel, under the room lock, before step 3 (RFC §10.1 step 2, gap
//!   G-061). A room CLOSED or ARCHIVED by then, PAUSED but neither the
//!   sender's conversation by step 1 nor bound to the channel as a group
//!   (gap G-045), whose binding names another sender, or that step 3 no
//!   longer admits the sender to (someone else heard on the channel
//!   meanwhile), is no longer the message's, which is routed once more
//!   (gap G-055).
//!
//! A new room for a message no room was found for is created with a fresh
//! id: a store that holds a room with it fails the message, since that room
//! is someone else's.
//!
//! Every attach and record runs under the room's lock, decided on the room
//! as the lock finds it, so that it never replaces a binding made
//! meanwhile, by an `ON_ROOM_CREATED` hook among others. What step 2 read
//! last is the room's context at step 3.
//!
//! The call's deadline bounds step 2's read of the room and its wait for
//! the room lock, routing's among them (RFC §10.4, §13.6): past it, step 2
//! stops with nothing written. A room step 2 creates is attached whatever
//! the deadline, its creation hooks bounded by their own timeouts: the
//! creation is a write, and a room created is completed (gap G-068).

use std::convert::Infallible;
use std::mem;

use super::InboundError;
use super::arrival_change::{ArrivalChange, Recorded, SenderRecord};
use crate::model::{ChannelBinding, ChannelType, ErrorRecord, Room, RoomContext, RoomId};
use crate::ops::io::Bound;
use crate::ops::lifecycle::locked::{LockedSection, SectionError, SectionResult};
use crate::ops::lifecycle::report::{self, Reported};
use crate::ops::protocol::{EffectId, Outcome, StoreRequest};
use crate::ops::{ChannelDeclaration, Cx, Progress};
use crate::rules::chain_depth::Draw;
use crate::rules::routing::{self, Recording, Sender};
use crate::rules::{attachment, organizations};

/// Who named the room step 2 works on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Mode {
    /// The caller.
    Named,
    /// A router (RFC §10.4).
    Routed,
    /// Nobody: the message was routed to no room, and the room is new.
    Fresh,
}

/// Step 2 for a room id in hand.
#[derive(Debug)]
pub(super) struct NamedRoom {
    room_id: RoomId,
    organization_id: Option<String>,
    channel: Box<ChannelDeclaration>,
    /// The message's sender, as routing names them (RFC §10.4).
    sender: Sender,
    mode: Mode,
    /// The call's deadline (RFC §13.6).
    bound: Bound,
    /// For a message the room's delivery work sent, the budget a room
    /// step 2 creates is charged on (gap G-070).
    opening: Option<Draw>,
    /// Whether step 2 charged `opening`, having read the room absent: the
    /// charge stays when another call created the room first (gap G-070).
    charged: bool,
    /// Whether step 2 created the room.
    created: bool,
    state: State,
}

/// What step 2 ends with.
#[derive(Debug)]
pub(super) enum Named {
    /// The room's context for step 3.
    Arrived(Box<RoomContext>),
    /// The routed room is no longer the message's: route it once more (gap
    /// G-055).
    Rerouted,
    /// Why the message fails.
    Failed(InboundError),
    /// The call's deadline passed: nothing was written (RFC §13.6).
    Expired,
    /// The room would be created past the reentry budget the room's work
    /// that routed the message draws on: nothing was written (gap G-070).
    PastBudget,
}

#[derive(Debug)]
enum State {
    Start,
    Reading(EffectId),
    Creating {
        effect: EffectId,
        room: Box<Room>,
    },
    /// The creation failed: the room may have been created meanwhile.
    Rereading {
        effect: EffectId,
        error: ErrorRecord,
    },
    /// The `ON_ROOM_CREATED` hooks run.
    Observing(EffectId),
    /// The attach or the record, under the room's lock.
    Changing {
        section: Box<LockedSection<ArrivalChange>>,
        /// Whether step 2 created the room.
        created: bool,
    },
    Done,
}

impl NamedRoom {
    /// Step 2 into `room_id`, named as `mode` says, for a caller acting for
    /// `organization_id`, of a message on `channel` from `sender`, within
    /// the call's `bound`.
    pub(super) fn new(
        room_id: RoomId,
        mode: Mode,
        organization_id: Option<String>,
        channel: Box<ChannelDeclaration>,
        sender: Sender,
        bound: Bound,
    ) -> Self {
        Self {
            room_id,
            organization_id,
            channel,
            sender,
            mode,
            bound,
            opening: None,
            charged: false,
            created: false,
            state: State::Start,
        }
    }

    /// The same step, charging `opening` one pass before it creates the
    /// room, refused past it (gap G-070).
    pub(super) fn opening_on(mut self, opening: Option<Draw>) -> Self {
        self.opening = opening;
        self
    }

    /// Whether step 2 charged its opening budget, for a room it read absent.
    pub(super) fn charged(&self) -> bool {
        self.charged
    }

    /// Whether step 2 created the room: the message's cascade is then the
    /// work of the delivery whose budget opened it (gap G-070).
    pub(super) fn created(&self) -> bool {
        self.created
    }

    /// The room step 2 works on.
    pub(super) fn room_id(&self) -> &RoomId {
        &self.room_id
    }

    /// Whether step 2 waits for `effect`.
    pub(super) fn awaits(&self, effect: EffectId) -> bool {
        match &self.state {
            State::Reading(awaited)
            | State::Observing(awaited)
            | State::Creating {
                effect: awaited, ..
            }
            | State::Rereading {
                effect: awaited, ..
            } => *awaited == effect,
            State::Changing { section, .. } => section.awaits(effect),
            State::Start | State::Done => false,
        }
    }

    /// Reads the room, or creates a new one at once. The read holds the
    /// room's recent events, since it is the context of step 3 when step 2
    /// writes nothing.
    pub(super) fn start(&mut self, cx: &mut Cx<'_>) {
        if !matches!(self.state, State::Start) {
            return;
        }
        if self.mode == Mode::Fresh {
            self.create(cx);
            return;
        }
        self.state = State::Reading(self.read(cx));
    }

    /// Reads the room, within the call's deadline.
    fn read(&self, cx: &mut Cx<'_>) -> EffectId {
        let request = StoreRequest::LoadContext {
            room_id: self.room_id.clone(),
            recent_events: cx.config.context_window,
        };
        cx.io.issue_within(request, self.bound)
    }

    /// Takes the driver's answer to step 2's outstanding effect.
    pub(super) fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        effect: EffectId,
        outcome: Outcome,
    ) -> Progress<Named> {
        if outcome == Outcome::DeadlinePassed {
            // Only a read and the wait for the lock carry the deadline: no
            // lock is held, nothing was written.
            self.state = State::Done;
            return Progress::Done(Named::Expired);
        }
        match mem::replace(&mut self.state, State::Done) {
            State::Reading(expected) if expected == effect => self.on_read(cx, outcome),
            State::Creating {
                effect: expected,
                room,
            } if expected == effect => self.on_created(cx, *room, outcome),
            State::Rereading {
                effect: expected,
                error,
            } if expected == effect => self.on_reread(cx, error, outcome),
            State::Observing(expected) if expected == effect => self.on_observed(cx, outcome),
            State::Changing {
                mut section,
                created,
            } if section.awaits(effect) => match section.accept(cx, effect, outcome) {
                Progress::Waiting => {
                    self.state = State::Changing { section, created };
                    Progress::Waiting
                }
                Progress::Done(changed) => self.on_changed(cx, changed.result, created),
            },
            state @ (State::Start
            | State::Reading(_)
            | State::Creating { .. }
            | State::Rereading { .. }
            | State::Observing(_)
            | State::Changing { .. }
            | State::Done) => {
                debug_assert!(false, "step 2 received {effect:?} in {state:?}");
                self.state = state;
                Progress::Waiting
            }
        }
    }

    fn on_read(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> Progress<Named> {
        match outcome.into_context() {
            Ok(Some(context)) => self.on_existing(cx, context),
            // A room the store lacks, named by the call or by its router:
            // the room's work that sent the message opens it, on its budget
            // (gap G-070).
            Ok(None) => match self.opening.take() {
                Some(draw) if !draw.take() => Progress::Done(Named::PastBudget),
                Some(_) => {
                    self.charged = true;
                    self.create(cx)
                }
                None => self.create(cx),
            },
            Err(error) => Progress::Done(Named::Failed(InboundError::Store(error))),
        }
    }

    /// A room that exists: the caller's, then its channel attached unless
    /// rule 7 refuses (gap G-053), or the routed sender recorded (gap
    /// G-061).
    fn on_existing(&mut self, cx: &mut Cx<'_>, context: Box<RoomContext>) -> Progress<Named> {
        if !organizations::admits(self.organization_id.as_deref(), &context.room) {
            let error = InboundError::OtherOrganization(self.room_id.clone());
            return Progress::Done(Named::Failed(error));
        }
        match self.mode {
            Mode::Routed => {
                let record = SenderRecord {
                    binding: self.sender_binding(cx),
                    sender: self.sender.clone(),
                    form: self.channel.form(),
                };
                self.change(cx, ArrivalChange::RecordSender(Box::new(record)), false)
            }
            Mode::Named | Mode::Fresh => {
                if !attachment::may_attach_automatically(&context, &self.channel.channel_id) {
                    return Progress::Done(Named::Arrived(context));
                }
                let binding = self.channel.binding(self.room_id.clone(), cx.env.now());
                self.change(
                    cx,
                    ArrivalChange::AttachAutomatically(Box::new(binding)),
                    false,
                )
            }
        }
    }

    /// Creates the room with the id, in the caller's organization.
    fn create(&mut self, cx: &mut Cx<'_>) -> Progress<Named> {
        let mut room = Room::new(self.room_id.clone(), cx.env.now());
        room.organization_id.clone_from(&self.organization_id);
        let effect = cx.io.issue(StoreRequest::CreateRoom { room: room.clone() });
        self.state = State::Creating {
            effect,
            room: Box::new(room),
        };
        Progress::Waiting
    }

    /// The room is created: its hooks are awaited, then `room_created` is
    /// emitted, before the channel is attached (RFC §10.1 step 2, gap
    /// G-050). A failed creation
    /// reads the room again, in case another call created it meanwhile; a
    /// fresh room's is never read again, since a room holding its id is
    /// someone else's.
    fn on_created(&mut self, cx: &mut Cx<'_>, room: Room, outcome: Outcome) -> Progress<Named> {
        if let Err(error) = outcome.into_stored() {
            if self.mode == Mode::Fresh {
                return Progress::Done(Named::Failed(InboundError::Store(error)));
            }
            let effect = self.read(cx);
            self.state = State::Rereading { effect, error };
            return Progress::Waiting;
        }
        self.created = true;
        let context = Box::new(RoomContext::new(room));
        match report::report_awaited(cx, Reported::RoomCreated, &context) {
            Some(effect) => {
                self.state = State::Observing(effect);
                Progress::Waiting
            }
            None => self.attach_sender(cx),
        }
    }

    /// A room created meanwhile is taken as any room that exists; with no
    /// room, the creation's failure fails the message.
    fn on_reread(
        &mut self,
        cx: &mut Cx<'_>,
        creation: ErrorRecord,
        outcome: Outcome,
    ) -> Progress<Named> {
        match outcome.into_context() {
            Ok(Some(context)) => self.on_existing(cx, context),
            Ok(None) | Err(_) => Progress::Done(Named::Failed(InboundError::Store(creation))),
        }
    }

    /// The `ON_ROOM_CREATED` hooks are over, what they raised the driver's
    /// to log: `room_created` is emitted, and the channel attached.
    fn on_observed(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> Progress<Named> {
        if let Err(error) = outcome.into_observed() {
            tracing::warn!(
                target: "roomkit::core::inbound",
                room_id = %self.room_id,
                error_type = %error.error_type,
                message = %error.message,
                "the ON_ROOM_CREATED hooks could not be run; the channel is attached all the same"
            );
        }
        report::announce(cx, Reported::RoomCreated, &self.room_id);
        self.attach_sender(cx)
    }

    /// Attaches the channel to the room step 2 created, its binding naming
    /// the sender.
    fn attach_sender(&mut self, cx: &mut Cx<'_>) -> Progress<Named> {
        let binding = self.sender_binding(cx);
        self.change(
            cx,
            ArrivalChange::AttachAutomatically(Box::new(binding)),
            true,
        )
    }

    /// The channel's binding at its defaults, with the sender recorded as a
    /// routed sender is (RFC §5.7, gap G-061): the message's address as its
    /// correspondent and, on a transport, as its recipient under the
    /// channel's recipient key. A message with no address (a chat message
    /// without its chat) and the framework's own sender `system` name
    /// neither. A binding at its defaults names no one, whoever the room's
    /// participants are.
    fn sender_binding(&self, cx: &mut Cx<'_>) -> ChannelBinding {
        let binding = self.channel.binding(self.room_id.clone(), cx.env.now());
        match routing::recording(&binding, &self.sender, &self.channel.form(), &[]) {
            Recording::Write(update) => update.apply(&binding),
            Recording::Kept | Recording::Taken => binding,
        }
    }

    /// Makes `change` under the room's lock, waiting for the lock within the
    /// call's deadline unless step 2 created the room, whose attach
    /// completes its creation.
    fn change(&mut self, cx: &mut Cx<'_>, change: ArrivalChange, created: bool) -> Progress<Named> {
        let bound = if created {
            Bound::Unbounded
        } else {
            self.bound
        };
        let mut section = Box::new(
            LockedSection::changing(self.room_id.clone(), None, change).waiting_within(bound),
        );
        section.start(cx);
        self.state = State::Changing { section, created };
        Progress::Waiting
    }

    /// The attach or the record is over, and reported: a room step 2
    /// created starts a session on a channel other than `VOICE` and
    /// `REALTIME_VOICE` (RFC §10.1 step 2).
    fn on_changed(
        &mut self,
        cx: &mut Cx<'_>,
        changed: SectionResult<ArrivalChange>,
        created: bool,
    ) -> Progress<Named> {
        let context = match changed {
            Ok(Recorded::Rerouted(_)) => return Progress::Done(Named::Rerouted),
            Ok(recorded) => recorded.into_context(),
            Err(error) => return Progress::Done(Named::Failed(change_failure(error))),
        };
        if created && starts_session(self.channel.channel_type) {
            let started = Reported::SessionStarted {
                channel_id: &self.channel.channel_id,
                channel_type: self.channel.channel_type,
            };
            report::report(cx, started, &context);
        }
        Progress::Done(Named::Arrived(context))
    }
}

/// Whether the creation of a room for a message on a channel of
/// `channel_type` starts a session (RFC §10.1 step 2, §26.1): every channel
/// but `VOICE` and `REALTIME_VOICE`, whose sessions start with their media.
fn starts_session(channel_type: ChannelType) -> bool {
    !matches!(
        channel_type,
        ChannelType::Voice | ChannelType::RealtimeVoice
    )
}

/// What a failed attach or record fails the message with; neither ever
/// refuses.
fn change_failure(error: SectionError<Infallible>) -> InboundError {
    match error {
        SectionError::RoomNotFound(room_id) => InboundError::RoomNotFound(room_id),
        SectionError::Store(error) => InboundError::Store(error),
        SectionError::Lock(error) => InboundError::Lock(error),
        SectionError::Refused(never) => match never {},
    }
}
