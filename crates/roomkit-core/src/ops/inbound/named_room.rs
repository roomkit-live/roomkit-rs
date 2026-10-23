//! Step 2 of the inbound pipeline for a room the caller names (RFC §10.1
//! step 2, §5.7, §7.5 rule 7, §17.2).
//!
//! The room is read for the organization the caller acts for: a room of
//! another organization fails the message with nothing written (gap G-051).
//! A missing room is created with the id, in the caller's organization:
//! `room_created` is emitted and the `ON_ROOM_CREATED` hooks run, awaited
//! (gap G-050); the channel is then attached with a binding naming the
//! sender and, where replies go to the sender's address, its recipient
//! (RFC §5.7); on a chat channel, whose correspondent is the chat, it names
//! neither until routing reads the chat (gap G-051); for a channel other
//! than `VOICE` and `REALTIME_VOICE`,
//! `ON_SESSION_STARTED` fires and `session_started` is emitted. A room that
//! exists has an unbound channel attached at the channel's defaults, unless
//! it was detached from the room (gap G-053): a detached channel stays
//! unbound, and step 11 refuses its event `source_unbound`.
//!
//! Every attach runs under the room's lock, decided on the room as the lock
//! finds it, so that it never replaces a binding made meanwhile, by an
//! `ON_ROOM_CREATED` hook among others. What step 2 read last is the room's
//! context at step 3.

use std::mem;

use serde_json::Value;

use super::InboundError;
use crate::model::{
    ChannelBinding, ChannelCategory, ChannelType, ErrorRecord, Room, RoomContext, RoomId,
};
use crate::ops::lifecycle::change::{Change, Changed};
use crate::ops::lifecycle::locked::LockedSection;
use crate::ops::lifecycle::report::{self, Reported};
use crate::ops::protocol::{EffectId, Outcome, Request};
use crate::ops::{ChannelDeclaration, Cx, LifecycleError, Progress};
use crate::rules::{addresses, attachment, organizations};

/// Step 2 for the room a caller names.
#[derive(Debug)]
pub(super) struct NamedRoom {
    room_id: RoomId,
    organization_id: Option<String>,
    channel: Box<ChannelDeclaration>,
    /// The sender, in the channel's form (RFC §10.1 step 1).
    sender: String,
    state: State,
}

/// What step 2 ends with: the room's context for step 3, or why the message
/// fails.
pub(super) type Named = Result<Box<RoomContext>, InboundError>;

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
    Attaching {
        section: Box<LockedSection>,
        /// Whether step 2 created the room.
        created: bool,
    },
    Done,
}

impl NamedRoom {
    /// Step 2 into `room_id`, for a caller acting for `organization_id`, of
    /// a message on `channel` from `sender`.
    pub(super) fn new(
        room_id: RoomId,
        organization_id: Option<String>,
        channel: Box<ChannelDeclaration>,
        sender: String,
    ) -> Self {
        Self {
            room_id,
            organization_id,
            channel,
            sender,
            state: State::Start,
        }
    }

    /// Reads the room. The read holds the room's recent events, since it is
    /// the context of step 3 when step 2 writes nothing.
    pub(super) fn start(&mut self, cx: &mut Cx<'_>) {
        if matches!(self.state, State::Start) {
            self.state = State::Reading(cx.io.issue(Request::LoadContext {
                room_id: self.room_id.clone(),
                recent_events: cx.config.context_window,
            }));
        }
    }

    /// Takes the driver's answer to step 2's outstanding effect.
    pub(super) fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        effect: EffectId,
        outcome: Outcome,
    ) -> Progress<Named> {
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
            State::Attaching {
                mut section,
                created,
            } if section.awaits(effect) => match section.accept(cx, effect, outcome) {
                Progress::Waiting => {
                    self.state = State::Attaching { section, created };
                    Progress::Waiting
                }
                Progress::Done(attached) => self.on_attached(cx, attached, created),
            },
            state @ (State::Start
            | State::Reading(_)
            | State::Creating { .. }
            | State::Rereading { .. }
            | State::Observing(_)
            | State::Attaching { .. }
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
            Ok(None) => self.create(cx),
            Err(error) => Progress::Done(Err(InboundError::Store(error))),
        }
    }

    /// A room that exists: the caller's, and its channel attached unless
    /// rule 7 refuses (gap G-053).
    fn on_existing(&mut self, cx: &mut Cx<'_>, context: Box<RoomContext>) -> Progress<Named> {
        if !organizations::admits(self.organization_id.as_deref(), &context.room) {
            return Progress::Done(Err(InboundError::OtherOrganization(self.room_id.clone())));
        }
        if !attachment::may_attach_automatically(&context, &self.channel.channel_id) {
            return Progress::Done(Ok(context));
        }
        let binding = self.channel.binding(self.room_id.clone(), cx.env.now());
        self.attach(cx, binding, false)
    }

    /// Creates the room with the id, in the caller's organization.
    fn create(&mut self, cx: &mut Cx<'_>) -> Progress<Named> {
        let mut room = Room::new(self.room_id.clone(), cx.env.now());
        room.organization_id.clone_from(&self.organization_id);
        let effect = cx.io.issue(Request::CreateRoom { room: room.clone() });
        self.state = State::Creating {
            effect,
            room: Box::new(room),
        };
        Progress::Waiting
    }

    /// The room is created: `room_created` is emitted and its hooks are
    /// awaited before the channel is attached (gap G-050). A failed creation
    /// reads the room again, in case another call created it meanwhile.
    fn on_created(&mut self, cx: &mut Cx<'_>, room: Room, outcome: Outcome) -> Progress<Named> {
        if let Err(error) = outcome.into_stored() {
            let effect = cx.io.issue(Request::LoadContext {
                room_id: self.room_id.clone(),
                recent_events: cx.config.context_window,
            });
            self.state = State::Rereading { effect, error };
            return Progress::Waiting;
        }
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
            Ok(None) | Err(_) => Progress::Done(Err(InboundError::Store(creation))),
        }
    }

    /// The `ON_ROOM_CREATED` hooks are over; what they raised was the
    /// driver's to log.
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
        self.attach_sender(cx)
    }

    /// Attaches the channel to the room step 2 created, its binding naming
    /// the sender.
    fn attach_sender(&mut self, cx: &mut Cx<'_>) -> Progress<Named> {
        let binding = self.sender_binding(cx);
        self.attach(cx, binding, true)
    }

    /// The binding of a room step 2 created: the channel's defaults, the
    /// sender as its correspondent (`participant_id`) and, on a transport,
    /// as its recipient, under the channel's recipient key (RFC §5.7, gap
    /// G-051). On a chat channel replies go to the chat, which is the
    /// correspondent as well: the binding names neither, the chat being
    /// routing's to read.
    fn sender_binding(&self, cx: &mut Cx<'_>) -> ChannelBinding {
        let mut binding = self.channel.binding(self.room_id.clone(), cx.env.now());
        if !self.channel.addressing().replies_to_sender() {
            return binding;
        }
        binding.participant_id = addresses::correspondent(&self.sender);
        let addressed =
            binding.participant_id.is_some() && binding.category == ChannelCategory::Transport;
        if let (true, Some(key)) = (addressed, &self.channel.recipient_key) {
            binding
                .metadata
                .insert(key.clone(), Value::String(self.sender.clone()));
        }
        binding
    }

    /// Attaches `binding` under the room's lock.
    fn attach(
        &mut self,
        cx: &mut Cx<'_>,
        binding: ChannelBinding,
        created: bool,
    ) -> Progress<Named> {
        let change = Change::AttachAutomatically(Box::new(binding));
        let mut section = Box::new(LockedSection::new(self.room_id.clone(), change));
        section.start(cx);
        self.state = State::Attaching { section, created };
        Progress::Waiting
    }

    /// The attach is over, and reported: a room step 2 created starts a
    /// session on a channel other than `VOICE` and `REALTIME_VOICE` (RFC §10.1
    /// step 2).
    fn on_attached(
        &mut self,
        cx: &mut Cx<'_>,
        attached: Result<Changed, LifecycleError>,
        created: bool,
    ) -> Progress<Named> {
        let context = match attached {
            Ok(changed) => changed.into_context(),
            Err(error) => return Progress::Done(Err(attach_failure(error))),
        };
        if created && starts_session(self.channel.channel_type) {
            let started = Reported::SessionStarted {
                channel_id: &self.channel.channel_id,
                channel_type: self.channel.channel_type,
            };
            report::report(cx, started, &context);
        }
        Progress::Done(Ok(context))
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

/// What a failed attach fails the message with.
fn attach_failure(error: LifecycleError) -> InboundError {
    match error {
        LifecycleError::RoomNotFound(room_id) => InboundError::RoomNotFound(room_id),
        LifecycleError::Store(error) => InboundError::Store(error),
        LifecycleError::Lock(error) => InboundError::Lock(error),
        // An automatic attach refuses nothing before the lock, and decides
        // nothing that fails under it.
        error @ (LifecycleError::ChannelNotBound { .. }
        | LifecycleError::InternalVisibility
        | LifecycleError::ReservedMetadataKey(_)) => {
            InboundError::Store(ErrorRecord::new("Internal", error.to_string()))
        }
    }
}
