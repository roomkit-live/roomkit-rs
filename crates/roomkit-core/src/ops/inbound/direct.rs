//! A direct injection's way to the room lock (RFC §10.5, gap G-069).
//!
//! The event the integrator injects takes the inbound pipeline without the
//! channel's parse: it is refused before anything is read when it cannot be
//! one (an unregistered channel, the visibility `internal`, an instruction
//! that names no agent or carries a key), then stamped, its source the
//! channel it names. It runs none of step 2: a room that does not exist is
//! not created, and a channel the room does not bind is not attached. The
//! call reads the room before the lock only when an off-lock hook fires for
//! the event, whose check reads it (RFC §9.5.1); otherwise its pass starts
//! at once. Every request up to its decision carries the call's deadline
//! (RFC §13.6).
//!
//! A room of another organization than the one the caller acts for, which
//! the call does not find, as a room that does not exist (RFC §17.2, gap
//! G-077), and a room that binds no channel the event names refuse it with
//! nothing written (gap G-069): the host named the room and the channel
//! itself, so the call is its error, with nothing to audit. Both are
//! checked under the room lock, before the pass's hooks or the record of an
//! injection past its caller's reentry budget, and on the room an off-lock
//! check reads, before its hooks too. The organization is checked on the room as the lock finds
//! it, before its due timers are applied, so nothing is written in a room
//! the caller does not reach (RFC §17.2).

use super::arrival::Arrived;
use super::cascade::replay_result;
use super::deadline::Stopped;
use super::host_post::HostPost;
use super::instructions::{self, Admission};
use super::work_read::{Read, WorkRead};
use super::{InboundError, InboundResult, Machine, Stage, stamp};
use crate::model::{
    ChannelBinding, ChannelId, ChannelType, DirectEvent, Post, Room, RoomContext, RoomEvent, RoomId,
};
use crate::ops::io::Bound;
use crate::ops::protocol::{EffectId, Outcome, StoreRequest};
use crate::ops::{ChannelDeclaration, Cx, KitConfig, Progress};
use crate::rules::permissions::WriteRefusal;
use crate::rules::{idempotency, organizations};

/// What a direct injection is checked for on the room, before its hooks
/// (RFC §10.5, §17.2, gap G-069).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct DirectCall {
    /// The organization its caller acts for; `None` reads the room
    /// unscoped.
    pub(super) organization_id: Option<String>,
}

impl DirectCall {
    /// Why the room as `context` holds it refuses `event`, if it does: it
    /// is another organization's than the caller's, or binds no channel the
    /// event names. Nothing is written for either.
    pub(super) fn refusal(&self, event: &RoomEvent, context: &RoomContext) -> Option<InboundError> {
        self.foreign(&context.room)
            .or_else(|| Self::unbound(event, context))
    }

    /// Why `room` is not the caller's, if it is not: another organization's,
    /// or one of none for a caller acting for an organization, which the
    /// injection does not find, as a room that does not exist (RFC §17.2,
    /// gap G-077). Checked on the room as the lock finds it, before its due
    /// timers are applied.
    pub(super) fn foreign(&self, room: &Room) -> Option<InboundError> {
        let admitted = organizations::admits(self.organization_id.as_deref(), room);
        (!admitted).then(|| InboundError::RoomNotFound(room.id.clone()))
    }

    /// Why the room as `context` holds it refuses `event` as the caller's
    /// room: it binds no channel the event names.
    pub(super) fn unbound(event: &RoomEvent, context: &RoomContext) -> Option<InboundError> {
        let bound = context.binding(&event.source.channel_id).is_some();
        (!bound).then(|| InboundError::Refused {
            reason: WriteRefusal::Unbound.blocked_by().to_owned(),
        })
    }
}

/// The read of a directly injected event's room for its off-lock check
/// (RFC §9.5.1).
#[derive(Debug)]
pub(super) struct DirectRead {
    event: RoomEvent,
    /// What the room read is checked for.
    call: DirectCall,
    /// The read, outstanding.
    effect: Option<EffectId>,
}

impl DirectRead {
    /// The read of the room of `event`, stamped already, for `call`.
    pub(super) fn new(event: RoomEvent, call: DirectCall) -> Self {
        Self {
            event,
            call,
            effect: None,
        }
    }

    /// Reads the room, within the call's `bound`.
    pub(super) fn start(&mut self, cx: &mut Cx<'_>, bound: Bound) {
        let request = StoreRequest::LoadContext {
            room_id: self.event.room_id.clone(),
            recent_events: cx.config.context_window,
        };
        self.effect = Some(cx.io.issue_within(request, bound));
    }

    /// Takes the driver's answer to the read: the room as the off-lock check
    /// reads it, or why the call stops, nothing written. A missing room
    /// fails the call, and so does one that refuses the injection, before
    /// the check's hooks run (gap G-069).
    pub(super) fn accept(
        &mut self,
        effect: EffectId,
        outcome: Outcome,
    ) -> Progress<Result<Arrived, Stopped>> {
        if self.effect != Some(effect) {
            debug_assert!(false, "a direct injection received {effect:?}");
            return Progress::Waiting;
        }
        self.effect = None;
        if outcome == Outcome::DeadlinePassed {
            return Progress::Done(Err(Stopped::Expired));
        }
        let failed = |error| Progress::Done(Err(Stopped::Failed(error)));
        match outcome.into_context() {
            Ok(Some(context)) => match self.call.refusal(&self.event, &context) {
                Some(refusal) => failed(refusal),
                None => Progress::Done(Ok(Arrived {
                    event: self.event.clone(),
                    context,
                })),
            },
            Ok(None) => failed(InboundError::RoomNotFound(self.event.room_id.clone())),
            Err(error) => failed(InboundError::Store(error)),
        }
    }
}

/// A directly injected event once admitted: stamped, or refused as an
/// instruction that cannot be one (RFC §10.1.1 rules 1 and 2).
#[derive(Debug)]
pub(super) enum Admitted {
    Event(Box<RoomEvent>),
    Refused(Box<InboundResult>),
}

/// Refuses `event` before anything is read when it cannot be one, or
/// stamps it for `room_id` (RFC §10.5): its channel must be registered,
/// and it is admitted as a message is, a blank idempotency key read as
/// none (RFC §13.4, §10.1.1, gap G-041).
///
/// # Errors
///
/// Returns why the event is refused before anything is read.
pub(super) fn admit(
    cx: &mut Cx<'_>,
    room_id: RoomId,
    event: DirectEvent,
) -> Result<Admitted, InboundError> {
    let channel = cx
        .config
        .channel(&event.channel_id)
        .cloned()
        .ok_or_else(|| InboundError::UnknownChannel(event.channel_id.clone()))?;
    admit_from(cx, room_id, event, &channel)
}

/// Admits `event` from `channel`, its source, as [`admit`] does once the
/// channel is found.
///
/// # Errors
///
/// Returns why the event is refused before anything is read.
fn admit_from(
    cx: &mut Cx<'_>,
    room_id: RoomId,
    mut event: DirectEvent,
    channel: &ChannelDeclaration,
) -> Result<Admitted, InboundError> {
    event.idempotency_key = idempotency::key(event.idempotency_key);
    if let Admission::Refused(refusal) = instructions::admit(cx.config, (&event).into())? {
        return Ok(Admitted::Refused(Box::new(instructions::refused(refusal))));
    }
    let id = cx.env.new_event_id();
    let now = cx.env.now();
    let event = stamp::stamp_direct(event, channel, room_id, id, now);
    Ok(Admitted::Event(Box::new(event)))
}

/// Refuses `post` before anything is read when it cannot be one, or stamps
/// it for `room_id` as the direct event of its host channel (RFC §10.5): the
/// channel it names, or the kit's own `host`, must be a registered channel
/// of type `HOST`, and its event is admitted as `send_event`'s is. Returns
/// the admitted event with the host channel's binding at its defaults,
/// which a first post attaches.
///
/// # Errors
///
/// Returns [`InboundError::UnknownChannel`] for a channel no one
/// registered, [`InboundError::NotAHostChannel`] for one of another type,
/// and what [`admit`] refuses.
pub(super) fn admit_post(
    cx: &mut Cx<'_>,
    room_id: RoomId,
    post: Post,
) -> Result<(Admitted, ChannelBinding), InboundError> {
    let channel = match &post.channel_id {
        Some(channel_id) => cx
            .config
            .channel(channel_id)
            .cloned()
            .ok_or_else(|| InboundError::UnknownChannel(channel_id.clone()))?,
        None => own_host_channel(cx.config),
    };
    let channel_id = channel.channel_id.clone();
    if channel.channel_type != ChannelType::Host {
        return Err(InboundError::NotAHostChannel(channel_id));
    }
    let binding = channel.binding(room_id.clone(), cx.env.now());
    let admitted = admit_from(cx, room_id, post.into_event(channel_id), &channel)?;
    Ok((admitted, binding))
}

/// The kit's own host channel, `host`, as `config` registers it, or, when
/// no channel holds the id, as it is registered on first use (RFC §10.5): a
/// call that started before the first post registered it adopts one all
/// the same.
fn own_host_channel(config: &KitConfig) -> ChannelDeclaration {
    let host = ChannelId::host();
    config
        .channel(&host)
        .cloned()
        .unwrap_or_else(|| ChannelDeclaration::host(host))
}

impl Machine {
    /// Starts a post's way (RFC §10.5), for a caller acting for
    /// `organization_id` (RFC §17.2): refused before anything is read when
    /// it cannot be one, its room read and its speaker checked, its host
    /// channel attached on its first post, then injected as `send_event`
    /// injects.
    pub(super) fn begin_post(
        &mut self,
        cx: &mut Cx<'_>,
        room_id: RoomId,
        organization_id: Option<String>,
        post: Post,
    ) {
        self.call.channel_id.clone_from(&post.channel_id);
        self.call.room_id = Some(room_id.clone());
        let call = DirectCall { organization_id };
        self.call.direct = Some(call.clone());
        let (event, binding) = match admit_post(cx, room_id, post) {
            Ok((Admitted::Event(event), binding)) => (*event, binding),
            Ok((Admitted::Refused(refused), _)) => return self.end(cx, Ok(*refused)),
            Err(error) => return self.end(cx, Err(error)),
        };
        self.call.channel_id = Some(event.source.channel_id.clone());
        let post = HostPost::start(cx, event, call, binding, self.call.bound);
        self.stage.enter(Stage::Posting(Box::new(post)));
    }

    /// Takes an answer of the post's way: once its room took it, it is
    /// injected.
    pub(super) fn on_post(
        &mut self,
        cx: &mut Cx<'_>,
        mut post: Box<HostPost>,
        effect: EffectId,
        outcome: Outcome,
    ) {
        match post.accept(cx, effect, outcome) {
            Progress::Waiting => self.stage.enter(Stage::Posting(post)),
            Progress::Done(Ok(())) => {
                let (event, call) = post.into_parts();
                self.inject_admitted(cx, event, call);
            }
            Progress::Done(Err(stopped)) => self.stop(cx, stopped),
        }
    }

    /// Starts a direct injection's way (RFC §10.5), for a caller acting for
    /// `organization_id` (RFC §17.2): refused before anything is read when
    /// it cannot be one, its room is read for its off-lock check when an
    /// off-lock hook fires for it, and its pass starts otherwise.
    pub(super) fn begin_direct(
        &mut self,
        cx: &mut Cx<'_>,
        room_id: RoomId,
        organization_id: Option<String>,
        event: DirectEvent,
    ) {
        self.call.channel_id = Some(event.channel_id.clone());
        self.call.room_id = Some(room_id.clone());
        let call = DirectCall { organization_id };
        self.call.direct = Some(call.clone());
        let event = match admit(cx, room_id, event) {
            Ok(Admitted::Event(event)) => *event,
            Ok(Admitted::Refused(refused)) => return self.end(cx, Ok(*refused)),
            Err(error) => return self.end(cx, Err(error)),
        };
        self.inject_admitted(cx, event, call);
    }

    /// An admitted injection goes in: one from the room's work reads its
    /// room and its key first, then draws on the budget of that work (RFC
    /// §9.5.1 Past the cap, §13.4, §17.2).
    fn inject_admitted(&mut self, cx: &mut Cx<'_>, event: RoomEvent, call: DirectCall) {
        if self.cascade.drawn_on().is_none() {
            return self.go_in(cx, event, call);
        }
        let read = WorkRead::start(cx, event, call, self.call.bound);
        self.stage.enter(Stage::ReadingWork(Box::new(read)));
    }

    /// Takes an answer of the reads before the budget: a refusal ends the
    /// call, drawing nothing; a replay ends it with the event the key
    /// committed, drawing nothing; any other event draws on the budget and
    /// goes in.
    pub(super) fn on_work_read(
        &mut self,
        cx: &mut Cx<'_>,
        mut read: Box<WorkRead>,
        effect: EffectId,
        outcome: Outcome,
    ) {
        match read.accept(cx, effect, outcome) {
            Progress::Waiting => self.stage.enter(Stage::ReadingWork(read)),
            Progress::Done(Ok(Read::Replay(original))) => {
                self.end(cx, Ok(replay_result(&original)));
            }
            Progress::Done(Ok(Read::Fresh(context))) => {
                let (event, call) = read.into_parts();
                self.draw_and_go_in(cx, event, call, context);
            }
            Progress::Done(Err(stopped)) => self.stop(cx, stopped),
        }
    }

    /// An event a delivery's code injects, its room read, is charged on the
    /// cascade that delivers, as an adopted one is: past it, it is stored
    /// capped, and no hook runs for it (gap G-070); within it, its
    /// off-lock check runs on the room as read, or its pass starts.
    fn draw_and_go_in(
        &mut self,
        cx: &mut Cx<'_>,
        event: RoomEvent,
        call: DirectCall,
        context: Box<RoomContext>,
    ) {
        if self.cascade.drawn_on().is_some_and(|budget| !budget.take()) {
            return self.start_capped_trigger(event, call);
        }
        if cx.config.hooks.off_lock_checks(&event.source).is_empty() {
            return self.start_trigger(event, None);
        }
        self.after_arrival(cx, Arrived { event, context });
    }

    /// The event's room is read for its off-lock check when an off-lock
    /// hook fires for it, and its pass starts otherwise.
    fn go_in(&mut self, cx: &mut Cx<'_>, event: RoomEvent, call: DirectCall) {
        if cx.config.hooks.off_lock_checks(&event.source).is_empty() {
            return self.start_trigger(event, None);
        }
        let mut read = Box::new(DirectRead::new(event, call));
        read.start(cx, self.call.bound);
        self.stage.enter(Stage::Reading(read));
    }

    /// Takes the answer of the room's read; its off-lock check starts.
    pub(super) fn on_read(
        &mut self,
        cx: &mut Cx<'_>,
        mut read: Box<DirectRead>,
        effect: EffectId,
        outcome: Outcome,
    ) {
        match read.accept(effect, outcome) {
            Progress::Waiting => self.stage.enter(Stage::Reading(read)),
            Progress::Done(Ok(arrived)) => self.after_arrival(cx, arrived),
            Progress::Done(Err(stopped)) => self.stop(cx, stopped),
        }
    }
}
