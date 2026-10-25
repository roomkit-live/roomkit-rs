//! A post's way to its injection (RFC §10.5 "The host's own voice"): the
//! room read, the speaker checked, and the host channel attached on its
//! first post.
//!
//! The room is read scoped to the caller's organization first: a room the
//! call does not find, another organization's included, fails the post
//! whoever its speaker names (RFC §17.2). A speaker must be a participant
//! the host channel reaches, by their id or their identity's (RFC §5.5,
//! `rules::host`). A room that never bound the host channel gets it in one
//! section under the room lock, decided on the room as the lock finds it,
//! its due timers applied: a binding made by then, the integrator's or a
//! concurrent post's, is kept as it is; a room whose status refuses events,
//! or that the host channel was detached from, refuses the post as an
//! injection would be refused (`room_closed`, `source_unbound`), nothing
//! written either way. The read and the wait for the lock are bounded by
//! the call's deadline (RFC §13.6); once the lock is held, the attach runs
//! to its end. Then the post is injected as `send_event` injects.

use std::collections::VecDeque;

use super::InboundError;
use super::deadline::Stopped;
use super::direct::DirectCall;
use crate::model::{ChannelBinding, RoomContext, RoomEvent, RoomId};
use crate::ops::io::Bound;
use crate::ops::lifecycle::change::{self, Attached, Decision, SectionChange};
use crate::ops::lifecycle::locked::{LockedSection, SectionError, SectionResult};
use crate::ops::lifecycle::report::Reported;
use crate::ops::protocol::{EffectId, Outcome, StoreRequest};
use crate::ops::{Cx, Progress};
use crate::rules::attachment;
use crate::rules::host::{self, Speaker};
use crate::rules::permissions::WriteRefusal;
use crate::rules::records::Record;

/// The first post's attach of its host channel, under the room's lock (RFC
/// §10.5).
#[derive(Clone, Debug)]
pub(super) struct HostAttach(Box<ChannelBinding>);

/// Why the room refuses the host channel's first attach, nothing written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum HostRefusal {
    /// The room's status refuses new events (RFC §5.1).
    Closed,
    /// The host channel was detached from the room: only an explicit attach
    /// grants it again (RFC §7.5 rule 7).
    Detached,
}

/// What the attach did.
#[derive(Debug)]
pub(super) enum HostAttached {
    Attached(Attached),
    /// The room binds the channel by now: the binding is kept as it is.
    Kept,
}

impl SectionChange for HostAttach {
    type Done = HostAttached;
    type Refusal = HostRefusal;

    fn decide(
        &self,
        mut context: Box<RoomContext>,
        cx: &Cx<'_>,
    ) -> Result<Decision<HostAttached>, HostRefusal> {
        let now = cx.env.now();
        let channel_id = &self.0.channel_id;
        if !context.room.status.accepts_events() {
            return Err(HostRefusal::Closed);
        }
        if context.binding(channel_id).is_some() {
            return Ok(Decision::unwritten(HostAttached::Kept));
        }
        if attachment::revoked(&context.room, channel_id) {
            return Err(HostRefusal::Detached);
        }
        // Attached as the section decides it, under the lock.
        let binding = ChannelBinding {
            attached_at: now,
            ..(*self.0).clone()
        };
        context.bindings.push(binding.clone());
        let mut writes = VecDeque::from([StoreRequest::StoreBinding {
            binding: binding.clone(),
        }]);
        let record = Record::attached(&binding);
        change::write_records(cx, &mut writes, &mut context, &[record]);
        Ok(Decision {
            writes,
            done: HostAttached::Attached(Attached {
                binding: Box::new(binding),
                context,
            }),
        })
    }

    /// An attach reports `ON_CHANNEL_ATTACHED` and `room_channel_attached`.
    fn reported(done: &HostAttached) -> Option<(Reported<'_>, &RoomContext)> {
        match done {
            HostAttached::Attached(attached) => Some(attached.reported()),
            HostAttached::Kept => None,
        }
    }
}

/// A post's read of its room, its speaker's check and its first attach.
#[derive(Debug)]
pub(super) struct HostPost {
    /// The post's event, stamped already: its source is the host channel,
    /// its participant the speaker.
    event: RoomEvent,
    call: DirectCall,
    /// The host channel's binding at its defaults, attached on a first
    /// post.
    binding: Box<ChannelBinding>,
    bound: Bound,
    state: State,
}

#[derive(Debug)]
enum State {
    Reading(EffectId),
    Attaching(Box<LockedSection<HostAttach>>),
    Done,
}

impl HostPost {
    /// Reads the room of `event`, the post's stamped event, for `call`,
    /// within the call's `bound`; `binding` is the host channel's at its
    /// defaults.
    pub(super) fn start(
        cx: &mut Cx<'_>,
        event: RoomEvent,
        call: DirectCall,
        binding: ChannelBinding,
        bound: Bound,
    ) -> Self {
        let request = StoreRequest::LoadContext {
            room_id: event.room_id.clone(),
            recent_events: 0,
        };
        let effect = cx.io.issue_within(request, bound);
        Self {
            event,
            call,
            binding: Box::new(binding),
            bound,
            state: State::Reading(effect),
        }
    }

    /// Whether the post waits for `effect`.
    pub(super) fn awaits(&self, effect: EffectId) -> bool {
        match &self.state {
            State::Reading(waited) => *waited == effect,
            State::Attaching(section) => section.awaits(effect),
            State::Done => false,
        }
    }

    /// Takes the driver's answer to one of the post's effects: once the
    /// room is read, its speaker checked and its host channel bound, the
    /// post goes on to its injection; else it stops, nothing written.
    pub(super) fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        effect: EffectId,
        outcome: Outcome,
    ) -> Progress<Result<(), Stopped>> {
        if !self.awaits(effect) {
            debug_assert!(false, "a post received {effect:?}");
            return Progress::Waiting;
        }
        if outcome == Outcome::DeadlinePassed {
            // Only the read and the wait for the lock carry the deadline: no
            // lock is held, nothing was written.
            self.state = State::Done;
            return Progress::Done(Err(Stopped::Expired));
        }
        match std::mem::replace(&mut self.state, State::Done) {
            State::Reading(_) => self.on_read(cx, outcome),
            State::Attaching(mut section) => match section.accept(cx, effect, outcome) {
                Progress::Waiting => {
                    self.state = State::Attaching(section);
                    Progress::Waiting
                }
                Progress::Done(ended) => Progress::Done(Self::attached(ended.result)),
            },
            State::Done => Progress::Waiting,
        }
    }

    /// The post's event and what it is checked for, once the room took it.
    pub(super) fn into_parts(self) -> (RoomEvent, DirectCall) {
        (self.event, self.call)
    }

    /// The room is read: a room the caller does not find fails the post, so
    /// does a speaker the host channel does not reach; a room that binds
    /// the channel takes the post at once, one that does not has it
    /// attached first.
    fn on_read(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> Progress<Result<(), Stopped>> {
        let failed = |error| Progress::Done(Err(Stopped::Failed(error)));
        let context = match outcome.into_context() {
            Ok(Some(context)) => context,
            Ok(None) => return failed(InboundError::RoomNotFound(self.room_id().clone())),
            Err(error) => return failed(InboundError::Store(error)),
        };
        if let Some(refusal) = self.call.foreign(&context.room) {
            return failed(refusal);
        }
        if let Some(refusal) = self.unreached_speaker(&context) {
            return failed(refusal);
        }
        if context.binding(&self.event.source.channel_id).is_some() {
            return Progress::Done(Ok(()));
        }
        let change = HostAttach(self.binding.clone());
        let organization_id = self.call.organization_id.clone();
        let mut section = Box::new(
            LockedSection::changing(self.room_id().clone(), organization_id, change)
                .waiting_within(self.bound),
        );
        section.start(cx);
        self.state = State::Attaching(section);
        Progress::Waiting
    }

    /// Why the post's speaker is refused, if it is: the id names no
    /// participant of the room the host channel reaches (RFC §10.5).
    fn unreached_speaker(&self, context: &RoomContext) -> Option<InboundError> {
        let participant_id = self.event.source.participant_id.as_ref()?;
        let channel_id = &self.event.source.channel_id;
        let reached_elsewhere =
            match host::speaker(&context.participants, participant_id, channel_id) {
                Speaker::Reached => return None,
                Speaker::ReachedElsewhere => true,
                Speaker::Nobody => false,
            };
        Some(InboundError::ParticipantNotFound {
            room_id: self.room_id().clone(),
            participant_id: participant_id.clone(),
            channel_id: channel_id.clone(),
            reached_elsewhere,
        })
    }

    /// The first attach is over: the post goes on, or fails as an injection
    /// the room refuses would, nothing written.
    fn attached(result: SectionResult<HostAttach>) -> Result<(), Stopped> {
        let refused = |reason: &str| {
            Err(Stopped::Failed(InboundError::Refused {
                reason: reason.to_owned(),
            }))
        };
        match result {
            Ok(_) => Ok(()),
            Err(SectionError::Refused(HostRefusal::Closed)) => refused("room_closed"),
            Err(SectionError::Refused(HostRefusal::Detached)) => {
                refused(WriteRefusal::Unbound.blocked_by())
            }
            Err(SectionError::RoomNotFound(room_id)) => {
                Err(Stopped::Failed(InboundError::RoomNotFound(room_id)))
            }
            Err(SectionError::Store(error)) => Err(Stopped::Failed(InboundError::Store(error))),
            Err(SectionError::Lock(error)) => Err(Stopped::Failed(InboundError::Lock(error))),
        }
    }

    fn room_id(&self) -> &RoomId {
        &self.event.room_id
    }
}
