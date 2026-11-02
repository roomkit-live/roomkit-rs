//! The release of held messages, as a change of the room's stored events
//! (RFC §11.7.6): under the room's lock, each still held is read again and
//! given back the visibility its mark recorded, its mark dropped; once the
//! lock is released, `ON_EVENT_UPDATED` fires for each, on the budget of
//! the work the release is made in. Python's `_restore_held` and the
//! `ON_EVENT_UPDATED` of `_release_held`.

use std::collections::{BTreeSet, VecDeque};

use super::MutationError;
use super::change::{DirectWork, EventChange};
use crate::model::{EventId, RoomContext, RoomEvent, RoomId};
use crate::ops::protocol::{EffectId, Outcome, StoreRequest};
use crate::ops::{Cx, Progress};
use crate::rules::hook_registry::HookTrigger;
use crate::rules::verification::release;

/// What a release gave back: the events restored, in the order it was
/// handed them, and those whose delivery was cancelled while they were
/// held (RFC §10.1 "Explicit delivery cancellation").
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Restored {
    /// The events as the release left them.
    pub events: Vec<RoomEvent>,
    /// The ids of those whose hold mark recorded a cancellation.
    pub cancelled: BTreeSet<EventId>,
}

/// The restore of the held events a release names, one at a time.
#[derive(Debug)]
pub(super) struct Restore {
    pending: VecDeque<EventId>,
    pub(super) work: DirectWork,
    restored: Restored,
    /// The room as the lock found it, its recent events kept as the
    /// release rewrites them.
    context: Option<Box<RoomContext>>,
    state: State,
}

#[derive(Debug)]
enum State {
    Start,
    Reading(EffectId),
    Writing {
        effect: EffectId,
        event: Box<RoomEvent>,
        cancelled: bool,
    },
    Over,
}

impl Restore {
    /// The restore of `event_ids`, in that order.
    pub(super) fn new(event_ids: Vec<EventId>, work: DirectWork) -> Self {
        Self {
            pending: event_ids.into(),
            work,
            restored: Restored::default(),
            context: None,
            state: State::Start,
        }
    }

    /// Reads the next event again, under the lock, or ends with what was
    /// restored.
    fn next(&mut self, cx: &mut Cx<'_>) -> Progress<Result<Restored, MutationError>> {
        let Some(event_id) = self.pending.pop_front() else {
            self.state = State::Over;
            return Progress::Done(Ok(self.restored.clone()));
        };
        self.state = State::Reading(cx.io.issue(StoreRequest::GetEvent { event_id }));
        Progress::Waiting
    }

    /// The event as the store holds it now: restored when it is still held
    /// in the room, skipped otherwise (released already, or changed).
    fn on_read(
        &mut self,
        cx: &mut Cx<'_>,
        outcome: Outcome,
    ) -> Progress<Result<Restored, MutationError>> {
        let current = match outcome.into_event() {
            Ok(current) => current,
            Err(error) => return Progress::Done(Err(MutationError::Store(error))),
        };
        let room_id = self.context.as_ref().map(|context| context.room.id.clone());
        let Some(current) = current.filter(|event| Some(&event.room_id) == room_id.as_ref()) else {
            return self.next(cx);
        };
        let Some((updates, cancelled)) = release::restoring(&current) else {
            return self.next(cx);
        };
        let mut event = (*current).clone();
        updates.apply(&mut event);
        let effect = cx.io.issue(StoreRequest::UpdateEvent {
            event_id: event.id.clone(),
            updates,
        });
        self.state = State::Writing {
            effect,
            event: Box::new(event),
            cancelled,
        };
        Progress::Waiting
    }

    /// The event is written: kept among the restored, the room's recent
    /// events read as it now stands.
    fn on_written(
        &mut self,
        cx: &mut Cx<'_>,
        event: RoomEvent,
        cancelled: bool,
    ) -> Progress<Result<Restored, MutationError>> {
        if let Some(context) = &mut self.context {
            for recent in &mut context.recent_events {
                if recent.id == event.id {
                    recent.clone_from(&event);
                }
            }
        }
        if cancelled {
            self.restored.cancelled.insert(event.id.clone());
        }
        self.restored.events.push(event);
        self.next(cx)
    }
}

impl EventChange for Restore {
    type Output = Restored;

    /// A missing room holds nothing to release.
    fn missing(&self, _room_id: &RoomId) -> Result<Restored, MutationError> {
        Ok(Restored::default())
    }

    fn begin(
        &mut self,
        cx: &mut Cx<'_>,
        _room_id: &RoomId,
        context: Box<RoomContext>,
    ) -> Progress<Result<Restored, MutationError>> {
        self.context = Some(context);
        self.next(cx)
    }

    fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        outcome: Outcome,
    ) -> Progress<Result<Restored, MutationError>> {
        match std::mem::replace(&mut self.state, State::Over) {
            State::Reading(_) => self.on_read(cx, outcome),
            State::Writing {
                event, cancelled, ..
            } => match outcome.into_stored() {
                Ok(()) => self.on_written(cx, *event, cancelled),
                Err(error) => Progress::Done(Err(MutationError::Store(error))),
            },
            state @ (State::Start | State::Over) => {
                self.state = state;
                Progress::Waiting
            }
        }
    }

    fn awaits(&self, effect: EffectId) -> bool {
        match &self.state {
            State::Reading(reading) => *reading == effect,
            State::Writing {
                effect: writing, ..
            } => *writing == effect,
            State::Start | State::Over => false,
        }
    }

    fn reported(&self) -> Option<&RoomContext> {
        if self.restored.events.is_empty() || !matches!(self.state, State::Over) {
            return None;
        }
        self.context.as_deref()
    }

    /// `ON_EVENT_UPDATED` for each event restored, with the event as it
    /// now stands, the room's work its hooks' bodies (RFC §10.3, §9.5.1).
    fn report(&self, cx: &mut Cx<'_>, context: &RoomContext) {
        for event in &self.restored.events {
            self.work
                .fire(cx, HookTrigger::OnEventUpdated, event, context);
        }
    }
}
