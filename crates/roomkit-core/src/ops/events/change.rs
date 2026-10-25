//! What each direct call reads, writes and reports: an update reads the
//! event and writes the fields it names, a delete removes the event and its
//! thread (RFC §10.3 "Direct mutation (host-owned)", gap G-063), a record
//! outside the pipeline is committed with the room's next index (RFC §10.5,
//! gap G-071). Each is a change of its own, with what it gives back.

use std::fmt::Debug;

use super::MutationError;
use crate::model::{EventId, EventUpdates, RoomContext, RoomEvent, RoomId};
use crate::ops::protocol::{EffectId, Outcome, RoomCounters, StoreRequest};
use crate::ops::rooting::{Rooted, Rooting};
use crate::ops::{Cx, Progress};
use crate::rules::hook_registry::HookTrigger;

/// What a direct call changes under the room's lock, and what it gives.
pub(super) trait EventChange: Debug {
    /// What the call gives back.
    type Output: Debug;

    /// What the call gives in `room_id`, a room that does not exist or that
    /// its caller does not reach.
    ///
    /// # Errors
    ///
    /// Returns [`MutationError::RoomNotFound`] for a call that a missing
    /// room fails.
    fn missing(&self, room_id: &RoomId) -> Result<Self::Output, MutationError>;

    /// Starts on `context`, the room `room_id` as its gate left it.
    fn begin(
        &mut self,
        cx: &mut Cx<'_>,
        room_id: &RoomId,
        context: Box<RoomContext>,
    ) -> Progress<Result<Self::Output, MutationError>>;

    /// Takes the answer to the effect the change waits for.
    fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        outcome: Outcome,
    ) -> Progress<Result<Self::Output, MutationError>>;

    /// Whether the change waits for `effect`.
    fn awaits(&self, effect: EffectId) -> bool;

    /// The room as the change left it, once it wrote what its trigger fires
    /// for; `None` when it fires nothing.
    fn reported(&self) -> Option<&RoomContext>;

    /// Fires the call's trigger for what it changed, with `context`, the
    /// room read once the lock is released (RFC §10.3).
    fn report(&self, cx: &mut Cx<'_>, context: &RoomContext);
}

/// The host's update of a stored event: the fields it names, each whole
/// (RFC §10.3).
#[derive(Debug)]
pub(super) struct Update {
    event_id: EventId,
    updates: Box<EventUpdates>,
    state: UpdateState,
}

#[derive(Debug)]
enum UpdateState {
    Start,
    Reading {
        effect: EffectId,
        context: Box<RoomContext>,
    },
    Writing {
        effect: EffectId,
        updated: Box<RoomEvent>,
        context: Box<RoomContext>,
    },
    /// The event as the update left it, and the room as the update left
    /// it; `None` when nothing was written.
    Over(Option<(Box<RoomEvent>, Box<RoomContext>)>),
}

impl Update {
    /// The update of `event_id` with `updates`.
    pub(super) fn new(event_id: EventId, updates: EventUpdates) -> Self {
        Self {
            event_id,
            updates: Box::new(updates),
            state: UpdateState::Start,
        }
    }

    /// The event is read: the room's is updated, unless the update changes
    /// nothing; another room's, or none, is a no-op (RFC §10.3).
    fn on_read(
        &mut self,
        cx: &mut Cx<'_>,
        mut context: Box<RoomContext>,
        outcome: Outcome,
    ) -> Progress<Result<Option<RoomEvent>, MutationError>> {
        let stored = match outcome.into_event() {
            Ok(stored) => stored.filter(|event| event.room_id == context.room.id),
            Err(error) => return Progress::Done(Err(MutationError::Store(error))),
        };
        let Some(stored) = stored else {
            return Progress::Done(Ok(None));
        };
        let mut updated = (*stored).clone();
        self.updates.apply(&mut updated);
        if updated == *stored {
            return Progress::Done(Ok(Some(updated)));
        }
        for recent in &mut context.recent_events {
            if recent.id == self.event_id {
                recent.clone_from(&updated);
            }
        }
        let effect = cx.io.issue(StoreRequest::UpdateEvent {
            event_id: self.event_id.clone(),
            updates: (*self.updates).clone(),
        });
        self.state = UpdateState::Writing {
            effect,
            updated: Box::new(updated),
            context,
        };
        Progress::Waiting
    }
}

impl EventChange for Update {
    type Output = Option<RoomEvent>;

    /// A missing room holds no event to update: a no-op (gap G-063).
    fn missing(&self, _room_id: &RoomId) -> Result<Option<RoomEvent>, MutationError> {
        Ok(None)
    }

    /// Reads the event first.
    fn begin(
        &mut self,
        cx: &mut Cx<'_>,
        _room_id: &RoomId,
        context: Box<RoomContext>,
    ) -> Progress<Result<Option<RoomEvent>, MutationError>> {
        let effect = cx.io.issue(StoreRequest::GetEvent {
            event_id: self.event_id.clone(),
        });
        self.state = UpdateState::Reading { effect, context };
        Progress::Waiting
    }

    fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        outcome: Outcome,
    ) -> Progress<Result<Option<RoomEvent>, MutationError>> {
        match std::mem::replace(&mut self.state, UpdateState::Over(None)) {
            UpdateState::Reading { context, .. } => self.on_read(cx, context, outcome),
            UpdateState::Writing {
                updated, context, ..
            } => match outcome.into_stored() {
                Ok(()) => {
                    let event = (*updated).clone();
                    self.state = UpdateState::Over(Some((updated, context)));
                    Progress::Done(Ok(Some(event)))
                }
                Err(error) => Progress::Done(Err(MutationError::Store(error))),
            },
            state @ (UpdateState::Start | UpdateState::Over(_)) => {
                self.state = state;
                Progress::Waiting
            }
        }
    }

    fn awaits(&self, effect: EffectId) -> bool {
        match &self.state {
            UpdateState::Reading {
                effect: reading, ..
            } => *reading == effect,
            UpdateState::Writing {
                effect: writing, ..
            } => *writing == effect,
            UpdateState::Start | UpdateState::Over(_) => false,
        }
    }

    fn reported(&self) -> Option<&RoomContext> {
        match &self.state {
            UpdateState::Over(Some((_, context))) => Some(context),
            UpdateState::Start
            | UpdateState::Reading { .. }
            | UpdateState::Writing { .. }
            | UpdateState::Over(None) => None,
        }
    }

    /// `ON_EVENT_UPDATED`, once, with the event as it now stands, when the
    /// update wrote it (gap G-063).
    fn report(&self, cx: &mut Cx<'_>, context: &RoomContext) {
        if let UpdateState::Over(Some((event, _))) = &self.state {
            cx.fire_observers(HookTrigger::OnEventUpdated, event, context);
        }
    }
}

/// The host's hard delete of a stored event, with its replies one level
/// deep when it cascades to them (RFC §10.3, gap G-063).
#[derive(Debug)]
pub(super) struct Delete {
    event_id: EventId,
    cascade_replies: bool,
    state: DeleteState,
}

#[derive(Debug)]
enum DeleteState {
    Start,
    Deleting {
        effect: EffectId,
        context: Box<RoomContext>,
    },
    /// The events removed, and the room as the delete left it; `None` when
    /// nothing was.
    Over(Option<(Vec<RoomEvent>, Box<RoomContext>)>),
}

impl Delete {
    /// The delete of `event_id`, with its replies one level deep when
    /// `cascade_replies` is set.
    pub(super) fn new(event_id: EventId, cascade_replies: bool) -> Self {
        Self {
            event_id,
            cascade_replies,
            state: DeleteState::Start,
        }
    }
}

impl EventChange for Delete {
    type Output = Vec<RoomEvent>;

    /// A missing room holds no event to delete: a no-op (gap G-063).
    fn missing(&self, _room_id: &RoomId) -> Result<Vec<RoomEvent>, MutationError> {
        Ok(Vec::new())
    }

    /// Removes the event at once, the store answering what it removed.
    fn begin(
        &mut self,
        cx: &mut Cx<'_>,
        room_id: &RoomId,
        context: Box<RoomContext>,
    ) -> Progress<Result<Vec<RoomEvent>, MutationError>> {
        let effect = cx.io.issue(StoreRequest::DeleteEvent {
            room_id: room_id.clone(),
            event_id: self.event_id.clone(),
            cascade_replies: self.cascade_replies,
        });
        self.state = DeleteState::Deleting { effect, context };
        Progress::Waiting
    }

    fn accept(
        &mut self,
        _cx: &mut Cx<'_>,
        outcome: Outcome,
    ) -> Progress<Result<Vec<RoomEvent>, MutationError>> {
        let DeleteState::Deleting { mut context, .. } =
            std::mem::replace(&mut self.state, DeleteState::Over(None))
        else {
            return Progress::Waiting;
        };
        match outcome.into_deleted() {
            Ok(removed) => {
                context
                    .recent_events
                    .retain(|recent| !removed.iter().any(|event| event.id == recent.id));
                self.state = DeleteState::Over(Some((removed.clone(), context)));
                Progress::Done(Ok(removed))
            }
            Err(error) => Progress::Done(Err(MutationError::Store(error))),
        }
    }

    fn awaits(&self, effect: EffectId) -> bool {
        matches!(
            &self.state,
            DeleteState::Deleting { effect: deleting, .. } if *deleting == effect
        )
    }

    fn reported(&self) -> Option<&RoomContext> {
        match &self.state {
            DeleteState::Over(Some((_, context))) => Some(context),
            DeleteState::Start | DeleteState::Deleting { .. } | DeleteState::Over(None) => None,
        }
    }

    /// `ON_EVENT_DELETED` once for each event removed, in the order the
    /// store removed them, each as it was stored (RFC §10.3, gap G-063).
    fn report(&self, cx: &mut Cx<'_>, context: &RoomContext) {
        if let DeleteState::Over(Some((removed, _))) = &self.state {
            for event in removed {
                cx.fire_observers(HookTrigger::OnEventDeleted, event, context);
            }
        }
    }
}

/// The commit of a record outside the pipeline (RFC §10.5): its thread
/// parent rooted under the lock, as every committed event's is (RFC §10.1
/// step 8), then the record with the room's next index.
#[derive(Debug)]
pub(super) struct Commit {
    record: Box<RoomEvent>,
    waiting: Waiting,
}

/// What a record's commit waits for.
#[derive(Debug)]
enum Waiting {
    Nothing,
    /// A read of the parent's rooting, then the commit with `counters`.
    Root {
        effect: EffectId,
        counters: RoomCounters,
        rooting: Box<Rooting>,
    },
    Commit(EffectId),
}

impl Commit {
    /// The commit of `record`.
    pub(super) fn new(record: RoomEvent) -> Self {
        Self {
            record: Box::new(record),
            waiting: Waiting::Nothing,
        }
    }

    /// Commits the record, the room's counters then `counters`.
    fn commit(&mut self, cx: &mut Cx<'_>, counters: RoomCounters) {
        let request = StoreRequest::Commit {
            event: self.record.clone(),
            counters,
            target: None,
        };
        let room_id = self.record.room_id.clone();
        self.waiting = Waiting::Commit(cx.io.issue_locked(request, room_id));
    }
}

impl EventChange for Commit {
    type Output = RoomEvent;

    /// A record grows a timeline: a missing room fails it (gap G-063).
    fn missing(&self, room_id: &RoomId) -> Result<RoomEvent, MutationError> {
        Err(MutationError::RoomNotFound(room_id.clone()))
    }

    /// Roots the record's thread parent and commits it with the room's next
    /// index, unless its status refuses events (RFC §10.5, §5.1, §10.1 step
    /// 8). The record moves the room's counters only: it is no activity,
    /// and a paused room stays paused (gap G-071).
    fn begin(
        &mut self,
        cx: &mut Cx<'_>,
        room_id: &RoomId,
        context: Box<RoomContext>,
    ) -> Progress<Result<RoomEvent, MutationError>> {
        let status = context.room.status;
        if !status.accepts_events() {
            return Progress::Done(Err(MutationError::Refused {
                room_id: room_id.clone(),
                status,
            }));
        }
        self.record.index = context.room.event_count;
        let counters = RoomCounters::after_commit(&context.room, self.record.index);
        match Rooting::of(&self.record) {
            Some((rooting, request)) => {
                let effect = cx.io.issue_locked(request, room_id.clone());
                self.waiting = Waiting::Root {
                    effect,
                    counters,
                    rooting: Box::new(rooting),
                };
            }
            None => self.commit(cx, counters),
        }
        Progress::Waiting
    }

    fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        outcome: Outcome,
    ) -> Progress<Result<RoomEvent, MutationError>> {
        match std::mem::replace(&mut self.waiting, Waiting::Nothing) {
            Waiting::Root {
                counters,
                mut rooting,
                ..
            } => match rooting.on_read(outcome) {
                Ok(Rooted::Read(request)) => {
                    let effect = cx.io.issue_locked(*request, self.record.room_id.clone());
                    self.waiting = Waiting::Root {
                        effect,
                        counters,
                        rooting,
                    };
                    Progress::Waiting
                }
                Ok(Rooted::Parent(parent)) => {
                    self.record.parent_event_id = parent;
                    self.commit(cx, counters);
                    Progress::Waiting
                }
                Err(error) => Progress::Done(Err(MutationError::Store(error))),
            },
            Waiting::Commit(_) | Waiting::Nothing => match outcome.into_committed() {
                Ok(()) => Progress::Done(Ok((*self.record).clone())),
                Err(error) => Progress::Done(Err(MutationError::Store(error))),
            },
        }
    }

    fn awaits(&self, effect: EffectId) -> bool {
        match &self.waiting {
            Waiting::Root { effect: waited, .. } | Waiting::Commit(waited) => *waited == effect,
            Waiting::Nothing => false,
        }
    }

    /// A record fires no hook (RFC §10.5).
    fn reported(&self) -> Option<&RoomContext> {
        None
    }

    fn report(&self, _cx: &mut Cx<'_>, _context: &RoomContext) {}
}
