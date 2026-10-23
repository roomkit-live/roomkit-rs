//! What each direct call reads, writes and reports: an update reads the
//! event and writes the fields it names, a delete removes the event and its
//! thread (RFC §10.3 "Direct mutation (host-owned)", gap G-063), a record
//! outside the pipeline is committed with the room's next index (RFC §10.5,
//! gap G-071).

use super::MutationError;
use super::section::{self, State};
use crate::model::{EventId, EventUpdates, RoomContext, RoomEvent, RoomId, RoomUpdates};
use crate::ops::protocol::{Outcome, Request, RoomCounters};
use crate::ops::{Cx, Notice};
use crate::rules::hook_registry::HookTrigger;
use crate::rules::organizations;

/// What a direct call changes.
#[derive(Clone, Debug)]
pub(super) enum Change {
    Update {
        event_id: EventId,
        updates: Box<EventUpdates>,
    },
    Delete {
        event_id: EventId,
        cascade_replies: bool,
    },
    /// A record committed outside the pipeline, for a caller acting for
    /// `organization_id` (RFC §10.5 `commit_event`).
    Commit {
        record: Box<RoomEvent>,
        organization_id: Option<String>,
    },
}

/// What a direct call did.
#[derive(Debug)]
pub(super) struct Done {
    /// The event as the update left it, or the events the delete removed;
    /// none when the room holds no such event.
    pub(super) events: Vec<RoomEvent>,
    /// Whether anything was written, and the trigger is to fire.
    pub(super) changed: bool,
    /// The room as the change left it, which the trigger's hooks receive;
    /// `None` when the room does not exist.
    pub(super) context: Option<Box<RoomContext>>,
}

impl Done {
    /// A change of a stored event in a room that does not exist: a no-op
    /// that fires nothing, as for a target the room does not hold (gap
    /// G-063).
    pub(super) fn nothing() -> Self {
        Self {
            events: Vec::new(),
            changed: false,
            context: None,
        }
    }
}

impl Change {
    /// Whether the call changes a stored event, which a missing room makes
    /// a no-op, rather than commit a record, which it refuses (gap G-063).
    pub(super) fn changes_a_stored_event(&self) -> bool {
        match self {
            Self::Update { .. } | Self::Delete { .. } => true,
            Self::Commit { .. } => false,
        }
    }
}

/// The update reads the event; the delete removes it at once, the store
/// answering what it removed; the record is committed.
pub(super) fn act(
    cx: &mut Cx<'_>,
    room_id: &RoomId,
    change: &Change,
    context: Box<RoomContext>,
) -> State {
    match change {
        Change::Commit {
            record,
            organization_id,
        } => commit(cx, room_id, record, organization_id.as_deref(), context),
        Change::Update { event_id, .. } => State::Reading {
            effect: cx.io.issue(Request::GetEvent {
                event_id: event_id.clone(),
            }),
            context,
        },
        Change::Delete {
            event_id,
            cascade_replies,
        } => State::Writing {
            effect: cx.io.issue(Request::DeleteEvent {
                room_id: room_id.clone(),
                event_id: event_id.clone(),
                cascade_replies: *cascade_replies,
            }),
            done: Box::new(Done {
                events: Vec::new(),
                changed: true,
                context: Some(context),
            }),
        },
    }
}

/// Commits `record` with the room's next index, unless the room is not the
/// caller's or its status refuses events (RFC §10.5, §5.1, §17.2): another
/// organization's room is not found. The record moves the room's counters
/// only: it is no activity, and a paused room stays paused (gap G-071).
fn commit(
    cx: &mut Cx<'_>,
    room_id: &RoomId,
    record: &RoomEvent,
    organization_id: Option<&str>,
    context: Box<RoomContext>,
) -> State {
    if !organizations::admits(organization_id, &context.room) {
        let error = MutationError::RoomNotFound(room_id.clone());
        return section::release(cx, room_id, Err(error));
    }
    let status = context.room.status;
    if !status.accepts_events() {
        let error = MutationError::Refused {
            room_id: room_id.clone(),
            status,
        };
        return section::release(cx, room_id, Err(error));
    }
    let mut record = record.clone();
    record.index = context.room.event_count;
    let counters = RoomCounters {
        event_count: context.room.event_count.saturating_add(1),
        latest_index: record.index,
        last_activity_at: context.room.timers.last_activity_at,
        room: RoomUpdates::default(),
    };
    let effect = cx.io.issue(Request::Commit {
        event: record.clone(),
        counters,
        target: None,
    });
    State::Writing {
        effect,
        done: Box::new(Done {
            events: vec![record],
            changed: true,
            context: Some(context),
        }),
    }
}

/// The event is read: the room's is updated, unless the update changes
/// nothing; another room's, or none, is a no-op (RFC §10.3).
pub(super) fn on_read(
    cx: &mut Cx<'_>,
    room_id: &RoomId,
    change: &Change,
    mut context: Box<RoomContext>,
    outcome: Outcome,
) -> State {
    let stored = match outcome.into_event() {
        Ok(stored) => stored.filter(|event| &event.room_id == room_id),
        Err(error) => return section::release(cx, room_id, Err(MutationError::Store(error))),
    };
    let (Some(stored), Change::Update { event_id, updates }) = (stored, change) else {
        let done = Done {
            events: Vec::new(),
            changed: false,
            context: Some(context),
        };
        return section::release(cx, room_id, Ok(Box::new(done)));
    };
    let mut updated = (*stored).clone();
    updates.apply(&mut updated);
    if updated == *stored {
        let done = Done {
            events: vec![updated],
            changed: false,
            context: Some(context),
        };
        return section::release(cx, room_id, Ok(Box::new(done)));
    }
    for recent in &mut context.recent_events {
        if &recent.id == event_id {
            recent.clone_from(&updated);
        }
    }
    let effect = cx.io.issue(Request::UpdateEvent {
        event_id: event_id.clone(),
        updates: (**updates).clone(),
    });
    State::Writing {
        effect,
        done: Box::new(Done {
            events: vec![updated],
            changed: true,
            context: Some(context),
        }),
    }
}

/// The write is stored: an update's, or a delete's, which names what it
/// removed.
pub(super) fn on_written(
    cx: &mut Cx<'_>,
    room_id: &RoomId,
    change: &Change,
    mut done: Box<Done>,
    outcome: Outcome,
) -> State {
    let written = match change {
        Change::Update { .. } => outcome.into_stored(),
        Change::Commit { .. } => outcome.into_committed(),
        Change::Delete { .. } => outcome.into_deleted().map(|removed| {
            done.changed = !removed.is_empty();
            if let Some(context) = done.context.as_mut() {
                context
                    .recent_events
                    .retain(|recent| !removed.iter().any(|event| event.id == recent.id));
            }
            done.events = removed;
        }),
    };
    match written {
        Ok(()) => section::release(cx, room_id, Ok(done)),
        Err(error) => section::release(cx, room_id, Err(MutationError::Store(error))),
    }
}

/// Fires the mutation trigger for each event changed: `ON_EVENT_UPDATED`
/// for the updated event, `ON_EVENT_DELETED` for each removed one (RFC
/// §10.3). A record committed outside the pipeline fires no hook (RFC
/// §10.5).
pub(super) fn report(cx: &mut Cx<'_>, change: &Change, done: &Done) {
    let trigger = match change {
        Change::Update { .. } => HookTrigger::OnEventUpdated,
        Change::Delete { .. } => HookTrigger::OnEventDeleted,
        Change::Commit { .. } => return,
    };
    let Some(context) = &done.context else {
        return;
    };
    for event in &done.events {
        let hooks = cx.config.hooks.observers(trigger, &event.source);
        if hooks.is_empty() {
            continue;
        }
        cx.io.notify(Notice::AsyncHooks {
            trigger,
            hooks,
            event: Box::new(event.clone()),
            context: context.clone(),
        });
    }
}
