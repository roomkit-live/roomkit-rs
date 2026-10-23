//! What each direct call reads, writes and reports: an update reads the
//! event and writes the fields it names, a delete removes the event and its
//! thread (RFC §10.3 "Direct mutation (host-owned)", gap G-063).

use super::MutationError;
use super::section::{self, State};
use crate::model::{EventId, EventUpdates, RoomContext, RoomEvent, RoomId};
use crate::ops::protocol::{Outcome, Request};
use crate::ops::{Cx, Notice};
use crate::rules::hook_registry::HookTrigger;

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
}

/// What a direct call did.
#[derive(Debug)]
pub(super) struct Done {
    /// The event as the update left it, or the events the delete removed;
    /// none when the room holds no such event.
    pub(super) events: Vec<RoomEvent>,
    /// Whether anything was written, and the trigger is to fire.
    pub(super) changed: bool,
    /// The room as the change left it, which the trigger's hooks receive.
    pub(super) context: Box<RoomContext>,
}

/// The update reads the event; the delete removes it at once, the store
/// answering what it removed.
pub(super) fn act(
    cx: &mut Cx<'_>,
    room_id: &RoomId,
    change: &Change,
    context: Box<RoomContext>,
) -> State {
    match change {
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
                context,
            }),
        },
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
            context,
        };
        return section::release(cx, room_id, Ok(Box::new(done)));
    };
    let mut updated = (*stored).clone();
    updates.apply(&mut updated);
    if updated == *stored {
        let done = Done {
            events: vec![updated],
            changed: false,
            context,
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
            context,
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
        Change::Delete { .. } => outcome.into_deleted().map(|removed| {
            done.changed = !removed.is_empty();
            done.context
                .recent_events
                .retain(|recent| !removed.iter().any(|event| event.id == recent.id));
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
/// §10.3).
pub(super) fn report(cx: &mut Cx<'_>, change: &Change, done: &Done) {
    let trigger = match change {
        Change::Update { .. } => HookTrigger::OnEventUpdated,
        Change::Delete { .. } => HookTrigger::OnEventDeleted,
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
            context: done.context.clone(),
        });
    }
}
