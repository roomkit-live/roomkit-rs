//! The in-memory store's hard delete (RFC §14.1 `delete_event`, §10.3):
//! the event, its replies, and the edits and deletes of what it removes;
//! and its threads' summaries.

use std::collections::{BTreeMap, BTreeSet};
use std::mem;

use roomkit_core::model::{EventContent, EventId, EventStatus, RoomEvent, ThreadSummary};
use roomkit_core::rules::edits::{self, DELETED, Mutation};

/// Removes `event_id` from `events`, with what goes with it (RFC §10.3,
/// gap G-063):
///
/// - with `cascade_replies`, its replies, the events whose
///   `parent_event_id` names it, one level deep: the pipeline roots every
///   thread parent (RFC §10.1 step 8), so a reply names its thread's root,
///   and an event naming one of the replies goes nowhere with them;
/// - whatever `cascade_replies` says, the edits and deletes of the target
///   and of its replies: an edit holds its target's content, and a change
///   of an event that no longer exists changes nothing.
///
/// Returns the events removed, the target first, then its replies in index
/// order, then the changes in index order, an event both a reply and a
/// change among the replies; none when `events` does not hold the target.
pub(super) fn remove(
    events: &mut Vec<RoomEvent>,
    event_id: &EventId,
    cascade_replies: bool,
) -> Vec<RoomEvent> {
    let Some(at) = events.iter().position(|event| &event.id == event_id) else {
        return Vec::new();
    };
    let target = events.remove(at);
    let replies = if cascade_replies {
        take(events, |event| {
            event.parent_event_id.as_ref() == Some(event_id)
        })
    } else {
        Vec::new()
    };
    let gone: BTreeSet<&EventId> = replies
        .iter()
        .map(|reply| &reply.id)
        .chain([event_id])
        .collect();
    let changes = take(events, |event| changes_one_of(event, &gone));
    [target].into_iter().chain(replies).chain(changes).collect()
}

/// Takes the events of `events` that `belongs` holds for out of it, in index
/// order.
fn take(events: &mut Vec<RoomEvent>, belongs: impl Fn(&RoomEvent) -> bool) -> Vec<RoomEvent> {
    let (mut taken, left): (Vec<RoomEvent>, Vec<RoomEvent>) = mem::take(events)
        .into_iter()
        .partition(|event| belongs(event));
    *events = left;
    taken.sort_by_key(|event| event.index);
    taken
}

/// Whether `event` is an edit or a delete of one of the events `gone`.
fn changes_one_of(event: &RoomEvent, gone: &BTreeSet<&EventId>) -> bool {
    edits::mutation(event)
        .ok()
        .flatten()
        .is_some_and(|mutation| gone.contains(mutation.target_event_id()))
}

/// Whether `event` is a delete of `target`, whose commit marks the target's
/// edits deleted too (RFC §10.3).
pub(super) fn deletes(event: &RoomEvent, target: &EventId) -> bool {
    matches!(
        edits::mutation(event),
        Ok(Some(Mutation::Delete(delete))) if &delete.target_event_id == target
    )
}

/// Marks every edit of `target` among `events` deleted, as the Python
/// implementation's stores do: an edit holds a version of the deleted text
/// (RFC §10.3), which a history withholds by the edit's own mark.
pub(super) fn mark_edits_deleted(events: &mut [RoomEvent], target: &EventId) {
    for event in events {
        if matches!(&event.content, EventContent::Edit(edit) if &edit.target_event_id == target) {
            event.metadata.insert(DELETED.to_owned(), true.into());
        }
    }
}

/// The summary of each root of `roots` that `events` holds replies to, the
/// replies the room received alone: a refused one is not counted, as a
/// default timeline read does not serve it (RFC §14.1).
pub(super) fn thread_summaries(
    events: &[RoomEvent],
    roots: &[EventId],
) -> BTreeMap<EventId, ThreadSummary> {
    let mut summaries: BTreeMap<EventId, ThreadSummary> = BTreeMap::new();
    let received = events
        .iter()
        .filter(|event| event.status != EventStatus::Blocked);
    for event in received {
        let Some(root) = event
            .parent_event_id
            .as_ref()
            .filter(|root| roots.contains(root))
        else {
            continue;
        };
        let summary = summaries
            .entry(root.clone())
            .or_insert_with(|| ThreadSummary::new(root.clone()));
        summary.reply_count = summary.reply_count.saturating_add(1);
        if summary
            .last_reply_at
            .is_none_or(|last| event.created_at > last)
        {
            summary.last_reply_at = Some(event.created_at);
        }
    }
    summaries
}
