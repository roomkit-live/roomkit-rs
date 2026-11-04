//! The commit of a lifecycle record (RFC §5.1 Lifecycle records, gap G-046):
//! the framework's own event, written by the change itself under the room's
//! lock, never through the pipeline.
//!
//! The record takes the room's next index and counts in `event_count`, in
//! one commit with the room's counters, as any event does (RFC §14.3). It
//! is no activity: `last_activity_at` stays, and a paused room stays paused.
//! It is `internal`, delivered to no channel, and runs no hook of its own.
//! A room whose status refuses new events gets none, but a timer's record
//! of the transition itself (RFC §5.1).

use crate::model::{
    ChannelId, ChannelType, EventContent, EventSource, EventStatus, RoomContext, RoomEvent,
    Visibility,
};
use crate::ops::Cx;
use crate::ops::protocol::{EffectId, StoreRequest};
use crate::rules::records::Record;

/// The commit of `record` as the next event of the room `context` holds,
/// which then reflects it: its counters, and the record the newest event of
/// its history, the oldest dropped past the room's context window. `None`
/// when the room's status refuses it.
pub(crate) fn commit(
    cx: &Cx<'_>,
    context: &mut RoomContext,
    record: &Record,
) -> Option<StoreRequest> {
    if !record.records_transition() && !context.room.status.accepts_events() {
        return None;
    }
    let room = &mut context.room;
    let source = EventSource::new(ChannelId::system(), ChannelType::SYSTEM);
    let mut event = RoomEvent::at(
        cx.env.new_event_id(),
        room.id.clone(),
        record.event_type(),
        source,
        EventContent::System(record.content()),
        cx.env.now(),
    );
    event.status = EventStatus::Delivered;
    event.visibility = Visibility::Internal;
    event.index = room.event_count;
    room.event_count = room.event_count.saturating_add(1);
    room.latest_index = Some(event.index);
    let history = &mut context.recent_events;
    history.push(event.clone());
    let excess = history
        .len()
        .saturating_sub(cx.config.recent_events_limit(&context.bindings));
    history.drain(..excess);
    Some(StoreRequest::Commit {
        event: Box::new(event),
        target: None,
        activity: None,
        author_register: None,
    })
}

/// Issues `request`, a write of a locked section: a commit on behalf of the
/// section that holds the room's lock (`Effect::locked`), as every commit
/// is; any other write as it is.
pub(crate) fn issue_write(cx: &mut Cx<'_>, request: StoreRequest) -> EffectId {
    match &request {
        StoreRequest::Commit { event, .. } => {
            let room_id = event.room_id.clone();
            cx.io.issue_locked(request, room_id)
        }
        StoreRequest::LoadContext { .. }
        | StoreRequest::GetEventByIdempotencyKey { .. }
        | StoreRequest::GetEvent { .. }
        | StoreRequest::ListEvents { .. }
        | StoreRequest::PersistSideEffects { .. }
        | StoreRequest::CreateRoom { .. }
        | StoreRequest::UpdateRoom { .. }
        | StoreRequest::StoreBinding { .. }
        | StoreRequest::DeleteBinding { .. }
        | StoreRequest::UpdateEvent { .. }
        | StoreRequest::DeleteEvent { .. }
        | StoreRequest::ResolveIdentity { .. }
        | StoreRequest::AddParticipant { .. }
        | StoreRequest::UpdateParticipant { .. }
        | StoreRequest::RecordDeliveryResults { .. }
        | StoreRequest::ListEventsPage { .. } => cx.io.issue(request),
    }
}
