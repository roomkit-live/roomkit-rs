//! The history an intelligence channel's turn reads (RFC §6.4): the room's
//! latest messages and the channel's own latest tool-call rows, read from
//! the store as the turn starts, as the Python implementation's AI channel
//! reads them (its context's `get_conversation`, its `tool_rows`).
//!
//! The context the pipeline hands every reader holds the room's latest 50
//! events of every kind, which hooks and transports keep. A turn reads its
//! conversation instead: the latest 50 messages, a blocked one included as
//! the store keeps it, and the channel's latest 60 rows of calls, a start
//! and an end for each of its 30 latest calls, so that a turn of many calls
//! leaves its question in the next turn's history and its rounds after it
//! (RFC §6.4 "The tools of earlier turns"). The two are merged in timeline
//! order and handed as the channel may know them ([`visible_events`]). The
//! history then crosses to the turn's executor whole, so a turn run apart
//! reads the same rows.

use roomkit_core::model::{ChannelBinding, EventType, RoomContext, RoomEvent};
use roomkit_core::rules::visible_events;

use crate::store::{ConversationStore, EventFilter, EventQuery, StoreError};

/// How many of the room's latest messages a turn reads: the Python
/// implementation's window for an AI channel (`AI_CHANNEL_CONTEXT_EVENTS`).
const MESSAGES: u64 = 50;

/// How many of the channel's latest tool-call rows a turn reads: a start
/// and an end for each of the 30 calls the Python implementation reads
/// (`TOOL_MEMORY_CALLS`).
const TOOL_ROWS: u64 = 60;

/// `context`, the one the pipeline hands `binding`'s channel for its turn
/// on `event`, with its history read from `store`: the room's latest
/// messages up to `event`, and the channel's own latest tool-call rows. A
/// store that cannot be read leaves the history as the pipeline handed it,
/// with a warning: the turn goes on.
pub(crate) async fn turn_context<S: ConversationStore>(
    store: &S,
    binding: &ChannelBinding,
    event: &RoomEvent,
    context: RoomContext,
) -> RoomContext {
    match read(store, binding, event).await {
        Ok(history) => {
            let whole = RoomContext {
                recent_events: history,
                ..context
            };
            let recent_events = visible_events(&whole, &binding.channel_id);
            RoomContext {
                recent_events,
                ..whole
            }
        }
        Err(error) => {
            tracing::warn!(
                target: "roomkit::turns",
                room_id = %event.room_id,
                channel_id = %binding.channel_id,
                %error,
                "could not read the turn's history: it reads the room's latest events"
            );
            context
        }
    }
}

/// The room's latest messages up to `event`, then `binding`'s channel's
/// latest tool-call rows, merged in timeline order.
async fn read<S: ConversationStore>(
    store: &S,
    binding: &ChannelBinding,
    event: &RoomEvent,
) -> Result<Vec<RoomEvent>, StoreError> {
    let messages = EventFilter {
        event_types: Some(vec![EventType::Message]),
        include_blocked: true,
        ..EventFilter::default()
    };
    let rows = EventFilter {
        event_types: Some(vec![EventType::ToolCallStart, EventType::ToolCallEnd]),
        source_channel_id: Some(binding.channel_id.clone()),
        ..EventFilter::default()
    };
    let mut history = store
        .list_events(&event.room_id, &messages_up_to(event, messages))
        .await?;
    let latest_rows = EventQuery {
        limit: TOOL_ROWS,
        event_filter: Some(rows),
        newest_first: true,
        ..EventQuery::default()
    };
    history.extend(store.list_events(&event.room_id, &latest_rows).await?);
    history.sort_by_key(|event| event.index);
    Ok(history)
}

/// The query of the room's latest messages that `event`'s turn reads:
/// those up to `event`, which came before its turn. An instruction, never
/// stored, holds the index the room's next event takes; an event the store
/// never numbered bounds nothing.
fn messages_up_to(event: &RoomEvent, filter: EventFilter) -> EventQuery {
    let bound = if event.event_type == EventType::Instruction {
        event.index
    } else {
        event.index.saturating_add(1)
    };
    EventQuery {
        limit: MESSAGES,
        event_filter: Some(filter),
        before_index: (event.index > 0).then_some(bound),
        newest_first: true,
        ..EventQuery::default()
    }
}
