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
/// messages up to `event`, `event` itself the newest when it is a stored
/// event of another type (an edit, a delete), and the channel's own latest
/// tool-call rows. A store that cannot be read leaves the history as the
/// pipeline handed it, with a warning: the turn goes on.
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

/// The room's latest messages up to `event`, `event` among them when it
/// is no message, then `binding`'s channel's latest tool-call rows, merged
/// in timeline order. The Python implementation's turn reads its trigger as
/// the newest event of its context (`provisional`); an instruction, never
/// stored, is not there.
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
    if !matches!(
        event.event_type,
        EventType::Message | EventType::Instruction
    ) {
        history.push(event.clone());
    }
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

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use roomkit_core::model::{
        ChannelCategory, ChannelDirection, ChannelId, ChannelType, EventContent, EventDirection,
        EventId, EventSource, EventStatus, Room, RoomId, RoomUpdates, Timestamp, ToolCallContent,
    };
    use roomkit_core::ops::RoomCounters;

    use super::*;
    use crate::store::InMemoryStore;

    fn room_id() -> RoomId {
        RoomId::new("room_1").unwrap()
    }

    fn binding(channel: &str, channel_type: ChannelType) -> ChannelBinding {
        ChannelBinding::new(
            ChannelId::new(channel).unwrap(),
            room_id(),
            channel_type,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
            Timestamp::UNIX_EPOCH,
        )
    }

    /// The room's event at `index`: a message on `sms`, or a tool-call row
    /// of `channel`.
    fn event(index: u64, channel: Option<&str>) -> RoomEvent {
        let (event_type, source, content) = match channel {
            None => (
                EventType::Message,
                EventSource::new(
                    ChannelId::new("sms").unwrap(),
                    ChannelType::SMS,
                    EventDirection::Inbound,
                ),
                EventContent::text(format!("message {index}")),
            ),
            Some(channel) => (
                EventType::ToolCallStart,
                EventSource::new(
                    ChannelId::new(channel).unwrap(),
                    ChannelType::AI,
                    EventDirection::Outbound,
                ),
                EventContent::ToolCall(ToolCallContent::new("lookup", format!("c{index}"))),
            ),
        };
        let mut event = RoomEvent::new(
            EventId::new(format!("evt_{index}")).unwrap(),
            room_id(),
            event_type,
            source,
            content,
            Timestamp::UNIX_EPOCH,
        );
        event.index = index;
        event
    }

    /// A store holding `room_1` and `events`, committed in their order.
    async fn store(events: Vec<RoomEvent>) -> InMemoryStore {
        let store = InMemoryStore::new();
        store
            .create_room(Room::new(room_id(), Timestamp::UNIX_EPOCH))
            .await
            .unwrap();
        for event in events {
            let counters = RoomCounters {
                event_count: event.index + 1,
                latest_index: event.index,
                last_activity_at: None,
                room: RoomUpdates::default(),
            };
            store.commit(event, counters, None).await.unwrap();
        }
        store
    }

    /// The indexes `agent`'s turn on `trigger` reads, and how many of them
    /// are messages.
    async fn read_by(store: &InMemoryStore, trigger: &RoomEvent) -> (Vec<u64>, usize) {
        let agent = binding("agent", ChannelType::AI);
        let mut context = RoomContext::new(Room::new(room_id(), Timestamp::UNIX_EPOCH));
        context.bindings = vec![binding("sms", ChannelType::SMS), agent.clone()];
        let read = turn_context(store, &agent, trigger, context).await;
        let indexes = read.recent_events.iter().map(|event| event.index).collect();
        let messages = read
            .recent_events
            .iter()
            .filter(|event| event.event_type == EventType::Message)
            .count();
        (indexes, messages)
    }

    /// The room's latest 50 messages up to the turn's own event, two
    /// blocked ones among them, which take their place in the window and
    /// reach no reader; and the agent's latest 60 tool rows, another
    /// channel's left out, whatever came after: 55 messages, then 70 rows
    /// of the agent's and 5 of another's, then a message after the trigger.
    #[tokio::test]
    async fn a_turn_reads_its_latest_messages_and_its_latest_tool_rows() {
        let mut events: Vec<RoomEvent> = (0..55).map(|index| event(index, None)).collect();
        for blocked in &mut events[52..54] {
            blocked.status = EventStatus::Blocked;
        }
        events.extend((55..125).map(|index| event(index, Some("agent"))));
        events.extend((125..130).map(|index| event(index, Some("other"))));
        events.push(event(130, None));
        let trigger = events[54].clone();
        let store = store(events).await;
        let (indexes, messages) = read_by(&store, &trigger).await;
        let mut expected: Vec<u64> = (5..52).collect();
        expected.push(54);
        expected.extend(65..125);
        assert_eq!((indexes, messages), (expected, 48));
    }

    /// A stored trigger that is no message, a delete here, is the history's
    /// newest event, as the Python implementation's turn reads it.
    #[tokio::test]
    async fn a_trigger_that_is_no_message_is_the_history_s_newest_event() {
        let mut events: Vec<RoomEvent> = (0..4).map(|index| event(index, None)).collect();
        let mut delete = event(4, None);
        delete.event_type = EventType::Delete;
        events.push(delete.clone());
        let store = store(events).await;
        assert_eq!(read_by(&store, &delete).await, (vec![0, 1, 2, 3, 4], 4));
    }

    /// An instruction, never stored, holds the index the room's next event
    /// takes: the messages it reads are the ones before it, a message
    /// committed at that index since left out.
    #[tokio::test]
    async fn an_instruction_reads_the_messages_before_its_index() {
        let store = store((0..4).map(|index| event(index, None)).collect()).await;
        let mut instruction = event(3, None);
        instruction.event_type = EventType::Instruction;
        assert_eq!(read_by(&store, &instruction).await, (vec![0, 1, 2], 3));
    }
}
