//! The history an intelligence channel's turn reads (RFC §6.4): the room's
//! latest messages and the channel's own latest tool-call rows, read from
//! the store as the turn starts, as the Python implementation's AI channel
//! reads them (its context's `get_conversation`, its `tool_rows`).
//!
//! The context the pipeline hands every reader holds the room's latest
//! events of every kind, as many as the room's window (the largest its
//! channels declare, at least 50 while a hook is registered), which hooks
//! and transports keep. A turn reads its conversation instead: as many of
//! the latest messages, and the event the turn is about besides, as
//! Python's context holds it, a blocked one included as the store keeps it,
//! and the channel's latest 60 rows of calls, a start
//! and an end for each of its 30 latest calls, so that a turn of many calls
//! leaves its question in the next turn's history and its rounds after it
//! (RFC §6.4 "The tools of earlier turns"). The two are merged in timeline
//! order and handed as the channel may know them ([`visible_events`]). The
//! history then crosses to the turn's executor whole, so a turn run apart
//! reads the same rows.

use roomkit_core::model::{ChannelBinding, EventType, RoomContext, RoomEvent};
use roomkit_core::rules::visible_events;

use crate::store::{ConversationStore, EventFilter, ListEventsOptions, StoreError};

/// How many of the channel's latest tool-call rows a turn reads: a start
/// and an end for each of the 30 calls the Python implementation reads
/// (`TOOL_MEMORY_CALLS`).
const TOOL_ROWS: u64 = 60;

/// `context`, the one the pipeline hands `binding`'s channel for its turn
/// on `event`, with its history read from `store`: the room's latest
/// `window` messages before `event`, then `event` itself, as stored, when it is
/// an event of another type (an edit, a delete), and the channel's own
/// latest tool-call rows. A store that cannot be read leaves the history as the
/// pipeline handed it, with a warning: the turn goes on.
pub(crate) async fn turn_context<S: ConversationStore>(
    store: &S,
    binding: &ChannelBinding,
    event: &RoomEvent,
    context: RoomContext,
    window: usize,
) -> RoomContext {
    match read(store, binding, event, window).await {
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
    window: usize,
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
        .list_events(&event.room_id, messages_up_to(event, messages, window))
        .await?;
    if !matches!(event.r#type, EventType::Message | EventType::Instruction) {
        history.push(stored(store, event).await);
    }
    let latest_rows = ListEventsOptions {
        limit: TOOL_ROWS,
        event_filter: Some(rows),
        newest_first: true,
        ..ListEventsOptions::default()
    };
    history.extend(store.list_events(&event.room_id, latest_rows).await?);
    history.sort_by_key(|event| event.index);
    Ok(history)
}

/// `event` as the store keeps it, which a turn's history reads: a target
/// that does not take its content natively is handed a transcoded copy (an
/// edit as a correction's text), which no history holds, as the Python
/// implementation's context holds the stored event. A read that fails, or
/// finds nothing, leaves the copy, with a warning.
async fn stored<S: ConversationStore>(store: &S, event: &RoomEvent) -> RoomEvent {
    let why = match store.get_event(&event.id).await {
        Ok(Some(stored)) => return stored,
        Ok(None) => "the store holds no such event".to_owned(),
        Err(error) => error.to_string(),
    };
    tracing::warn!(
        target: "roomkit::turns",
        room_id = %event.room_id,
        event_id = %event.id,
        error = %why,
        "could not read the turn's event as stored: its history reads the copy it was handed"
    );
    event.clone()
}

/// The query of the room's latest messages that `event`'s turn reads:
/// those up to `event`, which came before its turn. An instruction, never
/// stored, holds the index the room's next event takes; an event the store
/// never numbered bounds nothing.
fn messages_up_to(event: &RoomEvent, filter: EventFilter, window: usize) -> ListEventsOptions {
    let bound = if event.r#type == EventType::Instruction {
        event.index
    } else {
        event.index.saturating_add(1)
    };
    ListEventsOptions {
        // The window before the event, and the event itself.
        limit: u64::try_from(window).unwrap_or(u64::MAX).saturating_add(1),
        event_filter: Some(filter),
        before_index: (event.index > 0).then_some(bound),
        newest_first: true,
        ..ListEventsOptions::default()
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use roomkit_core::model::{
        ChannelId, ChannelType, EventContent, EventDirection, EventId, EventSource, EventStatus,
        Room, RoomId, TextContent, Timestamp, ToolCallContent,
    };

    use super::*;
    use crate::store::{CommitEventOptions, InMemoryStore};

    fn room_id() -> RoomId {
        RoomId::new("room_1").unwrap()
    }

    fn binding(channel: &str, channel_type: ChannelType) -> ChannelBinding {
        ChannelBinding::new(ChannelId::new(channel).unwrap(), room_id(), channel_type)
    }

    /// The room's event at `index`: a message on `sms`, or a tool-call row
    /// of `channel`.
    fn event(index: u64, channel: Option<&str>) -> RoomEvent {
        let (event_type, source, content) = match channel {
            None => (
                EventType::Message,
                EventSource::new(ChannelId::new("sms").unwrap(), ChannelType::SMS),
                TextContent::new(format!("message {index}")).into(),
            ),
            Some(channel) => (
                EventType::ToolCallStart,
                EventSource {
                    direction: EventDirection::Outbound,
                    ..EventSource::new(ChannelId::new(channel).unwrap(), ChannelType::AI)
                },
                EventContent::ToolCall(ToolCallContent::new("lookup", format!("c{index}"))),
            ),
        };
        let mut event = RoomEvent {
            id: EventId::new(format!("evt_{index}")).unwrap(),
            r#type: event_type,
            created_at: Timestamp::UNIX_EPOCH,
            ..RoomEvent::new(room_id(), source, content)
        };
        event.index = index;
        event
    }

    /// A store holding `room_1` and `events`, committed in their order.
    async fn store(events: Vec<RoomEvent>) -> InMemoryStore {
        let store = InMemoryStore::new();
        store.create_room(Room::new(room_id())).await.unwrap();
        for event in events {
            let options = CommitEventOptions {
                activity: false,
                ..CommitEventOptions::default()
            };
            store
                .commit_event(&room_id(), event, options)
                .await
                .unwrap();
        }
        store
    }

    /// The indexes `agent`'s turn on `trigger` reads, and how many of them
    /// are messages.
    async fn read_by(store: &InMemoryStore, trigger: &RoomEvent) -> (Vec<u64>, usize) {
        let agent = binding("agent", ChannelType::AI);
        let mut context = RoomContext::new(Room::new(room_id()));
        context.bindings = vec![binding("sms", ChannelType::SMS), agent.clone()];
        let read = turn_context(store, &agent, trigger, context, 50).await;
        let indexes = read.recent_events.iter().map(|event| event.index).collect();
        let messages = read
            .recent_events
            .iter()
            .filter(|event| event.r#type == EventType::Message)
            .count();
        (indexes, messages)
    }

    /// The room's latest 50 messages before the turn's own event, and the
    /// event, as Python's context holds them, two
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
        let mut expected: Vec<u64> = (4..52).collect();
        expected.push(54);
        expected.extend(65..125);
        assert_eq!((indexes, messages), (expected, 49));
    }

    /// A stored trigger that is no message, a delete here, is the history's
    /// newest event, as the Python implementation's turn reads it.
    #[tokio::test]
    async fn a_trigger_that_is_no_message_is_the_history_s_newest_event() {
        let mut events: Vec<RoomEvent> = (0..4).map(|index| event(index, None)).collect();
        let mut delete = event(4, None);
        delete.r#type = EventType::Delete;
        events.push(delete.clone());
        let store = store(events).await;
        assert_eq!(read_by(&store, &delete).await, (vec![0, 1, 2, 3, 4], 4));
    }

    /// The history holds the trigger as the store keeps it, not the copy
    /// its target was handed; a trigger the store does not hold leaves the
    /// copy.
    #[tokio::test]
    async fn the_trigger_is_read_as_stored_or_else_as_handed() {
        let mut delete = event(2, None);
        delete.r#type = EventType::Delete;
        let store = store(vec![event(0, None), event(1, None), delete.clone()]).await;
        let mut copy = delete.clone();
        copy.content = TextContent::new("[Message deleted]").into();
        let agent = binding("agent", ChannelType::AI);
        let mut context = RoomContext::new(Room::new(room_id()));
        context.bindings = vec![binding("sms", ChannelType::SMS), agent.clone()];
        let read = turn_context(&store, &agent, &copy, context.clone(), 50).await;
        assert_eq!(read.recent_events.last(), Some(&delete));
        let mut unknown = copy.clone();
        unknown.id = EventId::new("evt_unknown").unwrap();
        let read = turn_context(&store, &agent, &unknown, context, 50).await;
        assert_eq!(read.recent_events.last(), Some(&unknown));
    }

    /// An instruction, never stored, holds the index the room's next event
    /// takes: the messages it reads are the ones before it, a message
    /// committed at that index since left out.
    #[tokio::test]
    async fn an_instruction_reads_the_messages_before_its_index() {
        let store = store((0..4).map(|index| event(index, None)).collect()).await;
        let mut instruction = event(3, None);
        instruction.r#type = EventType::Instruction;
        assert_eq!(read_by(&store, &instruction).await, (vec![0, 1, 2], 3));
    }
}
