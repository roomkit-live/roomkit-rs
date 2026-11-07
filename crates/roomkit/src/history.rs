//! The history an intelligence channel's turn reads (RFC §6.4): the room's
//! latest messages and the channel's own tool-call rows, read from the
//! store as the turn starts, as the Python implementation's AI channel
//! reads them (its context's `get_conversation`, its `tool_rows` and its
//! `build_tool_history_loader`).
//!
//! The context the pipeline hands every reader holds the room's latest
//! events of every kind, as many as the room's window (the largest its
//! channels declare, at least 50 while a hook is registered), which hooks
//! and transports keep. A turn reads its conversation instead: as many of
//! the latest messages, and the event the turn is about besides, as
//! Python's context holds it, a blocked one included as the store keeps it;
//! every tool-call row of the channel's from the room's message before the
//! first of them on, read page by page, since a later turn replays every
//! round its history holds as the turn sent it (RFC §6.4 "The tools of
//! earlier turns", RMK-1370); and the channel's latest 60 rows of calls, a
//! start and an end for each of its 30 latest calls, which its tool memory
//! reads. They are merged in timeline order and handed as the channel may
//! know them ([`visible_events`]). The history then crosses to the turn's
//! executor whole, so a turn run apart reads the same rows.

use roomkit_core::model::{ChannelBinding, EventType, RoomContext, RoomEvent, RoomId};
use roomkit_core::rules::transcript::TOOL_MEMORY_ONLY;
use roomkit_core::rules::visible_events;
use serde_json::Value;

use crate::discussion_turn::discussion_turn;
use crate::store::{ConversationStore, EventFilter, ListEventsOptions, StoreError};

/// How many of the channel's latest tool-call rows a turn reads for its tool
/// memory: a start and an end for each of the 30 calls the Python
/// implementation reads (`TOOL_MEMORY_CALLS`).
const TOOL_ROWS: u64 = 60;

/// How many of the channel's own rows a turn's history reads from the store
/// at a time (Python's `_HISTORY_ROWS_PAGE`).
const HISTORY_ROWS_PAGE: u64 = 500;

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
    let read_through = context.recent_events.iter().map(|known| known.index).max();
    match read(store, binding, event, window, read_through).await {
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

/// `context`, the room as a turn that waited its place starts on (RFC §6.4
/// "A room's turns one after the other"), with the history
/// [`turn_context`] reads, and the channel's own messages committed since
/// `event`: the answers of the turns before it, never a message that came
/// after it, which waits a turn of its own. An edit or a deletion made
/// meanwhile applies, as the store holds it now.
pub(crate) async fn turn_context_as_asked<S: ConversationStore>(
    store: &S,
    binding: &ChannelBinding,
    event: &RoomEvent,
    context: RoomContext,
    window: usize,
) -> RoomContext {
    let mut context = turn_context(store, binding, event, context, window).await;
    let own = EventFilter {
        event_types: Some(vec![EventType::Message]),
        source_channel_id: Some(binding.channel_id.clone()),
        ..EventFilter::default()
    };
    let since = ListEventsOptions {
        limit: u64::try_from(window).unwrap_or(u64::MAX),
        event_filter: Some(own),
        after_index: Some(event.index),
        ..ListEventsOptions::default()
    };
    match store.list_events(&event.room_id, since).await {
        Ok(answers) => {
            let known: std::collections::HashSet<_> = context
                .recent_events
                .iter()
                .map(|known| known.id.clone())
                .collect();
            let answers = answers
                .into_iter()
                .filter(|answer| !known.contains(&answer.id));
            context.recent_events.extend(answers);
            context.recent_events.sort_by_key(|event| event.index);
            let recent_events = visible_events(&context, &binding.channel_id);
            RoomContext {
                recent_events,
                ..context
            }
        }
        Err(error) => {
            tracing::warn!(
                target: "roomkit::turns",
                room_id = %event.room_id,
                channel_id = %binding.channel_id,
                %error,
                "could not read the answers before a waiting turn: it reads the room as asked"
            );
            context
        }
    }
}

/// The room's latest messages up to `event`, or up to `read_through`, the
/// newest the pipeline had read when it handed the turn, when that is later,
/// `event` among them when it is no message, then `binding`'s channel's
/// own rows its history replays and its latest tool-call rows, merged in
/// timeline order. The Python
/// implementation's turn reads its trigger as the newest event of its
/// context (`provisional`); an instruction, never stored, is not there. A
/// regeneration's turn reads the room as the regeneration found it, the
/// answers given since its event among them, as Python's context read
/// under the lock holds them.
async fn read<S: ConversationStore>(
    store: &S,
    binding: &ChannelBinding,
    event: &RoomEvent,
    window: usize,
    read_through: Option<u64>,
) -> Result<Vec<RoomEvent>, StoreError> {
    let messages = EventFilter {
        event_types: Some(vec![EventType::Message]),
        include_blocked: true,
        ..EventFilter::default()
    };
    let rows = tool_rows(binding);
    let mut history = store
        .list_events(
            &event.room_id,
            messages_up_to(event, messages, window, read_through),
        )
        .await?;
    if !matches!(event.r#type, EventType::Message | EventType::Instruction) {
        history.push(stored(store, event).await);
    }
    let first = history
        .iter()
        .map(|known| known.index)
        .chain(answered_index(event))
        .min();
    // Where the rows the history replays start: after the room's message
    // before its first event; none for a history with no event.
    let from = match first {
        Some(first) => Some(message_before(store, &event.room_id, first).await?),
        None => None,
    };
    let replayed = match from {
        Some(after) => replayed_rows(store, binding, &event.room_id, after).await?,
        None => Vec::new(),
    };
    let latest_rows = ListEventsOptions {
        limit: TOOL_ROWS,
        event_filter: Some(rows),
        newest_first: true,
        ..ListEventsOptions::default()
    };
    let replays = |index: u64| from.is_some_and(|after| after.is_none_or(|after| index > after));
    let latest = store.list_events(&event.room_id, latest_rows).await?;
    history.extend(latest.into_iter().map(|mut row| {
        // One older than the history's rows is the tool memory's alone.
        row.metadata.shift_remove(TOOL_MEMORY_ONLY);
        if !replays(row.index) {
            row.metadata
                .insert(TOOL_MEMORY_ONLY.to_owned(), Value::Bool(true));
        }
        row
    }));
    history.extend(replayed);
    history.sort_by_key(|event| event.index);
    history.dedup_by(|later, earlier| later.id == earlier.id);
    Ok(history)
}

/// `binding`'s channel's own tool-call rows.
fn tool_rows(binding: &ChannelBinding) -> EventFilter {
    EventFilter {
        event_types: Some(vec![EventType::ToolCallStart, EventType::ToolCallEnd]),
        source_channel_id: Some(binding.channel_id.clone()),
        ..EventFilter::default()
    }
}

/// `binding`'s channel's own rows its history replays: its tool calls and
/// the instructions its turns read (RFC §6.4, §10.1.1). Python's
/// `read_history_rows`.
fn history_rows(binding: &ChannelBinding) -> EventFilter {
    EventFilter {
        event_types: Some(vec![
            EventType::ToolCallStart,
            EventType::ToolCallEnd,
            EventType::InstructionRead,
        ]),
        source_channel_id: Some(binding.channel_id.clone()),
        ..EventFilter::default()
    }
}

/// The index a discussion's turn on `event` answers, which its history's
/// rounds read after (RFC §19.7.5): one less for an instruction, which,
/// never stored, holds the index of what followed it; none for any other
/// turn, or an event the store never numbered. Python's `_answered_index`.
fn answered_index(event: &RoomEvent) -> Option<u64> {
    if !discussion_turn(event) || event.index == 0 {
        return None;
    }
    Some(if event.r#type == EventType::Instruction {
        event.index - 1
    } else {
        event.index
    })
}

/// Every row of `binding`'s channel's own that a turn's history replays,
/// after `after`, the room's message before the history's first event (from
/// the room's first with none): the rounds its history holds and the
/// instructions its turns read, the one that opened the first of them
/// included, read page by page, none held for the tool memory alone.
/// Python's `build_tool_history_loader`.
async fn replayed_rows<S: ConversationStore>(
    store: &S,
    binding: &ChannelBinding,
    room_id: &RoomId,
    mut after: Option<u64>,
) -> Result<Vec<RoomEvent>, StoreError> {
    let mut rows = Vec::new();
    loop {
        let page = ListEventsOptions {
            limit: HISTORY_ROWS_PAGE,
            event_filter: Some(history_rows(binding)),
            after_index: after,
            ..ListEventsOptions::default()
        };
        let page = store.list_events(room_id, page).await?;
        let full = u64::try_from(page.len()).is_ok_and(|read| read >= HISTORY_ROWS_PAGE);
        after = page.last().map(|last| last.index).or(after);
        rows.extend(page.into_iter().map(|mut row| {
            row.metadata.shift_remove(TOOL_MEMORY_ONLY);
            row
        }));
        if !full {
            return Ok(rows);
        }
    }
}

/// The index of the room's last message before `index`, a blocked one
/// included; none when there is none. Python's `read_message_before`.
async fn message_before<S: ConversationStore>(
    store: &S,
    room_id: &RoomId,
    index: u64,
) -> Result<Option<u64>, StoreError> {
    let messages = EventFilter {
        event_types: Some(vec![EventType::Message]),
        include_blocked: true,
        ..EventFilter::default()
    };
    let before = ListEventsOptions {
        limit: 1,
        event_filter: Some(messages),
        before_index: Some(index),
        ..ListEventsOptions::default()
    };
    let before = store.list_events(room_id, before).await?;
    Ok(before.last().map(|message| message.index))
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
/// those up to `event`, which came before its turn, or up to
/// `read_through` when the pipeline read later ones (a regeneration's
/// turn); a discussion's turn reads
/// its event at its place, before what was said after it, so the room's
/// latest whatever `event` (RFC §19.7.5). An instruction, never
/// stored, holds the index the room's next event takes; an event at index
/// 0, the room's first or one the store never numbered, bounds nothing when
/// the pipeline handed no read, and the read's newest event otherwise.
fn messages_up_to(
    event: &RoomEvent,
    filter: EventFilter,
    window: usize,
    read_through: Option<u64>,
) -> ListEventsOptions {
    let bound = if event.r#type == EventType::Instruction {
        event.index
    } else {
        event.index.saturating_add(1)
    };
    let bound = read_through.map_or(bound, |newest| bound.max(newest.saturating_add(1)));
    ListEventsOptions {
        // The window before the event, and the event itself.
        limit: u64::try_from(window).unwrap_or(u64::MAX).saturating_add(1),
        event_filter: Some(filter),
        // Index 0, the room's first event or one the store never numbered,
        // bounds nothing unless the pipeline handed the turn a read: then the
        // turn reads no message committed after that read.
        before_index: ((event.index > 0 || read_through.is_some()) && !discussion_turn(event))
            .then_some(bound),
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
    /// reach no reader; and every tool row of the agent's since the message
    /// before them, another channel's left out, whatever came after: 55
    /// messages, then 70 rows of the agent's and 5 of another's, then a
    /// message after the trigger.
    #[tokio::test]
    async fn a_turn_reads_its_latest_messages_and_every_row_of_its_calls() {
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
        expected.extend(55..125);
        assert_eq!((indexes, messages), (expected, 49));
    }

    // Python: tests/test_channels/test_tool_history.py::test_the_history_reads_every_row_page_by_page
    // (turns/_wiring.py build_tool_history_loader, _wiring_env.py
    // read_message_before): the agent's rows from the room's message before
    // the history's first on, every one, page by page (RMK-1370), and its
    // latest 60 for its tool memory; an older row neither reads is left out.
    #[tokio::test]
    async fn a_turn_reads_its_rows_from_the_message_before_its_history() {
        let mut events = vec![
            event(0, None),
            event(1, Some("agent")),
            event(2, None),
            event(3, Some("agent")),
        ];
        events.extend((4..55).map(|index| event(index, None)));
        events.extend((55..616).map(|index| event(index, Some("agent"))));
        let trigger = events[54].clone();
        let store = store(events).await;
        let (indexes, messages) = read_by(&store, &trigger).await;
        let mut expected = vec![3];
        expected.extend(4..616);
        assert_eq!((indexes, messages), (expected, 51));
    }

    // Python: turns/_wiring.py build_tool_usage_loader, which reads the latest
    // 60 tool rows for the tool memory, beside build_tool_history_loader,
    // which reads the history's from the room's message before its first on
    // (RMK-1370).
    /// A tool row among the latest 60 but older than the room's message
    /// before the history's first is held for the tool memory alone, marked
    /// so; one after that message is the history's, unmarked, whatever a
    /// stored row carried under the key.
    #[tokio::test]
    async fn an_older_tool_row_is_held_for_the_tool_memory_alone() {
        let mut gap = event(3, Some("agent"));
        gap.metadata
            .insert(TOOL_MEMORY_ONLY.to_owned(), Value::Bool(true));
        let mut events = vec![event(0, None), event(1, Some("agent")), event(2, None), gap];
        events.extend((4..55).map(|index| event(index, None)));
        let trigger = events[54].clone();
        let store = store(events).await;
        let agent = binding("agent", ChannelType::AI);
        let mut context = RoomContext::new(Room::new(room_id()));
        context.bindings = vec![binding("sms", ChannelType::SMS), agent.clone()];
        let read = turn_context(&store, &agent, &trigger, context, 50).await;
        let rows: Vec<(u64, bool)> = read
            .recent_events
            .iter()
            .filter(|event| event.r#type == EventType::ToolCallStart)
            .map(|row| (row.index, row.metadata.contains_key(TOOL_MEMORY_ONLY)))
            .collect();
        assert_eq!(rows, [(1, true), (3, false)]);
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

    /// The indexes a turn on `trigger` reads, its pipeline having handed it
    /// `read`: a regeneration's read under the lock, or an ordinary turn's
    /// context, never a message committed after that read.
    async fn regenerated(
        store: &InMemoryStore,
        trigger: &RoomEvent,
        read: &[RoomEvent],
    ) -> Vec<u64> {
        let agent = binding("agent", ChannelType::AI);
        let mut context = RoomContext::new(Room::new(room_id()));
        context.bindings = vec![binding("sms", ChannelType::SMS), agent.clone()];
        context.recent_events = read.to_vec();
        let read = turn_context(store, &agent, trigger, context, 50).await;
        read.recent_events.iter().map(|event| event.index).collect()
    }

    // Python: channels/ai.py, a turn reads its pipeline's context, the
    // event joined to it: a turn on the room's first message reads no
    // message committed after its pass, as a turn on a later one.
    #[tokio::test]
    async fn a_turn_on_the_first_message_reads_no_message_committed_after_its_pass() {
        let events: Vec<RoomEvent> = (0..2).map(|index| event(index, None)).collect();
        let store = store(events.clone()).await;
        assert_eq!(regenerated(&store, &events[0], &events[..1]).await, vec![0]);
    }

    // Python: core/mixins/regenerate.py::_plan_regeneration, the context
    // its turn reads built under the room's lock.
    #[tokio::test]
    async fn a_regeneration_reads_no_message_committed_after_its_read() {
        let events: Vec<RoomEvent> = (0..4).map(|index| event(index, None)).collect();
        let store = store(events.clone()).await;
        assert_eq!(
            regenerated(&store, &events[1], &events[..3]).await,
            vec![0, 1, 2]
        );
    }

    // Python: as above, the message the room's first event.
    #[tokio::test]
    async fn a_regeneration_of_the_first_message_reads_no_message_committed_after_its_read() {
        let events: Vec<RoomEvent> = (0..3).map(|index| event(index, None)).collect();
        let store = store(events.clone()).await;
        assert_eq!(
            regenerated(&store, &events[0], &events[..2]).await,
            vec![0, 1]
        );
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
