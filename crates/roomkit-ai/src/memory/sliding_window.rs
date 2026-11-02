//! The default memory: the room's latest events (RFC §20.3).

use std::future::{self, Future};
use std::pin::Pin;

use roomkit::ChannelError;
use roomkit::model::{RoomContext, RoomEvent, RoomId};

use super::{MemoryProvider, MemoryResult, RetrieveOptions};

/// What [`SlidingWindowMemory::new`] takes: Python's
/// `SlidingWindowMemory(max_events=50)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlidingWindowMemoryOptions {
    /// How many of the room's latest events a turn reads, its own among
    /// them.
    pub max_events: usize,
}

impl Default for SlidingWindowMemoryOptions {
    fn default() -> Self {
        Self { max_events: 50 }
    }
}

/// The room's latest events (RFC §20.3): Python's `SlidingWindowMemory`,
/// the default memory of an AI channel, its `max_events` the channel's
/// `max_context_events`.
///
/// A turn reads the last `max_events` events of the context it is handed,
/// the room's view its channel may know, its own event among them; it
/// leaves that one out, which the channel places as the turn's input. With
/// the turn's event the context's newest, the turn reads `max_events - 1`
/// events before it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlidingWindowMemory {
    max_events: usize,
}

impl SlidingWindowMemory {
    /// The window of `options.max_events` events.
    pub fn new(options: SlidingWindowMemoryOptions) -> Self {
        Self {
            max_events: options.max_events,
        }
    }

    /// The events of `context` a turn on `current_event` reads: its last
    /// `max_events`, `current_event` left out. A window of 0 reads every
    /// event, as Python's slice `[-0:]` does.
    fn window(self, current_event: &RoomEvent, context: &RoomContext) -> Vec<RoomEvent> {
        let events = &context.recent_events;
        let skip = match self.max_events {
            0 => 0,
            max => events.len().saturating_sub(max),
        };
        events
            .iter()
            .skip(skip)
            .filter(|event| event.id != current_event.id)
            .cloned()
            .collect()
    }
}

impl MemoryProvider for SlidingWindowMemory {
    fn name(&self) -> String {
        "SlidingWindowMemory".to_owned()
    }

    /// Its window: a turn reads no more than `max_events` of the room's
    /// latest events.
    fn recent_events_window(&self) -> usize {
        self.max_events
    }

    fn retrieve<'a>(
        &'a self,
        _room_id: &'a RoomId,
        current_event: &'a RoomEvent,
        context: &'a RoomContext,
        _options: RetrieveOptions,
    ) -> Pin<Box<dyn Future<Output = Result<MemoryResult, ChannelError>> + Send + 'a>> {
        let events = self.window(current_event, context);
        Box::pin(future::ready(Ok(MemoryResult {
            events,
            ..MemoryResult::default()
        })))
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use roomkit::model::{
        ChannelId, ChannelType, EventContent, EventId, EventSource, Room, TextContent,
    };

    use super::*;

    fn room_id() -> RoomId {
        RoomId::new("room_1").unwrap()
    }

    fn event(id: usize) -> RoomEvent {
        RoomEvent {
            id: EventId::new(format!("evt_{id}")).unwrap(),
            ..RoomEvent::new(
                room_id(),
                EventSource::new(ChannelId::new("sms").unwrap(), ChannelType::SMS),
                EventContent::Text(TextContent::new(format!("message {id}"))),
            )
        }
    }

    fn context(count: usize) -> RoomContext {
        RoomContext {
            recent_events: (0..count).map(event).collect(),
            ..RoomContext::new(Room::new(room_id()))
        }
    }

    async fn read(memory: SlidingWindowMemory, count: usize) -> Vec<String> {
        let result = memory
            .retrieve(
                &room_id(),
                &event(999),
                &context(count),
                RetrieveOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(result.messages, Vec::new());
        result
            .events
            .iter()
            .map(|event| event.id.to_string())
            .collect()
    }

    fn window(max_events: usize) -> SlidingWindowMemory {
        SlidingWindowMemory::new(SlidingWindowMemoryOptions { max_events })
    }

    // Python: tests/test_memory.py::TestSlidingWindowMemory::test_returns_last_n_events
    #[tokio::test]
    async fn returns_last_n_events() {
        assert_eq!(read(window(3), 10).await, ["evt_7", "evt_8", "evt_9"]);
    }

    // Python: tests/test_memory.py::TestSlidingWindowMemory::test_returns_all_when_fewer_than_max
    #[tokio::test]
    async fn returns_all_when_fewer_than_max() {
        assert_eq!(read(window(50), 2).await, ["evt_0", "evt_1"]);
    }

    // Python: tests/test_memory.py::TestSlidingWindowMemory::test_returns_empty_when_no_events
    #[tokio::test]
    async fn returns_empty_when_no_events() {
        assert_eq!(read(window(50), 0).await, Vec::<String>::new());
    }

    // Python: tests/test_memory.py::TestSlidingWindowMemory::test_name and
    // test_default_max_events
    #[test]
    fn its_name_and_its_default_window() {
        let memory = SlidingWindowMemory::new(SlidingWindowMemoryOptions::default());
        assert_eq!(
            (memory.name(), memory.recent_events_window()),
            ("SlidingWindowMemory".to_owned(), 50)
        );
    }

    /// The turn's own event is left out wherever it stands, and counts in
    /// the window; a window of 0 reads every event, as Python's slice.
    #[tokio::test]
    async fn the_turn_s_event_counts_in_the_window_and_a_window_of_zero_reads_all() {
        let memory = window(3);
        let mut context = context(5);
        let newest = context.recent_events.last().cloned().unwrap();
        let read = memory
            .retrieve(&room_id(), &newest, &context, RetrieveOptions::default())
            .await
            .unwrap();
        let ids: Vec<String> = read
            .events
            .iter()
            .map(|event| event.id.to_string())
            .collect();
        assert_eq!(ids, ["evt_2", "evt_3"]);
        context.recent_events.truncate(2);
        assert_eq!(read_all(window(0), &context).await, ["evt_0", "evt_1"]);
    }

    async fn read_all(memory: SlidingWindowMemory, context: &RoomContext) -> Vec<String> {
        memory
            .retrieve(&room_id(), &event(999), context, RetrieveOptions::default())
            .await
            .unwrap()
            .events
            .iter()
            .map(|event| event.id.to_string())
            .collect()
    }
}
