//! The memory an AI channel's turn reads its history through (RFC §20):
//! Python's `roomkit.memory`.
//!
//! - [`MemoryProvider`]: what a turn asks for its history, handed the
//!   channel's view of the room (RFC §20.1), and what learns the room's
//!   events as they come.
//! - [`MemoryResult`]: what it answers: messages it built (a summary), then
//!   the room's events the channel converts as it converts its history
//!   (RFC §20.2).
//! - [`SlidingWindowMemory`]: the default, the room's latest events (RFC
//!   §20.3).

mod sliding_window;

use std::any::type_name;
use std::future::{self, Future};
use std::pin::Pin;

use roomkit::ChannelError;
use roomkit::model::{ChannelId, RoomContext, RoomEvent, RoomId};

pub use self::sliding_window::{SlidingWindowMemory, SlidingWindowMemoryOptions};
use crate::provider::AiMessage;

/// How many of the room's latest events a provider that does not trim by
/// count reads (Python's `DEFAULT_RECENT_EVENTS_WINDOW`): the ceiling of
/// what a context holds.
const DEFAULT_RECENT_EVENTS_WINDOW: usize = 2_000;

/// What a memory provider answers a turn (RFC §20.2): Python's
/// `MemoryResult(messages=[], events=[])`. `messages` come first, then
/// `events`, converted by the channel as it converts its history.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MemoryResult {
    /// Messages the provider built (a summary, an instruction), read before
    /// the events.
    pub messages: Vec<AiMessage>,
    /// The room's events the channel converts, oldest first.
    pub events: Vec<RoomEvent>,
}

/// What [`MemoryProvider::retrieve`] takes beside the room, the event and
/// the context: Python's keyword-only `channel_id=None`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RetrieveOptions {
    /// The AI channel asking, when several share a room.
    pub channel_id: Option<ChannelId>,
}

/// What [`MemoryProvider::ingest`] takes beside the room and the event:
/// Python's keyword-only `channel_id=None`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IngestOptions {
    /// The AI channel the event was handed to.
    pub channel_id: Option<ChannelId>,
}

/// Where an AI channel's turn reads its history (RFC §20.1): Python's
/// `MemoryProvider`. A provider is handed history, it does not fetch it:
/// the context is the asking channel's view of the room (RFC §7.5 rule 8).
/// The turn's own event is the context's newest; a provider leaves it out,
/// and the channel places it as the turn's input.
///
/// Only [`retrieve`](Self::retrieve) is required: learning the room's
/// events, clearing a room and closing do nothing by default.
///
/// ```
/// use std::future::{Future, ready};
/// use std::pin::Pin;
///
/// use roomkit::ChannelError;
/// use roomkit::model::{RoomContext, RoomEvent, RoomId};
/// use roomkit_ai::memory::{MemoryProvider, MemoryResult, RetrieveOptions};
///
/// /// The room's latest two events, the turn's own left out.
/// struct LastTwo;
///
/// impl MemoryProvider for LastTwo {
///     fn retrieve<'a>(
///         &'a self,
///         _room_id: &'a RoomId,
///         current_event: &'a RoomEvent,
///         context: &'a RoomContext,
///         _options: RetrieveOptions,
///     ) -> Pin<Box<dyn Future<Output = Result<MemoryResult, ChannelError>> + Send + 'a>> {
///         let skip = context.recent_events.len().saturating_sub(2);
///         let events = context.recent_events.iter().skip(skip);
///         let events = events.filter(|event| event.id != current_event.id).cloned().collect();
///         Box::pin(ready(Ok(MemoryResult { events, ..MemoryResult::default() })))
///     }
/// }
/// ```
pub trait MemoryProvider: Send + Sync + 'static {
    /// The provider's name: Python's `name`, its type's by default.
    fn name(&self) -> String {
        let full = type_name::<Self>();
        let plain = full.split('<').next().unwrap_or(full);
        plain.rsplit("::").next().unwrap_or(plain).to_owned()
    }

    /// How many of the room's latest events the provider may read (RFC §6.1
    /// `recent_events_window`): Python's `recent_events_window`, the
    /// ceiling of what a context holds by default, for a provider that
    /// does not trim by count.
    fn recent_events_window(&self) -> usize {
        DEFAULT_RECENT_EVENTS_WINDOW
    }

    /// The history a turn on `current_event` in the room `room_id` reads,
    /// from `context`, the asking channel's view of the room (RFC §20.1):
    /// Python's `retrieve`. Its error fails the turn.
    fn retrieve<'a>(
        &'a self,
        room_id: &'a RoomId,
        current_event: &'a RoomEvent,
        context: &'a RoomContext,
        options: RetrieveOptions,
    ) -> Pin<Box<dyn Future<Output = Result<MemoryResult, ChannelError>> + Send + 'a>>;

    /// Learns `event` of the room `room_id`, as it reaches the channel,
    /// whether the channel answers it or not: Python's `ingest`, nothing by
    /// default. Its error is logged, and the turn goes on.
    fn ingest<'a>(
        &'a self,
        _room_id: &'a RoomId,
        _event: &'a RoomEvent,
        _options: IngestOptions,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChannelError>> + Send + 'a>> {
        Box::pin(future::ready(Ok(())))
    }

    /// Forgets what the provider keeps of the room `room_id`: Python's
    /// `clear`, nothing by default.
    fn clear<'a>(
        &'a self,
        _room_id: &'a RoomId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChannelError>> + Send + 'a>> {
        Box::pin(future::ready(Ok(())))
    }

    /// Releases what the provider holds, once its channel closes: Python's
    /// `close`, nothing by default.
    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(future::ready(()))
    }
}

#[cfg(test)]
mod tests {
    use roomkit::model::{ChannelType, EventContent, EventSource, TextContent};

    use super::*;

    /// A provider that implements `retrieve` alone.
    struct Bare;

    impl MemoryProvider for Bare {
        fn retrieve<'a>(
            &'a self,
            _room_id: &'a RoomId,
            _current_event: &'a RoomEvent,
            _context: &'a RoomContext,
            _options: RetrieveOptions,
        ) -> Pin<Box<dyn Future<Output = Result<MemoryResult, ChannelError>> + Send + 'a>> {
            Box::pin(future::ready(Ok(MemoryResult::default())))
        }
    }

    // Python: tests/test_memory.py::TestMemoryProviderDefaults (test_ingest_is_noop,
    // test_clear_is_noop, test_close_is_noop)
    #[tokio::test]
    async fn the_lifecycle_does_nothing_by_default() {
        let room = RoomId::new("room_1").unwrap();
        let event = RoomEvent::new(
            room.clone(),
            EventSource::new(ChannelId::new("sms").unwrap(), ChannelType::SMS),
            EventContent::Text(TextContent::new("Hi")),
        );
        assert!(
            Bare.ingest(&room, &event, IngestOptions::default())
                .await
                .is_ok()
        );
        assert!(Bare.clear(&room).await.is_ok());
        Bare.close().await;
        assert_eq!(
            (Bare.name(), Bare.recent_events_window()),
            ("Bare".to_owned(), 2_000)
        );
    }
}
