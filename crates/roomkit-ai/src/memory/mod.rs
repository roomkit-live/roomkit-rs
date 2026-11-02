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
//! - [`BudgetAwareMemory`]: another memory's events trimmed to a window of
//!   tokens, once the turn's footprint ([`current_turn_footprint`]) is paid
//!   for (RFC §20.3); the estimates it counts with
//!   ([`estimate_tokens`], [`history_budget`]...).
//! - [`SummarizingMemory`] and [`CompactingMemory`]: another memory's
//!   older events summarized by a model, the summary's message standing
//!   for them (RFC §20.3, §6.4).
//! - [`RetrievalMemory`]: another memory's result with what knowledge
//!   sources hold for the turn's words, as the turn's note (RFC §20.2).

mod budget_aware;
mod compacting;
mod footprint;
#[cfg(test)]
mod mock;
mod repr;
mod retrieval;
mod sliding_window;
mod summarizing;
mod summary;
#[cfg(test)]
mod testing;
mod token_estimator;

use std::any::type_name;
use std::future::{self, Future};
use std::pin::Pin;

use roomkit::ChannelError;
use roomkit::model::{ChannelId, RoomContext, RoomEvent, RoomId};

pub use self::budget_aware::{BudgetAwareMemory, BudgetAwareMemoryOptions};
pub use self::compacting::{CompactingMemory, CompactingMemoryOptions};
pub(crate) use self::footprint::measured;
pub use self::footprint::{TurnFootprint, current_turn_footprint};
#[cfg(test)]
pub(crate) use self::mock::{MockMemoryProvider, MockMemoryProviderOptions};
pub use self::retrieval::{RetrievalMemory, RetrievalMemoryOptions};
pub use self::sliding_window::{SlidingWindowMemory, SlidingWindowMemoryOptions};
pub use self::summarizing::{SummarizingMemory, SummarizingMemoryOptions};
pub(crate) use self::token_estimator::estimate_tool_tokens;
pub use self::token_estimator::{
    EstimateEventTokensOptions, HistoryBudgetOptions, estimate_context_tokens,
    estimate_event_tokens, estimate_message_tokens, estimate_notes_tokens, estimate_tokens,
    extract_event_text, history_budget,
};
use roomkit_core::rules::transcript::{RUNTIME_RECORD, without_mark_copies};
use serde_json::Value;

use crate::provider::{AiContentPart, AiMessage, AiMessageContent, AiRole, AiTextPart};

/// How many of the room's latest events a provider that does not trim by
/// count reads (Python's `DEFAULT_RECENT_EVENTS_WINDOW`): the ceiling of
/// what a context holds.
const DEFAULT_RECENT_EVENTS_WINDOW: usize = 2_000;

/// What a memory provider answers a turn (RFC §20.2): Python's
/// `MemoryResult(messages=[], events=[], notes=[], speakers=[])`.
/// `messages` come first, then `events`, converted by the channel as it
/// converts its history.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MemoryResult {
    /// Messages the provider built (a summary, an instruction), read before
    /// the events; a user text they end on joins the user message after
    /// them.
    pub messages: Vec<AiMessage>,
    /// The room's events the channel converts, oldest first.
    pub events: Vec<RoomEvent>,
    /// What the provider retrieved for this turn alone (knowledge
    /// passages): it changes from turn to turn, so the channel carries it
    /// with the turn's notes, after the input, never in the history a
    /// provider caches. A provider that wraps another and rebuilds its
    /// result carries the inner notes.
    pub notes: Vec<String>,
    /// The labels `messages` name participants by, as the runtime gives
    /// them (a summary's lines, `Alice`, `@sms1`): the channel counts them
    /// toward labelling the turns that follow (RFC §6.4).
    pub speakers: Vec<String>,
}

/// `memory`, the messages a memory built, then `rest`, the conversation: a
/// user text `memory` ends on joins the user message that opens `rest`, a
/// blank line between, rather than forming a second user message in a
/// row (RFC §20.2). Python's `_after_memory`.
pub(crate) fn after_memory(memory: &[AiMessage], mut rest: Vec<AiMessage>) -> Vec<AiMessage> {
    let mut messages = memory.to_vec();
    let Some(AiMessage {
        role: AiRole::User,
        content: AiMessageContent::Text(leading),
        ..
    }) = messages.last().cloned()
    else {
        messages.extend(rest);
        return messages;
    };
    match rest.first_mut() {
        Some(first) if first.role == AiRole::User => {
            messages.pop();
            first.content =
                match std::mem::replace(&mut first.content, AiMessageContent::Parts(Vec::new())) {
                    AiMessageContent::Text(text) if text.is_empty() => {
                        AiMessageContent::Text(leading)
                    }
                    AiMessageContent::Text(text) if leading.is_empty() => {
                        AiMessageContent::Text(text)
                    }
                    AiMessageContent::Text(text) => {
                        AiMessageContent::Text(format!("{leading}\n\n{text}"))
                    }
                    AiMessageContent::Parts(parts) => {
                        let lead = AiContentPart::Text(AiTextPart::new(leading));
                        AiMessageContent::Parts(std::iter::once(lead).chain(parts).collect())
                    }
                };
        }
        Some(_) | None => {}
    }
    messages.extend(rest);
    messages
}

/// `message`, a message a memory built, with each copy of a runtime mark
/// in its text replaced, unless a memory of the runtime built it (a
/// summary), whose marks are its own (RFC §6.4): Python's
/// `_runtime_or_cleaned`.
pub(crate) fn runtime_or_cleaned(mut message: AiMessage) -> AiMessage {
    let written_by_runtime = matches!(message.metadata.get(RUNTIME_RECORD), Some(Value::String(_)));
    if written_by_runtime {
        return message;
    }
    match &mut message.content {
        AiMessageContent::Text(text) => *text = without_mark_copies(text),
        AiMessageContent::Parts(parts) => {
            for part in parts {
                if let AiContentPart::Text(part) = part {
                    part.text = without_mark_copies(&part.text);
                }
            }
        }
    }
    message
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
