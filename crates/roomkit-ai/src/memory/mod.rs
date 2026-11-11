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
mod mock;
pub(crate) mod repr;
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
use roomkit::model::{ChannelId, Metadata, RoomContext, RoomEvent, RoomId};

pub use self::budget_aware::{BudgetAwareMemory, BudgetAwareMemoryOptions};
pub use self::compacting::{CompactingMemory, CompactingMemoryOptions};
pub(crate) use self::footprint::measured;
pub use self::footprint::{TurnFootprint, current_turn_footprint};
pub use self::mock::{MockMemoryProvider, MockMemoryProviderOptions};
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

/// Leave the input to the channel before history is trimmed or summarized.
fn without_current_event(mut result: MemoryResult, current_event: &RoomEvent) -> MemoryResult {
    result.events.retain(|event| event.id != current_event.id);
    result
}

/// `memory`, the messages a memory built, then `rest`, the conversation: a
/// user text `memory` ends on joins the user message that opens `rest`, a
/// blank line between, rather than forming a second user message in a
/// row (RFC §20.2), or stands as a user message of its own, its text alone.
/// Python's `_after_memory`.
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
        // A user message of its own, as Python's `with_leading_text` makes
        // it: the text alone, what the memory's message carried beside it
        // left out.
        Some(_) | None => {
            messages.pop();
            messages.push(AiMessage::new(AiRole::User, leading));
        }
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
        AiMessageContent::Parts(parts) => *parts = cleaned_parts(std::mem::take(parts)),
    }
    message
}

/// `parts` with each copy of a runtime mark in their text replaced, a run
/// of adjacent text parts read back to back, as the model reads it; any
/// other part kept as it is. Python's `cleaned_content`.
fn cleaned_parts(parts: Vec<AiContentPart>) -> Vec<AiContentPart> {
    let mut cleaned = Vec::with_capacity(parts.len());
    let mut run = Vec::new();
    for part in parts {
        match part {
            AiContentPart::Text(part) => run.push(part.text),
            other @ (AiContentPart::Image(_)
            | AiContentPart::Thinking(_)
            | AiContentPart::ToolCall(_)
            | AiContentPart::ToolResult(_)) => {
                cleaned.extend(cleaned_run(&std::mem::take(&mut run)));
                cleaned.push(other);
            }
        }
    }
    cleaned.extend(cleaned_run(&run));
    cleaned
}

/// `run`, the texts of adjacent text parts, each cleaned, or as one part
/// when the run cleaned back to back reads otherwise: a copy written over
/// two of them (a part may hold only its start, which reads as no copy
/// alone). Python's `_cleaned_run`.
fn cleaned_run(run: &[String]) -> Vec<AiContentPart> {
    if run.is_empty() {
        return Vec::new();
    }
    let parts: Vec<String> = run.iter().map(|text| without_mark_copies(text)).collect();
    let whole = without_mark_copies(&run.concat());
    let texts = if whole == parts.concat() {
        parts
    } else {
        vec![whole]
    };
    texts
        .into_iter()
        .map(|text| AiContentPart::Text(AiTextPart::new(text)))
        .collect()
}

/// What [`MemoryProvider::retrieve`] takes beside the room, the event and
/// the context: Python's keyword-only `channel_id=None`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RetrieveOptions {
    /// The AI channel asking, when several share a room.
    pub channel_id: Option<ChannelId>,
}

/// What [`MemoryProvider::compact`] takes beside the room, the history and
/// the usage: Python's keyword-only `channel_id=None`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompactOptions {
    /// The AI channel whose turn it is.
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
/// events, clearing a room and closing do nothing by default, and
/// [`compact`](Self::compact) changes nothing.
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

    /// The history to send in place of `history` between two of a turn's
    /// tool rounds, in the room `room_id` (RFC §6.4, §20.1): Python's
    /// `compact`, `None` by default, which leaves the turn as it is.
    ///
    /// The AI channel asks after each tool round it ran, before the next
    /// round's request: `history` is what the turn sends before its input,
    /// as the model reads it, and `usage` what the round's generation
    /// measured: the token counters its provider reported (`input_tokens`,
    /// `output_tokens` and the cache counters, disjoint), empty when it
    /// reported none. A history it returns stands for `history` in
    /// the next requests: the turn's input stays whole, a user text the
    /// messages the memory wrote ahead of the history's own end on joins
    /// the user message after them, and the long results of the turn's
    /// earlier rounds are stored for `read_stored_result`, the last
    /// round's left whole. A message it returns that is not one of
    /// `history`'s has a copy of a runtime mark replaced, unless a memory
    /// of the runtime built it (a summary). Its error is logged, and the
    /// turn goes on as it was.
    #[expect(
        clippy::type_complexity,
        reason = "the boxed future every async method of the trait answers"
    )]
    fn compact<'a>(
        &'a self,
        _room_id: &'a RoomId,
        _history: &'a [AiMessage],
        _usage: &'a Metadata,
        _options: CompactOptions,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<AiMessage>>, ChannelError>> + Send + 'a>>
    {
        Box::pin(future::ready(Ok(None)))
    }

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
    use std::sync::Arc;

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
        // Python: tests/test_memory.py::TestMemoryProviderDefaults::test_compact_changes_nothing
        let history = [AiMessage::new(AiRole::User, "hello")];
        let compacted = Bare
            .compact(&room, &history, &Metadata::new(), CompactOptions::default())
            .await;
        assert!(matches!(compacted, Ok(None)));
        assert_eq!(
            (Bare.name(), Bare.recent_events_window()),
            ("Bare".to_owned(), 2_000)
        );
    }

    /// What a compaction was handed: the room, the history, the usage and
    /// the options.
    type Asked = (RoomId, Vec<AiMessage>, Metadata, CompactOptions);

    /// A memory that compacts every history to its last message, keeping
    /// what it was asked.
    #[derive(Default)]
    struct Compacts {
        asked: std::sync::Mutex<Vec<Asked>>,
    }

    impl MemoryProvider for Compacts {
        fn retrieve<'a>(
            &'a self,
            _room_id: &'a RoomId,
            _current_event: &'a RoomEvent,
            _context: &'a RoomContext,
            _options: RetrieveOptions,
        ) -> Pin<Box<dyn Future<Output = Result<MemoryResult, ChannelError>> + Send + 'a>> {
            Box::pin(future::ready(Ok(MemoryResult::default())))
        }

        fn compact<'a>(
            &'a self,
            room_id: &'a RoomId,
            history: &'a [AiMessage],
            usage: &'a Metadata,
            options: CompactOptions,
        ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<AiMessage>>, ChannelError>> + Send + 'a>>
        {
            let asked = (room_id.clone(), history.to_vec(), usage.clone(), options);
            self.asked.lock().unwrap().push(asked);
            let last = history.last().cloned().into_iter().collect();
            Box::pin(future::ready(Ok(Some(last))))
        }
    }

    // Python: tests/test_memory_wrappers.py::test_compaction_reaches_inner
    #[tokio::test]
    async fn a_decorator_hands_the_compaction_to_the_memory_it_wraps() {
        let room = RoomId::new("r1").unwrap();
        let history = [
            AiMessage::new(AiRole::User, "a"),
            AiMessage::new(AiRole::Assistant, "b"),
        ];
        let mut usage = Metadata::new();
        usage.insert("input_tokens".to_owned(), Value::from(5));
        let options = CompactOptions {
            channel_id: Some(ChannelId::new("ai").unwrap()),
        };
        for kind in ["budget", "compacting", "retrieval", "handoff"] {
            let inner = Arc::new(Compacts::default());
            let wrapped: Arc<dyn MemoryProvider> = Arc::<Compacts>::clone(&inner);
            let memory: Arc<dyn MemoryProvider> = match kind {
                "budget" => Arc::new(BudgetAwareMemory::new(
                    wrapped,
                    1000,
                    BudgetAwareMemoryOptions::default(),
                )),
                "compacting" => Arc::new(CompactingMemory::new(
                    wrapped,
                    testing::Summarizer::default(),
                    1000,
                    CompactingMemoryOptions::default(),
                )),
                "retrieval" => Arc::new(RetrievalMemory::new(
                    Vec::new(),
                    wrapped,
                    RetrievalMemoryOptions::default(),
                )),
                _ => Arc::new(crate::handoff::HandoffMemoryProvider::new(wrapped)),
            };
            let compacted = memory
                .compact(&room, &history, &usage, options.clone())
                .await;
            assert_eq!(compacted.unwrap(), Some(history[1..].to_vec()), "{kind}");
            let asked = inner.asked.lock().unwrap().clone();
            let expected = (
                room.clone(),
                history.to_vec(),
                usage.clone(),
                options.clone(),
            );
            assert_eq!(asked, vec![expected]);
        }
    }
}
