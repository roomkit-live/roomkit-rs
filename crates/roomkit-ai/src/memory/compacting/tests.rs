//! Python: `tests/test_memory_new.py::TestCompactingMemory`, and the
//! compacting case of `tests/test_memory_wrappers.py` and
//! `tests/test_channels/test_memory_notes.py`.

use pretty_assertions::assert_eq;

use super::*;
use crate::memory::footprint::{TurnFootprint, measured};
use crate::memory::summary::is_summary;
use crate::memory::testing::{Summarizer, context, event, room};
use crate::memory::token_estimator::{estimate_message_tokens, estimate_notes_tokens};
use crate::memory::{
    MockMemoryProvider, MockMemoryProviderOptions, SlidingWindowMemory, SlidingWindowMemoryOptions,
};

fn inner(messages: Vec<AiMessage>, events: Vec<RoomEvent>) -> Arc<MockMemoryProvider> {
    Arc::new(MockMemoryProvider::new(MockMemoryProviderOptions {
        messages,
        events,
        ..MockMemoryProviderOptions::default()
    }))
}

/// Twenty events of a thousand tokens each.
fn heavy() -> Vec<RoomEvent> {
    (0..20)
        .map(|n| event(&"x".repeat(4000), &format!("e{n}")))
        .collect()
}

fn three() -> CompactingMemoryOptions {
    CompactingMemoryOptions {
        min_events: 3,
        ..CompactingMemoryOptions::default()
    }
}

fn memory(
    inner: Arc<dyn MemoryProvider>,
    summarizer: &Summarizer,
    window: usize,
    options: CompactingMemoryOptions,
) -> CompactingMemory<Summarizer> {
    CompactingMemory::new(inner, summarizer.clone(), window, options)
}

async fn read(memory: &CompactingMemory<Summarizer>) -> MemoryResult {
    let options = RetrieveOptions {
        channel_id: Some(ChannelId::new("ai1").unwrap()),
    };
    memory
        .retrieve(&room(), &event("current", "now"), &context(), options)
        .await
        .unwrap()
}

// Python: TestCompactingMemory::test_no_compaction_when_under_budget and
// test_name_includes_inner
#[tokio::test]
async fn within_its_window_it_hands_on_the_inner_result() {
    let summarizer = Summarizer::answering(&["summary"]);
    let events = vec![event("short", "e0")];
    let memory = memory(
        inner(Vec::new(), events.clone()),
        &summarizer,
        100_000,
        CompactingMemoryOptions::default(),
    );
    let result = read(&memory).await;
    assert_eq!(
        (
            result.events,
            result.messages.len(),
            summarizer.prompts().len(),
            memory.name()
        ),
        (
            events,
            0,
            0,
            "CompactingMemory(MockMemoryProvider)".to_owned()
        )
    );
}

// Python: TestCompactingMemory::test_compaction_generates_summary and
// test_preserves_inner_messages
#[tokio::test]
async fn past_its_window_the_oldest_events_are_summarized_after_the_inner_messages() {
    let summarizer = Summarizer::answering(&["The conversation discussed XYZ."]);
    let existing = AiMessage::new(AiRole::System, "existing context");
    let memory = memory(
        inner(vec![existing.clone()], heavy()),
        &summarizer,
        5000,
        three(),
    );
    let result = read(&memory).await;
    assert_eq!(result.messages.len(), 2);
    assert_eq!(result.messages[0], existing);
    assert!(is_summary(&result.messages[1]));
    assert!(
        result.messages[1]
            .content
            .text()
            .contains("The conversation discussed XYZ.")
    );
    // 4,250 tokens less the summary's 500: three events of 1,001.
    assert_eq!(result.events.len(), 3);
    assert!(summarizer.prompts()[0].starts_with(
        "Summarize this conversation concisely. Focus on: decisions made, key findings, tool \
         results, errors encountered, and current task state. Be specific about file names, \
         error messages, and action outcomes.\n\n[user]: “xxx"
    ));
}

// Python: TestCompactingMemory::test_summary_cache_avoids_regeneration
#[tokio::test]
async fn a_room_s_summary_is_reused_for_the_same_events() {
    let summarizer = Summarizer::answering(&["Cached summary"]);
    let memory = memory(inner(Vec::new(), heavy()), &summarizer, 5000, three());
    read(&memory).await;
    read(&memory).await;
    assert_eq!(summarizer.prompts().len(), 1);
}

// Python: TestCompactingMemory::test_summary_failure_graceful_fallback
#[tokio::test]
async fn a_summarizer_that_fails_leaves_a_summary_that_says_so() {
    let result = read(&memory(
        inner(Vec::new(), heavy()),
        &Summarizer::default(),
        5000,
        three(),
    ))
    .await;
    assert_eq!(result.messages.len(), 1);
    assert!(
        result.messages[0]
            .content
            .text()
            .contains("[Earlier conversation with 17 messages — summary unavailable]")
    );
}

// Python: TestCompactingMemory::test_close_closes_both and
// tests/test_memory_wrappers.py (compacting)
#[tokio::test]
async fn its_lifecycle_reaches_the_inner_memory_and_the_summarizer() {
    let inner = Arc::new(MockMemoryProvider::default());
    let summarizer = Summarizer::default();
    let nested = Arc::new(memory(
        Arc::<MockMemoryProvider>::clone(&inner),
        &summarizer,
        1000,
        CompactingMemoryOptions::default(),
    ));
    let memory = memory(
        nested,
        &summarizer,
        1000,
        CompactingMemoryOptions::default(),
    );
    memory
        .ingest(&room(), &event("remember", "e1"), IngestOptions::default())
        .await
        .unwrap();
    memory.clear(&room()).await.unwrap();
    memory.close().await;
    assert_eq!(
        (
            inner.ingest_calls().len(),
            inner.clear_calls(),
            inner.closed(),
            summarizer.asked.lock().unwrap().closed
        ),
        (1, vec![room()], true, true)
    );
}

/// A clear forgets the room's summaries: the next read asks again.
#[tokio::test]
async fn a_clear_forgets_the_room_s_summaries() {
    let summarizer = Summarizer::answering(&["summary"]);
    let memory = memory(inner(Vec::new(), heavy()), &summarizer, 5000, three());
    read(&memory).await;
    memory.clear(&room()).await.unwrap();
    read(&memory).await;
    assert_eq!(summarizer.prompts().len(), 2);
}

// Python: tests/test_channels/test_memory_notes.py::test_a_budget_pays_for_the_notes_it_carries
// (compacting) and test_a_memory_that_wraps_another_carries_its_notes
#[tokio::test]
async fn it_carries_the_inner_notes_and_pays_for_them() {
    let notes = vec!["passage ".repeat(6000)];
    let inner = Arc::new(MockMemoryProvider::new(MockMemoryProviderOptions {
        events: (0..40)
            .map(|n| event(&"x".repeat(4000), &format!("e{n}")))
            .collect(),
        notes: notes.clone(),
        ..MockMemoryProviderOptions::default()
    }));
    let options = CompactingMemoryOptions {
        min_events: 1,
        ..CompactingMemoryOptions::default()
    };
    let result = read(&memory(
        inner,
        &Summarizer::answering(&["summary"]),
        40_000,
        options,
    ))
    .await;
    let carried = result
        .messages
        .iter()
        .map(estimate_message_tokens)
        .sum::<usize>()
        + result
            .events
            .iter()
            .map(|event| estimate_event_tokens(event, EstimateEventTokensOptions::default()))
            .sum::<usize>()
        + estimate_notes_tokens(&result.notes);
    assert_eq!(result.notes, notes);
    assert!(carried <= 40_000);
}

// Python: tests/test_turn_footprint.py::test_the_reply_is_reserved_once_the_larger_of_the_margin_and_its_budget
// (compacting)
#[tokio::test]
async fn the_reply_is_reserved_once_the_larger_of_the_margin_and_its_budget() {
    let window = SlidingWindowMemory::new(SlidingWindowMemoryOptions { max_events: 100 });
    let memory = memory(
        Arc::new(window),
        &Summarizer::answering(&["summary"]),
        20_000,
        CompactingMemoryOptions::default(),
    );
    let current = event("now", "current");
    let mut recent_events: Vec<RoomEvent> = (0..80)
        .map(|n| event(&"history ".repeat(200), &format!("e{n}")))
        .collect();
    recent_events.push(current.clone());
    let context = RoomContext {
        recent_events,
        ..context()
    };
    let kept = |reply_tokens: usize| {
        let footprint = TurnFootprint::new(1000, reply_tokens);
        let room = room();
        let (memory, current, context) = (&memory, &current, &context);
        async move {
            let read = memory.retrieve(&room, current, context, RetrieveOptions::default());
            measured(footprint, read).await.unwrap().events.len()
        }
    };
    // The margin is 3,000 of 20,000.
    let within_margin = kept(2000).await;
    let over_margin = kept(9000).await;
    assert_eq!(within_margin, kept(0).await);
    assert!(over_margin < within_margin);
}
