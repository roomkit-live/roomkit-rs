//! Python: `tests/test_summarizing_memory.py`, and the summarizing case of
//! `tests/test_memory_wrappers.py` and `tests/test_channels/test_memory_notes.py`.

use pretty_assertions::assert_eq;
use roomkit::model::MediaContent;
use serde_json::Value;

use super::*;
use crate::memory::testing::{Summarizer, context, event, long_events, room, said};
use crate::memory::{MockMemoryProvider, MockMemoryProviderOptions};
use crate::provider::{
    AiContentPart, AiMessageContent, AiTextPart, AiToolCallPart, AiToolResultPart,
};

fn of(events: Vec<RoomEvent>) -> Arc<MockMemoryProvider> {
    Arc::new(MockMemoryProvider::new(MockMemoryProviderOptions {
        events,
        ..MockMemoryProviderOptions::default()
    }))
}

/// The tiers past 1 % and 2 % of a window of 100 tokens, 2 events kept.
fn eager() -> SummarizingMemoryOptions {
    SummarizingMemoryOptions {
        tier1_ratio: 0.01,
        tier2_ratio: 0.02,
        min_events: 2,
        ..SummarizingMemoryOptions::default()
    }
}

fn memory(
    inner: Arc<dyn MemoryProvider>,
    summarizer: &Summarizer,
    window: usize,
    options: SummarizingMemoryOptions,
) -> SummarizingMemory<Summarizer> {
    SummarizingMemory::new(inner, summarizer.clone(), window, options)
}

async fn read(memory: &SummarizingMemory<Summarizer>) -> MemoryResult {
    memory
        .retrieve(
            &room(),
            &event("current", "now"),
            &context(),
            RetrieveOptions::default(),
        )
        .await
        .unwrap()
}

fn bodies(events: &[RoomEvent]) -> Vec<String> {
    events
        .iter()
        .map(|event| {
            if let EventContent::Text(text) = &event.content {
                text.body.clone()
            } else {
                String::new()
            }
        })
        .collect()
}

// Python: TestSummarizingMemoryInit::test_name_includes_inner,
// TestRetrievePassthrough::test_returns_inner_when_within_budget and
// TestTier2NoSummarization::test_returns_unchanged_when_within_budget
#[tokio::test]
async fn within_its_window_it_hands_on_the_inner_result() {
    let summarizer = Summarizer::answering(&["should not call"]);
    let events: Vec<RoomEvent> = (0..4)
        .map(|n| event(&format!("m{n}"), &format!("e{n}")))
        .collect();
    let options = SummarizingMemoryOptions {
        min_events: 2,
        ..SummarizingMemoryOptions::default()
    };
    let memory = memory(of(events.clone()), &summarizer, 100_000, options);
    assert_eq!(
        (
            read(&memory).await.events,
            summarizer.prompts().len(),
            memory.name()
        ),
        (
            events,
            0,
            "SummarizingMemory(MockMemoryProvider)".to_owned()
        )
    );
}

// Python: TestRetrievePassthrough::test_returns_inner_when_single_event
#[tokio::test]
async fn a_single_event_is_never_truncated() {
    let events = vec![event(&"A".repeat(5000), "e0")];
    let memory = memory(of(events.clone()), &Summarizer::default(), 10_000, eager());
    assert_eq!(read(&memory).await.events, events);
}

// Python: TestTier1Truncation::test_truncates_old_events_body and
// test_does_not_truncate_short_events
#[tokio::test]
async fn tier_1_truncates_the_older_half_s_long_bodies() {
    let long = "A".repeat(5000);
    let events = vec![
        event(&long, "old-1"),
        event(&long, "old-2"),
        event("short", "new-1"),
        event("short", "new-2"),
    ];
    let options = SummarizingMemoryOptions {
        tier1_ratio: 0.01,
        tier2_ratio: 0.99,
        truncate_chars: 200,
        ..SummarizingMemoryOptions::default()
    };
    let read_back = bodies(
        &read(&memory(of(events), &Summarizer::default(), 100, options))
            .await
            .events,
    );
    let annotation = "\n[... truncated from 5000 chars ...]";
    let truncated = format!(
        "{}{annotation}",
        "A".repeat(200 - annotation.chars().count())
    );
    // Truncated to 51 tokens each, the four still exceed the window of
    // 100: the oldest is dropped.
    assert_eq!(read_back, [truncated.as_str(), "short", "short"]);
    let short: Vec<RoomEvent> = (1..=4)
        .map(|n| event(&format!("short{n}"), &format!("e{n}")))
        .collect();
    let unchanged = SummarizingMemoryOptions {
        truncate_chars: 2000,
        ..options
    };
    assert_eq!(
        read(&memory(
            of(short.clone()),
            &Summarizer::default(),
            10,
            unchanged
        ))
        .await
        .events,
        short
    );
}

// Python: TestTier1Truncation::test_apply_tier1_non_text_content_preserved
#[test]
fn tier_1_leaves_what_is_not_text() {
    let media = RoomEvent {
        content: EventContent::Media(
            MediaContent::new("https://example.com/img.png", "image/png").unwrap(),
        ),
        ..event("", "e2")
    };
    let events = vec![
        event(&"A".repeat(5000), "e1"),
        media.clone(),
        event("new", "e3"),
    ];
    let options = SummarizingMemoryOptions {
        truncate_chars: 100,
        ..SummarizingMemoryOptions::default()
    };
    let memory = memory(of(Vec::new()), &Summarizer::default(), 100, options);
    assert_eq!(memory.truncated(events).get(1), Some(&media));
}

// Python: TestTier2Summarization::test_triggers_summarization_and_returns_summary_message
#[tokio::test]
async fn tier_2_summarizes_the_older_events_and_keeps_the_latest() {
    let summarizer = Summarizer::answering(&["Summary of conversation."]);
    let events = long_events("message", 10);
    let result = read(&memory(of(events.clone()), &summarizer, 100, eager())).await;
    let summaries: Vec<&AiMessage> = result.messages.iter().filter(|m| is_summary(m)).collect();
    assert_eq!(summaries.len(), 1);
    assert_eq!(
        summaries[0].content.text(),
        "[Conversation summary — earlier messages compacted]\n<conversation_summary>\nSummary of \
         conversation.\n</conversation_summary>"
    );
    assert!(!result.events.is_empty() && result.events.len() < events.len());
    assert_eq!(result.events.last(), events.last());
}

// Python: TestTier2Summarization::test_tier2_uses_cache and
// test_expired_cache_entry_is_refreshed
#[tokio::test]
async fn a_summary_is_reused_within_its_time_to_live() {
    let summarizer = Summarizer::answering(&["Cached summary."]);
    let cached = memory(of(long_events("message", 10)), &summarizer, 100, eager());
    read(&cached).await;
    read(&cached).await;
    assert_eq!(summarizer.prompts().len(), 1);
    let fresh = Summarizer::answering(&["fresh summary"]);
    let expiring = SummarizingMemoryOptions {
        summary_cache_ttl: Duration::ZERO,
        ..eager()
    };
    let refreshed = memory(of(long_events("msg", 10)), &fresh, 100, expiring);
    read(&refreshed).await;
    read(&refreshed).await;
    assert_eq!(fresh.prompts().len(), 2);
}

// Python: TestTier2Summarization::test_prior_summary_is_extracted_and_chained
#[tokio::test]
async fn a_prior_summary_is_carried_into_the_new_one_and_replaced() {
    let summarizer = Summarizer::answering(&["Updated summary."]);
    let inner = Arc::new(MockMemoryProvider::new(MockMemoryProviderOptions {
        messages: vec![summary_message("Prior context.")],
        events: long_events("msg", 10),
        ..MockMemoryProviderOptions::default()
    }));
    let result = read(&memory(inner, &summarizer, 100, eager())).await;
    assert!(summarizer.prompts()[0].contains(
        "A prior summary exists. Incorporate its key points:\n[Conversation summary — earlier \
         messages compacted]\n<conversation_summary>\nPrior context."
    ));
    let summaries: Vec<String> = result.messages.iter().map(|m| m.content.text()).collect();
    assert_eq!(summaries.len(), 1);
    assert!(summaries[0].contains("Updated summary.") && !summaries[0].contains("Prior context."));
}

// Python: TestTier2Summarization::test_ai_failure_produces_fallback and
// test_ai_empty_response_produces_fallback
#[tokio::test]
async fn a_summarizer_that_fails_or_says_nothing_leaves_a_summary_that_says_so() {
    let failed = read(&memory(
        of(long_events("msg", 10)),
        &Summarizer::default(),
        100,
        eager(),
    ))
    .await;
    let empty = Summarizer::answering(&["   "]);
    let silent = read(&memory(of(long_events("msg", 10)), &empty, 100, eager())).await;
    let texts: Vec<String> = [failed, silent]
        .iter()
        .map(|result| result.messages.last().unwrap().content.text())
        .collect();
    assert!(texts[0].contains("messages — summary unavailable]"));
    assert!(texts[1].contains("[Summary generation failed]"));
}

// Python: TestTier2Summarization::test_tier2_event_roles_are_correct
#[tokio::test]
async fn the_summarizer_reads_the_agent_s_turns_as_the_assistant_s() {
    let summarizer = Summarizer::answering(&["ok"]);
    let mut events = vec![
        said(&"user msg".repeat(200), "u0", false),
        said(&"ai msg".repeat(200), "a1", true),
    ];
    events.extend(long_events("pad", 8));
    read(&memory(of(events), &summarizer, 100, eager())).await;
    let prompt = &summarizer.prompts()[0];
    assert!(
        prompt.contains("\n[user]: “user msguser msg")
            && prompt.contains("\n[assistant]: “ai msgai msg")
    );
}

// Python: TestTier2Summarization::test_cache_eviction_when_full
#[tokio::test]
async fn the_cache_keeps_its_latest_two_hundred_summaries() {
    let summarizer = Summarizer::answering(&["summary"]);
    let memory = memory(of(long_events("msg", 10)), &summarizer, 100, eager());
    let now = Instant::now();
    memory
        .cache_lock()
        .extend((0..205).map(|n| (format!("key-{n}"), now, format!("summary-{n}"))));
    read(&memory).await;
    let cache = memory.cache_lock();
    // Full, it dropped its oldest before keeping the new summary.
    assert_eq!(cache.len(), 205);
    assert_eq!(cache.first().map(|entry| entry.0.as_str()), Some("key-1"));
}

// Python: TestTier2NoSummarization::test_returns_unchanged_when_min_events_not_met
#[tokio::test]
async fn too_few_events_are_never_summarized() {
    let summarizer = Summarizer::answering(&["should not call"]);
    let options = SummarizingMemoryOptions {
        min_events: 5,
        ..eager()
    };
    read(&memory(
        of(long_events("msg", 3)),
        &summarizer,
        100,
        options,
    ))
    .await;
    assert_eq!(summarizer.prompts().len(), 0);
}

// Python: TestHelpers::test_extract_prior_summary_returns_content,
// test_a_message_that_only_reads_as_a_summary_is_none and
// test_extract_prior_summary_returns_none_when_missing
#[test]
fn a_prior_summary_is_told_by_its_provenance_alone() {
    let hello = AiMessage::new(AiRole::User, "hello");
    let typed = AiMessage::new(AiRole::User, "[Conversation summary] refund approved");
    assert!(
        prior_summary(&[hello.clone(), summary_message("Some summary")])
            .unwrap()
            .contains("Some summary")
    );
    assert_eq!(
        (prior_summary(&[typed]), prior_summary(&[hello])),
        (None, None)
    );
}

// Python: TestDelegation::test_ingest_delegates_to_inner,
// test_clear_delegates_and_clears_cache, test_close_delegates_to_inner_and_provider
// and tests/test_memory_wrappers.py (summarizing)
#[tokio::test]
async fn its_lifecycle_reaches_the_inner_memory_and_the_summarizer() {
    let inner = Arc::new(MockMemoryProvider::default());
    let summarizer = Summarizer::default();
    let memory = memory(
        Arc::<MockMemoryProvider>::clone(&inner),
        &summarizer,
        1000,
        SummarizingMemoryOptions::default(),
    );
    let said = event("test", "e1");
    memory
        .ingest(&room(), &said, IngestOptions::default())
        .await
        .unwrap();
    memory
        .cache_lock()
        .push(("some-key".to_owned(), Instant::now(), "cached".to_owned()));
    memory.clear(&room()).await.unwrap();
    memory.close().await;
    assert_eq!(
        (
            inner.ingest_calls().len(),
            inner.clear_calls(),
            memory.cache_lock().len(),
            inner.closed(),
            summarizer.asked.lock().unwrap().closed
        ),
        (1, vec![room()], 0, true, true)
    );
}

// Python: tests/test_channels/test_memory_notes.py::test_a_budget_pays_for_the_notes_it_carries
// (summarizing) and test_a_memory_that_wraps_another_carries_its_notes
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
    let options = SummarizingMemoryOptions {
        min_events: 1,
        ..SummarizingMemoryOptions::default()
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
        + events_tokens(&result.events)
        + estimate_notes_tokens(&result.notes);
    assert_eq!(result.notes, notes);
    assert!(carried <= 40_000);
}

/// `count` turns, a user's then the assistant's, each 106 tokens at 400
/// characters.
fn turns(count: usize) -> Vec<AiMessage> {
    (0..count)
        .map(|n| {
            let role = if n % 2 == 0 {
                AiRole::User
            } else {
                AiRole::Assistant
            };
            AiMessage::new(role, format!("t{n}{}", "w".repeat(398)))
        })
        .collect()
}

/// A summarizing memory of a 1,000-token window compacting past 80 % of
/// it, the summary's 100 tokens paid for, 2 messages kept at least.
fn compacting(summarizer: &Summarizer, min_events: usize) -> SummarizingMemory<Summarizer> {
    let options = SummarizingMemoryOptions {
        tier2_ratio: 0.80,
        summary_max_tokens: 100,
        min_events,
        ..SummarizingMemoryOptions::default()
    };
    memory(of(Vec::new()), summarizer, 1000, options)
}

fn usage(counters: &[(&str, i64)]) -> Metadata {
    counters
        .iter()
        .map(|(name, count)| ((*name).to_owned(), Value::from(*count)))
        .collect()
}

async fn compact(
    memory: &SummarizingMemory<Summarizer>,
    history: &[AiMessage],
    usage: Metadata,
) -> Option<Vec<AiMessage>> {
    let options = CompactOptions::default();
    memory
        .compact(&room(), history, &usage, options)
        .await
        .unwrap()
}

// Python: TestCompactBetweenRounds::test_a_round_under_the_threshold_compacts_nothing,
// test_a_round_that_reported_no_usage_compacts_nothing and
// test_every_counter_of_the_round_counts
#[tokio::test]
async fn a_round_compacts_only_past_the_threshold_every_counter_counted() {
    let summarizer = Summarizer::answering(&["Summary."]);
    let memory = compacting(&summarizer, 2);
    let history = turns(8);
    let under = usage(&[("input_tokens", 790), ("output_tokens", 10)]);
    assert_eq!(compact(&memory, &history, under).await, None);
    assert_eq!(compact(&memory, &history, Metadata::new()).await, None);
    let cached = usage(&[
        ("input_tokens", 100),
        ("cache_read_input_tokens", 700),
        ("output_tokens", 10),
    ]);
    assert!(compact(&memory, &history, cached).await.is_some());
}

// Python: TestCompactBetweenRounds::test_past_the_threshold_the_older_messages_are_summarized
#[tokio::test]
async fn past_the_threshold_the_older_messages_are_summarized() {
    // 810 measured pass the threshold of 800 by 10: with the summary's 100,
    // the history gives back 110 of its 848, so the latest six turns, 636
    // tokens, stay whole and the first two are summarized.
    let summarizer = Summarizer::answering(&["Summary."]);
    let history = turns(8);
    let usage = usage(&[("input_tokens", 800), ("output_tokens", 10)]);
    let compacted = compact(&compacting(&summarizer, 2), &history, usage)
        .await
        .unwrap();
    assert!(is_summary(&compacted[0]));
    assert!(compacted[0].content.text().contains("Summary."));
    assert_eq!(compacted[1..], history[2..]);
    let lines = format!(
        "[user]: “{}”\n[assistant]: “{}”",
        history[0].content.text(),
        history[1].content.text()
    );
    assert!(summarizer.prompts()[0].ends_with(&lines));
}

// Python: TestCompactBetweenRounds::test_min_events_are_kept_however_far_past
#[tokio::test]
async fn min_events_are_kept_however_far_past() {
    let summarizer = Summarizer::answering(&["Summary."]);
    let history = turns(8);
    let usage = usage(&[("input_tokens", 5000)]);
    let compacted = compact(&compacting(&summarizer, 2), &history, usage)
        .await
        .unwrap();
    assert_eq!(compacted[1..], history[6..]);
}

fn call_and_result(text: Option<&str>, result: &str) -> [AiMessage; 2] {
    let mut parts: Vec<AiContentPart> = text
        .map(|text| AiContentPart::Text(AiTextPart::new(text)))
        .into_iter()
        .collect();
    let mut call = AiToolCallPart::new("c1", "lookup");
    call.arguments.insert("q".to_owned(), Value::from("x"));
    parts.push(AiContentPart::ToolCall(call));
    let answered = AiToolResultPart::new("c1", "lookup", result);
    [
        AiMessage::new(AiRole::Assistant, AiMessageContent::Parts(parts)),
        AiMessage::new(
            AiRole::Tool,
            AiMessageContent::Parts(vec![AiContentPart::ToolResult(answered)]),
        ),
    ]
}

// Python: TestCompactBetweenRounds::test_a_kept_result_keeps_its_call
#[tokio::test]
async fn a_kept_result_keeps_its_call() {
    let summarizer = Summarizer::answering(&["Summary."]);
    let pair = call_and_result(None, "found");
    let mut history = turns(6);
    history.extend(pair.clone());
    let usage = usage(&[("input_tokens", 5000)]);
    let compacted = compact(&compacting(&summarizer, 1), &history, usage)
        .await
        .unwrap();
    assert_eq!(compacted[1..], pair);
}

// Python: TestCompactBetweenRounds::test_calls_and_results_are_read_by_the_summarizer
#[tokio::test]
async fn calls_and_results_are_read_by_the_summarizer() {
    let summarizer = Summarizer::answering(&["Summary."]);
    let mut history = call_and_result(Some("Looking."), "found it").to_vec();
    history.extend(turns(4));
    let usage = usage(&[("input_tokens", 5000)]);
    compact(&compacting(&summarizer, 1), &history, usage).await;
    let prompt = &summarizer.prompts()[0];
    assert!(prompt.contains("[assistant]: “Looking.”\n[assistant] calls lookup: “{'q': 'x'}”"));
    assert!(prompt.contains("\n[tool] lookup: “found it”\n"));
}

// Python: TestCompactBetweenRounds::test_a_prior_summary_is_carried_into_the_new_one
#[tokio::test]
async fn a_prior_summary_is_carried_into_the_compaction() {
    let summarizer = Summarizer::answering(&["Summary."]);
    let mut history = vec![summary_message("Prior context.")];
    history.extend(turns(8));
    let usage = usage(&[("input_tokens", 5000)]);
    let compacted = compact(&compacting(&summarizer, 2), &history, usage)
        .await
        .unwrap();
    assert_eq!(compacted.iter().filter(|m| is_summary(m)).count(), 1);
    let prompt = &summarizer.prompts()[0];
    assert!(prompt.contains("Prior context."));
    assert!(!prompt.contains("[user]: “[Conversation summary"));
}

// Python: TestCompactBetweenRounds::test_the_wrapped_memory_is_not_asked
#[tokio::test]
async fn the_wrapped_memory_is_not_asked_to_compact() {
    struct Refuses;
    impl MemoryProvider for Refuses {
        fn retrieve<'a>(
            &'a self,
            _room_id: &'a RoomId,
            _current_event: &'a RoomEvent,
            _context: &'a RoomContext,
            _options: RetrieveOptions,
        ) -> Pin<Box<dyn Future<Output = Result<MemoryResult, ChannelError>> + Send + 'a>> {
            Box::pin(std::future::ready(Ok(MemoryResult::default())))
        }

        fn compact<'a>(
            &'a self,
            _room_id: &'a RoomId,
            _history: &'a [AiMessage],
            _usage: &'a Metadata,
            _options: CompactOptions,
        ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<AiMessage>>, ChannelError>> + Send + 'a>>
        {
            Box::pin(std::future::ready(Err("asked".into())))
        }
    }
    let summarizer = Summarizer::answering(&["Summary."]);
    let memory = SummarizingMemory::new(
        Arc::new(Refuses),
        summarizer,
        1000,
        SummarizingMemoryOptions::default(),
    );
    let usage = usage(&[("input_tokens", 10)]);
    let compacted = memory
        .compact(&room(), &turns(8), &usage, CompactOptions::default())
        .await;
    assert!(matches!(compacted, Ok(None)));
}
