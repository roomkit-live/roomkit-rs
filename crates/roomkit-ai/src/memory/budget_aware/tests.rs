//! Python: `tests/test_memory_new.py::TestBudgetAwareMemory`,
//! `tests/test_memory_wrappers.py`, `tests/test_turn_footprint.py` and
//! `tests/test_channels/test_memory_notes.py`, for the budget-aware memory.

use pretty_assertions::assert_eq;
use roomkit::model::{ChannelId, ChannelType, EventContent, EventSource, Room, TextContent};

use super::*;
use crate::memory::footprint::{TurnFootprint, measured};
use crate::memory::token_estimator::{estimate_message_tokens, estimate_notes_tokens};
use crate::memory::{
    MockMemoryProvider, MockMemoryProviderOptions, SlidingWindowMemory, SlidingWindowMemoryOptions,
};
use crate::provider::{AiMessage, AiRole};

fn room() -> RoomId {
    RoomId::new("r1").unwrap()
}

fn event(body: &str) -> RoomEvent {
    RoomEvent::new(
        room(),
        EventSource::new(ChannelId::new("sms1").unwrap(), ChannelType::SMS),
        EventContent::Text(TextContent::new(body)),
    )
}

fn events(count: usize, body: &str) -> Vec<RoomEvent> {
    (0..count).map(|_| event(body)).collect()
}

fn mock(options: MockMemoryProviderOptions) -> Arc<MockMemoryProvider> {
    Arc::new(MockMemoryProvider::new(options))
}

fn of(events: Vec<RoomEvent>) -> Arc<MockMemoryProvider> {
    mock(MockMemoryProviderOptions {
        events,
        ..MockMemoryProviderOptions::default()
    })
}

fn budget(inner: Arc<dyn MemoryProvider>, window: usize) -> BudgetAwareMemory {
    BudgetAwareMemory::new(inner, window, BudgetAwareMemoryOptions::default())
}

fn with(
    inner: Arc<dyn MemoryProvider>,
    window: usize,
    options: BudgetAwareMemoryOptions,
) -> BudgetAwareMemory {
    BudgetAwareMemory::new(inner, window, options)
}

async fn read(memory: &BudgetAwareMemory, current: &RoomEvent) -> MemoryResult {
    let context = RoomContext::new(Room::new(room()));
    memory
        .retrieve(&room(), current, &context, RetrieveOptions::default())
        .await
        .unwrap()
}

fn cost(events: &[RoomEvent]) -> usize {
    events
        .iter()
        .map(|event| estimate_event_tokens(event, EstimateEventTokensOptions::default()))
        .sum()
}

// Python: TestBudgetAwareMemory::test_a_returned_current_event_is_not_charged_twice
#[tokio::test]
async fn a_returned_current_event_is_not_charged_twice() {
    let before = event(&"a".repeat(100));
    let current = event(&"q".repeat(1200));
    let later = event(&"b".repeat(100));
    let inner = mock(MockMemoryProviderOptions {
        events: vec![before.clone(), current.clone(), later.clone()],
        notes: vec!["Keep this note.".to_owned()],
        ..MockMemoryProviderOptions::default()
    });
    let memory = with(
        inner,
        500,
        BudgetAwareMemoryOptions {
            safety_margin_ratio: 0.0,
            min_events: 0,
            ..BudgetAwareMemoryOptions::default()
        },
    );
    let result = read(&memory, &current).await;
    assert_eq!(result.events, vec![before, later]);
    assert_eq!(result.notes, vec!["Keep this note."]);
}

// Python: TestBudgetAwareMemory::test_returns_all_events_when_under_budget,
// test_empty_events and test_preserves_inner_messages
#[tokio::test]
async fn under_budget_it_hands_on_what_the_inner_memory_retrieved() {
    let summary = AiMessage::new(AiRole::System, "Previous summary");
    let inner = mock(MockMemoryProviderOptions {
        messages: vec![summary.clone()],
        events: (0..5).map(|n| event(&format!("msg{n}"))).collect(),
        ..MockMemoryProviderOptions::default()
    });
    let read_all = read(&budget(inner, 100_000), &event("current")).await;
    let empty = read(&budget(of(Vec::new()), 100_000), &event("current")).await;
    assert_eq!(
        (read_all.events.len(), read_all.messages, empty.events.len()),
        (5, vec![summary], 0)
    );
}

// Python: TestBudgetAwareMemory::test_trims_oldest_events_when_over_budget and
// test_no_reserve_and_no_messages_keeps_the_plain_window_budget
#[tokio::test]
async fn over_budget_it_keeps_the_latest_that_fit() {
    let mut events = events(10, &"x".repeat(1000));
    events.last_mut().unwrap().content =
        EventContent::Text(TextContent::new(format!("{}!", "x".repeat(999))));
    let last = events.last().cloned().unwrap();
    let kept = read(&budget(of(events), 1000), &event("current")).await;
    assert!(!kept.events.is_empty() && kept.events.len() < 10);
    assert!(cost(&kept.events) <= 850);
    assert_eq!(kept.events.last(), Some(&last));
}

// Python: TestBudgetAwareMemory::test_respects_min_events_floor
#[tokio::test]
async fn it_keeps_min_events_even_when_they_do_not_fit() {
    let kept = read(
        &budget(of(events(5, &"x".repeat(10_000))), 100),
        &event("current"),
    )
    .await;
    assert_eq!(kept.events.len(), 3);
}

// Python: TestBudgetAwareMemory::test_reserved_tokens_shrink_the_history_budget
// and test_injected_messages_count_against_the_budget
#[tokio::test]
async fn what_is_reserved_and_injected_is_not_the_history_s_to_spend() {
    let history = events(10, &"x".repeat(1000));
    let plain = read(&budget(of(history.clone()), 4000), &event("current")).await;
    let reserved = BudgetAwareMemoryOptions {
        reserved_tokens: 2000,
        ..BudgetAwareMemoryOptions::default()
    };
    let reserving = read(
        &with(of(history.clone()), 4000, reserved),
        &event("current"),
    )
    .await;
    let block = AiMessage::new(AiRole::User, "y".repeat(8000));
    let injecting = mock(MockMemoryProviderOptions {
        messages: vec![block.clone()],
        events: history,
        ..MockMemoryProviderOptions::default()
    });
    let injected = read(&budget(injecting, 4000), &event("current")).await;
    assert!(reserving.events.len() < plain.events.len());
    assert!(injected.events.len() < plain.events.len());
    // Not trimmed away: subtracted from the budget, then handed on.
    assert_eq!(injected.messages, vec![block]);
}

// Python: TestBudgetAwareMemory::test_result_fits_the_window_it_was_given
#[tokio::test]
async fn the_result_fits_the_window_it_was_given() {
    let block = AiMessage::new(AiRole::User, "y".repeat(4000));
    let inner = mock(MockMemoryProviderOptions {
        messages: vec![block],
        events: events(40, &"x".repeat(2000)),
        ..MockMemoryProviderOptions::default()
    });
    let options = BudgetAwareMemoryOptions {
        reserved_tokens: 25_000,
        min_events: 1,
        ..BudgetAwareMemoryOptions::default()
    };
    let kept = read(&with(inner, 40_000, options), &event("current")).await;
    let carried = 25_000
        + kept
            .messages
            .iter()
            .map(estimate_message_tokens)
            .sum::<usize>()
        + cost(&kept.events);
    assert!(carried <= 34_000);
}

// Python: TestBudgetAwareMemory::test_safety_margin_reduces_effective_budget
#[tokio::test]
async fn the_safety_margin_reduces_the_budget() {
    let kept = read(
        &budget(of(events(10, &"x".repeat(4000))), 10_000),
        &event("current"),
    )
    .await;
    // 8,500 tokens less the current event's 2: 8 events of 1,001.
    assert_eq!(kept.events.len(), 8);
}

// Python: TestBudgetAwareMemory::test_close_closes_inner, test_name_includes_inner,
// tests/test_memory_wrappers.py::test_lifecycle_reaches_inner and
// test_nested_wrappers_preserve_history_window (budget)
#[tokio::test]
async fn its_lifecycle_and_its_window_are_the_inner_memory_s() {
    let inner = mock(MockMemoryProviderOptions::default());
    let nested = budget(
        Arc::new(budget(Arc::<MockMemoryProvider>::clone(&inner), 1000)),
        1000,
    );
    let remembered = event("remember");
    let options = IngestOptions {
        channel_id: Some(ChannelId::new("ai").unwrap()),
    };
    nested.ingest(&room(), &remembered, options).await.unwrap();
    nested.clear(&room()).await.unwrap();
    nested.close().await;
    let window = SlidingWindowMemory::new(SlidingWindowMemoryOptions { max_events: 50 });
    let windowed = budget(Arc::new(budget(Arc::new(window), 1000)), 1000);
    assert_eq!(
        (
            inner.ingest_calls(),
            inner.clear_calls(),
            inner.closed(),
            nested.name(),
            windowed.recent_events_window(),
        ),
        (
            vec![(room(), remembered, Some(ChannelId::new("ai").unwrap()))],
            vec![room()],
            true,
            "BudgetAwareMemory(BudgetAwareMemory(MockMemoryProvider))".to_owned(),
            50,
        )
    );
}

/// A current event that alone exceeds a known window fails the read, not
/// retryable, its context overflowed; a window of 0 is not known.
#[tokio::test]
async fn a_current_event_past_the_window_fails_the_read() {
    let memory = budget(of(Vec::new()), 10);
    let context = RoomContext::new(Room::new(room()));
    let long = event(&"x".repeat(100));
    let error = memory
        .retrieve(&room(), &long, &context, RetrieveOptions::default())
        .await
        .unwrap_err();
    let error = error.downcast_ref::<ProviderError>().unwrap();
    assert_eq!(
        (error.context_overflow, error.retryable),
        (Some(true), false)
    );
    let unknown = read(&budget(of(Vec::new()), 0), &long).await;
    assert_eq!(unknown.events, Vec::new());
}

// Python: tests/test_turn_footprint.py::test_a_budget_aware_memory_reserves_at_least_the_measured_footprint
#[tokio::test]
async fn it_reserves_at_least_the_measured_footprint() {
    let history = events(30, &"history ".repeat(200));
    let current = event("now");
    let kept = |reserved: usize, footprint: Option<TurnFootprint>| {
        let memory = with(
            of(history.clone()),
            20_000,
            BudgetAwareMemoryOptions {
                reserved_tokens: reserved,
                ..BudgetAwareMemoryOptions::default()
            },
        );
        let current = current.clone();
        async move {
            match footprint {
                Some(footprint) => measured(footprint, read(&memory, &current)).await,
                None => read(&memory, &current).await,
            }
            .events
            .len()
        }
    };
    let unmeasured = kept(0, None).await;
    let measured_input = kept(0, Some(TurnFootprint::new(12_000, 0))).await;
    let declared_larger = kept(12_000, Some(TurnFootprint::new(2000, 0))).await;
    assert!(measured_input < unmeasured);
    assert_eq!(declared_larger, measured_input);
}

// Python: tests/test_turn_footprint.py::test_the_reply_is_reserved_once_the_larger_of_the_margin_and_its_budget
#[tokio::test]
async fn the_reply_is_reserved_once_the_larger_of_the_margin_and_its_budget() {
    let window = SlidingWindowMemory::new(SlidingWindowMemoryOptions { max_events: 100 });
    let memory = budget(Arc::new(window), 20_000);
    let mut recent_events = events(80, &"history ".repeat(200));
    let current = event("now");
    recent_events.push(current.clone());
    let context = RoomContext {
        recent_events,
        ..RoomContext::new(Room::new(room()))
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
    let within_margin = kept(2000).await;
    let over_margin = kept(9000).await;
    assert_eq!(within_margin, kept(0).await);
    assert!(over_margin < within_margin);
}

// Python: tests/test_channels/test_memory_notes.py::test_a_memory_that_wraps_another_carries_its_notes
// and test_a_budget_pays_for_the_notes_it_carries (budget_aware)
#[tokio::test]
async fn it_carries_the_inner_notes_and_pays_for_them() {
    let notes = vec!["passage ".repeat(6000)];
    let inner = mock(MockMemoryProviderOptions {
        events: events(40, &"x".repeat(4000)),
        notes: notes.clone(),
        ..MockMemoryProviderOptions::default()
    });
    let options = BudgetAwareMemoryOptions {
        min_events: 1,
        ..BudgetAwareMemoryOptions::default()
    };
    let kept = read(&with(inner, 40_000, options), &event("q")).await;
    let carried = kept
        .messages
        .iter()
        .map(estimate_message_tokens)
        .sum::<usize>()
        + cost(&kept.events)
        + estimate_notes_tokens(&kept.notes);
    assert_eq!(kept.notes, notes);
    assert!(kept.events.len() < 40);
    assert!(carried <= 40_000);
}
