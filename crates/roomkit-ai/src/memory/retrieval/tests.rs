//! Python: `tests/test_retrieval_memory.py`, `tests/test_memory_wrappers.py`
//! (retrieval) and `tests/test_channels/test_memory_notes.py`.

use std::future::{self, Future};
use std::pin::Pin;

use pretty_assertions::assert_eq;
use roomkit::model::{
    AudioContent, CompositeContent, Latitude, LocationContent, Longitude, TextContent,
};

use super::*;
use crate::knowledge::{MockKnowledgeSource, MockKnowledgeSourceOptions};
use crate::memory::testing::{context, event, room};
use crate::memory::{
    MockMemoryProvider, MockMemoryProviderOptions, SlidingWindowMemory, SlidingWindowMemoryOptions,
};

/// A source whose every call fails.
struct Broken;

impl KnowledgeSource for Broken {
    fn search<'a>(
        &'a self,
        _query: &'a str,
        _options: SearchOptions,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<KnowledgeResult>, ChannelError>> + Send + 'a>> {
        Box::pin(future::ready(Err("search down".into())))
    }

    fn index<'a>(
        &'a self,
        _content: &'a str,
        _options: IndexOptions,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChannelError>> + Send + 'a>> {
        Box::pin(future::ready(Err("index down".into())))
    }
}

fn passage(content: &str, score: f64, source: &str) -> KnowledgeResult {
    KnowledgeResult {
        score,
        source: source.to_owned(),
        ..KnowledgeResult::new(content)
    }
}

fn inner(events: Vec<RoomEvent>) -> Arc<MockMemoryProvider> {
    Arc::new(MockMemoryProvider::new(MockMemoryProviderOptions {
        events,
        ..MockMemoryProviderOptions::default()
    }))
}

fn source(results: Vec<KnowledgeResult>) -> Arc<MockKnowledgeSource> {
    Arc::new(MockKnowledgeSource::new(MockKnowledgeSourceOptions {
        results,
    }))
}

fn memory(
    sources: Vec<Arc<dyn KnowledgeSource>>,
    options: RetrievalMemoryOptions,
) -> RetrievalMemory {
    RetrievalMemory::new(sources, inner(vec![event("past", "e0")]), options)
}

async fn read(memory: &RetrievalMemory, question: &str) -> MemoryResult {
    memory
        .retrieve(
            &room(),
            &event(question, "now"),
            &context(),
            RetrieveOptions::default(),
        )
        .await
        .unwrap()
}

// Python: TestRetrievalMemoryInit::test_name_includes_inner,
// TestRetrieve::test_returns_inner_when_no_sources, test_returns_inner_when_query_too_short,
// test_returns_inner_when_no_results and test_all_sources_fail_returns_inner
#[tokio::test]
async fn without_a_passage_it_hands_on_the_inner_result() {
    let none = memory(Vec::new(), RetrievalMemoryOptions::default());
    let short = memory(
        vec![source(vec![passage("fact", 1.0, "")])],
        RetrievalMemoryOptions {
            min_query_length: 5,
            ..RetrievalMemoryOptions::default()
        },
    );
    let empty = memory(vec![source(Vec::new())], RetrievalMemoryOptions::default());
    let failing = memory(vec![Arc::new(Broken)], RetrievalMemoryOptions::default());
    for memory in [&none, &short, &empty, &failing] {
        let result = read(memory, "Hi?").await;
        assert_eq!(
            (
                result.events.len(),
                result.messages.len(),
                result.notes.len()
            ),
            (1, 0, 0)
        );
    }
    assert_eq!(none.name(), "RetrievalMemory(MockMemoryProvider)");
}

// Python: TestRetrieve::test_the_passages_are_the_turns_note_each_set_apart and
// TestFormatResults::test_formats_with_source_prefix, test_formats_without_source_prefix
#[tokio::test]
async fn the_passages_are_the_turn_s_note_each_set_apart() {
    let handbook = source(vec![
        passage("Relevant fact 1", 0.9, "faq"),
        passage("Relevant fact 2", 0.8, ""),
    ]);
    let result = read(
        &memory(
            vec![Arc::<MockKnowledgeSource>::clone(&handbook)],
            RetrievalMemoryOptions::default(),
        ),
        "What is the policy?",
    )
    .await;
    assert_eq!(result.messages, Vec::new());
    assert_eq!(
        result.notes,
        [
            "Relevant context from knowledge sources:\n<knowledge>\n[faq] Relevant fact 1\n</knowledge>\n<knowledge>\nRelevant fact 2\n</knowledge>"
        ]
    );
    assert_eq!(
        handbook.asked().searched,
        [("What is the policy?".to_owned(), Some(room()), 5)]
    );
}

// Python: TestRetrieve::test_deduplicates_results_by_content, test_respects_max_results,
// test_tolerates_source_failure and test_sorts_by_score_descending
#[tokio::test]
async fn the_best_passages_once_each_the_best_first_a_failing_source_left_out() {
    let first = source(vec![
        passage("low", 0.1, ""),
        passage("same content", 0.5, "a"),
        passage("mid", 0.5, ""),
    ]);
    let second = source(vec![
        passage("same content", 0.9, "b"),
        passage("high", 0.95, ""),
    ]);
    let options = RetrievalMemoryOptions {
        max_results: 3,
        ..RetrievalMemoryOptions::default()
    };
    let result = read(
        &memory(vec![first, Arc::new(Broken), second], options),
        "a long question",
    )
    .await;
    assert_eq!(
        result.notes,
        [
            "Relevant context from knowledge sources:\n<knowledge>\nhigh\n</knowledge>\n<knowledge>\n[b] same content\n</knowledge>\n<knowledge>\nmid\n</knowledge>"
        ]
    );
}

// Python: TestExtractQuery::test_text_content, test_composite_content and
// test_non_text_content_returns_empty
#[test]
fn a_query_is_the_author_s_words() {
    let composite = CompositeContent::new(vec![
        TextContent::new("part 1").into(),
        AudioContent {
            transcript: Some("part 2".to_owned()),
            ..AudioContent::new("https://example.com/a.ogg").unwrap()
        }
        .into(),
    ])
    .unwrap();
    let location =
        LocationContent::new(Latitude::new(45.5).unwrap(), Longitude::new(-73.5).unwrap());
    assert_eq!(
        [
            words(&TextContent::new("hello world").into()),
            words(&composite.into()),
            words(&location.into()),
        ],
        ["hello world", "part 1\npart 2", ""]
    );
}

// Python: TestIngest::test_delegates_to_inner, test_indexes_text_in_all_sources,
// test_tolerates_index_failure and test_skips_indexing_for_non_text
#[tokio::test]
async fn an_event_learnt_is_handed_on_then_indexed_in_every_source() {
    let (first, second) = (source(Vec::new()), source(Vec::new()));
    let inner = inner(Vec::new());
    let memory = RetrievalMemory::new(
        vec![
            Arc::<MockKnowledgeSource>::clone(&first),
            Arc::new(Broken),
            Arc::<MockKnowledgeSource>::clone(&second),
        ],
        Arc::<MockMemoryProvider>::clone(&inner),
        RetrievalMemoryOptions::default(),
    );
    let learnt = event("index me", "e1");
    memory
        .ingest(&room(), &learnt, IngestOptions::default())
        .await
        .unwrap();
    let location = RoomEvent {
        content: LocationContent::new(Latitude::new(1.0).unwrap(), Longitude::new(2.0).unwrap())
            .into(),
        ..event("", "e2")
    };
    memory
        .ingest(&room(), &location, IngestOptions::default())
        .await
        .unwrap();
    let indexed = first.asked().indexed.clone();
    assert_eq!(inner.ingest_calls().len(), 2);
    assert_eq!(indexed.len(), 1);
    let (text, metadata) = &indexed[0];
    let metadata = metadata.clone().unwrap();
    assert_eq!(
        (
            text.as_str(),
            metadata["room_id"].as_str(),
            metadata["event_id"].as_str()
        ),
        ("index me", Some("room-1"), Some("e1"))
    );
    assert_eq!(second.asked().indexed.len(), 1);
}

// Python: TestLifecycle::test_clear_delegates_to_inner, test_close_delegates_to_inner_and_sources,
// test_close_tolerates_source_failure, and tests/test_memory_wrappers.py (retrieval)
#[tokio::test]
async fn its_lifecycle_reaches_the_inner_memory_and_every_source() {
    let handbook = source(Vec::new());
    let inner = inner(Vec::new());
    let memory = RetrievalMemory::new(
        vec![
            Arc::new(Broken),
            Arc::<MockKnowledgeSource>::clone(&handbook),
        ],
        Arc::<MockMemoryProvider>::clone(&inner),
        RetrievalMemoryOptions::default(),
    );
    memory.clear(&room()).await.unwrap();
    memory.close().await;
    let window = SlidingWindowMemory::new(SlidingWindowMemoryOptions { max_events: 50 });
    let nested = RetrievalMemory::new(
        Vec::new(),
        Arc::new(RetrievalMemory::new(
            Vec::new(),
            Arc::new(window),
            RetrievalMemoryOptions::default(),
        )),
        RetrievalMemoryOptions::default(),
    );
    assert_eq!(
        (
            inner.clear_calls(),
            inner.closed(),
            handbook.asked().closed,
            nested.recent_events_window()
        ),
        (vec![room()], true, true, 50)
    );
}

// Python: tests/test_channels/test_memory_notes.py::test_retrieval_and_handoff_keep_the_inner_notes
#[tokio::test]
async fn it_keeps_the_inner_notes_before_its_own() {
    let inner = Arc::new(MockMemoryProvider::new(MockMemoryProviderOptions {
        notes: vec!["NOTE".to_owned()],
        ..MockMemoryProviderOptions::default()
    }));
    let memory = RetrievalMemory::new(
        vec![source(vec![passage("a passage", 1.0, "handbook")])],
        inner,
        RetrievalMemoryOptions::default(),
    );
    let notes = read(&memory, "what about returns?").await.notes;
    assert_eq!(notes.len(), 2);
    assert_eq!(notes[0], "NOTE");
    assert!(notes[1].contains("<knowledge>"));
}
