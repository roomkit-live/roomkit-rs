//! Python's `tests/test_knowledge_postgres.py`, on a real database: a document
//! indexed, then found. Each test skips itself without `POSTGRES_DSN`.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod support;

use std::sync::Arc;

use deadpool_postgres::{Manager, Pool};
use pretty_assertions::assert_eq;
use roomkit::model::{ChannelId, ChannelType, InboundMessage, Metadata, RoomId, TextContent};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, ChannelError, CreateRoomOptions, ProcessInboundOptions, RoomKit,
    RoomKitOptions, StoreError,
};
use roomkit_ai::knowledge::{IndexOptions, KnowledgeSource, SearchOptions};
use roomkit_ai::memory::{
    RetrievalMemory, RetrievalMemoryOptions, SlidingWindowMemory, SlidingWindowMemoryOptions,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions};
use roomkit_postgres::{PostgresKnowledgeSource, PostgresKnowledgeSourceOptions};
use serde_json::{Value, json};
use tokio_postgres::NoTls;

use crate::support::{SMALL_POOL, TempSchema, dsn, full_text};

fn metadata(value: Value) -> Metadata {
    let Value::Object(metadata) = value else {
        panic!("not an object");
    };
    metadata
}

fn of_schema(schema: &TempSchema, language: &str) -> PostgresKnowledgeSource {
    PostgresKnowledgeSource::new(PostgresKnowledgeSourceOptions {
        dsn: Some(schema.dsn()),
        language: language.to_owned(),
        ..PostgresKnowledgeSourceOptions::default()
    })
}

/// A schema and a source on it, initialized, or `None` without `POSTGRES_DSN`.
async fn fresh(language: &str) -> Option<(TempSchema, PostgresKnowledgeSource)> {
    dsn()?;
    let schema = TempSchema::new().await;
    let source = of_schema(&schema, language);
    source.init(SMALL_POOL).await.unwrap();
    Some((schema, source))
}

/// Python: `test_index_search_round_trip_on_real_pools`, its pool the source's,
/// the integrator's or a store's: the metadata comes back as it went, and is a
/// JSON object in the table.
#[tokio::test]
async fn an_index_and_a_search_round_trip_on_every_pool() {
    if dsn().is_none() {
        return;
    }
    for shared in [false, true] {
        let schema = TempSchema::new().await;
        let source = if shared {
            let config: tokio_postgres::Config = schema.dsn().parse().unwrap();
            let pool = Pool::builder(Manager::new(config, NoTls))
                .max_size(2)
                .build()
                .unwrap();
            PostgresKnowledgeSource::new(PostgresKnowledgeSourceOptions {
                pool: Some(pool),
                ..PostgresKnowledgeSourceOptions::default()
            })
        } else {
            of_schema(&schema, "english")
        };
        source.init(SMALL_POOL).await.unwrap();
        let held = metadata(json!({
            "id": "doc-1",
            "room_id": "r1",
            "nested": {"tags": ["faq", "café"]},
        }));
        source
            .index(
                "Refunds available within thirty days",
                IndexOptions {
                    metadata: Some(held.clone()),
                },
            )
            .await
            .unwrap();
        let found = source
            .search(
                "refund",
                SearchOptions {
                    room_id: Some(RoomId::new("r1").unwrap()),
                    ..SearchOptions::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].metadata, held);
        assert_eq!(found[0].source, "postgres");
        assert!(found[0].score > 0.0, "{found:?}");
        let kind: String = schema
            .client()
            .await
            .query_one(
                "SELECT jsonb_typeof(metadata) FROM knowledge_documents WHERE id = 'doc-1'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(kind, "object");
        source.close().await;
        if shared {
            // The pool stays the integrator's: the source still reads through it.
            let again = source.search("refund", SearchOptions::default()).await;
            assert_eq!(again.unwrap().len(), 1);
        }
    }
}

/// Python: `test_the_configured_language_stems_on_a_real_database`: `french` finds
/// "chevaux" when asked for "cheval", `english` does not. It also covers
/// `test_index_and_search_use_the_configured_language`, which reads a mock's
/// arguments: the language reaches the index and the search.
#[tokio::test]
async fn the_configured_language_stems_on_a_real_database() {
    for (language, stemmed) in [("french", true), ("english", false)] {
        let Some((_schema, source)) = fresh(language).await else {
            return;
        };
        source
            .index(
                "Les chevaux mangent dans le pré",
                IndexOptions {
                    metadata: Some(metadata(json!({"id": "doc-1", "room_id": "r1"}))),
                },
            )
            .await
            .unwrap();
        let found = source
            .search(
                "cheval",
                SearchOptions {
                    room_id: Some(RoomId::new("r1").unwrap()),
                    ..SearchOptions::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(!found.is_empty(), stemmed, "{language}");
    }
}

/// Python: `test_init_refuses_a_language_the_database_lacks`: the pool it created
/// is closed with the refusal.
#[tokio::test]
async fn init_refuses_a_language_the_database_lacks() {
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let source = of_schema(&schema, "klingon");
    let refused = source.init(SMALL_POOL).await.unwrap_err();
    assert_eq!(
        refused,
        StoreError::Value("PostgreSQL has no text search configuration 'klingon'".to_owned())
    );
    let closed: ChannelError = source
        .search("x", SearchOptions::default())
        .await
        .unwrap_err();
    assert!(
        closed
            .to_string()
            .contains("init() must be called before use"),
        "{closed}"
    );
}

/// Python's `index`: an id is the metadata's `event_id`, else its `id`, each
/// skipped when it reads as false, else a new one; indexing it again replaces the
/// text and what it found; a room and a source are the metadata's, and a document's
/// source its name when it has none.
#[tokio::test]
async fn a_document_is_named_and_replaced_as_python_s() {
    let Some((schema, source)) = fresh("english").await else {
        return;
    };
    let index = |content: &'static str, held: Value| {
        let source = source.clone();
        async move {
            source
                .index(
                    content,
                    IndexOptions {
                        metadata: Some(metadata(held)),
                    },
                )
                .await
        }
    };
    index("Refunds one", json!({"event_id": "e1", "id": "ignored"}))
        .await
        .unwrap();
    index("Refunds two", json!({"event_id": "", "id": "i1"}))
        .await
        .unwrap();
    index("Refunds three", json!({"id": 0, "source": "faq"}))
        .await
        .unwrap();
    source
        .index("Refunds four", IndexOptions::default())
        .await
        .unwrap();
    index("Refunds again", json!({"event_id": "e1", "room_id": "r9"}))
        .await
        .unwrap();
    let client = schema.client().await;
    let rows = client
        .query(
            "SELECT id, content, room_id, source FROM knowledge_documents ORDER BY content",
            &[],
        )
        .await
        .unwrap();
    let read: Vec<(String, String, Option<String>, String)> = rows
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
        .collect();
    assert_eq!(read.len(), 4, "{read:?}");
    // An id replaced keeps its room and its source, as Python's upsert does.
    assert_eq!(
        read[0],
        (
            "e1".to_owned(),
            "Refunds again".to_owned(),
            None,
            "postgres".to_owned()
        )
    );
    assert_eq!(read[1].0.len(), 32, "{:?}", read[1]);
    assert!(read[1].0.chars().all(|c| c.is_ascii_hexdigit()));
    assert_eq!(read[2].3, "faq");
    assert_eq!(read[3].0, "i1");
    let found = source
        .search("refund", SearchOptions::default())
        .await
        .unwrap();
    assert_eq!(found.len(), 4);
    assert!(
        index("x", json!({"id": 5})).await.is_err(),
        "an id that is no text is refused"
    );
    assert!(
        index("x", json!({"source": null})).await.is_err(),
        "a source null violates its column"
    );
}

/// Python's `name` and defaults, without a database.
#[test]
fn its_name_holds_the_source_name() {
    let source = PostgresKnowledgeSource::new(PostgresKnowledgeSourceOptions {
        source_name: "faq".to_owned(),
        ..PostgresKnowledgeSourceOptions::default()
    });
    assert_eq!(source.name(), "PostgresKnowledgeSource(faq)");
    let options = PostgresKnowledgeSourceOptions::default();
    assert_eq!(
        (options.source_name.as_str(), options.language.as_str()),
        ("postgres", "english")
    );
}

/// A search before `init` fails in Python's words, and no DSN is written by `Debug`.
#[tokio::test]
async fn an_operation_before_init_fails_and_says_so() {
    let options = PostgresKnowledgeSourceOptions {
        dsn: Some("postgresql://user:secret@127.0.0.1:1/none".to_owned()),
        ..PostgresKnowledgeSourceOptions::default()
    };
    assert!(!format!("{options:?}").contains("secret"));
    let source = PostgresKnowledgeSource::new(options);
    assert!(!format!("{source:?}").contains("secret"));
    let refused = source
        .search("x", SearchOptions::default())
        .await
        .unwrap_err();
    assert!(
        full_text(&*refused).contains("PostgresKnowledgeSource.init() must be called before use"),
        "{refused}"
    );
}

/// The way an integrator takes it: a kit on a `PostgresStore`, its agent's memory a
/// `RetrievalMemory` over this source on the same database. What a room's document
/// says reaches the model's request for a message in that room.
#[tokio::test]
async fn an_agent_reads_a_room_s_document_through_a_retrieval_memory() {
    let Some((schema, source)) = fresh("english").await else {
        return;
    };
    source
        .index(
            "Refunds are available within 30 days.",
            IndexOptions {
                metadata: Some(metadata(json!({"id": "doc-1", "room_id": "r1"}))),
            },
        )
        .await
        .unwrap();
    source
        .index(
            "Refunds of another room are never read.",
            IndexOptions {
                metadata: Some(metadata(json!({"id": "doc-2", "room_id": "r2"}))),
            },
        )
        .await
        .unwrap();
    let memory = RetrievalMemory::new(
        vec![Arc::new(source)],
        Arc::new(SlidingWindowMemory::new(
            SlidingWindowMemoryOptions::default(),
        )),
        RetrievalMemoryOptions::default(),
    );
    let model = ScriptedProvider::new([ScriptedRound::answering("Within 30 days.")]);
    let agent = AiChannel::new(
        ChannelId::new("assistant").unwrap(),
        model.clone(),
        AiChannelOptions {
            memory: Some(Arc::new(memory)),
            ..AiChannelOptions::default()
        },
    )
    .unwrap();
    let kit = RoomKit::new(RoomKitOptions::with_store(schema.initialized_store().await));
    let sms = RecordingTransport::new(
        ChannelId::new("sms").unwrap(),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    kit.register_channel(agent).unwrap();
    let room = RoomId::new("r1").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    })
    .await
    .unwrap();
    for id in ["sms", "assistant"] {
        kit.attach_channel(
            &room,
            &ChannelId::new(id).unwrap(),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    }
    kit.process_inbound(
        InboundMessage::new(
            ChannelId::new("sms").unwrap(),
            "+15550001",
            TextContent::new("Are refunds available?"),
        ),
        ProcessInboundOptions {
            room_id: Some(room),
            ..ProcessInboundOptions::default()
        },
    )
    .await
    .unwrap();
    let asked = format!("{:?}", model.requests().remove(0));
    assert!(
        asked.contains("Refunds are available within 30 days."),
        "{asked}"
    );
    assert!(!asked.contains("never read"), "{asked}");
    kit.close().await.unwrap();
}

/// Python's source and this one share the table: each finds what the other indexed,
/// when `ROOMKIT_PYTHON` names a roomkit checkout's interpreter.
#[tokio::test]
async fn python_s_source_and_this_one_read_each_other_s_documents() {
    let Ok(python) = std::env::var("ROOMKIT_PYTHON") else {
        return;
    };
    let Some((schema, source)) = fresh("english").await else {
        return;
    };
    source
        .index(
            "Refunds from Rust",
            IndexOptions {
                metadata: Some(metadata(json!({"id": "rs", "nested": {"tag": "café"}}))),
            },
        )
        .await
        .unwrap();
    let script = r#"
import asyncio, json, sys
from roomkit.knowledge import PostgresKnowledgeSource

async def main():
    source = PostgresKnowledgeSource(dsn=sys.argv[1])
    await source.init(min_size=1, max_size=1)
    await source.index("Refunds from Python", {"id": "py", "nested": {"tag": "thé"}})
    found = await source.search("refund", limit=5)
    await source.close()
    print(json.dumps(sorted(
        ([r.content, r.source, r.metadata] for r in found), key=lambda r: r[0]
    )))

asyncio.run(main())
"#;
    let output = std::process::Command::new(python)
        .arg("-c")
        .arg(script)
        .arg(schema.dsn())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let python_read: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        python_read,
        json!([
            ["Refunds from Python", "postgres", {"id": "py", "nested": {"tag": "thé"}}],
            ["Refunds from Rust", "postgres", {"id": "rs", "nested": {"tag": "café"}}],
        ])
    );
    let mut found = source
        .search("refund", SearchOptions::default())
        .await
        .unwrap();
    found.sort_by(|one, two| one.content.cmp(&two.content));
    assert_eq!(found[0].content, "Refunds from Python");
    assert_eq!(
        found[0].metadata,
        metadata(json!({"id": "py", "nested": {"tag": "thé"}}))
    );
}
