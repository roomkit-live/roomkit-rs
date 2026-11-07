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
use roomkit_ai::knowledge::{IndexOptions, KnowledgeResult, KnowledgeSource, SearchOptions};
use roomkit_ai::memory::{
    RetrievalMemory, RetrievalMemoryOptions, SlidingWindowMemory, SlidingWindowMemoryOptions,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions};
use roomkit_postgres::{
    PostgresKnowledgeSource, PostgresKnowledgeSourceOptions, PostgresStore, PostgresStoreOptions,
};
use serde_json::{Value, json};
use tokio_postgres::NoTls;

use crate::support::{SMALL_POOL, TempSchema, dsn, full_text, sessions_blocked_by, until_ended};

fn metadata(value: Value) -> Metadata {
    serde_json::from_value(value).unwrap()
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

/// The pool of `schema`, the integrator's.
fn pool_of(schema: &TempSchema) -> Pool {
    let config: tokio_postgres::Config = schema.dsn().parse().unwrap();
    Pool::builder(Manager::new(config, NoTls))
        .max_size(2)
        .build()
        .unwrap()
}

/// Python: `test_index_search_round_trip_on_real_pools`, its pool the source's, the
/// integrator's or one a store shares, and `test_search_decodes_jsonb`: the metadata
/// comes back as it went, and is a JSON object in the table. A pool handed in stays
/// its owner's: the source's close leaves it open.
#[tokio::test]
async fn an_index_and_a_search_round_trip_on_every_pool() {
    if dsn().is_none() {
        return;
    }
    for pool_kind in ["owned", "standard", "store"] {
        let schema = TempSchema::new().await;
        let handed = (pool_kind != "owned").then(|| pool_of(&schema));
        if pool_kind == "store" {
            let store = PostgresStore::new(PostgresStoreOptions {
                pool: handed.clone(),
                ..PostgresStoreOptions::default()
            });
            store.init(SMALL_POOL).await.unwrap();
        }
        let source = if let Some(pool) = handed {
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
            "nested": {"tags": ["faq", "caf\u{e9}"]},
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
        let found = in_room(&source, "refund", "r1").await;
        assert_eq!(found.len(), 1, "{pool_kind}");
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
        if pool_kind != "owned" {
            assert_eq!(
                in_room(&source, "refund", "r1").await.len(),
                1,
                "{pool_kind}"
            );
        }
    }
}

/// Python: `test_the_configured_language_stems_on_a_real_database` and
/// `test_index_and_search_use_the_configured_language`, which reads a mock's
/// arguments: the language reaches the index and the search. French stems "chevaux"
/// to "cheval" on both sides, English on neither.
#[tokio::test]
async fn the_configured_language_stems_on_a_real_database() {
    for (language, asked, found) in [
        ("french", "cheval", true),
        ("french", "chevaux", true),
        ("english", "cheval", false),
        ("english", "chevaux", true),
    ] {
        let Some((_schema, source)) = fresh(language).await else {
            return;
        };
        source
            .index(
                "Les chevaux mangent dans le pr\u{e9}",
                IndexOptions {
                    metadata: Some(metadata(json!({"id": "doc-1", "room_id": "r1"}))),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            !in_room(&source, asked, "r1").await.is_empty(),
            found,
            "{language}, {asked}"
        );
        assert_eq!(
            in_all(&source, asked).await > 0,
            found,
            "{language}, {asked}"
        );
        index(
            &source,
            "Les chevaux dorment encore",
            json!({"id": "doc-1", "room_id": "r1"}),
        )
        .await
        .unwrap();
        assert_eq!(
            in_all(&source, asked).await > 0,
            found,
            "again, {language}, {asked}"
        );
    }
}

/// Python: `test_init_refuses_a_language_the_database_lacks`: the pool it created is
/// closed with the refusal, and the table it made stays.
#[tokio::test]
async fn init_refuses_a_language_the_database_lacks() {
    if dsn().is_none() {
        return;
    }
    for (language, written) in [("klingon", "'klingon'"), ("it's", "\"it's\"")] {
        let schema = TempSchema::new().await;
        let source = of_schema(&schema, language);
        let refused = source.init(SMALL_POOL).await.unwrap_err();
        assert_eq!(
            refused,
            StoreError::Value(format!(
                "PostgreSQL has no text search configuration {written}"
            ))
        );
        let closed: ChannelError = source
            .search("x", SearchOptions::default())
            .await
            .unwrap_err();
        assert!(
            full_text(&*closed).contains("init() must be called before use"),
            "{closed}"
        );
        let client = schema.client().await;
        assert!(
            client
                .query_one("SELECT count(*) FROM knowledge_documents", &[])
                .await
                .is_ok()
        );
    }
}

/// `SCHEMA` is Python's `_SCHEMA`, byte for byte, when `ROOMKIT_PYTHON` names a
/// roomkit checkout's interpreter.
#[test]
fn the_table_is_python_s() {
    let Ok(python) = std::env::var("ROOMKIT_PYTHON") else {
        return;
    };
    let output = std::process::Command::new(python)
        .arg("-c")
        .arg(
            "from roomkit.knowledge.postgres import _SCHEMA; import sys; sys.stdout.write(_SCHEMA)",
        )
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        include_str!("../src/knowledge_schema.sql")
    );
}

/// The passages `asked` finds in the room `room`.
async fn in_room(
    source: &PostgresKnowledgeSource,
    asked: &str,
    room: &str,
) -> Vec<KnowledgeResult> {
    source
        .search(
            asked,
            SearchOptions {
                room_id: Some(RoomId::new(room).unwrap()),
                ..SearchOptions::default()
            },
        )
        .await
        .unwrap()
}

/// Indexes `content` under `held`.
async fn index(
    source: &PostgresKnowledgeSource,
    content: &str,
    held: Value,
) -> Result<(), ChannelError> {
    source
        .index(
            content,
            IndexOptions {
                metadata: Some(metadata(held)),
            },
        )
        .await
}

/// Python's `index`: an id is the metadata's `event_id`, else its `id`, each skipped
/// when it reads as false, else a new one's 32 hex digits. A room and a source are
/// the metadata's. What is no text is refused, a source null by its column.
#[tokio::test]
async fn a_document_is_named_as_python_s() {
    let Some((schema, source)) = fresh("english").await else {
        return;
    };
    index(&source, "Refunds e", json!({"event_id": "e1", "id": "no"}))
        .await
        .unwrap();
    index(&source, "Refunds i", json!({"event_id": "", "id": "i1"}))
        .await
        .unwrap();
    for (case, falsy) in [
        json!(0),
        json!(0.0),
        json!(false),
        json!(null),
        json!([]),
        json!({}),
        json!(""),
    ]
    .into_iter()
    .enumerate()
    {
        index(&source, &format!("Refunds {case}"), json!({"id": falsy}))
            .await
            .unwrap();
    }
    let client = schema.client().await;
    let id_of = async |content: &str| -> String {
        client
            .query_one(
                "SELECT id FROM knowledge_documents WHERE content = $1",
                &[&content],
            )
            .await
            .unwrap()
            .get(0)
    };
    assert_eq!(id_of("Refunds e").await, "e1");
    assert_eq!(id_of("Refunds i").await, "i1");
    for case in 0..7 {
        let id = id_of(&format!("Refunds {case}")).await;
        assert_eq!(id.len(), 32, "{id}");
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()), "{id}");
    }
    for refused in [
        json!({"id": 5}),
        json!({"event_id": [1]}),
        json!({"room_id": 5}),
        json!({"source": 5}),
        json!({"source": null}),
    ] {
        assert!(
            index(&source, "x", refused.clone()).await.is_err(),
            "{refused}"
        );
    }
    let written: i64 = client
        .query_one(
            "SELECT count(*) FROM knowledge_documents WHERE content = 'x'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(written, 0);
}

/// Python's upsert: a document indexed again takes its text, what the text finds and
/// its metadata; it keeps its room and its source.
#[tokio::test]
async fn a_document_indexed_again_is_replaced_as_python_s() {
    let Some((_schema, source)) = fresh("english").await else {
        return;
    };
    index(
        &source,
        "Refunds early",
        json!({"id": "d1", "room_id": "r1", "source": "faq", "v": 1}),
    )
    .await
    .unwrap();
    index(
        &source,
        "Exchanges later",
        json!({"id": "d1", "room_id": "r2", "source": "other", "v": 2}),
    )
    .await
    .unwrap();
    assert_eq!(in_room(&source, "refund", "r1").await.len(), 0);
    let found = in_room(&source, "exchange", "r1").await;
    assert_eq!(found[0].content, "Exchanges later");
    assert_eq!(found[0].source, "faq");
    assert_eq!(found[0].metadata["v"], json!(2));
}

/// Python: `test_a_cancelled_index_writes_nothing` (HITL 3ebe4596, RMK-1398): an
/// index its caller cuts while a lock holds the document is cancelled on the server,
/// as asyncpg cancels it: once the lock goes, the document is as it was. The call is
/// cut where its caller drops it, beside the wait that saw it blocked.
#[tokio::test]
async fn a_cancelled_index_writes_nothing() {
    let Some((schema, source)) = fresh("english").await else {
        return;
    };
    index(&source, "Refund before", json!({"id": "d1"}))
        .await
        .unwrap();
    let holder = schema.client().await;
    holder
        .batch_execute("BEGIN; SELECT 1 FROM knowledge_documents WHERE id = 'd1' FOR UPDATE")
        .await
        .unwrap();
    // Boxed, so that the drop below drops the call itself.
    let mut indexing = Box::pin(index(&source, "Refund cancelled", json!({"id": "d1"})));
    // Cut once the server shows the write waiting: one cut before proves nothing.
    let waiting = tokio::select! {
        _ = &mut indexing => panic!("the index ran while the document was held"),
        waiting = sessions_blocked_by(&schema, &holder) => waiting,
    };
    drop(indexing);
    // The cancel leaves once the call is dropped: the lock goes once the server ended
    // the statement, as Python's caller has its hand back only then.
    until_ended(&schema, &waiting).await;
    holder.batch_execute("COMMIT").await.unwrap();
    until_ended(&schema, &waiting).await;
    let content: String = holder
        .query_one(
            "SELECT content FROM knowledge_documents WHERE id = 'd1'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(content, "Refund before");
}

/// Python's `search`: the best first, at most its limit, a room's only when it names
/// one, a document with no source under the source's name.
#[tokio::test]
async fn a_search_ranks_limits_and_scopes_as_python_s() {
    let Some((_schema, source)) = fresh("english").await else {
        return;
    };
    index(
        &source,
        "refund",
        json!({"id": "a", "room_id": "r1", "source": ""}),
    )
    .await
    .unwrap();
    index(
        &source,
        "refund refund refund",
        json!({"id": "b", "room_id": "r1", "source": "faq"}),
    )
    .await
    .unwrap();
    index(
        &source,
        "refund twice refund",
        json!({"id": "c", "room_id": "r2"}),
    )
    .await
    .unwrap();
    let in_one = in_room(&source, "refund", "r1").await;
    assert_eq!(in_one.len(), 2);
    assert_eq!(in_one[0].content, "refund refund refund");
    assert_eq!(in_one[1].source, "postgres");
    assert!(in_one[0].score > in_one[1].score, "{in_one:?}");
    let everywhere = source
        .search("refund", SearchOptions::default())
        .await
        .unwrap();
    assert_eq!(everywhere.len(), 3);
    let first = source
        .search(
            "refund",
            SearchOptions {
                limit: 1,
                ..SearchOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].content, "refund refund refund");
    let first_of_room = source
        .search(
            "refund",
            SearchOptions {
                room_id: Some(RoomId::new("r1").unwrap()),
                limit: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(first_of_room.len(), 1);
    assert_eq!(first_of_room[0].content, "refund refund refund");
}

/// Python's `close`: it waits for a statement in flight, and until it ends the source
/// refuses another use; a second close changes nothing, and `init` opens the pool again.
#[tokio::test]
async fn a_close_waits_for_a_statement_in_flight() {
    let Some((schema, source)) = fresh("english").await else {
        return;
    };
    let holder = schema.client().await;
    holder
        .batch_execute("BEGIN; LOCK TABLE knowledge_documents IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let writing = tokio::spawn({
        let source = source.clone();
        async move { index(&source, "Refunds", json!({"id": "d1"})).await.is_ok() }
    });
    let pid: i32 = holder
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let watcher = schema.client().await;
    let mut blocked = 0_i64;
    for _ in 0..400 {
        blocked = watcher
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))",
                &[&pid],
            )
            .await
            .unwrap()
            .get(0);
        if blocked > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert_eq!(blocked, 1, "the write never waited on the lock");
    let closing = tokio::spawn({
        let source = source.clone();
        async move { source.close().await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!closing.is_finished(), "the close did not wait");
    assert!(
        source
            .search("refund", SearchOptions::default())
            .await
            .is_err(),
        "the source took a search while closing"
    );
    // Bounded: an init on a pool of its own would wait on this test's lock.
    let init = tokio::time::timeout(std::time::Duration::from_secs(1), source.init(SMALL_POOL));
    assert!(
        matches!(init.await, Ok(Err(_))),
        "the source took an init while closing"
    );
    holder.batch_execute("COMMIT").await.unwrap();
    assert!(writing.await.unwrap());
    closing.await.unwrap();
    source.close().await;
    source.init(SMALL_POOL).await.unwrap();
    assert_eq!(in_all(&source, "refund").await, 1);
    source.close().await;
}

/// How many passages `asked` finds in every room.
async fn in_all(source: &PostgresKnowledgeSource, asked: &str) -> usize {
    source
        .search(asked, SearchOptions::default())
        .await
        .unwrap()
        .len()
}

/// Python's `name`, without a database.
#[test]
fn its_name_holds_the_source_name() {
    let source = PostgresKnowledgeSource::new(PostgresKnowledgeSourceOptions {
        source_name: "faq".to_owned(),
        ..PostgresKnowledgeSourceOptions::default()
    });
    assert_eq!(source.name(), "PostgresKnowledgeSource(faq)");
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
    let early = index(&source, "x", json!({"id": 5})).await.unwrap_err();
    assert!(
        full_text(&*early).contains("init() must be called before use"),
        "{early}"
    );
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
            "Refunds are available in another room, and never read.",
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
import asyncio, json, os
from roomkit.knowledge import PostgresKnowledgeSource

async def main():
    source = PostgresKnowledgeSource(dsn=os.environ["KNOWLEDGE_DSN"])
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
        .env("KNOWLEDGE_DSN", schema.dsn())
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
