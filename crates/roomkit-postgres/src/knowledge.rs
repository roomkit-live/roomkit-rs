//! Full-text keyword search as a knowledge source, on the table the Python
//! implementation's source keeps: Python's `roomkit.knowledge.postgres`.
//!
//! The statements are Python's, with one change the client forces: a language
//! reaches `regconfig` through `text`, which is what asyncpg sends for it.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use deadpool_postgres::{Object, Pool};
use roomkit::model::Metadata;
use roomkit::{ChannelError, StoreError};
use roomkit_ai::knowledge::{IndexOptions, KnowledgeResult, KnowledgeSource, SearchOptions};
use roomkit_core::ops::{Environment, SystemEnvironment};
use serde_json::Value;
use tokio_postgres::Row;
use tokio_postgres::error::SqlState;

use crate::connect::Tls;
use crate::error::Failure;
use crate::lent::{Lent, cancellable};
use crate::options::{InitOptions, PostgresKnowledgeSourceOptions};
use crate::pool;
use crate::values::{execute, query, query_opt};

/// Python's `_SCHEMA`, byte for byte.
const SCHEMA: &str = include_str!("knowledge_schema.sql");

const INSERT: &str = "
INSERT INTO knowledge_documents (id, content, room_id, source, metadata, tsv)
VALUES ($1, $2, $3, $4, $5::text::jsonb, to_tsvector($6::text::regconfig, $2))
ON CONFLICT (id) DO UPDATE
    SET content = EXCLUDED.content,
        metadata = EXCLUDED.metadata,
        tsv = to_tsvector($6::text::regconfig, EXCLUDED.content);
";

const SEARCH_ALL: &str = "
SELECT content, source, metadata::text AS metadata,
       ts_rank_cd(tsv, query) AS score
FROM knowledge_documents, plainto_tsquery($2::text::regconfig, $1) query
WHERE tsv @@ query
ORDER BY score DESC
LIMIT $3;
";

const SEARCH_ROOM: &str = "
SELECT content, source, metadata::text AS metadata,
       ts_rank_cd(tsv, query) AS score
FROM knowledge_documents, plainto_tsquery($2::text::regconfig, $1) query
WHERE tsv @@ query AND room_id = $4
ORDER BY score DESC
LIMIT $3;
";

/// What an operation before [`PostgresKnowledgeSource::init`], or after the
/// close of a pool the source owned, fails with, in Python's words.
const NOT_INITIALIZED: &str = "PostgresKnowledgeSource.init() must be called before use";

/// How long an operation waits for a connection: Python's `_acquire_timeout`.
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a close waits for the connections in use: the wait after which
/// asyncpg's `Pool.close` warns. Past it, a close warns and returns, as a caller's
/// unfinished future holding a connection would never let it end; each such
/// connection closes when it comes back.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(60);

/// A full-text keyword search in a Postgres database: Python's
/// `PostgresKnowledgeSource`, on the same table, so that processes of both
/// implementations share its documents. It matches keywords through `tsvector`
/// (`plainto_tsquery`, ranked by `ts_rank_cd`); it is no semantic search. Its
/// clones share the pool.
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// use roomkit_ai::knowledge::{IndexOptions, KnowledgeSource, SearchOptions};
/// use roomkit_postgres::{
///     InitOptions, PostgresKnowledgeSource, PostgresKnowledgeSourceOptions,
/// };
///
/// let source = PostgresKnowledgeSource::new(PostgresKnowledgeSourceOptions {
///     dsn: Some("postgresql://localhost/mydb".to_owned()),
///     ..PostgresKnowledgeSourceOptions::default()
/// });
/// source.init(InitOptions::default()).await?;
/// source
///     .index("Refunds are available within 30 days.", IndexOptions::default())
///     .await?;
/// let found = source
///     .search("how do I get a refund?", SearchOptions::default())
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct PostgresKnowledgeSource {
    shared: Arc<Shared>,
}

struct Shared {
    dsn: Option<String>,
    pool: RwLock<Option<Pool>>,
    /// What a cancel request connects with, for a pool the source opened.
    tls: RwLock<Option<Tls>>,
    owns_pool: bool,
    source_name: String,
    language: String,
}

impl fmt::Debug for PostgresKnowledgeSource {
    /// Writes no DSN: one holds a password.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresKnowledgeSource")
            .field("owns_pool", &self.shared.owns_pool)
            .field("source_name", &self.shared.source_name)
            .field("language", &self.shared.language)
            .finish_non_exhaustive()
    }
}

impl PostgresKnowledgeSource {
    /// The source of `options.dsn`, or of `options.pool`, which stays the
    /// integrator's: no I/O happens here. [`init`](Self::init) must run before
    /// any other operation.
    pub fn new(options: PostgresKnowledgeSourceOptions) -> Self {
        let PostgresKnowledgeSourceOptions {
            dsn,
            pool,
            source_name,
            language,
        } = options;
        Self {
            shared: Arc::new(Shared {
                dsn,
                owns_pool: pool.is_none(),
                pool: RwLock::new(pool),
                tls: RwLock::new(None),
                source_name,
                language,
            }),
        }
    }

    /// Creates the connection pool if needed, and ensures the table exists:
    /// Python's `init(min_size=2, max_size=10)`.
    ///
    /// # Errors
    ///
    /// Fails [`StoreError::Value`] for a language the database has no text
    /// search configuration of, the pool it opened closed with the refusal, and
    /// for sizes or a connection string [`PostgresStore::init`](crate::PostgresStore::init)
    /// refuses so; [`StoreError::Unavailable`] for what else fails.
    pub async fn init(&self, options: InitOptions) -> Result<(), StoreError> {
        if self.pool().is_none() {
            let (pool, tls) = pool::open(
                self.shared.dsn.as_deref(),
                options.min_size,
                options.max_size,
            )
            .await?;
            *self
                .shared
                .tls
                .write()
                .unwrap_or_else(PoisonError::into_inner) = tls;
            *self
                .shared
                .pool
                .write()
                .unwrap_or_else(PoisonError::into_inner) = Some(pool);
        }
        let known = cancellable(async {
            let client = self.client().await?;
            client
                .batch_execute(SCHEMA)
                .await
                .map_err(StoreError::unavailable)?;
            self.language_known(&client).await
        })
        .await?;
        if known {
            return Ok(());
        }
        KnowledgeSource::close(self).await;
        Err(StoreError::Value(format!(
            "PostgreSQL has no text search configuration {}",
            repr(&self.shared.language)
        )))
    }

    /// Whether the database has the text search configuration of the language.
    async fn language_known(&self, client: &Object) -> Result<bool, StoreError> {
        let known = query_opt(
            client,
            "SELECT $1::text::regconfig::text",
            &[&self.shared.language],
        )
        .await;
        match known {
            Ok(_) => Ok(true),
            Err(Failure::Database(error)) if error.code() == Some(&SqlState::UNDEFINED_OBJECT) => {
                Ok(false)
            }
            Err(failure) => Err(failure.into()),
        }
    }

    /// The pool, once [`init`](Self::init) made it or if the integrator gave one.
    fn pool(&self) -> Option<Pool> {
        self.shared
            .pool
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// A connection of the pool, waited for at most [`ACQUIRE_TIMEOUT`], lent to
    /// the operation running: one its caller cuts has its statement cancelled on
    /// the server, as asyncpg cancels it (HITL 3ebe4596).
    async fn client(&self) -> Result<Lent, StoreError> {
        let Some(pool) = self.pool() else {
            return Err(StoreError::unavailable(NOT_INITIALIZED));
        };
        let tls = self
            .shared
            .tls
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        match tokio::time::timeout(ACQUIRE_TIMEOUT, pool.get()).await {
            Ok(client) => Ok(Lent::new(client.map_err(StoreError::unavailable)?, tls)),
            Err(_) => Err(StoreError::unavailable(
                "no connection of the pool came free in time",
            )),
        }
    }

    /// Closes the pool if the source opened it, waiting at most `timeout`. The slot is
    /// cleared of the pool it closed only: a close that started earlier and ends later
    /// leaves the pool an `init` opened since.
    async fn close_within(&self, timeout: Duration) {
        if !self.shared.owns_pool {
            return;
        }
        let Some(pool) = self.pool() else {
            return;
        };
        pool::close_own_pool(&pool, timeout, "PostgresKnowledgeSource").await;
        self.clear_a_closed_pool();
    }

    /// Clears the slot of a closed pool, never of one an `init` opened since.
    fn clear_a_closed_pool(&self) {
        let mut slot = self
            .shared
            .pool
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        if slot.as_ref().is_some_and(Pool::is_closed) {
            *slot = None;
        }
    }

    /// The passages `query` finds, the room's only if it names one.
    async fn found(
        &self,
        query_text: &str,
        options: SearchOptions,
    ) -> Result<Vec<KnowledgeResult>, ChannelError> {
        let client = self.client().await?;
        let limit = i64::try_from(options.limit).unwrap_or(i64::MAX);
        let rows = match &options.room_id {
            Some(room_id) => {
                query(
                    &*client,
                    SEARCH_ROOM,
                    &[
                        &query_text,
                        &self.shared.language,
                        &limit,
                        &room_id.as_str(),
                    ],
                )
                .await
            }
            None => {
                query(
                    &*client,
                    SEARCH_ALL,
                    &[&query_text, &self.shared.language, &limit],
                )
                .await
            }
        }
        .map_err(StoreError::from)?;
        rows.iter().map(|row| self.result(row)).collect()
    }

    /// The passage a row holds, under the source's name if it was indexed with
    /// no source.
    fn result(&self, row: &Row) -> Result<KnowledgeResult, ChannelError> {
        let source: &str = row.try_get("source")?;
        let metadata: &str = row.try_get("metadata")?;
        // `index` writes an object: another form is a row written directly, and a
        // passage has only an object to keep.
        let metadata = match serde_json::from_str(metadata)? {
            Value::Object(metadata) => metadata,
            Value::Null
            | Value::Bool(_)
            | Value::Number(_)
            | Value::String(_)
            | Value::Array(_) => Metadata::new(),
        };
        Ok(KnowledgeResult {
            content: row.try_get("content")?,
            score: f64::from(row.try_get::<_, f32>("score")?),
            source: if source.is_empty() {
                self.shared.source_name.clone()
            } else {
                source.to_owned()
            },
            metadata,
        })
    }

    /// Indexes `content` under the id its metadata names, or a new one.
    async fn indexed(&self, content: &str, options: IndexOptions) -> Result<(), ChannelError> {
        let client = self.client().await?;
        let metadata = options.metadata.unwrap_or_default();
        let id = match document_id(&metadata) {
            Some(named) => text("an id", named)?.to_owned(),
            None => SystemEnvironment.new_event_id().as_str().to_owned(),
        };
        let room_id = member(&metadata, "room_id")?;
        let source = match metadata.get("source") {
            None => Some(self.shared.source_name.as_str()),
            Some(_) => member(&metadata, "source")?,
        };
        let written = serde_json::to_string(&metadata)?;
        execute(
            &*client,
            INSERT,
            &[
                &id,
                &content,
                &room_id,
                &source,
                &written,
                &self.shared.language,
            ],
        )
        .await
        .map_err(StoreError::from)?;
        Ok(())
    }
}

/// `text` as Python's `repr` writes it: in single quotes unless it holds one and no
/// double quote, a backslash doubled.
fn repr(text: &str) -> String {
    let quote = if text.contains('\'') && !text.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut written = String::with_capacity(text.len() + 2);
    written.push(quote);
    for character in text.chars() {
        if character == '\\' || character == quote {
            written.push('\\');
        }
        written.push(character);
    }
    written.push(quote);
    written
}

/// The member of `metadata` that names a document: `event_id`, else `id`, each
/// skipped when Python reads it as false.
fn document_id(metadata: &Metadata) -> Option<&Value> {
    ["event_id", "id"]
        .into_iter()
        .filter_map(|key| metadata.get(key))
        .find(|value| reads_true(value))
}

/// Whether Python reads `value` as true.
fn reads_true(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64() != Some(0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(value) => !value.is_empty(),
        Value::Object(value) => !value.is_empty(),
    }
}

/// `value` as text: a column takes nothing else, and asyncpg refuses it too.
fn text<'a>(what: &str, value: &'a Value) -> Result<&'a str, ChannelError> {
    value
        .as_str()
        .ok_or_else(|| format!("{what} of a document must be a text").into())
}

/// The text under `key`: none for a member missing or null.
fn member<'a>(metadata: &'a Metadata, key: &str) -> Result<Option<&'a str>, ChannelError> {
    match metadata.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => text(key, value).map(Some),
    }
}

impl KnowledgeSource for PostgresKnowledgeSource {
    /// Python's `name`: `PostgresKnowledgeSource(<source_name>)`.
    fn name(&self) -> String {
        format!("PostgresKnowledgeSource({})", self.shared.source_name)
    }

    fn search<'a>(
        &'a self,
        query: &'a str,
        options: SearchOptions,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<KnowledgeResult>, ChannelError>> + Send + 'a>> {
        Box::pin(cancellable(self.found(query, options)))
    }

    fn index<'a>(
        &'a self,
        content: &'a str,
        options: IndexOptions,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChannelError>> + Send + 'a>> {
        Box::pin(cancellable(self.indexed(content, options)))
    }

    /// Closes the pool if the source opened it, once the connections in use came
    /// back, at most 60 s later. Meanwhile the source refuses any other use, its pool
    /// closed, and a second close waits too, as Python's.
    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(self.close_within(CLOSE_TIMEOUT))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A close that ends after a later `init` leaves the pool that `init` opened.
    #[test]
    fn a_late_close_leaves_a_pool_opened_since() {
        let config: tokio_postgres::Config =
            "postgresql://nobody@127.0.0.1:1/none".parse().unwrap();
        let opened = Pool::builder(deadpool_postgres::Manager::new(
            config,
            tokio_postgres::NoTls,
        ))
        .max_size(1)
        .runtime(deadpool_postgres::Runtime::Tokio1)
        .build()
        .unwrap();
        let source = PostgresKnowledgeSource::new(PostgresKnowledgeSourceOptions::default());
        *source.shared.pool.write().unwrap() = Some(opened.clone());
        source.clear_a_closed_pool();
        assert!(source.pool().is_some());
        opened.close();
        source.clear_a_closed_pool();
        assert!(source.pool().is_none());
    }

    /// A future kept and no longer polled holds its connection for ever: the close
    /// warns and returns, here within its short bound, and the pool's slot is free.
    #[tokio::test]
    async fn a_close_returns_though_a_connection_stays_lent() {
        let Ok(dsn) = std::env::var("POSTGRES_DSN") else {
            return;
        };
        let source = PostgresKnowledgeSource::new(PostgresKnowledgeSourceOptions {
            dsn: Some(dsn),
            ..PostgresKnowledgeSourceOptions::default()
        });
        source
            .init(InitOptions {
                min_size: 1,
                max_size: 1,
            })
            .await
            .unwrap();
        let kept = source.pool().unwrap().get().await.unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            source.close_within(Duration::from_millis(20)),
        )
        .await
        .unwrap();
        assert!(source.pool().is_none());
        drop(kept);
    }
}
