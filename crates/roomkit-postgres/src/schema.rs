//! The Python implementation's schema (`store/postgres_schema.py`): the
//! additive batch `init` runs at every connect, and the explicit, opt-in
//! maintenance beside it. The batch is Python's text, byte for byte.

use deadpool_postgres::Object;
use roomkit::model::Metadata;
use serde_json::{Value, json};

use crate::error::{Failure, PostgresSchemaError};
use crate::values::{execute, query, query_opt};

/// The additive, idempotent batch, as Python's `SCHEMA` builds it.
const SCHEMA: &str = include_str!("schema.sql");

/// The table layout's generation `migrate` reports.
const SCHEMA_VERSION: i32 = 2;

/// The advisory lock serializing schema migrations across processes:
/// Python's `_MIGRATION_LOCK_KEY`, "room" in ASCII, so that processes of both
/// implementations wait for each other.
const MIGRATION_LOCK_KEY: i64 = 0x726F_6F6D;

/// Takes [`MIGRATION_LOCK_KEY`] for the transaction.
const TAKE_MIGRATION_LOCK: &str = "SELECT pg_advisory_xact_lock($1)";

/// The tables of a version 1 schema, children before parents.
const V1_TABLES: [&str; 11] = [
    "read_markers",
    "observations",
    "tasks",
    "identity_addresses",
    "identities",
    "participants",
    "bindings",
    "event_sequences",
    "events",
    "rooms",
    "schema_version",
];

/// Whether the `rooms` table of the schema this session writes to is the
/// version 1 one, its record in a `data` column.
const V1_DETECT: &str = "\
SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = current_schema()
      AND table_name = 'rooms' AND column_name = 'data'
)
";

/// The definition of an index of the schema this session writes to.
const INDEX_DEF: &str =
    "SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() AND indexname = $1";

/// The statement dropping every table of [`V1_TABLES`] of the schema this
/// session writes to, never a namesake of another; the names are this
/// module's own.
fn v1_to_v2_drop() -> String {
    V1_TABLES
        .iter()
        .map(|table| {
            format!(
                "DO $$ BEGIN EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE', current_schema(), '{table}'); END $$;"
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The statement dropping the index `name`, one of this module's, of the
/// schema this session writes to.
fn drop_own_index(name: &str) -> String {
    format!(
        "DO $$ BEGIN EXECUTE format('DROP INDEX IF EXISTS %I.%I', current_schema(), '{name}'); END $$"
    )
}

/// A report as Python's maintenance methods return one, their `dict`.
fn report(value: Value) -> Metadata {
    match value {
        Value::Object(report) => report,
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Array(_) => {
            Metadata::new()
        }
    }
}

/// Readies the database: under the migration lock, in one transaction,
/// refuses a version 1 schema and runs the additive batch, then warns when
/// the events index could not be made unique (Python's `init`).
pub(crate) async fn prepare(client: &mut Object) -> Result<(), Failure> {
    let transaction = client.transaction().await?;
    execute(&transaction, TAKE_MIGRATION_LOCK, &[&MIGRATION_LOCK_KEY]).await?;
    if is_v1(&transaction).await? {
        return Err(PostgresSchemaError::new(
            "Legacy v1 (JSONB-blob) schema detected. init() will not modify it \u{2014} that \
             migration drops every table (data loss). Back up your database, then run \
             PostgresStore.migrate(dry_run=False, confirm=True).",
        )
        .into());
    }
    transaction.batch_execute(SCHEMA).await?;
    if let Some(definition) = index_definition(&transaction, "idx_events_room_index").await?
        && !definition.contains("UNIQUE")
    {
        tracing::warn!(
            target: "roomkit.store.postgres",
            "idx_events_room_index is not UNIQUE: duplicate (room_id, index) rows exist, so \
             multi-process event-index safety is NOT enforced. Deduplicate the events table, \
             then recreate the index UNIQUE."
        );
    }
    transaction.commit().await?;
    Ok(())
}

async fn is_v1(client: &deadpool_postgres::Transaction<'_>) -> Result<bool, Failure> {
    let row = query_opt(client, V1_DETECT, &[]).await?;
    Ok(row.map(|row| row.try_get(0)).transpose()?.unwrap_or(false))
}

async fn index_definition(
    client: &deadpool_postgres::Transaction<'_>,
    name: &str,
) -> Result<Option<String>, Failure> {
    let row = query_opt(client, INDEX_DEF, &[&name]).await?;
    Ok(row.map(|row| row.try_get(0)).transpose()?)
}

/// The explicit migration, Python's `migrate`: only `dry_run: false` with
/// `confirm` drops a version 1 schema's tables, irreversible; a version 2
/// schema takes the additive batch.
pub(crate) async fn migrate(
    client: &mut Object,
    dry_run: bool,
    confirm: bool,
) -> Result<Metadata, Failure> {
    let transaction = client.transaction().await?;
    execute(&transaction, TAKE_MIGRATION_LOCK, &[&MIGRATION_LOCK_KEY]).await?;
    if !is_v1(&transaction).await? {
        transaction.batch_execute(SCHEMA).await?;
        transaction.commit().await?;
        return Ok(report(json!({
            "detected_version": SCHEMA_VERSION,
            "action": "noop",
            "dropped_tables": [],
        })));
    }
    if dry_run {
        return Ok(report(json!({
            "detected_version": 1,
            "action": "dry_run",
            "dropped_tables": V1_TABLES,
        })));
    }
    if !confirm {
        return Err(PostgresSchemaError::new(format!(
            "v1 schema detected. Migrating to v2 DROPs all tables ({}) \u{2014} irreversible \
             data loss. Take a backup, then call migrate(dry_run=False, confirm=True).",
            V1_TABLES.join(", ")
        ))
        .into());
    }
    tracing::warn!(
        target: "roomkit.store.postgres",
        "Running DESTRUCTIVE v1->v2 migration: dropping tables {V1_TABLES:?}"
    );
    transaction.batch_execute(&v1_to_v2_drop()).await?;
    transaction.batch_execute(SCHEMA).await?;
    transaction.commit().await?;
    Ok(report(json!({
        "detected_version": 1,
        "action": "migrated",
        "dropped_tables": V1_TABLES,
    })))
}

/// Repairs duplicate event indexes, then enforces their uniqueness, in one
/// transaction: Python's `dedupe_event_indices`.
pub(crate) async fn dedupe_event_indices(
    client: &mut Object,
    dry_run: bool,
) -> Result<Metadata, Failure> {
    let transaction = client.transaction().await?;
    let affected = query(
        &transaction,
        "SELECT room_id, count(*) - count(DISTINCT index) AS extra \
         FROM events GROUP BY room_id HAVING count(*) <> count(DISTINCT index)",
        &[],
    )
    .await?;
    let rooms = affected
        .iter()
        .map(|row| row.try_get("room_id"))
        .collect::<Result<Vec<String>, _>>()?;
    let duplicate_rows = affected
        .iter()
        .map(|row| row.try_get::<_, i64>("extra"))
        .sum::<Result<i64, _>>()?;
    let unique = index_definition(&transaction, "idx_events_room_index")
        .await?
        .is_some_and(|definition| definition.contains("UNIQUE"));
    let state = |action: &str, now_unique: bool| {
        report(json!({
            "duplicate_rows": duplicate_rows,
            "affected_rooms": rooms.len(),
            "now_unique": now_unique,
            "action": action,
        }))
    };
    if dry_run {
        return Ok(state("dry_run", unique));
    }
    if rooms.is_empty() && unique {
        return Ok(state("noop", unique));
    }
    if !rooms.is_empty() {
        execute(
            &transaction,
            "WITH renum AS (\
               SELECT id, row_number() OVER \
                 (PARTITION BY room_id ORDER BY index, created_at, id) - 1 AS new_index \
               FROM events WHERE room_id = ANY($1::text[])) \
             UPDATE events e SET index = r.new_index FROM renum r \
             WHERE e.id = r.id AND e.index <> r.new_index",
            &[&rooms],
        )
        .await?;
        execute(
            &transaction,
            "UPDATE rooms rm SET latest_index = s.mx, event_count = s.cnt \
             FROM (SELECT room_id, max(index) AS mx, count(*) AS cnt FROM events \
                   WHERE room_id = ANY($1::text[]) GROUP BY room_id) s \
             WHERE rm.id = s.room_id",
            &[&rooms],
        )
        .await?;
    }
    if !unique {
        transaction
            .batch_execute(&drop_own_index("idx_events_room_index"))
            .await?;
        transaction
            .batch_execute("CREATE UNIQUE INDEX idx_events_room_index ON events(room_id, index)")
            .await?;
    }
    transaction.commit().await?;
    Ok(state("repaired", true))
}

/// Drops the single-column `idx_events_parent` the composite index supersedes,
/// on request only: Python's `drop_legacy_parent_index`.
pub(crate) async fn drop_legacy_parent_index(
    client: &mut Object,
    dry_run: bool,
) -> Result<Metadata, Failure> {
    let present = query_opt(&*client, INDEX_DEF, &[&"idx_events_parent"])
        .await?
        .is_some();
    let action = match (present, dry_run) {
        (false, _) => "noop",
        (true, true) => "dry_run",
        (true, false) => {
            client
                .batch_execute(&drop_own_index("idx_events_parent"))
                .await?;
            "dropped"
        }
    };
    Ok(report(json!({ "action": action })))
}
