//! The Python implementation's schema, version 14, which a file is created
//! at or checked against when it is opened; a version 12 or 13 file is migrated.

use std::time::Duration;

use rusqlite::{Connection, TransactionBehavior};

use roomkit::StoreError;

use crate::error::{OpenFailure, SqliteSchemaError};

/// The schema version this crate reads and writes: the Python
/// implementation's `SQLiteStore` at its version 14.
pub(crate) const SCHEMA_VERSION: i64 = 14;

/// Gives each full-text row its event row's rowid, the one key FTS5 indexes,
/// and drops a row naming no stored event: Python's `_migrate_v12_to_v13`
/// (RMK-1031).
const MIGRATE_12_TO_13: &str = "
CREATE TEMP TABLE fts_by_event AS
    SELECT e.rowid AS event_rowid, f.text, f.event_id, f.room_id
    FROM events_fts f JOIN events e ON e.id = f.event_id
    GROUP BY e.rowid;
DELETE FROM events_fts;
INSERT INTO events_fts(rowid, text, event_id, room_id)
    SELECT event_rowid, text, event_id, room_id FROM fts_by_event;
DROP TABLE fts_by_event;
";

/// An address version 3's migration left unscoped for an identity of an
/// organization moves to that organization, if the identity still declares it; one
/// whose organization holds the address already yields to that row; one it no longer
/// declares, a link since revoked, goes, and the address is free: Python's
/// `_migrate_v13_to_v14` (HITL 0f96a597, 6928c418; RFC §17.2).
const MIGRATE_13_TO_14: &str = "
DELETE FROM identity_addresses
WHERE organization_id = ''
  AND EXISTS (
      SELECT 1 FROM identities i JOIN identity_addresses held
        ON held.channel_type = identity_addresses.channel_type
       AND held.address = identity_addresses.address
       AND held.organization_id = json_extract(i.data, '$.organization_id')
      WHERE i.id = identity_addresses.identity_id
        AND COALESCE(json_extract(i.data, '$.organization_id'), '') <> '');
UPDATE identity_addresses
SET organization_id = (
    SELECT json_extract(i.data, '$.organization_id') FROM identities i
    WHERE i.id = identity_addresses.identity_id)
WHERE organization_id = ''
  AND COALESCE((
      SELECT json_extract(i.data, '$.organization_id') FROM identities i
      WHERE i.id = identity_addresses.identity_id), '') <> ''
  AND EXISTS (
      SELECT 1 FROM identities i,
           json_each(i.data, '$.channel_addresses') declared,
           json_each(declared.value) held
      WHERE i.id = identity_addresses.identity_id
        AND declared.key = identity_addresses.channel_type
        AND held.value = identity_addresses.address);
DELETE FROM identity_addresses
WHERE organization_id = ''
  AND COALESCE((
      SELECT json_extract(i.data, '$.organization_id') FROM identities i
      WHERE i.id = identity_addresses.identity_id), '') <> '';
";

/// The tables and indexes, as the Python implementation writes them
/// (roomkit `store/sqlite.py`, `_SCHEMA`), byte for byte: SQLite keeps each
/// statement's text, so a file this crate creates holds the same schema as
/// one Python creates.
const SCHEMA: &str = include_str!("schema.sql");

/// The index of the answers to an event, which the Python implementation
/// keeps apart from its schema (`_RESPONDS_TO_INDEX`).
const RESPONDS_TO_INDEX: &str =
    "CREATE INDEX IF NOT EXISTS idx_events_room_responds_to ON events(room_id, responds_to);";

/// Each room's next index from its events and its counters, kept ahead of
/// both: the highest index the timeline holds, and the latest index the room
/// gave out, a hard delete's included (Python's `_populate_event_sequences`).
const POPULATE_SEQUENCES: &str = "
INSERT INTO event_sequences(room_id, next_index)
    SELECT room_id, MAX(idx) + 1 FROM events GROUP BY room_id
    ON CONFLICT(room_id) DO UPDATE SET
        next_index = MAX(event_sequences.next_index, excluded.next_index);
INSERT INTO event_sequences(room_id, next_index)
    SELECT id, CASE
        WHEN COALESCE(json_extract(data, '$.event_count'), 0) > 0
            AND json_extract(data, '$.latest_index') IS NOT NULL
        THEN json_extract(data, '$.latest_index') + 1
        ELSE 0 END
    FROM rooms WHERE true
    ON CONFLICT(room_id) DO UPDATE SET
        next_index = MAX(event_sequences.next_index, excluded.next_index);
";

/// How long a write waits for another connection's, as the Python
/// implementation's connections do.
pub(crate) const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Readies `connection`'s file: creates the schema in an empty file, repairs
/// what a file at this version may lack, as the Python implementation does on
/// every open, and refuses a file of another version. Then sets the
/// write-ahead log's mode.
///
/// # Errors
///
/// Fails [`StoreError::Unavailable`], its cause a [`SqliteSchemaError`] for
/// a file of another version, which is left as it is, or SQLite's error.
pub(crate) fn prepare(connection: &mut Connection) -> Result<(), StoreError> {
    connection
        .busy_timeout(BUSY_TIMEOUT)
        .map_err(OpenFailure::Database)?;
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(OpenFailure::Database)?;
    match version {
        0 => create(connection).map_err(OpenFailure::Database)?,
        SCHEMA_VERSION => repair(connection).map_err(OpenFailure::Database)?,
        12 => {
            migrate(connection, 13, MIGRATE_12_TO_13).map_err(OpenFailure::Database)?;
            migrate(connection, 14, MIGRATE_13_TO_14).map_err(OpenFailure::Database)?;
        }
        13 => migrate(connection, 14, MIGRATE_13_TO_14).map_err(OpenFailure::Database)?,
        newer if newer > SCHEMA_VERSION => {
            return Err(SqliteSchemaError::newer(newer, SCHEMA_VERSION).into());
        }
        older => return Err(SqliteSchemaError::older(older, SCHEMA_VERSION).into()),
    }
    write_ahead(connection).map_err(OpenFailure::Database)?;
    Ok(())
}

/// Sets the write-ahead log's mode, with the synchronous level it allows.
fn write_ahead(connection: &Connection) -> rusqlite::Result<()> {
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "NORMAL")
}

/// Creates the schema and stamps its version, in one transaction.
fn create(connection: &mut Connection) -> rusqlite::Result<()> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(SCHEMA)?;
    transaction.execute_batch(RESPONDS_TO_INDEX)?;
    transaction.execute_batch(POPULATE_SEQUENCES)?;
    transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    transaction.commit()?;
    Ok(())
}

/// Runs `step` and stamps the file `version`, in one transaction, as Python does
/// on open; a file another connection migrated meanwhile is left as it is.
fn migrate(connection: &mut Connection, version: i64, step: &str) -> rusqlite::Result<()> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let found: i64 = transaction.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if found < version {
        transaction.execute_batch(step)?;
        transaction.pragma_update(None, "user_version", version)?;
    }
    transaction.commit()?;
    Ok(())
}

/// Adds what a file at this version lacks, never its version: every
/// statement is idempotent.
fn repair(connection: &mut Connection) -> rusqlite::Result<()> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(SCHEMA)?;
    transaction.execute_batch(RESPONDS_TO_INDEX)?;
    transaction.execute_batch(POPULATE_SEQUENCES)?;
    transaction.commit()?;
    Ok(())
}
