//! The Python implementation's schema, version 12, which a file is created
//! at or checked against when it is opened.

use std::time::Duration;

use rusqlite::{Connection, TransactionBehavior};

use crate::error::OpenError;

/// The schema version this crate reads and writes: the Python
/// implementation's `SQLiteStore` at its version 12.
pub const SCHEMA_VERSION: i64 = 12;

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
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Readies `connection`'s file: creates the schema in an empty file, repairs
/// what a file at this version may lack, as the Python implementation does on
/// every open, and refuses a file of another version. Then sets the
/// write-ahead log's mode.
pub(crate) fn prepare(connection: &mut Connection) -> Result<(), OpenError> {
    connection.busy_timeout(BUSY_TIMEOUT)?;
    let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    match version {
        0 => create(connection)?,
        SCHEMA_VERSION => repair(connection)?,
        newer if newer > SCHEMA_VERSION => {
            return Err(OpenError::NewerSchema {
                found: newer,
                supported: SCHEMA_VERSION,
            });
        }
        older => {
            return Err(OpenError::OlderSchema {
                found: older,
                supported: SCHEMA_VERSION,
            });
        }
    }
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "NORMAL")?;
    Ok(())
}

/// Creates the schema and stamps its version, in one transaction.
fn create(connection: &mut Connection) -> Result<(), OpenError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(SCHEMA)?;
    transaction.execute_batch(RESPONDS_TO_INDEX)?;
    transaction.execute_batch(POPULATE_SEQUENCES)?;
    transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    transaction.commit()?;
    Ok(())
}

/// Adds what a file at this version lacks, never its version: every
/// statement is idempotent.
fn repair(connection: &mut Connection) -> Result<(), OpenError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(SCHEMA)?;
    transaction.execute_batch(RESPONDS_TO_INDEX)?;
    transaction.execute_batch(POPULATE_SEQUENCES)?;
    transaction.commit()?;
    Ok(())
}
