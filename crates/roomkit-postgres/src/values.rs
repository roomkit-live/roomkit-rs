//! The values a row holds beside text and JSON: an instant's wire form, an
//! integer's range, an enumeration's name, and the statements' plumbing.

use std::error::Error;

use bytes::BytesMut;
use deadpool_postgres::GenericClient;
use roomkit::model::Timestamp;
use serde::Serialize;
use serde_json::Value;
use tokio_postgres::types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use tokio_postgres::{Row, Statement};

use crate::error::Failure;

/// Microseconds from the Unix epoch to Postgres's, 2000-01-01.
const POSTGRES_EPOCH: i64 = 946_684_800_000_000;

/// A parameter of a statement.
pub(crate) type Param<'a> = &'a (dyn ToSql + Sync);

/// A parameter a statement built at run time owns.
pub(crate) type Owned = Box<dyn ToSql + Sync + Send>;

/// An instant in a `TIMESTAMPTZ` column, at its microsecond: the precision
/// Python's `datetime` holds.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Instant(pub(crate) Timestamp);

/// The microseconds from the Unix epoch to `instant`, a finer precision left
/// out.
fn micros(instant: Timestamp) -> Option<i64> {
    match instant.duration_since(Timestamp::UNIX_EPOCH) {
        Some(after) => i64::try_from(after.as_micros()).ok(),
        None => Timestamp::UNIX_EPOCH
            .duration_since(instant)
            .and_then(|before| i64::try_from(before.as_micros()).ok())
            .map(|before| -before),
    }
}

/// The instant `value` microseconds after the Unix epoch.
fn instant(value: i64) -> Option<Timestamp> {
    let millis = value.div_euclid(1000);
    let rest = u64::try_from(value.rem_euclid(1000)).ok()?;
    Timestamp::from_unix_millis(millis)
        .ok()?
        .checked_add(std::time::Duration::from_micros(rest))
}

impl ToSql for Instant {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let value = micros(self.0)
            .and_then(|value| value.checked_sub(POSTGRES_EPOCH))
            .ok_or("an instant out of a timestamp's range")?;
        out.extend_from_slice(&value.to_be_bytes());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::TIMESTAMPTZ
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for Instant {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let raw: [u8; 8] = raw.try_into()?;
        i64::from_be_bytes(raw)
            .checked_add(POSTGRES_EPOCH)
            .and_then(instant)
            .map(Self)
            .ok_or_else(|| "a timestamp out of an instant's range".into())
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::TIMESTAMPTZ
    }
}

/// An index or a count as an `INTEGER` column holds it.
pub(crate) fn integer(value: u64) -> Result<i32, Failure> {
    i32::try_from(value).map_err(|_| Failure::OutOfRange("an index"))
}

/// A page's bound as `LIMIT` and `OFFSET` take it, a value past their range
/// read as its largest.
pub(crate) fn bound(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// The name a value of the model's enumerations, or of its open names, is
/// written as: its JSON string.
pub(crate) fn name<T: Serialize>(value: &T) -> Result<String, Failure> {
    match serde_json::to_value(value)? {
        Value::String(name) => Ok(name),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Array(_) | Value::Object(_) => {
            Err(Failure::OutOfRange("a name written other than as a string"))
        }
    }
}

/// The statement of `sql`, prepared once per connection.
async fn prepared<C: GenericClient>(client: &C, sql: &str) -> Result<Statement, Failure> {
    Ok(client.prepare_cached(sql).await?)
}

/// Runs `sql`; how many rows it wrote.
pub(crate) async fn execute<C: GenericClient>(
    client: &C,
    sql: &str,
    params: &[Param<'_>],
) -> Result<u64, Failure> {
    let statement = prepared(client, sql).await?;
    Ok(client.execute(&statement, params).await?)
}

/// The rows of `sql`.
pub(crate) async fn query<C: GenericClient>(
    client: &C,
    sql: &str,
    params: &[Param<'_>],
) -> Result<Vec<Row>, Failure> {
    let statement = prepared(client, sql).await?;
    Ok(client.query(&statement, params).await?)
}

/// The row of `sql`, if it reads one.
pub(crate) async fn query_opt<C: GenericClient>(
    client: &C,
    sql: &str,
    params: &[Param<'_>],
) -> Result<Option<Row>, Failure> {
    let statement = prepared(client, sql).await?;
    Ok(client.query_opt(&statement, params).await?)
}

/// `owned` as a statement takes its parameters.
pub(crate) fn borrowed(owned: &[Owned]) -> Vec<Param<'_>> {
    owned
        .iter()
        .map(|param| -> Param<'_> { param.as_ref() })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An instant keeps its microsecond through the column's form, before
    /// and after the epochs.
    #[test]
    fn an_instant_keeps_its_microsecond_on_either_side_of_the_epochs() {
        for value in [
            0,
            1,
            -1,
            999,
            -1_001,
            POSTGRES_EPOCH,
            1_792_677_780_123_456,
            -1_500_000,
        ] {
            let at = instant(value).unwrap();
            assert_eq!(micros(at), Some(value), "{value}");
        }
    }
}
