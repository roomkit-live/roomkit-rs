//! Identity verifications and their requests (RFC §11.7, §14.1), in the
//! `verifications` and `verification_requests` tables, each scoped by
//! organization, none being the empty string, as the Python implementation
//! writes them.

use roomkit::model::{IdentityId, Verification, VerificationRequest, VerificationStatus};
use rusqlite::{Connection, OptionalExtension, params};

use crate::error::Failure;
use crate::records::{self, Kind};

/// The organization column's value for `organization`.
fn scope(organization: Option<&str>) -> &str {
    organization.unwrap_or_default()
}

/// The JSON of the verification of `identity_id` in `organization`.
fn stored(
    connection: &Connection,
    identity_id: &IdentityId,
    organization: Option<&str>,
) -> Result<Option<String>, Failure> {
    Ok(connection
        .query_row(
            "SELECT data FROM verifications WHERE identity_id = ? AND organization_id = ?",
            [identity_id.as_str(), scope(organization)],
            |row| row.get(0),
        )
        .optional()?)
}

/// The verification of `identity_id` in `organization`.
pub(crate) fn get(
    connection: &Connection,
    identity_id: &IdentityId,
    organization: Option<&str>,
) -> Result<Option<Verification>, Failure> {
    stored(connection, identity_id, organization)?
        .map(|data| records::read(&data))
        .transpose()
}

/// Stores `verification`, replacing the one its identity holds in its
/// organization (Python's `INSERT OR REPLACE`).
pub(crate) fn put(connection: &Connection, verification: &Verification) -> Result<(), Failure> {
    let organization = verification.organization_id.as_deref();
    let kept = stored(connection, &verification.identity_id, organization)?;
    connection.execute(
        "INSERT OR REPLACE INTO verifications(identity_id, organization_id, status, data)
         VALUES(?, ?, ?, ?)",
        params![
            verification.identity_id.as_str(),
            scope(organization),
            records::name(&verification.status)?,
            records::written(verification, Kind::Verification, kept.as_deref())?,
        ],
    )?;
    Ok(())
}

/// Every verification VERIFIED with an expiry, across organizations.
pub(crate) fn in_force(connection: &Connection) -> Result<Vec<Verification>, Failure> {
    let mut statement = connection.prepare("SELECT data FROM verifications WHERE status = ?")?;
    let rows = statement.query_map([records::name(&VerificationStatus::Verified)?], |row| {
        row.get::<_, String>(0)
    })?;
    let mut verifications = Vec::new();
    for data in rows {
        let verification: Verification = records::read(&data?)?;
        if verification.expires_at.is_some() {
            verifications.push(verification);
        }
    }
    Ok(verifications)
}

/// The JSON of the request `request_id`.
fn stored_request(connection: &Connection, request_id: &str) -> Result<Option<String>, Failure> {
    Ok(connection
        .query_row(
            "SELECT data FROM verification_requests WHERE id = ?",
            [request_id],
            |row| row.get(0),
        )
        .optional()?)
}

/// The request `request_id`.
pub(crate) fn request(
    connection: &Connection,
    request_id: &str,
) -> Result<Option<VerificationRequest>, Failure> {
    stored_request(connection, request_id)?
        .map(|data| records::read(&data))
        .transpose()
}

/// Stores `request`: a creation is refused when the file holds a request of
/// its id, as Python's `INSERT` is, and an update replaces it (Python's
/// `INSERT OR REPLACE`).
pub(crate) fn save_request(
    connection: &Connection,
    request: &VerificationRequest,
    create: bool,
) -> Result<(), Failure> {
    let kept = if create {
        None
    } else {
        stored_request(connection, &request.id)?
    };
    let sql = if create {
        "INSERT INTO verification_requests(id, identity_id, organization_id, status, data)
         VALUES(?, ?, ?, ?, ?)"
    } else {
        "INSERT OR REPLACE INTO verification_requests(id, identity_id, organization_id, status, data)
         VALUES(?, ?, ?, ?, ?)"
    };
    connection.execute(
        sql,
        params![
            request.id,
            request.identity_id.as_str(),
            scope(request.organization_id.as_deref()),
            records::name(&request.status)?,
            records::written(request, Kind::VerificationRequest, kept.as_deref())?,
        ],
    )?;
    Ok(())
}
