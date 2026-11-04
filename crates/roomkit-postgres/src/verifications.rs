//! Identity verification (RFC §11.7): each record a JSONB document, the
//! columns what lookups filter on, with the Python implementation's SQL. A
//! member of a stored document the model does not read stays.

use deadpool_postgres::GenericClient;
use roomkit::model::{IdentityId, Verification, VerificationRequest, VerificationStatus};
use serde_json::Value;

use crate::error::Failure;
use crate::values::{execute, name, query, query_opt};

/// The record a row's `data` holds.
fn read<T: serde::de::DeserializeOwned>(data: Value) -> Result<T, Failure> {
    Ok(serde_json::from_value(data)?)
}

/// The identity's verification in `organization`.
pub(crate) async fn get<C: GenericClient>(
    client: &C,
    identity_id: &IdentityId,
    organization: Option<&str>,
) -> Result<Option<Verification>, Failure> {
    let row = query_opt(
        client,
        "SELECT data FROM verifications WHERE identity_id = $1 AND organization_id = $2",
        &[&identity_id.as_str(), &organization.unwrap_or_default()],
    )
    .await?;
    row.map(|row| read(row.try_get(0)?)).transpose()
}

/// Stores `verification`, in place of the identity's in its organization.
pub(crate) async fn put<C: GenericClient>(
    client: &C,
    verification: &Verification,
) -> Result<(), Failure> {
    execute(
        client,
        "INSERT INTO verifications (identity_id, organization_id, status, data) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (identity_id, organization_id) \
         DO UPDATE SET status = $3, data = verifications.data || $4",
        &[
            &verification.identity_id.as_str(),
            &verification.organization_id.as_deref().unwrap_or_default(),
            &name(&verification.status)?,
            &serde_json::to_value(verification)?,
        ],
    )
    .await?;
    Ok(())
}

/// The verifications in force that expire (RFC §11.7).
pub(crate) async fn in_force<C: GenericClient>(client: &C) -> Result<Vec<Verification>, Failure> {
    let rows = query(
        client,
        "SELECT data FROM verifications WHERE status = $1",
        &[&name(&VerificationStatus::Verified)?],
    )
    .await?;
    let mut verifications = Vec::with_capacity(rows.len());
    for row in rows {
        let verification: Verification = read(row.try_get(0)?)?;
        if verification.expires_at.is_some() {
            verifications.push(verification);
        }
    }
    Ok(verifications)
}

/// Stores a new verification request.
pub(crate) async fn create_request<C: GenericClient>(
    client: &C,
    request: &VerificationRequest,
) -> Result<(), Failure> {
    execute(
        client,
        "INSERT INTO verification_requests (id, identity_id, organization_id, status, data) \
         VALUES ($1, $2, $3, $4, $5)",
        &[
            &request.id,
            &request.identity_id.as_str(),
            &request.organization_id.as_deref().unwrap_or_default(),
            &name(&request.status)?,
            &serde_json::to_value(request)?,
        ],
    )
    .await?;
    Ok(())
}

/// The verification request `request_id`.
pub(crate) async fn request<C: GenericClient>(
    client: &C,
    request_id: &str,
) -> Result<Option<VerificationRequest>, Failure> {
    let row = query_opt(
        client,
        "SELECT data FROM verification_requests WHERE id = $1",
        &[&request_id],
    )
    .await?;
    row.map(|row| read(row.try_get(0)?)).transpose()
}

/// Writes `request` over its row, if there is one.
pub(crate) async fn update_request<C: GenericClient>(
    client: &C,
    request: &VerificationRequest,
) -> Result<(), Failure> {
    execute(
        client,
        "UPDATE verification_requests SET status = $2, data = data || $3 WHERE id = $1",
        &[
            &request.id,
            &name(&request.status)?,
            &serde_json::to_value(request)?,
        ],
    )
    .await?;
    Ok(())
}
