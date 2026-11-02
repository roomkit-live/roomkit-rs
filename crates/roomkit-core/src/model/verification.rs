//! Identity verification's records (RFC §11.7): an identity's verification
//! and the requests that ask a sender to prove who they are.
//!
//! Identification says whose an address is; verification says the person
//! writing has proven it. RoomKit keeps the state and its lifecycle; how a
//! person proves who they are (a page behind a link, a code, a login) is the
//! integrator's.

use serde::{Deserialize, Serialize};

use super::{ChannelId, IdentityId, RoomId, Timestamp, VerificationStatus};

/// An identity's verification, valid on every channel its addresses reach
/// (RFC §11.7.1), within its organization (RFC §11.7.7).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Verification {
    /// The identity verified.
    pub identity_id: IdentityId,
    /// The organization it holds in, `None` for an identity of none.
    #[serde(default)]
    pub organization_id: Option<String>,
    /// Where it stands.
    #[serde(default = "unverified")]
    pub status: VerificationStatus,
    /// The integrator's name for how it was proven (`link`, `code`, `sso`).
    #[serde(default)]
    pub method: Option<String>,
    /// The channel the completing request was opened on.
    #[serde(default)]
    pub verified_via: Option<ChannelId>,
    /// When it was proven.
    #[serde(default)]
    pub verified_at: Option<Timestamp>,
    /// When it ends, unless it ends before.
    #[serde(default)]
    pub expires_at: Option<Timestamp>,
    /// Why it ended: `expired`, `revoked`, `failed`, or the integrator's
    /// reason for a revocation.
    #[serde(default)]
    pub ended_reason: Option<String>,
    /// The request open for the identity, if any.
    #[serde(default)]
    pub request_id: Option<String>,
    /// The request that completed it: its end is announced in that
    /// request's room, on its channel.
    #[serde(default)]
    pub completed_request_id: Option<String>,
}

impl Verification {
    /// An unverified identity of no organization, with nothing proven yet,
    /// as the Python implementation's model defaults it. A caller sets the
    /// other fields by struct update.
    pub fn new(identity_id: IdentityId) -> Self {
        Self {
            identity_id,
            organization_id: None,
            status: VerificationStatus::Unverified,
            method: None,
            verified_via: None,
            verified_at: None,
            expires_at: None,
            ended_reason: None,
            request_id: None,
            completed_request_id: None,
        }
    }
}

/// The status a verification record that names none reads with.
fn unverified() -> VerificationStatus {
    VerificationStatus::Unverified
}

/// Where a verification request stands (RFC §11.7.2).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum VerificationRequestStatus {
    /// Waiting for its secret.
    #[default]
    Open,
    /// Completed: the identity is verified.
    Completed,
    /// Its attempts ran out.
    Failed,
    /// Its time ran out unanswered.
    Expired,
    /// Ended by a new request, or by a revocation.
    Cancelled,
}

/// A request asking an identified sender to prove who they are (RFC
/// §11.7.2).
///
/// Its secret is never stored: only its hash, salted with the request's id
/// (RFC §11.7.7).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationRequest {
    /// The request's id, its public reference.
    pub id: String,
    /// The identity asked.
    pub identity_id: IdentityId,
    /// Its room's organization, `None` for a room of none.
    #[serde(default)]
    pub organization_id: Option<String>,
    /// The room it was opened in.
    pub room_id: RoomId,
    /// The channel it was opened on.
    pub channel_id: ChannelId,
    /// How the secret is proven: `link`, `code`, or the integrator's own.
    pub method: String,
    /// The secret's hash.
    pub secret_hash: String,
    /// When the message that opened it was received: the start of the
    /// window whose held messages are released once verified (RFC §11.7.6).
    pub window_start: Timestamp,
    /// When it was opened.
    pub created_at: Timestamp,
    /// When it can no longer be completed.
    pub expires_at: Timestamp,
    /// The wrong secrets and failed checks it counted.
    #[serde(default)]
    pub attempts: u32,
    /// The attempts it accepts before it fails.
    pub max_attempts: u32,
    /// Where it stands.
    #[serde(default)]
    pub status: VerificationRequestStatus,
}

impl VerificationRequest {
    /// An open request of no organization that counted no attempt yet, as
    /// the Python implementation's model defaults it. A caller sets the
    /// other fields by struct update.
    #[expect(
        clippy::too_many_arguments,
        reason = "a model's new takes the fields Python's model requires, ten here"
    )]
    pub fn new(
        id: impl Into<String>,
        identity_id: IdentityId,
        room_id: RoomId,
        channel_id: ChannelId,
        method: impl Into<String>,
        secret_hash: impl Into<String>,
        window_start: Timestamp,
        created_at: Timestamp,
        expires_at: Timestamp,
        max_attempts: u32,
    ) -> Self {
        Self {
            id: id.into(),
            identity_id,
            organization_id: None,
            room_id,
            channel_id,
            method: method.into(),
            secret_hash: secret_hash.into(),
            window_start,
            created_at,
            expires_at,
            attempts: 0,
            max_attempts,
            status: VerificationRequestStatus::Open,
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    /// A record naming only its identity reads with the defaults of the
    /// Python implementation's model, as a row it wrote is read.
    #[test]
    fn a_verification_naming_only_its_identity_reads_unverified() {
        let read: Verification = serde_json::from_value(json!({ "identity_id": "alice" })).unwrap();
        assert_eq!(read, Verification::new(IdentityId::new("alice").unwrap()));
    }

    /// A request read without its attempts and status reads with the
    /// defaults of the Python implementation's model.
    #[test]
    fn a_request_without_attempts_or_status_reads_open() {
        let read: VerificationRequest = serde_json::from_value(json!({
            "id": "req-1", "identity_id": "alice", "room_id": "room-1", "channel_id": "sms",
            "method": "link", "secret_hash": "h", "window_start": "2030-01-01T00:00:00Z",
            "created_at": "2030-01-01T00:00:00Z", "expires_at": "2030-01-01T00:10:00Z",
            "max_attempts": 3,
        }))
        .unwrap();
        assert_eq!(
            (read.attempts, read.status),
            (0, VerificationRequestStatus::Open)
        );
    }
}
