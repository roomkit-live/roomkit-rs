//! Identity verification's records (RFC §11.7): an identity's verification
//! and the requests that ask a sender to prove who they are.
//!
//! Identification says whose an address is; verification says the person
//! writing has proven it. RoomKit keeps the state and its lifecycle; how a
//! person proves who they are (a page behind a link, a code, a login) is the
//! integrator's.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::{ChannelId, IdentityId, ModelError, RoomId, Timestamp, VerificationStatus};

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

/// How long a verification and its requests last, how many attempts a
/// request accepts, and what happens to an unverified sender's messages
/// (RFC §11.7), Python's `VerificationPolicy`: its defaults in [`Default`],
/// a caller setting the others by struct update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerificationPolicy {
    /// How long a completed verification stays in force: 900 s.
    pub ttl: Duration,
    /// How long a request may be completed in: 600 s.
    pub request_ttl: Duration,
    /// How long a code request may be completed in, a code being short
    /// (RFC §11.7.7): 300 s.
    pub code_request_ttl: Duration,
    /// The wrong secrets a request accepts before it fails: 3, at least 1.
    pub max_attempts: u32,
    /// The digits of a code sent for the person to reply with: 6, from 6 to
    /// 12.
    pub code_length: u32,
    /// Whether the messages held during a request are released to the
    /// room's agents when it completes (RFC §11.7.6): `true`.
    pub release_on_verify: bool,
    /// At most this many held messages are released, the latest ones: 20,
    /// at least 1.
    pub release_max_messages: u32,
    /// Held messages older than this are not released: 3600 s.
    pub release_max_age: Duration,
    /// Whether an unverified sender's message is held from the room's
    /// agents unless an `ON_VERIFICATION_REQUIRED` hook passes it (RFC
    /// §11.7.3); `false` only labels it: `true`.
    pub hold_unverified: bool,
}

impl Default for VerificationPolicy {
    fn default() -> Self {
        Self {
            ttl: Duration::from_mins(15),
            request_ttl: Duration::from_mins(10),
            code_request_ttl: Duration::from_mins(5),
            max_attempts: 3,
            code_length: 6,
            release_on_verify: true,
            release_max_messages: 20,
            release_max_age: Duration::from_hours(1),
            hold_unverified: true,
        }
    }
}

impl VerificationPolicy {
    /// Refuses what the Python implementation's model refuses when built: a
    /// lifetime of zero (`gt=0`), no attempt or no message released (`ge=1`),
    /// a code of fewer than 6 or more than 12 digits. Outside the public API:
    /// the kit's verification calls run it on the policy the kit was built
    /// with, a struct literal running no code.
    ///
    /// # Errors
    ///
    /// [`ModelError::NotPositive`] for a lifetime of zero,
    /// [`ModelError::OutOfRange`] for a count outside its bounds.
    #[doc(hidden)]
    pub fn check(&self) -> Result<(), ModelError> {
        for (name, lifetime) in [
            ("ttl", self.ttl),
            ("request_ttl", self.request_ttl),
            ("code_request_ttl", self.code_request_ttl),
            ("release_max_age", self.release_max_age),
        ] {
            if lifetime.is_zero() {
                return Err(ModelError::NotPositive {
                    name,
                    value: "0.0".to_owned(),
                });
            }
        }
        for (name, value, min, max) in [
            ("max_attempts", self.max_attempts, 1, u32::MAX),
            ("code_length", self.code_length, 6, 12),
            (
                "release_max_messages",
                self.release_max_messages,
                1,
                u32::MAX,
            ),
        ] {
            if !(min..=max).contains(&value) {
                return Err(ModelError::OutOfRange {
                    name,
                    value: f64::from(value),
                    min: f64::from(min),
                    max: f64::from(max),
                });
            }
        }
        Ok(())
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

    // Python: src/roomkit/models/verification.py, VerificationPolicy's
    // Field bounds (the ttls gt=0, max_attempts ge=1, code_length 6..=12).
    #[test]
    fn a_policy_out_of_its_bounds_is_refused() {
        let policy = VerificationPolicy::default;
        assert!(policy().check().is_ok());
        for refused in [
            VerificationPolicy {
                ttl: Duration::ZERO,
                ..policy()
            },
            VerificationPolicy {
                max_attempts: 0,
                ..policy()
            },
            VerificationPolicy {
                code_length: 5,
                ..policy()
            },
            VerificationPolicy {
                code_length: 13,
                ..policy()
            },
            VerificationPolicy {
                release_max_messages: 0,
                ..policy()
            },
        ] {
            assert!(refused.check().is_err(), "{refused:?} should be refused");
        }
    }
}
