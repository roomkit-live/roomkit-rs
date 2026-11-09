//! Identity verification (RFC §11.7): requests, completion, revocation and
//! the state an identity reads as, the Python implementation's
//! `VerificationMixin`.
//!
//! RoomKit keeps the state and the lifecycle; the integrator keeps the
//! method (how a person proves who they are) and calls
//! [`RoomKit::complete_verification`] once its own check passed. A
//! request's secret is returned once and stored only as a salted hash.
//!
//! - this module: the kit's calls;
//! - `transitions`: the steps each call takes under the identity's lock;
//! - `inbound`: what the inbound pipeline's step 5b asks the kit (RFC
//!   §10.1 step 5b): a sender's verification, a code reply's attempt;
//! - `release`: what a completed request releases (RFC §11.7.6);
//! - `timers`: the ends that come when their time runs out (RFC §11.7.5);
//! - `announce`: the framework events and the hooks;
//! - `state`, `secret`, `runtime`: what a record reads as, the secrets, and
//!   what the kit holds.

mod announce;
mod held;
pub(crate) mod inbound;
mod release;
mod runtime;
mod secret;
mod state;
mod timers;
mod transitions;

use roomkit_core::model::{
    ChannelId, IdentityId, RoomContext, RoomEvent, RoomId, Timestamp, Verification,
    VerificationRequest, VerificationStatus,
};
use roomkit_core::rules::hook_registry::HookTrigger;
use roomkit_core::rules::verification::participant_identity;
use uuid::Uuid;

use self::announce::{completion_data, end_data};
pub(crate) use self::runtime::Verifications;
use self::state::{ENDED_VERIFICATION, Outcome, in_force, in_scope, lock_key};
use super::RoomKit;
use crate::GetRoomOptions;
use crate::error::RoomKitError;
use crate::store::ConversationStore;

/// A secret carried in a link to the integrator's page (high entropy).
const METHOD_LINK: &str = "link";
/// A short code the person replies with in the conversation (RFC §11.7.4).
const METHOD_CODE: &str = "code";

/// What [`RoomKit::request_verification`] takes beside the identity, the
/// room and the channel: Python's `request_verification(identity_id, *,
/// room_id, channel_id, method='link', window_start=None,
/// organization_id=None)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestVerificationOptions {
    /// Python `method='link'`: how the secret is proven, `link`, `code`
    /// (digits the person replies with), or the integrator's own.
    pub method: String,
    /// Python `window_start=None`: when the message that called for the
    /// request was received, now when `None`: the held messages released on
    /// completion are those from then on (RFC §11.7.6).
    pub window_start: Option<Timestamp>,
    /// Python `organization_id=None`: the organization the caller acts for
    /// (RFC §17.2); a room of another is not found.
    pub organization_id: Option<String>,
}

impl Default for RequestVerificationOptions {
    fn default() -> Self {
        Self {
            method: METHOD_LINK.to_owned(),
            window_start: None,
            organization_id: None,
        }
    }
}

/// What [`RoomKit::complete_verification`] takes beside the request and
/// its secret: Python's `complete_verification(request_id, secret, *,
/// method=None, organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompleteVerificationOptions {
    /// Python `method=None`: how the person proved it, the request's method
    /// when `None`.
    pub method: Option<String>,
    /// Python `organization_id=None`: the organization the caller acts for;
    /// a request of another is not found.
    pub organization_id: Option<String>,
}

/// What [`RoomKit::record_failed_attempt`] takes beside the request:
/// Python's `record_failed_attempt(request_id, *, organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecordFailedAttemptOptions {
    /// Python `organization_id=None`: the organization the caller acts for;
    /// a request of another is not found.
    pub organization_id: Option<String>,
}

/// What [`RoomKit::revoke_verification`] takes beside the identity:
/// Python's `revoke_verification(identity_id, *, reason='revoked',
/// organization_id=None)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RevokeVerificationOptions {
    /// Python `reason='revoked'`: why it ended, recorded as its
    /// `ended_reason`.
    pub reason: String,
    /// Python `organization_id=None`: the organization whose verification
    /// of the identity ends.
    pub organization_id: Option<String>,
}

impl Default for RevokeVerificationOptions {
    fn default() -> Self {
        Self {
            reason: "revoked".to_owned(),
            organization_id: None,
        }
    }
}

/// What [`RoomKit::verification_of`] takes beside the identity: Python's
/// `verification_of(identity_id, *, organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VerificationOfOptions {
    /// Python `organization_id=None`: the organization whose verification
    /// of the identity is read (RFC §11.7.7).
    pub organization_id: Option<String>,
}

impl<S: ConversationStore> RoomKit<S> {
    /// Opens a verification request for `identity_id` in `room_id`, on
    /// `channel_id`, and returns it with its secret (RFC §11.7.2): Python's
    /// `request_verification`.
    ///
    /// The secret is returned once and never stored: put it in the link to
    /// your page, or send it as a code (`method` `code`: digits the person
    /// replies with). Opening a request cancels the identity's earlier open
    /// one. An identity that is not verified becomes PENDING; one already
    /// verified stays so until this request completes or its verification
    /// ends. The request takes the room's organization, the one whose
    /// messages it verifies, and ends at its expiry, announced then (RFC
    /// §11.7.5).
    ///
    /// # Errors
    ///
    /// [`RoomKitError::RoomNotFound`] for a room the store lacks, or of
    /// another organization than the one `options` act for, nothing
    /// written; [`RoomKitError::Value`] for a kit's policy out of its
    /// bounds; [`RoomKitError::Store`] for the store's failure.
    pub async fn request_verification(
        &self,
        identity_id: &IdentityId,
        room_id: &RoomId,
        channel_id: &ChannelId,
        options: RequestVerificationOptions,
    ) -> Result<(VerificationRequest, String), RoomKitError> {
        let RequestVerificationOptions {
            method,
            window_start,
            organization_id,
        } = options;
        let room = self
            .get_room(room_id, GetRoomOptions { organization_id })
            .await?;
        let policy = &self.inner.verification.policy;
        policy.check()?;
        let now = self.inner.environment().now();
        let code = method == METHOD_CODE;
        let secret = if code {
            secret::code(policy.code_length)?
        } else {
            secret::link_secret()?
        };
        let ttl = if code {
            policy.code_request_ttl
        } else {
            policy.request_ttl
        };
        let id = Uuid::new_v4().simple().to_string();
        let secret_hash = secret::secret_hash(&id, &secret).await?;
        let request = VerificationRequest {
            organization_id: room.organization_id,
            ..VerificationRequest::new(
                id,
                identity_id.clone(),
                room_id.clone(),
                channel_id.clone(),
                method,
                secret_hash,
                window_start.unwrap_or(now),
                now,
                later(now, ttl)?,
                policy.max_attempts,
            )
        };
        let cancelled = self.open_request(&request, now).await?;
        // Announced once the lock is released: a handler may call back in.
        if let Some(cancelled) = cancelled {
            self.announce_superseded(&cancelled).await;
        }
        self.announce_request("verification_requested", &request, None)
            .await;
        self.arm_request_expiry(&request);
        Ok((request, secret))
    }

    /// Completes the request `request_id` once your own check passed, and
    /// returns the verification (RFC §11.7.2): Python's
    /// `complete_verification`. The identity is VERIFIED until the policy's
    /// `ttl` from now, `verification_completed` is emitted and
    /// `ON_VERIFICATION_COMPLETED` fired in the request's room. With the
    /// policy's `release_on_verify`, what the sender wrote while the request
    /// was open is then released (RFC §11.7.6): each held message gets back
    /// the scope it had, `ON_EVENT_UPDATED` fires for it, and the room's
    /// agents answer the last one, the call returning once they did. A wrong secret
    /// counts as an attempt; the last one fails the request, which ends a
    /// verification in force with it (fail closed).
    ///
    /// # Errors
    ///
    /// [`RoomKitError::VerificationRefused`] when the request is unknown
    /// (or another organization's than the one `options` act for:
    /// `not_found`), no longer open (`not_open`), expired (`expired`), or
    /// the secret does not match (`mismatch`, and `failed` for the last
    /// attempt); [`RoomKitError::Store`] for the store's failure.
    pub async fn complete_verification(
        &self,
        request_id: &str,
        secret: &str,
        options: CompleteVerificationOptions,
    ) -> Result<Verification, RoomKitError> {
        let CompleteVerificationOptions {
            method,
            organization_id,
        } = options;
        self.inner.verification.policy.check()?;
        let request = self
            .request_in_scope(request_id, organization_id.as_deref())
            .await?;
        let key = lock_key(&request.identity_id, request.organization_id.as_deref());
        let attempt = {
            let _held = self.lock_identity(&key).await?;
            self.attempt(request_id, secret, method).await?
        };
        let (outcome, request, state) = attempt;
        if outcome != Outcome::Completed {
            self.after_unfulfilled(outcome, &request, &state).await;
            return Err(refused(outcome.refusal(&request)));
        }
        self.inner
            .verification
            .disarm(&timers::request_key(&request.id));
        self.arm_expiry(&state);
        self.announce_request("verification_completed", &request, None)
            .await;
        let data = completion_data(state.method.as_deref(), state.expires_at);
        self.verification_hook(HookTrigger::OnVerificationCompleted, &request, data)
            .await;
        if self.inner.verification.policy.release_on_verify {
            self.release_held(&request).await?;
        }
        Ok(state)
    }

    /// Counts a failed check of your own method against the request
    /// `request_id`, and returns the request (RFC §11.7.2): Python's
    /// `record_failed_attempt`. Your page's wrong PIN or refused login is
    /// an attempt like a wrong secret: the request's bound applies to it, so
    /// a PIN cannot be guessed through your page. The request comes back
    /// OPEN with one attempt more, or FAILED when that was the last one,
    /// which ends it as a wrong secret does, a verification in force
    /// included.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::VerificationRefused`] when the request is unknown
    /// (`not_found`), no longer open (`not_open`) or expired (`expired`);
    /// [`RoomKitError::Store`] for the store's failure.
    pub async fn record_failed_attempt(
        &self,
        request_id: &str,
        options: RecordFailedAttemptOptions,
    ) -> Result<VerificationRequest, RoomKitError> {
        self.inner.open()?;
        let request = self
            .request_in_scope(request_id, options.organization_id.as_deref())
            .await?;
        let key = lock_key(&request.identity_id, request.organization_id.as_deref());
        let (outcome, request, state) = {
            let _held = self.lock_identity(&key).await?;
            let (outcome, request, state) = self.open_request_read(request_id).await?;
            if let Some(outcome) = outcome {
                (outcome, request, state)
            } else {
                let (outcome, request) = self.count_failure(request, &state).await?;
                (outcome, request, state)
            }
        };
        self.after_unfulfilled(outcome, &request, &state).await;
        if matches!(outcome, Outcome::NotOpen | Outcome::Expired) {
            return Err(refused(outcome.refusal(&request)));
        }
        Ok(request)
    }

    /// Ends the verification of `identity_id` before its time, and returns
    /// it; `None` when none was in force (RFC §11.7.5): Python's
    /// `revoke_verification`. Its open request is cancelled in every case,
    /// verified or still pending: a link sent before the revocation can
    /// never verify the person after it (RFC §11.7.7).
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Store`] for the store's failure.
    pub async fn revoke_verification(
        &self,
        identity_id: &IdentityId,
        options: RevokeVerificationOptions,
    ) -> Result<Option<Verification>, RoomKitError> {
        let RevokeVerificationOptions {
            reason,
            organization_id,
        } = options;
        let organization = organization_id.as_deref();
        self.inner
            .verification
            .disarm(&lock_key(identity_id, organization));
        let (revoked, completing, cancelled) =
            self.revoke(identity_id, organization, &reason).await?;
        if let Some(cancelled) = cancelled {
            self.announce_superseded(&cancelled).await;
        }
        if revoked.is_some() {
            self.announce_verification_end(identity_id, &reason).await;
            if let Some(completing) = completing {
                let data = end_data(&reason, ENDED_VERIFICATION);
                self.verification_hook(HookTrigger::OnVerificationEnded, &completing, data)
                    .await;
            }
        }
        Ok(revoked)
    }

    /// Ends the verifications whose time passed, arms the timers of the
    /// others, and returns the ones it ended (RFC §11.7.5): Python's
    /// `check_verifications`. A verification ends when its time runs out,
    /// by a timer this process arms when it completes;
    /// [`start`](Self::start) calls this, ending what expired while nothing
    /// was running and re-arming the rest.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Store`] for the store's failure, a store that does
    /// not keep verifications (Level 2) refusing by name.
    pub async fn check_verifications(&self) -> Result<Vec<Verification>, RoomKitError> {
        let now = self.inner.environment().now();
        let in_force_now = self
            .store()
            .list_verifications_in_force()
            .await
            .map_err(|error| self.read_failed(error))?;
        let mut ended = Vec::new();
        for verification in in_force_now {
            if in_force(&verification, now) {
                self.arm_expiry(&verification);
                continue;
            }
            if let Some(expired) = self.expire(&verification).await? {
                ended.push(expired);
            }
        }
        Ok(ended)
    }

    /// The verification of `identity_id` as it stands now, UNVERIFIED for
    /// an identity with none (RFC §11.7.1): Python's `verification_of`. A
    /// verification whose time has passed reads EXPIRED, and an open
    /// request past its expiry reads as ended (no `request_id`, a PENDING
    /// identity back to UNVERIFIED), whether or not the end has been
    /// recorded yet.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Store`] for the store's failure.
    pub async fn verification_of(
        &self,
        identity_id: &IdentityId,
        options: VerificationOfOptions,
    ) -> Result<Verification, RoomKitError> {
        let organization = options.organization_id.as_deref();
        let stored = self.stored_verification(identity_id, organization).await?;
        let current = self.without_lapsed_request(stored).await?;
        let now = self.inner.environment().now();
        if current.status == VerificationStatus::Verified && !in_force(&current, now) {
            return Ok(Verification {
                status: VerificationStatus::Expired,
                ended_reason: Some("expired".to_owned()),
                ..current
            });
        }
        Ok(current)
    }

    /// The verification of `event`'s sender in its room's organization, the
    /// one `context` holds (RFC §11.7.3): Python's `sender_verification`.
    /// What an `ON_VERIFICATION_REQUIRED` hook reads before opening a
    /// request: whose verification it is (`identity_id`) and whether a
    /// request is open already (`request_id`). `None` when the pipeline did
    /// not label the event: its sender is not identified, or the kit has no
    /// verification policy. The sender's participant may not be in
    /// `context` yet on their first message; this reads it all the same.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Value`] for a sender id no identity can have;
    /// [`RoomKitError::Store`] for the store's failure.
    pub async fn sender_verification(
        &self,
        event: &RoomEvent,
        context: &RoomContext,
    ) -> Result<Option<Verification>, RoomKitError> {
        if event.source.verification.is_none() {
            return Ok(None);
        }
        let identity_id = match participant_identity(event, context) {
            Some(identity_id) => identity_id,
            None => match &event.source.participant_id {
                Some(sender) => IdentityId::new(sender.as_str())?,
                None => return Ok(None),
            },
        };
        let options = VerificationOfOptions {
            organization_id: context.room.organization_id.clone(),
        };
        self.verification_of(&identity_id, options).await.map(Some)
    }

    /// The request `request_id` a caller acting for `organization_id` sees.
    async fn request_in_scope(
        &self,
        request_id: &str,
        organization_id: Option<&str>,
    ) -> Result<VerificationRequest, RoomKitError> {
        let request = self
            .store()
            .get_verification_request(request_id)
            .await
            .map_err(|error| self.read_failed(error))?;
        request
            .filter(|request| in_scope(request, organization_id))
            .ok_or_else(|| refused("not_found"))
    }
}

/// The refusal of a request, for `reason`.
fn refused(reason: &str) -> RoomKitError {
    RoomKitError::VerificationRefused {
        reason: reason.to_owned(),
    }
}

/// `now` plus `lifetime`.
fn later(now: Timestamp, lifetime: std::time::Duration) -> Result<Timestamp, RoomKitError> {
    now.checked_add(lifetime).ok_or_else(|| {
        RoomKitError::Value("a verification lifetime past the range of an instant".to_owned())
    })
}
