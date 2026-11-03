//! The steps of a verification's transitions, each under its identity's
//! lock where Python's take it (RFC §11.7.2, §11.7.5): what is read and
//! written, the announcements left to the calls once the lock is released.

use roomkit_core::model::{
    IdentityId, RoomId, Timestamp, Verification, VerificationRequest, VerificationRequestStatus,
    VerificationStatus,
};
use roomkit_core::rules::hook_registry::HookTrigger;

use super::announce::end_data;
use super::secret;
use super::state::{
    ENDED_REQUEST, ENDED_VERIFICATION, Outcome, in_force, lapsed, lock_key, settled,
};
use super::{later, refused};
use crate::error::RoomKitError;
use crate::kit::RoomKit;
use crate::locks::HeldLock;
use crate::store::{ConversationStore, GetVerificationOptions};

/// An attempt's outcome, the request as it left it, and the identity's
/// verification: the one it completed, or the one it found.
pub(super) type Attempt = (Outcome, VerificationRequest, Verification);

/// What a revocation wrote: the verification it ended, the request that had
/// completed it, and the open request it cancelled.
pub(super) type Revoked = (
    Option<Verification>,
    Option<VerificationRequest>,
    Option<VerificationRequest>,
);

impl<S: ConversationStore> RoomKit<S> {
    /// Stores `request`, under its identity's lock: the identity's open
    /// request cancelled, the identity PENDING, or VERIFIED still when its
    /// verification is in force at `now`. Returns the request it cancelled.
    pub(super) async fn open_request(
        &self,
        request: &VerificationRequest,
        now: Timestamp,
    ) -> Result<Option<VerificationRequest>, RoomKitError> {
        let organization = request.organization_id.as_deref();
        let key = lock_key(&request.identity_id, organization);
        let _held = self.lock_identity(&key).await?;
        let current = self
            .stored_verification(&request.identity_id, organization)
            .await?;
        let cancelled = self.cancel_open_request(&current).await?;
        self.store()
            .create_verification_request(request.clone())
            .await
            .map_err(|error| self.read_failed(error))?;
        let status = if in_force(&current, now) {
            VerificationStatus::Verified
        } else {
            VerificationStatus::Pending
        };
        self.put_verification(Verification {
            status,
            request_id: Some(request.id.clone()),
            ..current
        })
        .await?;
        Ok(cancelled)
    }

    /// One attempt at the request `request_id`, under its identity's lock
    /// (Python's `_attempt`).
    pub(super) async fn attempt(
        &self,
        request_id: &str,
        secret: &str,
        method: Option<String>,
    ) -> Result<Attempt, RoomKitError> {
        let (outcome, request, current) = self.open_request_read(request_id).await?;
        if let Some(outcome) = outcome {
            return Ok((outcome, request, current));
        }
        let computed = secret::secret_hash(&request.id, secret).await?;
        if !secret::same_hash(&request.secret_hash, &computed) {
            let (outcome, request) = self.count_failure(request, &current).await?;
            return Ok((outcome, request, current));
        }
        let now = self.inner.environment().now();
        let request = VerificationRequest {
            status: VerificationRequestStatus::Completed,
            ..request
        };
        self.update_request(request.clone()).await?;
        let verification = Verification {
            organization_id: request.organization_id.clone(),
            status: VerificationStatus::Verified,
            method: Some(method.unwrap_or_else(|| request.method.clone())),
            verified_via: Some(request.channel_id.clone()),
            verified_at: Some(now),
            expires_at: Some(later(now, self.inner.verification.policy.ttl)?),
            completed_request_id: Some(request.id.clone()),
            ..Verification::new(request.identity_id.clone())
        };
        self.put_verification(verification.clone()).await?;
        Ok((Outcome::Completed, request, verification))
    }

    /// The request `request_id`, read under its identity's lock, with the
    /// identity's verification (Python's `_open_request`): its outcome
    /// `None` when it is open and in time; otherwise `NotOpen`, or `Expired`
    /// once its expiry is recorded and the identity settled.
    pub(super) async fn open_request_read(
        &self,
        request_id: &str,
    ) -> Result<(Option<Outcome>, VerificationRequest, Verification), RoomKitError> {
        let request = self
            .store()
            .get_verification_request(request_id)
            .await
            .map_err(|error| self.read_failed(error))?
            .ok_or_else(|| refused("not_found"))?;
        let current = self
            .stored_verification(&request.identity_id, request.organization_id.as_deref())
            .await?;
        if request.status != VerificationRequestStatus::Open {
            return Ok((Some(Outcome::NotOpen), request, current));
        }
        if lapsed(&request, self.inner.environment().now()) {
            let request = VerificationRequest {
                status: VerificationRequestStatus::Expired,
                ..request
            };
            self.update_request(request.clone()).await?;
            self.settle_pending(&current, &request).await?;
            return Ok((Some(Outcome::Expired), request, current));
        }
        Ok((None, request, current))
    }

    /// One failed attempt at `request`: `Mismatch`, or `Failed` when it used
    /// the last one, the identity settled (Python's `_count_failure`).
    pub(super) async fn count_failure(
        &self,
        request: VerificationRequest,
        current: &Verification,
    ) -> Result<(Outcome, VerificationRequest), RoomKitError> {
        let attempts = request.attempts.saturating_add(1);
        let failed = attempts >= request.max_attempts;
        let status = if failed {
            VerificationRequestStatus::Failed
        } else {
            VerificationRequestStatus::Open
        };
        let request = VerificationRequest {
            attempts,
            status,
            ..request
        };
        self.update_request(request.clone()).await?;
        if !failed {
            return Ok((Outcome::Mismatch, request));
        }
        self.settle_pending(current, &request).await?;
        Ok((Outcome::Failed, request))
    }

    /// Announces a request that ended without completing, expired or
    /// failed (Python's `_after_unfulfilled`). `before` is the identity's
    /// verification when the attempt began: a failed request that was its
    /// open one ended the verification in force too (fail closed), and the
    /// end says so.
    pub(super) async fn after_unfulfilled(
        &self,
        outcome: Outcome,
        request: &VerificationRequest,
        before: &Verification,
    ) {
        if !matches!(outcome, Outcome::Expired | Outcome::Failed) {
            return;
        }
        let mut ended = ENDED_REQUEST;
        if outcome == Outcome::Failed {
            // It may have ended a verification in force: no timer outlives it.
            let organization = request.organization_id.as_deref();
            self.inner
                .verification
                .disarm(&lock_key(&request.identity_id, organization));
            let now = self.inner.environment().now();
            if before.request_id.as_deref() == Some(request.id.as_str()) && in_force(before, now) {
                ended = ENDED_VERIFICATION;
            }
        }
        let reason = outcome.name();
        self.announce_request("verification_ended", request, Some((reason, ended)))
            .await;
        self.verification_hook(
            HookTrigger::OnVerificationEnded,
            request,
            end_data(reason, ended),
        )
        .await;
    }

    /// Settles the identity once its open request ended unfulfilled
    /// ([`settled`]): asking a verified person again, on a suspicion, and
    /// getting wrong secrets must not leave the old verification standing.
    async fn settle_pending(
        &self,
        current: &Verification,
        request: &VerificationRequest,
    ) -> Result<(), RoomKitError> {
        if current.request_id.as_deref() != Some(request.id.as_str()) {
            return Ok(());
        }
        let failed = request.status == VerificationRequestStatus::Failed;
        self.put_verification(settled(current, failed)).await
    }

    /// Cancels the identity's open request, if it has one, and returns it to
    /// announce: one past its expiry, whose timer did not run here, is
    /// recorded as expired, which is how it ended (Python's
    /// `_cancel_open_request`).
    async fn cancel_open_request(
        &self,
        current: &Verification,
    ) -> Result<Option<VerificationRequest>, RoomKitError> {
        let Some(request_id) = &current.request_id else {
            return Ok(None);
        };
        let request = self
            .store()
            .get_verification_request(request_id)
            .await
            .map_err(|error| self.read_failed(error))?;
        let Some(request) = request.filter(|r| r.status == VerificationRequestStatus::Open) else {
            return Ok(None);
        };
        self.inner
            .verification
            .disarm(&super::timers::request_key(&request.id));
        let status = if lapsed(&request, self.inner.environment().now()) {
            VerificationRequestStatus::Expired
        } else {
            VerificationRequestStatus::Cancelled
        };
        let ended = VerificationRequest { status, ..request };
        self.update_request(ended.clone()).await?;
        Ok(Some(ended))
    }

    /// The revocation of `identity_id`'s verification for `reason`, under
    /// its lock: the open request cancelled, a verification in force
    /// REVOKED, a PENDING identity with nothing open back to UNVERIFIED.
    pub(super) async fn revoke(
        &self,
        identity_id: &IdentityId,
        organization_id: Option<&str>,
        reason: &str,
    ) -> Result<Revoked, RoomKitError> {
        let now = self.inner.environment().now();
        let _held = self
            .lock_identity(&lock_key(identity_id, organization_id))
            .await?;
        let current = self
            .stored_verification(identity_id, organization_id)
            .await?;
        let completing = self.completing_request(&current).await?;
        let cancelled = self.cancel_open_request(&current).await?;
        if in_force(&current, now) {
            let ended = Verification {
                status: VerificationStatus::Revoked,
                ended_reason: Some(reason.to_owned()),
                request_id: None,
                ..current
            };
            self.put_verification(ended.clone()).await?;
            return Ok((Some(ended), completing, cancelled));
        }
        if cancelled.is_some() {
            // Pending, now with nothing open: back to unverified.
            self.put_verification(Verification {
                status: VerificationStatus::Unverified,
                request_id: None,
                ..current
            })
            .await?;
        }
        Ok((None, completing, cancelled))
    }

    /// Ends `verification` if it is still the one in force and its time
    /// passed, and returns it ended (Python's `_expire`).
    pub(super) async fn expire(
        &self,
        verification: &Verification,
    ) -> Result<Option<Verification>, RoomKitError> {
        let identity_id = &verification.identity_id;
        let organization = verification.organization_id.as_deref();
        let (ended, completing) = {
            let _held = self
                .lock_identity(&lock_key(identity_id, organization))
                .await?;
            let current = self.stored_verification(identity_id, organization).await?;
            let still_it = current.status == VerificationStatus::Verified
                && current.expires_at == verification.expires_at;
            if !still_it || in_force(&current, self.inner.environment().now()) {
                return Ok(None);
            }
            let completing = self.completing_request(&current).await?;
            let ended = Verification {
                status: VerificationStatus::Expired,
                ended_reason: Some("expired".to_owned()),
                ..current
            };
            self.put_verification(ended.clone()).await?;
            (ended, completing)
        };
        self.announce_verification_end(identity_id, "expired").await;
        if let Some(completing) = completing {
            let data = end_data("expired", ENDED_VERIFICATION);
            self.verification_hook(HookTrigger::OnVerificationEnded, &completing, data)
                .await;
        }
        Ok(Some(ended))
    }

    /// `current` as it stands once its open request, past its expiry, ended
    /// (Python's `_without_lapsed_request`): the timer that ends a request
    /// runs in the process that opened it, and one that restarted, or
    /// another process, finds it still open in the store.
    pub(super) async fn without_lapsed_request(
        &self,
        current: Verification,
    ) -> Result<Verification, RoomKitError> {
        let Some(request_id) = &current.request_id else {
            return Ok(current);
        };
        let request = self
            .store()
            .get_verification_request(request_id)
            .await
            .map_err(|error| self.read_failed(error))?;
        let now = self.inner.environment().now();
        let ended = request.is_some_and(|request| {
            request.status == VerificationRequestStatus::Open && lapsed(&request, now)
        });
        Ok(if ended {
            settled(&current, false)
        } else {
            current
        })
    }

    /// The request that completed the verification in force, if any.
    async fn completing_request(
        &self,
        current: &Verification,
    ) -> Result<Option<VerificationRequest>, RoomKitError> {
        let Some(request_id) = &current.completed_request_id else {
            return Ok(None);
        };
        if current.status != VerificationStatus::Verified {
            return Ok(None);
        }
        self.store()
            .get_verification_request(request_id)
            .await
            .map_err(|error| self.read_failed(error))
    }

    /// Waits for the lock of `key`, one identity's, among the kit's
    /// verification locks (RFC §13.5): apart from the room locks, so that a
    /// room named like the key never holds it, as Python's
    /// `self._verification_locks.locked(_lock_key(...))`.
    pub(super) async fn lock_identity(&self, key: &str) -> Result<HeldLock, RoomKitError> {
        let key = RoomId::new(key)?;
        self.inner.verification_locks.lock(&key).await
    }

    /// The identity's stored verification, UNVERIFIED when it has none.
    pub(super) async fn stored_verification(
        &self,
        identity_id: &IdentityId,
        organization_id: Option<&str>,
    ) -> Result<Verification, RoomKitError> {
        let options = GetVerificationOptions {
            organization_id: organization_id.map(str::to_owned),
        };
        let stored = self
            .store()
            .get_verification(identity_id, options)
            .await
            .map_err(|error| self.read_failed(error))?;
        Ok(stored.unwrap_or_else(|| Verification {
            organization_id: organization_id.map(str::to_owned),
            ..Verification::new(identity_id.clone())
        }))
    }

    async fn put_verification(&self, verification: Verification) -> Result<(), RoomKitError> {
        self.store()
            .put_verification(verification)
            .await
            .map(drop)
            .map_err(|error| self.read_failed(error))
    }

    async fn update_request(&self, request: VerificationRequest) -> Result<(), RoomKitError> {
        self.store()
            .update_verification_request(request)
            .await
            .map(drop)
            .map_err(|error| self.read_failed(error))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use roomkit_core::model::{ChannelId, IdentityId, RoomId};

    use crate::{CreateRoomOptions, RequestVerificationOptions, RoomKit, RoomKitOptions};

    // Python: tests/test_identity_verification.py::test_a_room_named_like_a_verification_key_holds_no_verification
    #[tokio::test]
    async fn a_room_named_like_a_verification_key_holds_no_verification() {
        let kit = RoomKit::new(RoomKitOptions::default());
        let room = RoomId::new("r1").unwrap();
        let create = CreateRoomOptions {
            room_id: Some(room.clone()),
            ..CreateRoomOptions::default()
        };
        kit.create_room(create).await.unwrap();
        let alice = IdentityId::new("alice").unwrap();
        let named = RoomId::new(super::lock_key(&alice, None)).unwrap();
        let section = kit.inner.locks.lock(&named).await.unwrap();
        let sms = ChannelId::new("sms").unwrap();
        let request =
            kit.request_verification(&alice, &room, &sms, RequestVerificationOptions::default());
        let (request, _) = tokio::time::timeout(Duration::from_secs(5), request)
            .await
            .expect("alice's verification waits on no room's lock")
            .unwrap();
        drop(section);
        assert_eq!(request.identity_id, alice);
        kit.close().await.unwrap();
    }
}
