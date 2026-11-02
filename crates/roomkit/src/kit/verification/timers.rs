//! The ends that come when their time runs out (RFC §11.7.5): a
//! verification's at its `expires_at`, an unanswered request's at its own,
//! each by a timer this process arms, announced when it happens rather than
//! on the sender's next message. A timer holds the kit weakly, and the
//! kit's close aborts them all.

use std::time::Duration;

use roomkit_core::model::{Timestamp, Verification, VerificationRequest};

use super::state::lock_key;
use crate::error::RoomKitError;
use crate::kit::RoomKit;
use crate::store::ConversationStore;

/// The key of a request's expiry timer, beside the identities' timers
/// (Python's `_REQUEST_TIMER`).
pub(super) fn request_key(request_id: &str) -> String {
    format!("request:{request_id}")
}

impl<S: ConversationStore> RoomKit<S> {
    /// Resumes the verifications in force as the kit starts (Python's
    /// `_resume_verifications`). A store that does not keep verifications
    /// (RFC §11.7 is Level 2) has none to resume: the kit starts all the
    /// same. So does one that cannot answer yet: a verification whose time
    /// passed reads EXPIRED all the same, only its end is announced late, at
    /// the next [`check_verifications`](Self::check_verifications).
    pub(crate) async fn resume_verifications(&self) {
        match self.check_verifications().await {
            // The store's refusal by name, Python's `NotImplementedError`.
            Ok(_) | Err(RoomKitError::Value(_)) => {}
            Err(error) => {
                tracing::warn!(
                    target: "roomkit::framework",
                    %error,
                    "Verifications were not resumed: the store did not answer"
                );
            }
        }
    }

    /// Arms, or re-arms, the timer that ends `verification` when it
    /// expires (Python's `_arm_expiry`).
    pub(super) fn arm_expiry(&self, verification: &Verification) {
        let Some(expires_at) = verification.expires_at else {
            return;
        };
        if self.inner.tasks.is_closing() {
            return;
        }
        let key = lock_key(
            &verification.identity_id,
            verification.organization_id.as_deref(),
        );
        let delay = self.delay_until(expires_at);
        let (kit, verification) = (self.downgrade(), verification.clone());
        let fired = key.clone();
        self.inner.verification.arm(key, move |generation| async move {
            tokio::time::sleep(delay).await;
            let Some(kit) = kit.upgrade() else {
                return;
            };
            kit.inner.verification.fired(&fired, generation);
            if let Err(error) = kit.expire(&verification).await {
                tracing::warn!(target: "roomkit::framework", %error, "a verification's expiry failed");
            }
        });
    }

    /// Arms the timer that ends `request` if it is still open at its expiry
    /// (Python's `_arm_request_expiry`).
    pub(super) fn arm_request_expiry(&self, request: &VerificationRequest) {
        if self.inner.tasks.is_closing() {
            return;
        }
        let key = request_key(&request.id);
        let delay = self.delay_until(request.expires_at);
        let (kit, request) = (self.downgrade(), request.clone());
        let fired = key.clone();
        self.inner
            .verification
            .arm(key, move |generation| async move {
                tokio::time::sleep(delay).await;
                let Some(kit) = kit.upgrade() else {
                    return;
                };
                kit.inner.verification.fired(&fired, generation);
                kit.end_request_when_due(&request).await;
            });
    }

    /// Ends `request`, unanswered at its expiry (Python's
    /// `_end_request_when_due`): nothing for a request completed, ended or
    /// gone from the store meanwhile.
    async fn end_request_when_due(&self, request: &VerificationRequest) {
        let key = lock_key(&request.identity_id, request.organization_id.as_deref());
        let read = {
            match self.lock_identity(&key).await {
                Ok(_held) => self.open_request_read(&request.id).await,
                Err(error) => Err(error),
            }
        };
        match read {
            Ok((Some(outcome), ended, before)) => {
                self.after_unfulfilled(outcome, &ended, &before).await;
            }
            Ok((None, _, _)) | Err(RoomKitError::VerificationRefused { .. }) => {}
            Err(error) => {
                tracing::warn!(target: "roomkit::framework", %error, "a request's expiry failed");
            }
        }
    }

    /// How long until `instant` on the kit's clock; nothing for an instant
    /// past.
    fn delay_until(&self, instant: Timestamp) -> Duration {
        let now = self.inner.environment().now();
        instant.duration_since(now).unwrap_or_default()
    }
}
