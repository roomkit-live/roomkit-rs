//! What a verification and its requests read as at an instant, and how an
//! identity is left once its open request ended (RFC §11.7.2, §11.7.5): the
//! Python implementation's module functions of `core/mixins/verification.py`.

use roomkit_core::model::{
    IdentityId, Timestamp, Verification, VerificationRequest, VerificationRequestStatus,
    VerificationStatus,
};

/// What an end names (`ended`): the identity's verification in force ended,
/// by expiry, revocation, or a request that failed while it held.
pub(super) const ENDED_VERIFICATION: &str = "verification";
/// What an end names (`ended`): a request ended (expired, failed,
/// cancelled), the identity's verification left as it was or never reached.
pub(super) const ENDED_REQUEST: &str = "request";

/// How one attempt at a request, or the read of it, came out: Python's
/// outcome strings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Outcome {
    /// The secret matched: the identity is verified.
    Completed,
    /// The request is no longer open: completed, failed, expired or
    /// cancelled already.
    NotOpen,
    /// Its time ran out now, which this read recorded.
    Expired,
    /// A wrong secret, counted as an attempt.
    Mismatch,
    /// The wrong secret, or the failed check, that used up its attempts.
    Failed,
}

impl Outcome {
    /// Python's name of the outcome, the reason a refusal carries.
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::NotOpen => "not_open",
            Self::Expired => "expired",
            Self::Mismatch => "mismatch",
            Self::Failed => "failed",
        }
    }

    /// The reason an attempt is refused: `expired` for an expired request,
    /// whether this attempt or its timer recorded the expiry (Python's
    /// `_refusal`).
    pub(super) fn refusal(self, request: &VerificationRequest) -> &'static str {
        if request.status == VerificationRequestStatus::Expired {
            "expired"
        } else {
            self.name()
        }
    }
}

/// Whether `verification` is in force at `now`: VERIFIED, with an expiry
/// still to come.
pub(super) fn in_force(verification: &Verification, now: Timestamp) -> bool {
    verification.status == VerificationStatus::Verified
        && verification.expires_at.is_some_and(|expires| expires > now)
}

/// Whether `request`'s time to be answered has passed at `now`.
pub(super) fn lapsed(request: &VerificationRequest, now: Timestamp) -> bool {
    request.expires_at <= now
}

/// Whether a caller acting for `organization_id` sees `request` (RFC
/// §17.2): unscoped, any; scoped, its own organization's only.
pub(super) fn in_scope(request: &VerificationRequest, organization_id: Option<&str>) -> bool {
    organization_id
        .is_none_or(|organization| request.organization_id.as_deref() == Some(organization))
}

/// The key one identity's transitions are serialized under, Python's
/// `verification:<organization>:<identity>`.
pub(super) fn lock_key(identity_id: &IdentityId, organization_id: Option<&str>) -> String {
    format!(
        "verification:{}:{}",
        organization_id.unwrap_or_default(),
        identity_id.as_str()
    )
}

/// The identity `current` once its open request ended unfulfilled (Python's
/// `_settled`). An expired request proves nothing either way: the identity
/// is left as it was before it, PENDING back to UNVERIFIED. A failed one is
/// someone who could not prove it, so a verification in force ends with it
/// (fail closed).
pub(super) fn settled(current: &Verification, failed: bool) -> Verification {
    let mut settled = Verification {
        request_id: None,
        ..current.clone()
    };
    if failed {
        settled.status = VerificationStatus::Unverified;
        settled.ended_reason = Some("failed".to_owned());
    } else if current.status == VerificationStatus::Pending {
        settled.status = VerificationStatus::Unverified;
    }
    settled
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alice(status: VerificationStatus) -> Verification {
        Verification {
            status,
            request_id: Some("req-1".to_owned()),
            ..Verification::new(IdentityId::new("alice").unwrap())
        }
    }

    // Python: core/mixins/verification.py::_settled, an expired request.
    #[test]
    fn an_expired_request_leaves_the_identity_as_it_was() {
        let pending = settled(&alice(VerificationStatus::Pending), false);
        let verified = settled(&alice(VerificationStatus::Verified), false);
        assert_eq!(
            (pending.status, pending.request_id),
            (VerificationStatus::Unverified, None)
        );
        assert_eq!(
            (verified.status, verified.ended_reason),
            (VerificationStatus::Verified, None)
        );
    }

    // Python: core/mixins/verification.py::_settled, a failed request.
    #[test]
    fn a_failed_request_ends_the_verification_in_force() {
        let failed = settled(&alice(VerificationStatus::Verified), true);
        assert_eq!(
            (failed.status, failed.ended_reason.as_deref()),
            (VerificationStatus::Unverified, Some("failed"))
        );
    }

    // Python: core/mixins/verification.py::_lock_key.
    #[test]
    fn an_identity_s_lock_key_names_its_organization() {
        let alice = IdentityId::new("alice").unwrap();
        assert_eq!(lock_key(&alice, Some("org-a")), "verification:org-a:alice");
        assert_eq!(lock_key(&alice, None), "verification::alice");
    }
}
