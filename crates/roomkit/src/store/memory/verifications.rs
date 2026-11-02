//! The verifications and verification requests an in-memory store keeps
//! (RFC §11.7, §14.1), as Python's `InMemoryStore` keeps them.

use std::collections::BTreeMap;

use roomkit_core::model::{IdentityId, Verification, VerificationRequest, VerificationStatus};

/// Every verification, by its identity and its organization (none being the
/// empty string, as Python keys it), and every request, by id.
#[derive(Debug, Default)]
pub(super) struct Verifications {
    by_identity: BTreeMap<(IdentityId, String), Verification>,
    requests: BTreeMap<String, VerificationRequest>,
}

impl Verifications {
    /// The verification of `identity_id` in `organization`.
    pub(super) fn get(
        &self,
        identity_id: &IdentityId,
        organization: Option<&str>,
    ) -> Option<Verification> {
        let key = (
            identity_id.clone(),
            organization.unwrap_or_default().to_owned(),
        );
        self.by_identity.get(&key).cloned()
    }

    /// Stores `verification`, replacing the one its identity holds in its
    /// organization.
    pub(super) fn put(&mut self, verification: Verification) {
        let key = (
            verification.identity_id.clone(),
            verification.organization_id.clone().unwrap_or_default(),
        );
        self.by_identity.insert(key, verification);
    }

    /// Every verification VERIFIED with an expiry.
    pub(super) fn in_force(&self) -> Vec<Verification> {
        self.by_identity
            .values()
            .filter(|verification| {
                verification.status == VerificationStatus::Verified
                    && verification.expires_at.is_some()
            })
            .cloned()
            .collect()
    }

    /// Stores `request` under its id, replacing the one stored there, as
    /// Python's store does for a creation and an update alike.
    pub(super) fn save_request(&mut self, request: VerificationRequest) {
        self.requests.insert(request.id.clone(), request);
    }

    /// The request `request_id`.
    pub(super) fn request(&self, request_id: &str) -> Option<VerificationRequest> {
        self.requests.get(request_id).cloned()
    }
}
