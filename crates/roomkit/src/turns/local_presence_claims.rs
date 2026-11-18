//! Presence changes claimed by a transport serving one process.
use std::collections::BTreeMap;
use std::sync::Mutex;

/// The default presence state for a single-process turn backend (RFC §10.6.7).
/// Hold one instance per backend. A distributed backend implements the same
/// operation in the store shared by its room owners.
#[derive(Debug, Default)]
pub(super) struct LocalPresenceClaims(Mutex<BTreeMap<(String, String), bool>>);

impl LocalPresenceClaims {
    /// Claim a transition once. Forgetting a detached room/channel pair returns false.
    pub(super) fn claim(&self, channel_id: &str, room_id: &str, online: Option<bool>) -> bool {
        let mut claims = super::lock(&self.0);
        let key = (channel_id.to_owned(), room_id.to_owned());
        if let Some(value) = online {
            claims.insert(key, value) != Some(value)
        } else {
            claims.remove(&key);
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::LocalPresenceClaims;

    #[test]
    fn claims_are_per_room_and_channel_and_detach_allows_the_next_attach() {
        let claims = LocalPresenceClaims::default();
        assert!(claims.claim("alice", "one", Some(true)));
        assert!(!claims.claim("alice", "one", Some(true)));
        assert!(claims.claim("alice", "two", Some(true)));
        assert!(claims.claim("bob", "one", Some(true)));
        assert!(claims.claim("alice", "one", Some(false)));
        assert!(!claims.claim("alice", "one", None));
        assert!(claims.claim("alice", "one", Some(false)));
        assert!(!claims.claim("bob", "one", Some(true)));
    }
}
