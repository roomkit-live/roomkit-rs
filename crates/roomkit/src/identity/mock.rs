//! A resolver that answers from a mapping, for tests: the Python
//! implementation's `MockIdentityResolver`.

use std::collections::BTreeMap;

use roomkit_core::model::{
    IdentificationStatus, Identity, IdentityResult, InboundMessage, RoomContext,
};

use super::{IdentityFuture, IdentityResolver};

/// What [`MockIdentityResolver::new`] takes, Python's
/// `MockIdentityResolver(mapping=None, ambiguous=None,
/// unknown_status=IdentificationStatus.UNKNOWN)`.
#[derive(Clone, Debug, PartialEq)]
pub struct MockIdentityResolverOptions {
    /// The identity of each sender it identifies, by sender id.
    pub mapping: BTreeMap<String, Identity>,
    /// The candidates of each sender it finds ambiguous, by sender id.
    pub ambiguous: BTreeMap<String, Vec<Identity>>,
    /// What it answers for any other sender: `UNKNOWN` by default.
    pub unknown_status: IdentificationStatus,
}

impl Default for MockIdentityResolverOptions {
    fn default() -> Self {
        Self {
            mapping: BTreeMap::new(),
            ambiguous: BTreeMap::new(),
            unknown_status: IdentificationStatus::Unknown,
        }
    }
}

/// Resolves a sender from a pre-configured mapping: a sender id the mapping
/// holds is identified, one the ambiguous mapping holds is ambiguous
/// between its candidates, any other gets the unknown status.
///
/// ```
/// use std::collections::BTreeMap;
///
/// use roomkit::identity::{MockIdentityResolver, MockIdentityResolverOptions};
/// use roomkit::model::{Identity, IdentityId};
///
/// let alice = Identity::new(IdentityId::new("alice")?);
/// let resolver = MockIdentityResolver::new(MockIdentityResolverOptions {
///     mapping: BTreeMap::from([("+15551234567".to_owned(), alice)]),
///     ..Default::default()
/// });
/// # let _ = resolver;
/// # Ok::<(), roomkit::model::ModelError>(())
/// ```
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MockIdentityResolver {
    options: MockIdentityResolverOptions,
}

impl MockIdentityResolver {
    /// A resolver answering as `options` say.
    pub fn new(options: MockIdentityResolverOptions) -> Self {
        Self { options }
    }

    /// What the resolver answers for `sender_id`: an exact match first,
    /// then its candidates, then the unknown status.
    fn answer(&self, sender_id: &str) -> IdentityResult {
        if let Some(identity) = self.options.mapping.get(sender_id) {
            return IdentityResult {
                identity: Some(identity.clone()),
                ..IdentityResult::new(IdentificationStatus::Identified)
            };
        }
        match self.options.ambiguous.get(sender_id) {
            Some(candidates) if !candidates.is_empty() => IdentityResult {
                candidates: candidates.clone(),
                ..IdentityResult::new(IdentificationStatus::Ambiguous)
            },
            Some(_) | None => IdentityResult::new(self.options.unknown_status),
        }
    }
}

impl IdentityResolver for MockIdentityResolver {
    fn resolve<'a>(
        &'a self,
        message: &'a InboundMessage,
        _context: &'a RoomContext,
    ) -> IdentityFuture<'a> {
        let answer = self.answer(&message.sender_id);
        Box::pin(async move { Ok(answer) })
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use roomkit_core::model::{ChannelId, IdentityId, Room, RoomId, TextContent};

    use super::*;

    fn identity(id: &str, name: &str) -> Identity {
        Identity {
            display_name: Some(name.to_owned()),
            ..Identity::new(IdentityId::new(id).unwrap())
        }
    }

    async fn resolve(resolver: &MockIdentityResolver, sender: &str) -> IdentityResult {
        let message = InboundMessage::new(
            ChannelId::new("ch1").unwrap(),
            sender,
            TextContent::new("hi"),
        );
        let context = RoomContext::new(Room::new(RoomId::new("r1").unwrap()));
        resolver.resolve(&message, &context).await.unwrap()
    }

    // Python: tests/test_identity_pipeline.py::TestMockResolverEnhanced::test_ambiguous_result
    #[tokio::test]
    async fn ambiguous_result() {
        let resolver = MockIdentityResolver::new(MockIdentityResolverOptions {
            ambiguous: BTreeMap::from([(
                "user1".to_owned(),
                vec![identity("a1", "Alice"), identity("b1", "Bob")],
            )]),
            ..Default::default()
        });
        let result = resolve(&resolver, "user1").await;
        assert_eq!(
            (result.status, result.candidates.len()),
            (IdentificationStatus::Ambiguous, 2)
        );
    }

    // Python: tests/test_identity_pipeline.py::TestMockResolverEnhanced::test_unknown_status_configurable
    #[tokio::test]
    async fn unknown_status_configurable() {
        let resolver = MockIdentityResolver::new(MockIdentityResolverOptions {
            unknown_status: IdentificationStatus::Pending,
            ..Default::default()
        });
        assert_eq!(
            resolve(&resolver, "nobody").await.status,
            IdentificationStatus::Pending
        );
    }

    // Python: tests/test_identity_pipeline.py::TestMockResolverEnhanced::test_exact_match_takes_priority
    #[tokio::test]
    async fn exact_match_takes_priority() {
        let alice = identity("a1", "Alice");
        let resolver = MockIdentityResolver::new(MockIdentityResolverOptions {
            mapping: BTreeMap::from([("user1".to_owned(), alice.clone())]),
            ambiguous: BTreeMap::from([(
                "user1".to_owned(),
                vec![alice.clone(), identity("b1", "Bob")],
            )]),
            ..Default::default()
        });
        let result = resolve(&resolver, "user1").await;
        assert_eq!(
            (result.status, result.identity),
            (IdentificationStatus::Identified, Some(alice))
        );
    }
}
