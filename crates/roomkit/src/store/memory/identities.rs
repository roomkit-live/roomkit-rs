//! The identities an in-memory store keeps (RFC §5.6, §14.1, §17.2).
//!
//! An address belongs to one identity within an organization, and may
//! belong to another in each other organization (RFC §14.1, gap G-060), so
//! that resolving it is never a guess.

use std::collections::BTreeMap;

use roomkit_core::model::{ChannelType, Identity, IdentityId};

use crate::store::StoreError;

/// Every identity, by id.
#[derive(Debug, Default)]
pub(super) struct Identities {
    by_id: BTreeMap<IdentityId, Identity>,
}

impl Identities {
    /// Stores `identity`, unless its id is taken or another identity of its
    /// organization uses one of its addresses.
    pub(super) fn create(&mut self, identity: Identity) -> Result<(), StoreError> {
        if self.by_id.contains_key(&identity.id) {
            return Err(StoreError::IdentityExists(identity.id));
        }
        let organization = identity.organization_id.as_deref();
        for (channel_type, addresses) in &identity.channel_addresses {
            for address in addresses {
                self.check_free(organization, *channel_type, address, &identity.id)?;
            }
        }
        self.by_id.insert(identity.id.clone(), identity);
        Ok(())
    }

    /// The identity `identity_id`.
    pub(super) fn get(&self, identity_id: &IdentityId) -> Option<Identity> {
        self.by_id.get(identity_id).cloned()
    }

    /// The identity of `organization` that uses `address` on channels of
    /// `channel_type`.
    pub(super) fn resolve(
        &self,
        channel_type: ChannelType,
        address: &str,
        organization: Option<&str>,
    ) -> Option<Identity> {
        self.holder(organization, channel_type, address).cloned()
    }

    /// Links `address` to the identity `identity_id` of `organization`.
    pub(super) fn link(
        &mut self,
        identity_id: &IdentityId,
        channel_type: ChannelType,
        address: &str,
        organization: Option<&str>,
    ) -> Result<(), StoreError> {
        self.check_in(identity_id, organization)?;
        self.check_free(organization, channel_type, address, identity_id)?;
        if let Some(identity) = self.by_id.get_mut(identity_id) {
            identity.link(channel_type, address);
        }
        Ok(())
    }

    /// Removes `address` from the identity `identity_id` of `organization`.
    pub(super) fn unlink(
        &mut self,
        identity_id: &IdentityId,
        channel_type: ChannelType,
        address: &str,
        organization: Option<&str>,
    ) -> Result<(), StoreError> {
        self.check_in(identity_id, organization)?;
        if let Some(identity) = self.by_id.get_mut(identity_id) {
            identity.unlink(channel_type, address);
        }
        Ok(())
    }

    /// Refuses `identity_id` unless it is an identity of `organization`.
    fn check_in(
        &self,
        identity_id: &IdentityId,
        organization: Option<&str>,
    ) -> Result<(), StoreError> {
        let in_organization = self
            .by_id
            .get(identity_id)
            .is_some_and(|identity| identity.organization_id.as_deref() == organization);
        if in_organization {
            Ok(())
        } else {
            Err(StoreError::IdentityNotFound(identity_id.clone()))
        }
    }

    /// Refuses `address` when an identity of `organization` other than
    /// `owner` uses it.
    fn check_free(
        &self,
        organization: Option<&str>,
        channel_type: ChannelType,
        address: &str,
        owner: &IdentityId,
    ) -> Result<(), StoreError> {
        match self.holder(organization, channel_type, address) {
            Some(holder) if &holder.id != owner => Err(StoreError::AddressLinked {
                channel_type,
                address: address.to_owned(),
                identity_id: holder.id.clone(),
            }),
            Some(_) | None => Ok(()),
        }
    }

    /// The identity of `organization` that uses `address`.
    fn holder(
        &self,
        organization: Option<&str>,
        channel_type: ChannelType,
        address: &str,
    ) -> Option<&Identity> {
        self.by_id.values().find(|identity| {
            identity.organization_id.as_deref() == organization
                && identity.uses(channel_type, address)
        })
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    const ALICE: &str = "+15550000001";

    fn identity(id: &str, organization: Option<&str>) -> Identity {
        let mut identity = Identity::new(
            IdentityId::new(id).unwrap(),
            organization.map(str::to_owned),
        );
        identity.link(ChannelType::Sms, ALICE);
        identity
    }

    fn resolved(book: &Identities, organization: Option<&str>) -> Option<String> {
        book.resolve(ChannelType::Sms, ALICE, organization)
            .map(|identity| identity.id.to_string())
    }

    #[test]
    fn an_address_resolves_to_the_identity_of_the_caller_s_organization() {
        let mut book = Identities::default();
        book.create(identity("id_bank", Some("bank"))).unwrap();
        book.create(identity("id_clinic", Some("clinic"))).unwrap();
        assert_eq!(
            resolved(&book, Some("clinic")),
            Some("id_clinic".to_owned())
        );
    }

    #[test]
    fn an_identity_of_an_organization_does_not_resolve_for_a_caller_naming_none() {
        let mut book = Identities::default();
        book.create(identity("id_bank", Some("bank"))).unwrap();
        assert_eq!(resolved(&book, None), None);
    }

    #[test]
    fn an_identity_of_no_organization_resolves_for_a_caller_naming_none() {
        let mut book = Identities::default();
        book.create(identity("id_alice", None)).unwrap();
        assert_eq!(resolved(&book, None), Some("id_alice".to_owned()));
    }

    #[test]
    fn an_address_on_another_channel_type_does_not_resolve() {
        let mut book = Identities::default();
        book.create(identity("id_alice", None)).unwrap();
        assert_eq!(book.resolve(ChannelType::Whatsapp, ALICE, None), None);
    }

    /// RFC §14.1: the same address may belong to a different identity in
    /// each organization, and to one only within an organization.
    #[test]
    fn an_address_another_identity_of_the_organization_uses_is_refused() {
        let mut book = Identities::default();
        book.create(identity("id_one", Some("bank"))).unwrap();
        assert_eq!(
            book.create(identity("id_two", Some("bank"))),
            Err(StoreError::AddressLinked {
                channel_type: ChannelType::Sms,
                address: ALICE.to_owned(),
                identity_id: IdentityId::new("id_one").unwrap(),
            })
        );
    }

    #[test]
    fn an_identity_id_cannot_be_created_twice() {
        let mut book = Identities::default();
        book.create(identity("id_alice", None)).unwrap();
        assert_eq!(
            book.create(Identity::new(IdentityId::new("id_alice").unwrap(), None)),
            Err(StoreError::IdentityExists(
                IdentityId::new("id_alice").unwrap()
            ))
        );
    }

    #[test]
    fn a_linked_address_resolves_to_its_identity() {
        let mut book = Identities::default();
        let id = IdentityId::new("id_jean").unwrap();
        book.create(Identity::new(id.clone(), None)).unwrap();
        book.link(&id, ChannelType::Sms, ALICE, None).unwrap();
        assert_eq!(resolved(&book, None), Some("id_jean".to_owned()));
    }

    #[test]
    fn linking_to_an_identity_of_another_organization_is_refused() {
        let mut book = Identities::default();
        let id = IdentityId::new("id_jean").unwrap();
        book.create(Identity::new(id.clone(), Some("bank".to_owned())))
            .unwrap();
        assert_eq!(
            book.link(&id, ChannelType::Sms, ALICE, Some("clinic")),
            Err(StoreError::IdentityNotFound(id))
        );
    }

    /// RFC §5.6: moving an address is an unlink, then a link.
    #[test]
    fn an_unlinked_address_can_be_linked_to_another_identity() {
        let mut book = Identities::default();
        book.create(identity("id_one", None)).unwrap();
        let one = IdentityId::new("id_one").unwrap();
        let two = IdentityId::new("id_two").unwrap();
        book.create(Identity::new(two.clone(), None)).unwrap();
        book.unlink(&one, ChannelType::Sms, ALICE, None).unwrap();
        book.link(&two, ChannelType::Sms, ALICE, None).unwrap();
        assert_eq!(resolved(&book, None), Some("id_two".to_owned()));
    }

    #[test]
    fn an_unlinked_address_resolves_to_no_one() {
        let mut book = Identities::default();
        book.create(identity("id_one", None)).unwrap();
        let one = IdentityId::new("id_one").unwrap();
        book.unlink(&one, ChannelType::Sms, ALICE, None).unwrap();
        assert_eq!(resolved(&book, None), None);
    }

    #[test]
    fn unlinking_an_address_the_identity_does_not_use_changes_nothing() {
        let mut book = Identities::default();
        book.create(identity("id_one", None)).unwrap();
        let one = IdentityId::new("id_one").unwrap();
        book.unlink(&one, ChannelType::Whatsapp, ALICE, None)
            .unwrap();
        assert_eq!(resolved(&book, None), Some("id_one".to_owned()));
    }

    #[test]
    fn unlinking_from_an_identity_of_another_organization_is_refused() {
        let mut book = Identities::default();
        book.create(identity("id_bank", Some("bank"))).unwrap();
        let bank = IdentityId::new("id_bank").unwrap();
        assert_eq!(
            book.unlink(&bank, ChannelType::Sms, ALICE, Some("clinic")),
            Err(StoreError::IdentityNotFound(bank))
        );
    }

    #[test]
    fn unlinking_from_an_unknown_identity_is_refused() {
        let mut book = Identities::default();
        let ghost = IdentityId::new("id_ghost").unwrap();
        assert_eq!(
            book.unlink(&ghost, ChannelType::Sms, ALICE, None),
            Err(StoreError::IdentityNotFound(ghost))
        );
    }

    #[test]
    fn linking_an_address_another_identity_of_the_organization_uses_is_refused() {
        let mut book = Identities::default();
        book.create(identity("id_one", None)).unwrap();
        let two = IdentityId::new("id_two").unwrap();
        book.create(Identity::new(two.clone(), None)).unwrap();
        assert!(matches!(
            book.link(&two, ChannelType::Sms, ALICE, None),
            Err(StoreError::AddressLinked { .. })
        ));
    }
}
