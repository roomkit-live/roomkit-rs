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
    /// Stores `identity`, replacing the one stored under its id, as
    /// Python's store does, unless another identity of its organization
    /// uses one of its addresses.
    pub(super) fn create(&mut self, identity: Identity) -> Result<(), StoreError> {
        let organization = identity.organization_id.as_deref();
        for (channel_type, addresses) in &identity.channel_addresses {
            for address in addresses {
                self.check_free(organization, channel_type, address, &identity.id)?;
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
        channel_type: &ChannelType,
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
        self.check_free(organization, &channel_type, address, identity_id)?;
        if let Some(identity) = self.by_id.get_mut(identity_id) {
            identity.link(channel_type, address);
        }
        Ok(())
    }

    /// Removes `address` from the identity `identity_id` of `organization`.
    pub(super) fn unlink(
        &mut self,
        identity_id: &IdentityId,
        channel_type: &ChannelType,
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
        channel_type: &ChannelType,
        address: &str,
        owner: &IdentityId,
    ) -> Result<(), StoreError> {
        match self.holder(organization, channel_type, address) {
            Some(holder) if &holder.id != owner => Err(StoreError::AddressLinked {
                channel_type: channel_type.clone(),
            }),
            Some(_) | None => Ok(()),
        }
    }

    /// The identity of `organization` that uses `address`.
    fn holder(
        &self,
        organization: Option<&str>,
        channel_type: &ChannelType,
        address: &str,
    ) -> Option<&Identity> {
        self.by_id.values().find(|identity| {
            identity.organization_id.as_deref() == organization
                && identity.uses(channel_type, address)
        })
    }
}
