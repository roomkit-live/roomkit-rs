//! A person known across channels (RFC §5.6).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{ChannelType, IdentityId, Metadata};

/// A person known across channels, by the addresses they use on each type
/// of channel (RFC §5.6).
///
/// Identities are scoped by organization (RFC §17.2): the same address may
/// belong to a different identity in each organization (RFC §14.1), and to
/// one only within an organization (gap G-060).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identity {
    /// The identity's id.
    pub id: IdentityId,
    /// The organization it belongs to, `None` for an identity of none.
    #[serde(default)]
    pub organization_id: Option<String>,
    /// The human-readable name.
    #[serde(default)]
    pub display_name: Option<String>,
    /// The person's email address.
    #[serde(default)]
    pub email: Option<String>,
    /// The person's phone number.
    #[serde(default)]
    pub phone: Option<String>,
    /// The addresses it uses, by channel type: `{sms: ["+15551234"], email:
    /// ["john@example.com"]}`.
    #[serde(default)]
    pub channel_addresses: BTreeMap<ChannelType, Vec<String>>,
    /// A CRM's or another system's reference.
    #[serde(default)]
    pub external_id: Option<String>,
    /// Other systems' references, by system.
    #[serde(default)]
    pub external_ids: BTreeMap<String, String>,
    /// Integrator-defined data.
    #[serde(default)]
    pub metadata: Metadata,
}

impl Identity {
    /// An identity of no organization, with no name and no address yet. A
    /// caller sets the other fields by struct update.
    pub fn new(id: IdentityId) -> Self {
        Self {
            id,
            organization_id: None,
            display_name: None,
            email: None,
            phone: None,
            channel_addresses: BTreeMap::new(),
            external_id: None,
            external_ids: BTreeMap::new(),
            metadata: Metadata::new(),
        }
    }

    /// Whether the identity uses `address` on channels of `channel_type`,
    /// compared as written. Hidden: the stores use it, and Python has no
    /// such helper.
    #[doc(hidden)]
    pub fn uses(&self, channel_type: &ChannelType, address: &str) -> bool {
        self.channel_addresses
            .get(channel_type)
            .is_some_and(|addresses| addresses.iter().any(|linked| linked == address))
    }

    /// Links `address` on channels of `channel_type` to the identity; an
    /// address linked already is kept once. Hidden, as `uses` is.
    #[doc(hidden)]
    pub fn link(&mut self, channel_type: ChannelType, address: &str) {
        if self.uses(&channel_type, address) {
            return;
        }
        self.channel_addresses
            .entry(channel_type)
            .or_default()
            .push(address.to_owned());
    }

    /// Removes `address` on channels of `channel_type` from the identity;
    /// an address it does not use changes nothing. A channel type left
    /// with no address is dropped. Hidden, as `uses` is.
    #[doc(hidden)]
    pub fn unlink(&mut self, channel_type: &ChannelType, address: &str) {
        let Some(addresses) = self.channel_addresses.get_mut(channel_type) else {
            return;
        };
        addresses.retain(|linked| linked != address);
        if addresses.is_empty() {
            self.channel_addresses.remove(channel_type);
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    fn jean() -> Identity {
        Identity {
            organization_id: Some("bank".to_owned()),
            ..Identity::new(IdentityId::new("id_jean").unwrap())
        }
    }

    #[test]
    fn a_linked_address_is_used_on_its_channel_type_only() {
        let mut identity = jean();
        identity.link(ChannelType::SMS, "+15551234567");
        assert_eq!(
            (
                identity.uses(&ChannelType::SMS, "+15551234567"),
                identity.uses(&ChannelType::WHATSAPP, "+15551234567")
            ),
            (true, false)
        );
    }

    #[test]
    fn an_address_linked_twice_is_kept_once() {
        let mut identity = jean();
        identity.link(ChannelType::SMS, "+15551234567");
        identity.link(ChannelType::SMS, "+15551234567");
        assert_eq!(
            serde_json::to_value(&identity.channel_addresses).unwrap(),
            json!({ "sms": ["+15551234567"] })
        );
    }

    #[test]
    fn an_unlinked_address_leaves_the_other_addresses_of_its_type() {
        let mut identity = jean();
        identity.link(ChannelType::SMS, "+15551234567");
        identity.link(ChannelType::SMS, "+15557654321");
        identity.unlink(&ChannelType::SMS, "+15551234567");
        assert_eq!(
            serde_json::to_value(&identity.channel_addresses).unwrap(),
            json!({ "sms": ["+15557654321"] })
        );
    }

    #[test]
    fn a_channel_type_left_with_no_address_is_dropped() {
        let mut identity = jean();
        identity.link(ChannelType::SMS, "+15551234567");
        identity.unlink(&ChannelType::SMS, "+15551234567");
        assert_eq!(
            serde_json::to_value(&identity.channel_addresses).unwrap(),
            json!({})
        );
    }
}
