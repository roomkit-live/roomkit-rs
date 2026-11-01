//! A person known across channels (RFC §5.6), and what identity resolution
//! says of a sender (RFC §11.1, §11.3).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{ChannelType, IdentificationStatus, IdentityId, InjectedEvent, Metadata};

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

/// What an identity resolver says of a message's sender (RFC §11.1), the
/// Python implementation's `IdentityResult`.
///
/// The pipeline fills `address` with the message's sender and
/// `channel_type` with its channel's type where the resolver left them
/// unset, before the identity hooks read it (RFC §11.2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentityResult {
    /// Where resolution stands: identified, ambiguous, pending, unknown,
    /// challenge sent or rejected.
    pub status: IdentificationStatus,
    /// The sender, when identified.
    #[serde(default)]
    pub identity: Option<Identity>,
    /// The identities the sender may be, when ambiguous.
    #[serde(default)]
    pub candidates: Vec<Identity>,
    /// The address resolved.
    #[serde(default)]
    pub address: Option<String>,
    /// The type of the channel the address was used on.
    #[serde(default)]
    pub channel_type: Option<ChannelType>,
    /// The kind of challenge a resolver put to the sender.
    #[serde(default)]
    pub challenge_type: Option<String>,
    /// The resolver's words: a challenge's are the reason the message is
    /// blocked with (RFC §10.1 step 5).
    #[serde(default)]
    pub message: Option<String>,
}

impl IdentityResult {
    /// A result of `status` and nothing else, Python's
    /// `IdentityResult(status=…)`: a caller sets the other fields by struct
    /// update.
    pub fn new(status: IdentificationStatus) -> Self {
        Self {
            status,
            identity: None,
            candidates: Vec::new(),
            address: None,
            channel_type: None,
            challenge_type: None,
            message: None,
        }
    }
}

/// What an identity hook answers (RFC §11.3), the Python implementation's
/// `IdentityHookResult`.
///
/// ```
/// use roomkit_core::model::identity::RejectOptions;
/// use roomkit_core::model::{IdentificationStatus, IdentityHookResult};
///
/// let result = IdentityHookResult::reject(RejectOptions::default());
/// assert_eq!(result.status, IdentificationStatus::Rejected);
/// assert_eq!(result.reason.as_deref(), Some("Unknown sender"));
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IdentityHookResult {
    /// The outcome: identified, pending, challenge sent or rejected.
    pub status: IdentificationStatus,
    /// The sender, for an identified outcome.
    #[serde(default)]
    pub identity: Option<Identity>,
    /// A pending sender's name; not read by the framework.
    #[serde(default)]
    pub display_name: Option<String>,
    /// The identities a pending sender may be, read on
    /// `ON_IDENTITY_UNKNOWN` only.
    #[serde(default)]
    pub candidates: Option<Vec<Identity>>,
    /// A challenge's event, committed as an injected event (RFC §9.4).
    #[serde(default)]
    pub inject: Option<InjectedEvent>,
    /// A rejection's reason, the block's.
    #[serde(default)]
    pub reason: Option<String>,
    /// The kind of challenge; not read by the framework.
    #[serde(default)]
    pub challenge_type: Option<String>,
    /// A challenge's words; not read by the framework.
    #[serde(default)]
    pub message: Option<String>,
}

/// What [`IdentityHookResult::pending`] takes: Python's keywords.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PendingOptions {
    /// The sender's name, as far as the hook knows it.
    pub display_name: Option<String>,
    /// The identities the sender may be.
    pub candidates: Option<Vec<Identity>>,
}

/// What [`IdentityHookResult::challenge`] takes beside its event: Python's
/// keywords.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ChallengeOptions {
    /// The challenge's words.
    pub message: Option<String>,
}

/// What [`IdentityHookResult::reject`] takes: Python's keywords.
#[derive(Clone, Debug, PartialEq)]
pub struct RejectOptions {
    /// Why the sender is refused, the block's reason: Python's `reason="Unknown sender"`.
    pub reason: String,
}

impl Default for RejectOptions {
    fn default() -> Self {
        Self {
            reason: "Unknown sender".to_owned(),
        }
    }
}

impl IdentityHookResult {
    /// A result of `status` and nothing else, Python's
    /// `IdentityHookResult(status=…)`: a caller sets the other fields by
    /// struct update.
    pub fn new(status: IdentificationStatus) -> Self {
        Self {
            status,
            identity: None,
            display_name: None,
            candidates: None,
            inject: None,
            reason: None,
            challenge_type: None,
            message: None,
        }
    }

    /// The sender is `identity` (RFC §11.3).
    pub fn resolved(identity: Identity) -> Self {
        Self {
            identity: Some(identity),
            ..Self::new(IdentificationStatus::Identified)
        }
    }

    /// The sender joins pending, to be resolved later (RFC §11.3).
    pub fn pending(options: PendingOptions) -> Self {
        Self {
            display_name: options.display_name,
            candidates: options.candidates,
            ..Self::new(IdentificationStatus::Pending)
        }
    }

    /// The message is held and `inject`, the question put to the sender,
    /// committed (RFC §11.3, §9.4).
    pub fn challenge(inject: InjectedEvent, options: ChallengeOptions) -> Self {
        Self {
            inject: Some(inject),
            message: options.message,
            ..Self::new(IdentificationStatus::ChallengeSent)
        }
    }

    /// The message is refused, with the options' reason (RFC §11.3).
    pub fn reject(options: RejectOptions) -> Self {
        Self {
            reason: Some(options.reason),
            ..Self::new(IdentificationStatus::Rejected)
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
