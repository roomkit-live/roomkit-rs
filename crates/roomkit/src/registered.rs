//! What a router and a caller read of a registered channel
//! ([`RoomKit::get_channel`](crate::RoomKit::get_channel)): its synchronous
//! members, as the kit holds them.

use std::fmt::Debug;

use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelType,
};
use roomkit_core::ops::ChannelDeclaration;
use roomkit_core::rules::addresses::AddressForm;

/// A registered channel as the kit holds it, an object a trait with
/// `async` methods cannot be: its id, type, category and direction, and how
/// it writes the addresses a router compares (RFC §5.7). Only the kit makes
/// one.
pub trait RegisteredChannel: sealed::Sealed + Debug + Send + Sync {
    /// The channel's id.
    fn channel_id(&self) -> &ChannelId;

    /// Its type.
    fn channel_type(&self) -> &ChannelType;

    /// What it does.
    fn category(&self) -> ChannelCategory;

    /// What it can do physically.
    fn direction(&self) -> ChannelDirection;

    /// The binding metadata key it delivers to (RFC §5.7), if any.
    fn recipient_key(&self) -> Option<&str>;

    /// `address` in the channel's form (RFC §5.7): a phone number in E.164,
    /// an email address lower-case.
    fn normalize_address(&self, address: &str) -> String;

    /// The recipient `binding` delivers to, in the channel's form: its value
    /// under the recipient key, when it is one.
    fn recipient_address(&self, binding: &ChannelBinding) -> Option<String>;
}

/// The kit's own view: a channel's declaration.
#[derive(Debug)]
pub(crate) struct Registered(pub(crate) ChannelDeclaration);

impl RegisteredChannel for Registered {
    fn channel_id(&self) -> &ChannelId {
        &self.0.channel_id
    }

    fn channel_type(&self) -> &ChannelType {
        &self.0.channel_type
    }

    fn category(&self) -> ChannelCategory {
        self.0.category
    }

    fn direction(&self) -> ChannelDirection {
        self.0.direction
    }

    fn recipient_key(&self) -> Option<&str> {
        self.0.recipient_key.as_deref()
    }

    fn normalize_address(&self, address: &str) -> String {
        self.0.form().normalize(address)
    }

    fn recipient_address(&self, binding: &ChannelBinding) -> Option<String> {
        self.0.form().recipient(binding)
    }
}

impl sealed::Sealed for Registered {
    fn form(&self) -> AddressForm {
        self.0.form()
    }
}

/// What makes [`RegisteredChannel`] the kit's alone, and what the default
/// router reads of it beside its members.
pub(crate) mod sealed {
    use roomkit_core::rules::addresses::AddressForm;

    /// Implemented by the kit's views only.
    pub trait Sealed {
        /// The form the channel's addresses are compared in.
        fn form(&self) -> AddressForm;
    }
}
