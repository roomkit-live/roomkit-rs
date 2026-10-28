//! What a router and a caller read of a registered channel
//! ([`RoomKit::get_channel`](crate::RoomKit::get_channel)): its synchronous
//! members, as the kit holds them, and its close, for a channel
//! [`RoomKit::unregister_channel`](crate::RoomKit::unregister_channel)
//! handed back.

use std::fmt::{self, Debug};
use std::sync::Arc;

use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelType,
};
use roomkit_core::ops::ChannelDeclaration;
use roomkit_core::rules::addresses::AddressForm;

use crate::channel::{BoxFuture, DynChannel};

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

    /// Releases what the channel holds ([`Channel::close`](crate::Channel::close),
    /// RFC §6.1): the caller's to call once
    /// [`unregister_channel`](crate::RoomKit::unregister_channel) handed the
    /// channel back, as the Python implementation's caller awaits
    /// `channel.close()`; the kit's close closes only the channels
    /// registered then.
    fn close(&self) -> BoxFuture<'_, ()>;
}

/// The kit's own view: a channel's declaration, and the channel.
pub(crate) struct Registered {
    pub(crate) declaration: ChannelDeclaration,
    pub(crate) channel: Arc<dyn DynChannel>,
}

impl Debug for Registered {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Registered")
            .field("declaration", &self.declaration)
            .finish_non_exhaustive()
    }
}

impl RegisteredChannel for Registered {
    fn channel_id(&self) -> &ChannelId {
        &self.declaration.channel_id
    }

    fn channel_type(&self) -> &ChannelType {
        &self.declaration.channel_type
    }

    fn category(&self) -> ChannelCategory {
        self.declaration.category
    }

    fn direction(&self) -> ChannelDirection {
        self.declaration.direction
    }

    fn recipient_key(&self) -> Option<&str> {
        self.declaration.recipient_key.as_deref()
    }

    fn normalize_address(&self, address: &str) -> String {
        self.declaration.form().normalize(address)
    }

    fn recipient_address(&self, binding: &ChannelBinding) -> Option<String> {
        self.declaration.form().recipient(binding)
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        self.channel.close()
    }
}

impl sealed::Sealed for Registered {
    fn form(&self) -> AddressForm {
        self.declaration.form()
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
