//! What a kit's operations fail with.

use roomkit_core::model::ChannelId;
use roomkit_core::ops::{ConfigError, InboundError, LifecycleError, ProtocolError};
use roomkit_core::rules::hook_registry::RegistrationError;
use thiserror::Error;

/// Why a kit call failed.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum KitError {
    /// The inbound pipeline refused or failed the message.
    #[error(transparent)]
    Inbound(#[from] InboundError),
    /// A room lifecycle change failed.
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    /// The kit's configuration refused a channel.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// A hook registration broke a rule of RFC §9.1.
    #[error(transparent)]
    Registration(#[from] RegistrationError),
    /// No channel is registered under the id.
    #[error("no channel {0} is registered")]
    UnknownChannel(ChannelId),
    /// The driver broke the effect protocol: a defect of this crate.
    #[error("the driver broke the effect protocol: {0}")]
    Protocol(#[from] ProtocolError),
    /// An operation waited for effects the driver never received: a defect
    /// of this crate.
    #[error("an operation waited for effects that were never issued")]
    Stalled,
}
