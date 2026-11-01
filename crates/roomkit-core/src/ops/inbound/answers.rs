//! What the channel a message arrives on answers of it (RFC §5.7, §10.4):
//! the address naming its conversation and where a reply to it goes, which
//! Python's framework asks the channel's `conversation_address` and
//! `reply_metadata` methods. The driver asks the channel as the message
//! enters; the core routes and records the sender by the answers.

use std::collections::BTreeMap;

use crate::model::InboundMessage;
use crate::ops::ChannelConfig;
use crate::rules::addresses::SYSTEM_SENDER;
use crate::rules::routing::{self, Sender};

/// What the channel a message arrives on answers of it, as the message
/// enters (RFC §5.7, §10.4): the core routes the message by its
/// conversation and records the sender with its reply metadata (RFC §10.1
/// step 2). A message entered without them is answered by its channel's
/// declaration ([`MessageAnswers::declared`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MessageAnswers {
    /// The address naming the message's conversation, Python's
    /// `conversation_address(message)`: its sender's, or on a chat channel
    /// the chat it was posted in; empty for a message with no address.
    pub conversation: String,
    /// The binding metadata that makes the room reply where the message
    /// came from, Python's `reply_metadata(message)`: the sender's address
    /// or the chat under the channel's recipient key, none on a channel
    /// with no addressee of its own.
    pub reply_metadata: BTreeMap<String, String>,
}

impl MessageAnswers {
    /// What `channel` answers of `message` by its declaration: the chat
    /// read under its chat key, else the sender's address on a channel
    /// whose replies go to its senders (`routing_address`), written under
    /// its recipient key as where replies go.
    pub(crate) fn declared(channel: &ChannelConfig, message: &InboundMessage) -> Self {
        let address = routing::routing_address(
            channel.addressing,
            &message.sender_id,
            &message.metadata,
            channel.chat_key.as_deref(),
        );
        let reply_metadata = address
            .iter()
            .zip(&channel.recipient_key)
            .map(|(address, key)| (key.clone(), address.clone()))
            .collect();
        Self {
            conversation: address.unwrap_or_default(),
            reply_metadata,
        }
    }

    /// The sender of `message` as routing names them (RFC §10.4): the
    /// conversation as their address, none for a blank one and for the
    /// framework's own sender `system`, which names no correspondent.
    pub(crate) fn sender(self, message: &InboundMessage) -> Sender {
        let named = message.sender_id != SYSTEM_SENDER && !self.conversation.trim().is_empty();
        Sender {
            address: named.then_some(self.conversation),
            identity_id: None,
            reply_metadata: self.reply_metadata,
        }
    }
}
