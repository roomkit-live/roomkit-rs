//! The channel's own address methods, asked where the Python
//! implementation's framework calls them (RFC §5.7, §10.4): a message's
//! sender, conversation and reply metadata as it enters, and the
//! correspondent an attach or a binding metadata update names.

use roomkit_core::model::{ChannelBinding, InboundMessage, ParticipantId};
use roomkit_core::ops::MessageAnswers;

use super::Inner;
use crate::channel::DynChannel;
use crate::context::ExecutionContext;
use crate::delivery::targets::SYSTEM_SENDER_ID;

impl<S> Inner<S> {
    /// Writes `message`'s sender in its channel's form by the channel's own
    /// `normalize_address`, before anything reads it (RFC §10.1 step 1), and
    /// what the channel answers of the message: the conversation it belongs
    /// to and where replies to it go (`conversation_address`,
    /// `reply_metadata`), which the core routes and records it by, and the
    /// depth of the turn the call runs within, which its event takes at
    /// least. `None` for a channel not registered, which the core fails.
    pub(crate) fn answer(&self, message: &mut InboundMessage) -> Option<MessageAnswers> {
        let channel = self.channel(&message.channel_id)?;
        message.sender_id = channel.normalize_address(&message.sender_id);
        Some(MessageAnswers {
            conversation: channel.conversation_address(message),
            reply_metadata: channel.reply_metadata(message),
            chain_floor: ExecutionContext::current().turn_depth().unwrap_or(0),
            publication: None,
        })
    }
}

/// `binding` naming its correspondent as the router compares senders (RFC
/// §5.7, §10.4), as Python's `_named_correspondent` writes it: its
/// recipient written where `channel` reads it (`recipient_address`,
/// `recipient_metadata`), the correspondent the host gives in the form the
/// channel writes an inbound sender in (`normalize_address`), and a binding
/// that names no one, not `group`, naming the person it delivers to. The
/// framework's own name holds no one's place: a recipient replaces it, and
/// with none the binding keeps it (a room opened by a message with no
/// address, RFC §5.7).
pub(crate) fn named_correspondent(
    mut binding: ChannelBinding,
    channel: &dyn DynChannel,
) -> ChannelBinding {
    let recipient = channel.recipient_address(&binding);
    if let Some(recipient) = &recipient {
        for (key, value) in channel.recipient_metadata(recipient) {
            binding.metadata.insert(key, value.into());
        }
    }
    let named = match &binding.participant_id {
        Some(participant) if participant.as_str() != SYSTEM_SENDER_ID || recipient.is_none() => {
            Some(channel.normalize_address(participant.as_str()))
        }
        Some(_) | None if binding.group => None,
        Some(_) | None => recipient,
    };
    binding.participant_id = named.and_then(|named| ParticipantId::new(named).ok());
    binding
}
