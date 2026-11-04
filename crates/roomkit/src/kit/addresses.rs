//! The channel's own address methods, asked where the Python
//! implementation's framework calls them (RFC §5.7, §10.4): a message's
//! sender, conversation and reply metadata as it enters, and the
//! correspondent an attach or a binding metadata update names.

use roomkit_core::model::{ChannelBinding, InboundMessage, ParticipantId};
use roomkit_core::ops::MessageAnswers;
use roomkit_core::rules::addresses::written_recipient;
use serde_json::Value;

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

/// Who `binding` delivers to by `channel`, whose declared key is
/// `recipient_key`, whether or not it is `group` (RFC §5.7), Python's
/// `channel_recipient`: the channel's `recipient_address`, else the value
/// under the key in its form; none for no value and for a value under the key
/// that is not a recipient, whatever the channel answers.
pub(crate) fn channel_recipient(
    channel: &dyn DynChannel,
    recipient_key: Option<&str>,
    binding: &ChannelBinding,
) -> Option<String> {
    if recipient_key.is_some_and(|key| holds_another_value(binding, key)) {
        return None;
    }
    channel.recipient_address(binding).or_else(|| {
        let held = written_recipient(&binding.metadata, recipient_key?)?;
        Some(channel.normalize_address(&held))
    })
}

/// Whether `binding` holds a value under `key` that is no recipient, and no
/// absence either: neither absent, null nor a blank string, and neither a
/// string nor an integer (RFC §5.7).
pub(crate) fn holds_another_value(binding: &ChannelBinding, key: &str) -> bool {
    match binding.metadata.get(key) {
        None | Some(Value::Null) => false,
        Some(Value::String(text)) if text.trim().is_empty() => false,
        Some(_) => written_recipient(&binding.metadata, key).is_none(),
    }
}

/// `binding` naming its correspondent as the router compares senders (RFC
/// §5.7, §10.4), as Python's `_named_correspondent` writes it: its
/// recipient written where `channel` reads it ([`channel_recipient`],
/// `recipient_metadata`), the correspondent the host gives in the form the
/// channel writes an inbound sender in (`normalize_address`), and a binding
/// that names no one, not `group`, naming the person it delivers to. The
/// framework's own name holds no one's place: a recipient replaces it, and
/// with none the binding keeps it as it is, a channel's form never making it
/// a sender's (a room opened by a message with no address, RFC §5.7). A value
/// under the key that is not a recipient is kept, and names no one.
pub(crate) fn named_correspondent(
    mut binding: ChannelBinding,
    channel: &dyn DynChannel,
    recipient_key: Option<&str>,
) -> ChannelBinding {
    let recipient = channel_recipient(channel, recipient_key, &binding);
    if let Some(recipient) = &recipient {
        for (key, value) in channel.recipient_metadata(recipient) {
            binding.metadata.insert(key, value.into());
        }
    }
    let named = match &binding.participant_id {
        Some(participant) if participant.as_str() != SYSTEM_SENDER_ID => {
            Some(channel.normalize_address(participant.as_str()))
        }
        Some(_) if recipient.is_none() => Some(SYSTEM_SENDER_ID.to_owned()),
        Some(_) | None if binding.group => None,
        Some(_) | None => recipient,
    };
    binding.participant_id = named.and_then(|named| ParticipantId::new(named).ok());
    binding
}
