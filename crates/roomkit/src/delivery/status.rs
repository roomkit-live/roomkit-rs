//! The room a provider's delivery status belongs to (RFC §10.4, A delivery
//! status follows its message), as the Python implementation's
//! `_room_of_status` finds it.

use roomkit_core::model::{ChannelBinding, ChannelId, DeliveryStatus, RoomId, RoomStatus};
use roomkit_core::rules::addresses::written_recipient;
use serde_json::Value;

use crate::delivery::targets::SYSTEM_SENDER_ID;
use crate::registered::RegisteredChannel;
use crate::store::{
    ConversationStore, FindRoomIdByBindingOptions, FindRoomIdsByChannelOptions, RoomFilter,
    StoreError,
};

/// The room `status` belongs to, routed on `channel_id`, the channel kit
/// holds as `channel` (`None` once unregistered), among the rooms of
/// `organization_id` when it names one (RFC §17.2): the newest ACTIVE or
/// PAUSED room whose binding of the channel names its recipient, as step 1
/// finds a sender's, in the channel's form; else the one ACTIVE room bound
/// to the channel, unless its binding names another correspondent: its
/// participant, else its recipient, a value that is not a recipient
/// included (RFC §5.7). Never one room of several: on a number shared by
/// many correspondents, the oldest room is another customer's. A status is
/// no activity, and resumes no room (RFC §5.1).
pub(crate) async fn room_of_status<S: ConversationStore>(
    store: &S,
    status: &DeliveryStatus,
    channel_id: &ChannelId,
    channel: Option<&dyn RegisteredChannel>,
    organization_id: Option<&str>,
) -> Result<Option<RoomId>, StoreError> {
    let of = |statuses| RoomFilter {
        statuses: Some(statuses),
        organization_id: organization_id.map(str::to_owned),
    };
    let recipient_key = channel
        .and_then(RegisteredChannel::recipient_key)
        .map(str::to_owned);
    // In the channel's form, as step 1 reads a sender (RFC §10.4).
    let recipient = match channel {
        Some(channel) if !status.recipient.is_empty() => {
            channel.normalize_address(&status.recipient)
        }
        Some(_) | None => status.recipient.clone(),
    };
    if recipient == SYSTEM_SENDER_ID {
        // No one's name (RFC §10.4): a binding naming it delivers to no one.
        return Ok(None);
    }
    if !recipient.is_empty() {
        let by_binding = FindRoomIdByBindingOptions {
            identity_id: None,
            room_filter: Some(of(vec![RoomStatus::Active, RoomStatus::Paused])),
            recipient_key: recipient_key.clone(),
        };
        let found = store
            .find_room_id_by_binding(channel_id, &recipient, by_binding)
            .await?;
        if found.is_some() {
            return Ok(found);
        }
    }
    let active = FindRoomIdsByChannelOptions {
        room_filter: Some(of(vec![RoomStatus::Active])),
        limit: 2,
        group: None,
    };
    let candidates = store.find_room_ids_by_channel(channel_id, active).await?;
    let [sole] = candidates.as_slice() else {
        return Ok(None);
    };
    if recipient.is_empty() {
        return Ok(Some(sole.clone()));
    }
    let Some(binding) = store.get_binding(sole, channel_id).await? else {
        return Ok(None);
    };
    let delivers_to = delivers_to(channel, recipient_key.as_deref(), &binding);
    Ok(takes_the_status(&binding, &recipient, &delivers_to).then(|| sole.clone()))
}

/// Who a binding delivers to, as routing reads it (RFC §5.7, §10.4), the
/// Python implementation's `routed_recipient`.
#[derive(Debug, PartialEq, Eq)]
enum DeliversTo {
    /// No one: a group binding, no value, or a channel whose recipient
    /// names no conversation.
    NoOne,
    /// The correspondent at this address, in the channel's form.
    Recipient(String),
    /// A value under the channel's recipient key that is not a recipient,
    /// which reads as someone else's address.
    NotARecipient,
}

/// Who `binding` delivers to, on `channel` whose recipient key is
/// `recipient_key`.
fn delivers_to(
    channel: Option<&dyn RegisteredChannel>,
    recipient_key: Option<&str>,
    binding: &ChannelBinding,
) -> DeliversTo {
    let Some(channel) = channel else {
        return DeliversTo::NoOne;
    };
    if binding.group {
        return DeliversTo::NoOne;
    }
    if let Some(key) = recipient_key
        && holds_another_value(binding, key)
    {
        return DeliversTo::NotARecipient;
    }
    channel
        .recipient_address(binding)
        .map_or(DeliversTo::NoOne, DeliversTo::Recipient)
}

/// Whether `binding` holds a value under `key` that is no recipient, and no
/// absence either: neither absent, null nor a blank string, and neither a
/// string nor an integer (RFC §5.7).
fn holds_another_value(binding: &ChannelBinding, key: &str) -> bool {
    match binding.metadata.get(key) {
        None | Some(Value::Null) => false,
        Some(Value::String(text)) if text.trim().is_empty() => false,
        Some(_) => written_recipient(&binding.metadata, key).is_none(),
    }
}

/// Whether the one active room bound to the channel, by its binding
/// `binding` delivering to `delivers_to`, takes a status for `recipient`:
/// its binding is a group's, names `recipient` or no one; and naming no
/// one, it delivers to no one or to `recipient` (the Python
/// implementation's `binding_admits` and its recipient check).
fn takes_the_status(binding: &ChannelBinding, recipient: &str, delivers_to: &DeliversTo) -> bool {
    let admits = binding.group
        || binding
            .participant_id
            .as_ref()
            .is_none_or(|participant| participant.as_str() == recipient);
    if !admits {
        return false;
    }
    let named_by_recipient = !binding.group && binding.participant_id.is_none();
    !named_by_recipient
        || match delivers_to {
            DeliversTo::NoOne => true,
            DeliversTo::Recipient(address) => address == recipient,
            DeliversTo::NotARecipient => false,
        }
}
