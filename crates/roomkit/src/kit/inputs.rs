//! A turn's human-input requests, answered on the room's owner (RFC
//! §10.6.5): Python's `RoomKit.answer_input` and `reject_input`, and the
//! `ON_USER_INPUT_REQUIRED` hooks a turn's requests fire (RFC §9.3).

use std::sync::Arc;

use roomkit_core::model::{ChannelId, ChannelType, RoomId};
use roomkit_core::rules::hook_registry::{HookSpec, HookSubject, HookTrigger};

use super::{Inner, RoomKit, read};
use crate::error::RoomKitError;
use crate::hooks::{HookHandler, Kept};
use crate::human_input::PendingInputEvent;
use crate::store::ConversationStore;

impl<S: ConversationStore> RoomKit<S> {
    /// Answers the human-input request `request_id` of a turn this kit
    /// started, wherever the turn runs (RFC §10.6.5): Python's
    /// `answer_input(request_id, value)`. `true` when the tool waiting on
    /// it reads `value`; `false` when the request was settled otherwise
    /// first (answered, rejected, timed out, withdrawn). An answer for a
    /// request not open yet waits for it, at most the turns'
    /// `input_hold_timeout` (60 seconds by default).
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::KitClosed`] once the kit's close started
    /// (RFC §12.10.4).
    pub async fn answer_input(&self, request_id: &str, value: &str) -> Result<bool, RoomKitError> {
        self.inner.open()?;
        Ok(self.inner.turns.answer_input(request_id, value).await)
    }

    /// Rejects the human-input request `request_id` for `reason`: Python's
    /// `reject_input(request_id, reason='')`. `true` when the tool waiting
    /// on it reads the rejection, as a refusal (RFC §9.3); `false` for one
    /// settled otherwise first.
    ///
    /// # Errors
    ///
    /// As [`answer_input`](Self::answer_input).
    pub async fn reject_input(
        &self,
        request_id: &str,
        options: RejectInputOptions,
    ) -> Result<bool, RoomKitError> {
        self.inner.open()?;
        Ok(self
            .inner
            .turns
            .reject_input(request_id, &options.reason)
            .await)
    }
}

/// What [`RoomKit::reject_input`] takes beside the request: Python's
/// `reject_input(request_id, reason='')`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RejectInputOptions {
    /// Python `reason=''`: why, which the waiting tool reads.
    pub reason: String,
}

impl<S: ConversationStore> Inner<S> {
    /// The `ON_USER_INPUT_REQUIRED` hooks that fire for the requests of the
    /// agent `channel_id` of `channel_type` in the room `room_id`, the
    /// kit's and the room's: its SYNC chain in chain order, then its
    /// observers (RFC §9.1, §9.3).
    pub(crate) fn input_hooks(
        &self,
        (room_id, channel_id): (&RoomId, &ChannelId),
        channel_type: &ChannelType,
    ) -> Vec<(HookSpec, Arc<dyn HookHandler<PendingInputEvent>>)> {
        let trigger = HookTrigger::OnUserInputRequired;
        let config = Arc::clone(&read(&self.config));
        let handlers = read(&self.hooks);
        let registry = &config.hooks;
        let subject = HookSubject::of_channel(channel_id, channel_type).in_room(room_id);
        registry
            .chain(trigger, subject)
            .into_iter()
            .chain(registry.observers(trigger, subject))
            .filter_map(|id| {
                let Kept::Input(handler) = handlers.get(&id)? else {
                    return None;
                };
                Some((registry.get(id)?.clone(), Arc::clone(handler)))
            })
            .collect()
    }
}
