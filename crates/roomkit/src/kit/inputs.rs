//! A turn's human-input requests, answered on the room's owner (RFC
//! §10.6.5): Python's `RoomKit.answer_input` and `reject_input`, and the
//! `ON_USER_INPUT_REQUIRED` hooks a request fires (RFC §9.3), made in a
//! turn or outside the kit's turns.

use std::sync::Arc;

use roomkit_core::model::{ChannelId, RoomId};
use roomkit_core::rules::hook_registry::{HookSpec, HookSubject, HookTrigger};

use super::{Inner, RoomKit, read};
use crate::channel::BoxFuture;
use crate::error::RoomKitError;
use crate::hooks::{HookHandler, Kept};
use crate::human_input::{HumanInputHandler, PendingInputEvent};
use crate::store::ConversationStore;
use crate::telemetry::within_kit;

use crate::turns::TurnWiring;

impl<S: ConversationStore> RoomKit<S> {
    /// Reads an open human-input request of a turn this kit started
    /// (RFC §10.6.5): Python's `input_request(request_id, organization_id=None)`.
    /// The owned snapshot preserves the request's creation time and its
    /// room, channel, tool call and actor. Returns `None` once settled or
    /// withdrawn, for another owner's id, or outside the requested tenant.
    #[must_use]
    pub fn input_request(
        &self,
        request_id: &str,
        options: InputRequestOptions,
    ) -> Option<PendingInputEvent> {
        let InputRequestOptions { organization_id } = options;
        self.inner
            .turns
            .inputs
            .request(request_id, organization_id.as_deref())
    }

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

/// What [`RoomKit::input_request`] takes beside the request id.
#[derive(Clone, Debug, Default)]
pub struct InputRequestOptions {
    /// Python `organization_id=None`: only read requests of this tenant's
    /// rooms when given (RFC §17.2).
    pub organization_id: Option<String>,
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
        event: &PendingInputEvent,
    ) -> Vec<(HookSpec, Arc<dyn HookHandler<PendingInputEvent>>)> {
        let trigger = HookTrigger::OnUserInputRequired;
        let config = Arc::clone(&read(&self.config));
        let handlers = read(&self.hooks);
        let registry = super::named_hooks::registry_for_turn(&config.hooks, room_id);
        let subject = HookSubject::of_channel(channel_id, &event.channel_type).in_room(room_id);
        registry
            .chain(trigger, subject)
            .into_iter()
            .chain(registry.observers(trigger, subject))
            .filter_map(|id| {
                let spec = registry.get(id)?;
                // The bound channel routes the notification. Only the channel
                // named by the request can match an explicit hook filter.
                if event.channel_id != channel_id.as_str() && spec.filter.channel_ids.is_some() {
                    return None;
                }
                let Kept::Input(handler) = handlers.get(&id)? else {
                    return None;
                };
                Some((spec.clone(), Arc::clone(handler)))
            })
            .collect()
    }

    /// Hands `handler`, the human-input handler of the channel
    /// `channel_id` this kit registers, the kit's `ON_USER_INPUT_REQUIRED`
    /// hooks for the channel's requests made outside its turns, as from a
    /// host's webhook or job (RFC §9.3): Python's `_wire_ai_channel`
    /// handing the handler `build_on_user_input_required_hook`.
    pub(crate) fn wire_human_input(
        self: &Arc<Self>,
        channel_id: &ChannelId,
        handler: &HumanInputHandler,
        wiring: TurnWiring,
    ) {
        let registered = channel_id.clone();
        let telemetry = self.telemetry.clone();
        handler.set_on_input_required(
            channel_id.as_str(),
            Arc::new(move |event| -> BoxFuture<'static, bool> {
                let (wiring, channel) = (wiring.clone(), registered.clone());
                let required = async move {
                    let Some(notification) = wiring.input_notification(&channel, &event).await
                    else {
                        return true;
                    };
                    notification.run(event).await
                };
                Box::pin(within_kit(&telemetry, None, required))
            }),
        );
    }
}
