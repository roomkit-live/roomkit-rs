//! A turn's human-input requests, answered on the room's owner (RFC
//! §10.6.5): Python's `RoomKit.answer_input` and `reject_input`, and the
//! `ON_USER_INPUT_REQUIRED` hooks a request fires (RFC §9.3), made in a
//! turn or outside the kit's turns.

use std::sync::{Arc, Weak};

use roomkit_core::model::{ChannelId, ChannelType, RoomContext, RoomId};
use roomkit_core::rules::hook_registry::{HookSpec, HookSubject, HookTrigger};

use super::{Inner, RoomKit, read};
use crate::channel::BoxFuture;
use crate::driver;
use crate::error::RoomKitError;
use crate::hooks::{HookHandler, Kept};
use crate::human_input::{HumanInputHandler, PendingInputEvent};
use crate::store::ConversationStore;
use crate::telemetry::within_kit;
use crate::turn::{InputNotification, TurnKit};

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

    /// Hands `handler`, the human-input handler of the channel
    /// `channel_id` this kit registers, the kit's `ON_USER_INPUT_REQUIRED`
    /// hooks for the channel's requests made outside its turns, as from a
    /// host's webhook or job (RFC §9.3): Python's `_wire_ai_channel`
    /// handing the handler `build_on_user_input_required_hook`.
    pub(crate) fn wire_human_input(
        self: &Arc<Self>,
        channel_id: &ChannelId,
        handler: &HumanInputHandler,
    ) {
        let kit = Arc::downgrade(self);
        let registered = channel_id.clone();
        let telemetry = self.telemetry.clone();
        handler.set_on_input_required(
            channel_id.as_str(),
            Arc::new(move |event| -> BoxFuture<'static, bool> {
                let required = user_input_required(Weak::clone(&kit), registered.clone(), event);
                // Outside a turn, under the kit's telemetry all the same.
                Box::pin(within_kit(&telemetry, None, required))
            }),
        );
    }

    /// The context a room's `ON_USER_INPUT_REQUIRED` hooks read, as
    /// Python's `hook_context` builds it; `None`, logged, for a room the
    /// store lacks, or a store that fails the read.
    async fn input_context(&self, room_id: &RoomId) -> Option<RoomContext> {
        let recent_events = self.config().recent_events();
        match driver::load_context(&*self.store, room_id, recent_events).await {
            Ok(Some(context)) => Some(context),
            Ok(None) => {
                tracing::warn!(target: "roomkit::hooks", %room_id, "the room of a human-input request is not found: its hooks do not run");
                None
            }
            Err(error) => {
                tracing::warn!(target: "roomkit::hooks", %room_id, %error, "the hooks of a human-input request could not run");
                None
            }
        }
    }
}

/// The kit's `ON_USER_INPUT_REQUIRED` hooks on `event`, a request of the
/// agent `registered` made outside the kit's turns, as
/// [`InputNotification::run`] says, about the request's room and agent:
/// Python's `build_on_user_input_required_hook`. Whether the request
/// stands; it stands, nothing fired, for a request naming no room, a kit
/// gone, or a room whose context its hooks cannot read.
async fn user_input_required<S: ConversationStore>(
    kit: Weak<Inner<S>>,
    registered: ChannelId,
    event: PendingInputEvent,
) -> bool {
    let (Some(inner), Ok(room_id)) = (kit.upgrade(), RoomId::new(&event.room_id)) else {
        return true;
    };
    let channel_id = ChannelId::new(&event.channel_id).unwrap_or(registered);
    let hooks = inner.input_hooks((&room_id, &channel_id), &event.channel_type);
    // A context is store reads: built only for a hook that reads it.
    let hooks = if hooks.is_empty() {
        None
    } else {
        let Some(context) = inner.input_context(&room_id).await else {
            return true;
        };
        Some((hooks, context))
    };
    drop(inner);
    let kit: Arc<dyn TurnKit> = Arc::new(kit);
    let notification = InputNotification {
        kit,
        ids: (room_id, channel_id),
        hooks,
        turn: None,
    };
    notification.run(event).await
}
