//! The room reads and outlets an executor gives the channels it wires (RFC §10.6.2).
mod hooks;
mod kit;
pub use hooks::ChannelTurnHooks;
mod rows;
mod worker;
pub(crate) use kit::KitWiringEnv;
pub(crate) use worker::WorkerWiringEnv;

use super::{TurnFuture, TurnRequest};
use crate::model::{ChannelId, Metadata, RoomContext, RoomEvent, RoomId, Timestamp};
use crate::realtime::{EphemeralEvent, EphemeralEventType, RealtimeBackend};
use crate::{Channel, FrameworkEvent, HookTrigger, RoomKitError};
use std::sync::Arc;

/// Room data available to an executor independently of its transport.
/// A kit reads its store. A separate worker reads its owner's request and link.
pub trait WiringEnv: Send + Sync + 'static {
    /// Read the current room for a hook. The caller may already have loaded its history.
    fn context<'a>(
        &'a self,
        room: &'a RoomId,
        carrying: Option<&'a RoomContext>,
    ) -> TurnFuture<'a, Result<RoomContext, RoomKitError>>;
    /// Read this channel's latest tool-call rows, returned oldest first.
    fn tool_rows<'a>(
        &'a self,
        room: &'a RoomId,
        channel: &'a ChannelId,
        limit: u64,
    ) -> TurnFuture<'a, Result<Vec<RoomEvent>, RoomKitError>>;
    /// Read the first page of this channel's tool and instruction rows after a position.
    fn history_rows<'a>(
        &'a self,
        room: &'a RoomId,
        channel: &'a ChannelId,
        after: Option<u64>,
        limit: u64,
    ) -> TurnFuture<'a, Result<Vec<RoomEvent>, RoomKitError>>;
    /// The last message index before this position, or none if no earlier message is available.
    fn message_before<'a>(
        &'a self,
        room: &'a RoomId,
        index: u64,
    ) -> TurnFuture<'a, Result<Option<u64>, RoomKitError>>;
    /// The backend for publications made outside a turn.
    fn realtime(&self) -> Option<Arc<dyn RealtimeBackend>> {
        None
    }
    /// Emit a framework event outside a turn.
    fn emit(&self, _event: FrameworkEvent) -> TurnFuture<'_, ()> {
        Box::pin(async {})
    }
    /// Creation mark used to scope a publication to the right incarnation of its room.
    fn room_mark<'a>(&'a self, _room: &'a RoomId) -> TurnFuture<'a, Option<Timestamp>> {
        Box::pin(async { None })
    }
    /// Remember what a turn of an admitted organization brings.
    fn turn_started(&self, _request: &TurnRequest) {}
    /// Release conversation data when a channel detaches.
    fn room_left(&self, _room: &RoomId, _channel: &ChannelId) {}
    /// Background tasks known to this environment outside a turn.
    fn room_tasks<'a>(
        &'a self,
        _room: &'a RoomId,
    ) -> TurnFuture<'a, Result<Vec<Metadata>, RoomKitError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

/// A snapshot of the environment selected for wiring channels on an executor.
#[derive(Clone)]
pub struct TurnWiring {
    pub(crate) kit: Arc<dyn crate::turn::TurnKit>,
    pub(crate) kit_defaults: bool,
    /// Reads and outlets supplied by the host.
    pub env: Arc<dyn WiringEnv>,
}

impl std::fmt::Debug for TurnWiring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TurnWiring").finish_non_exhaustive()
    }
}

impl TurnWiring {
    pub(crate) fn for_kit<S: crate::ConversationStore>(
        kit: std::sync::Weak<crate::kit::Inner<S>>,
    ) -> Self {
        Self {
            kit_defaults: true,
            env: Arc::new(KitWiringEnv(std::sync::Weak::clone(&kit))),
            kit: Arc::new(kit),
        }
    }

    /// Give an external handler the executor's gate and reporting hooks.
    pub fn wire_external_tool_handler(
        &self,
        channel: &ChannelId,
        bridge: &crate::external_tools::ExternalToolBridge,
    ) {
        self.kit.wire_external_tools(channel, bridge, self.clone());
    }

    /// Wire the channel's callbacks and its input handler before registration.
    ///
    /// # Errors
    /// A human-input handler closed as a whole refuses new registrations.
    pub fn wire(&self, channel: &impl Channel) -> Result<(), RoomKitError> {
        self.bind(
            channel,
            &channel.channel_id(),
            channel.human_input_handler(),
        )
    }

    pub(crate) fn bind(
        &self,
        channel: &dyn crate::channel::DynChannel,
        id: &ChannelId,
        input: Option<crate::human_input::HumanInputHandler>,
    ) -> Result<(), RoomKitError> {
        channel.set_turn_wiring(self.clone());
        if let Some(handler) = channel.external_tool_bridge() {
            self.wire_external_tool_handler(id, &handler);
        }
        if let Some(publication) = channel.channel_publication() {
            publication.wire_to(self.clone());
        }
        if let Some(handler) = input {
            let registration = handler.register(id.as_str())?;
            channel.human_input_registered(registration);
            self.kit.wire_human_input(id, &handler, self.clone());
        }
        Ok(())
    }

    /// Read the hook's room. A failed read leaves the hook without a context.
    pub async fn hook_context(
        &self,
        room: &RoomId,
        trigger: HookTrigger,
        carrying: Option<&RoomContext>,
    ) -> Option<RoomContext> {
        match self.env.context(room, carrying).await {
            Ok(context) => Some(context),
            Err(error) => {
                tracing::warn!(%room, ?trigger, %error, "could not build the room context for a turn hook");
                None
            }
        }
    }

    /// Publish an event to its room, through the owner of the current turn when available.
    pub async fn publish_to_room(&self, event: EphemeralEvent) {
        if let Some(host) = crate::turn::turn_in_scope() {
            host.publish_from_channel(event);
        } else {
            self.publish_outside(event).await;
        }
    }

    pub(crate) async fn publish_outside(&self, mut event: EphemeralEvent) {
        if event.r#type == EphemeralEventType::TextDelta {
            return;
        }
        if let Some(backend) = self.env.realtime() {
            let room = event.room_id.clone();
            if event.room_created_at.is_none() {
                event.room_created_at = self.env.room_mark(&room).await;
            }
            if let Err(error) = backend.publish_to_room(&room, event).await {
                tracing::debug!(%room, %error, "could not publish a channel notification");
            }
        }
    }

    /// Emit a framework event to the owner of the turn or the environment outside it.
    pub async fn emit(&self, event: FrameworkEvent) {
        if let Some(host) = crate::turn::turn_in_scope() {
            host.emit_framework_event(event);
        } else {
            self.env.emit(event).await;
        }
    }

    /// Background tasks as read by a text channel's notes.
    ///
    /// # Errors
    /// Propagates failure to read the room's tasks.
    pub async fn room_tasks(&self, room: &RoomId) -> Result<Vec<Metadata>, RoomKitError> {
        if let Some(host) = crate::current_turn_host().filter(crate::TurnHost::open) {
            return host.room_tasks().await;
        }
        self.env.room_tasks(room).await
    }
}
