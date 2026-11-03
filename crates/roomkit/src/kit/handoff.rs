//! The kit's side of the handoff (RFC §19.6): what
//! `orchestration/handoff.rs` needs of its hooks and of its owner tools.

use roomkit_core::model::{ChannelId, ChannelType, EventSource, EventStatus, EventType, Metadata};
use roomkit_core::model::{RoomEvent, RoomId, TextContent, Visibility};
use roomkit_core::rules::hook_registry::HookTrigger as CoreTrigger;

use super::RoomKit;
use crate::error::RoomKitError;
use crate::orchestration::ConversationRouter;
use crate::orchestration::handoff::HANDOFF_TOOL_NAME;
use crate::store::ConversationStore;
use crate::turns::served::OwnerToolEntry;
use crate::{HookExecution, HookOptions, HookTrigger};

impl<S: ConversationStore> RoomKit<S> {
    /// Fires `trigger`'s async hooks on `event`, best effort: Python's
    /// `hook_engine._start_async_hooks`.
    pub(crate) async fn observe_orchestration(&self, trigger: CoreTrigger, event: RoomEvent) {
        self.inner.observe_event(trigger, event).await;
    }

    /// An internal event of `room_id` from the agent `agent_id`, never
    /// stored, delivered: what an orchestration hook is handed.
    pub(crate) fn internal_event(
        &self,
        room_id: &RoomId,
        agent_id: &ChannelId,
        r#type: EventType,
        body: String,
        metadata: Metadata,
    ) -> RoomEvent {
        let environment = self.inner.environment();
        let source = EventSource::new(agent_id.clone(), ChannelType::AI);
        let mut event = RoomEvent::at(
            environment.new_event_id(),
            room_id.clone(),
            r#type,
            source,
            TextContent::new(body),
            environment.now(),
        );
        event.status = EventStatus::Delivered;
        event.visibility = Visibility::Internal;
        event.metadata = metadata;
        event
    }

    /// Whether the owner serves `agent` the handoff tool where it would be
    /// declared: in every room, or in `room_id`.
    pub(crate) fn serves_handoff(&self, agent: &ChannelId, room_id: Option<&RoomId>) -> bool {
        let tools = &self.inner.owner_tools;
        match room_id {
            None => tools.serves(agent, HANDOFF_TOOL_NAME),
            Some(room_id) => tools.lookup(agent, room_id, HANDOFF_TOOL_NAME).is_some(),
        }
    }

    /// Serves `agent` `entry` in every room, or in `room_id`'s only.
    pub(crate) fn serve_handoff(
        &self,
        agent: &ChannelId,
        entry: OwnerToolEntry<S>,
        room_id: Option<&RoomId>,
    ) -> Result<(), RoomKitError> {
        match room_id {
            None => self.register_owner_tools(agent, vec![entry]),
            Some(room_id) => self.register_room_tools(agent, room_id, vec![entry], 0),
        }
    }

    /// Registers `router`'s hook on every room's `BEFORE_BROADCAST`, sync,
    /// at `priority`: Python's `kit.hook(...)(router.as_hook())`.
    pub(crate) fn install_router_hook(
        &self,
        router: &ConversationRouter,
        priority: i32,
    ) -> Result<(), RoomKitError> {
        let options = HookOptions {
            execution: HookExecution::Sync,
            priority,
            ..HookOptions::default()
        };
        self.hook(HookTrigger::BeforeBroadcast, router.as_hook(), options)
    }
}
