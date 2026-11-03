//! The kit's side of the handoff (RFC §19.6): what
//! `orchestration/handoff.rs` needs of its hooks and of its owner tools.

use roomkit_core::model::{ChannelId, ChannelType, EventSource, EventStatus, EventType, Metadata};
use roomkit_core::model::{RoomEvent, RoomId, TextContent, Visibility};
use roomkit_core::rules::hook_registry::HookTrigger as CoreTrigger;

use super::RoomKit;
use crate::error::{RoomKitError, repr};
use crate::kit::background_tools::collision;
use crate::orchestration::handoff::HANDOFF_TOOL_NAME;
use crate::orchestration::{ConversationRouter, StrategyAgent};
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

    /// Refuses what serves no handoff: an id naming no channel the kit holds
    /// or no intelligence channel, or a channel whose turns never declare the
    /// tools its room's owner serves (RFC §10.6.5). Python's `owner_tools_of`.
    pub(crate) fn takes_handoff(&self, agent: &StrategyAgent) -> Result<(), RoomKitError> {
        let Some(live) = agent.live() else {
            return self.registered_agent(agent.channel_id());
        };
        if live.channel.declaration().takes_owner_tools {
            return Ok(());
        }
        Err(RoomKitError::Value(format!(
            "Channel {} does not take the tools its room owner serves (RFC §10.6.5): its turns \
             would never declare them",
            repr(agent.channel_id().as_str())
        )))
    }

    /// Serves `agent` `entry` in every room, or in `room_id`'s only: a tool
    /// its room already serves refuses the kit-wide one, whichever it is
    /// (RFC §19.7, a tool is declared once in a room).
    pub(crate) fn serve_handoff(
        &self,
        agent: &ChannelId,
        entry: OwnerToolEntry<S>,
        room_id: Option<&RoomId>,
    ) -> Result<(), RoomKitError> {
        let Some(room_id) = room_id else {
            return self.register_owner_tools(agent, vec![entry]);
        };
        let tools = &self.inner.owner_tools;
        let own = self
            .inner
            .channel(agent)
            .map(|channel| channel.own_tool_names())
            .unwrap_or_default();
        if own.iter().any(|name| name == entry.name()) {
            return Err(collision(
                entry.name(),
                "one of the channel's own tools",
                agent,
            ));
        }
        tools
            .register_all_in_room(agent, room_id, vec![entry], 0)
            .map_err(|(name, clash)| collision(&name, &clash, agent))
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
