//! Phases and routing (roomkit-specs `conformance/README.md`, "Setup" and
//! "Calls"; RFC §19.2, §19.4): a hook with a `router` is a conversation
//! router's hook, the `ConversationRouter` built from it, recording no call;
//! `save_conversation_state` makes phase transitions from the state of the
//! room the store holds, then saves the state the last one leaves;
//! `default_router.route` asks the default router over the kit's store.

use std::sync::Arc;

use roomkit::model::{ChannelId, ChannelType, RoomId};
use roomkit::orchestration::{ConversationRouterOptions, TransitionOptions};
use roomkit::{
    ConversationRouter, ConversationState, ConversationStore, DefaultInboundRoomRouter,
    DefaultInboundRoomRouterOptions, InboundRoomRouter, RoomKitError, RouteOptions, RoutingRule,
    get_conversation_state, save_conversation_state,
};
use serde_json::{Value, json};

use crate::calls::{Called, Unreadable, field, finish, optional};
use crate::run::Run;

/// The members of a router, of its rules and of their conditions, and of a
/// saved transition.
pub(crate) const ROUTER: &[&str] = &["rules", "default_agent_id", "supervisor_id"];
pub(crate) const ROUTING_RULE: &[&str] = &["agent_id", "priority", "conditions"];
pub(crate) const CONDITIONS: &[&str] =
    &["phases", "channel_types", "intents", "source_channel_ids"];
pub(crate) const TRANSITION: &[&str] = &["to_phase", "to_agent", "reason", "metadata"];

/// The router a hook's `router` describes.
pub(crate) fn router(spec: &Value) -> Result<ConversationRouter, Unreadable> {
    let rules: Option<Vec<RoutingRule>> = optional(spec, "rules")?;
    Ok(ConversationRouter::new(ConversationRouterOptions {
        rules: rules.unwrap_or_default(),
        default_agent_id: optional::<ChannelId>(spec, "default_agent_id")?,
        supervisor_id: optional::<ChannelId>(spec, "supervisor_id")?,
    }))
}

/// The arguments of the routing calls, for the shape check.
pub(crate) fn arguments(name: &str) -> Option<&'static [&'static str]> {
    match name {
        "save_conversation_state" => Some(&["room_id", "transitions"]),
        "default_router.route" => Some(&[
            "channel_id",
            "channel_type",
            "participant_id",
            "identity_id",
        ]),
        _ => None,
    }
}

/// Makes `save_conversation_state`.
pub(crate) async fn call(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let room_id = field::<RoomId>(spec, "room_id")?;
    let store = run.kit().store();
    let room = store
        .get_room(&room_id)
        .await
        .map_err(|error| format!("read room {room_id}: {error}"))?;
    let mut state = match room {
        Some(room) => get_conversation_state(&room).map_err(|error| error.to_string())?,
        None => ConversationState::new(),
    };
    for transition in spec["transitions"].as_array().into_iter().flatten() {
        let options = TransitionOptions {
            to_agent: optional(transition, "to_agent")?,
            reason: optional(transition, "reason")?.unwrap_or_default(),
            metadata: optional(transition, "metadata")?.unwrap_or_default(),
        };
        state = state.transition(field::<String>(transition, "to_phase")?, options);
    }
    let saved = save_conversation_state(store, &room_id, &state).await;
    Ok(finish(run, name, saved, |()| Value::Null))
}

/// Makes `default_router.route`: the default router over the kit's store,
/// reading no channel, whatever router the kit holds.
pub(crate) async fn default_route(
    run: &Run,
    name: &str,
    spec: &Value,
) -> Result<Called, Unreadable> {
    let channel_id = field::<ChannelId>(spec, "channel_id")?;
    let channel_type = field::<ChannelType>(spec, "channel_type")?;
    let identity = optional::<String>(spec, "identity_id")?
        .map(|id| serde_json::from_value(json!({ "id": id })))
        .transpose()
        .map_err(|error| format!("identity_id: {error}"))?;
    let options = RouteOptions {
        participant_id: optional(spec, "participant_id")?,
        identity,
        ..RouteOptions::default()
    };
    let router =
        DefaultInboundRoomRouter::new(run.store(), DefaultInboundRoomRouterOptions::default());
    let answered = router
        .route(&channel_id, &channel_type, options)
        .await
        .map_err(|error| RoomKitError::Runtime(error.to_string()));
    Ok(finish(run, name, answered, |room| json!(room)))
}
