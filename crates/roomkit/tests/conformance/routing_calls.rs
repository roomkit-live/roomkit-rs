//! Phases and routing (roomkit-specs `conformance/README.md`, "Setup" and
//! "Calls"; RFC §19.2, §19.4): a hook with a `router` is a conversation
//! router's hook, the `ConversationRouter` built from it, recording no call;
//! `save_conversation_state` makes phase transitions from the state of the
//! room the store holds, then saves the state the last one leaves;
//! `default_router.route` asks the default router over the kit's store;
//! `addressing_of.normalize` writes an address with the addressing a channel
//! type takes by default (RFC §5.7).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use roomkit::model::{ChannelId, ChannelType, RoomContext, RoomEvent, RoomId};
use roomkit::orchestration::{ConversationRouterOptions, TransitionOptions};
use roomkit::{
    ConversationRouter, ConversationState, ConversationStore, CountryCode,
    DefaultInboundRoomRouter, DefaultInboundRoomRouterOptions, InboundRoomRouter, NormalizeOptions,
    RoomKitError, RouteOptions, RoutingRule, addressing_of, get_conversation_state,
    save_conversation_state,
};
use serde_json::{Value, json};

use crate::calls::{Called, Unreadable, field, finish, optional};
use crate::errors::named;
use crate::run::Run;

/// The members of a router, of its rules and of their conditions, and of a
/// saved transition.
pub(crate) const ROUTER: &[&str] = &["rules", "default_agent_id", "supervisor_id"];
pub(crate) const ROUTING_RULE: &[&str] = &["agent_id", "priority", "conditions"];
pub(crate) const CONDITIONS: &[&str] = &[
    "phases",
    "channel_types",
    "intents",
    "source_channel_ids",
    "custom",
];
pub(crate) const CUSTOM: &[&str] = &["name", "answers"];
pub(crate) const TRANSITION: &[&str] = &["to_phase", "to_agent", "reason", "metadata"];

/// The router a hook's `router` describes, its rules' scripted predicates
/// recording on `run` (README: `custom_conditions`).
pub(crate) fn router(run: &Arc<Run>, spec: &Value) -> Result<ConversationRouter, Unreadable> {
    let mut rules: Vec<RoutingRule> = optional(spec, "rules")?.unwrap_or_default();
    for (rule, written) in rules
        .iter_mut()
        .zip(spec["rules"].as_array().into_iter().flatten())
    {
        if let Some(custom) = written["conditions"].get("custom") {
            rule.conditions.custom = Some(scripted_predicate(run, custom)?);
        }
    }
    Ok(ConversationRouter::new(ConversationRouterOptions {
        rules,
        default_agent_id: optional::<ChannelId>(spec, "default_agent_id")?,
        supervisor_id: optional::<ChannelId>(spec, "supervisor_id")?,
    }))
}

/// A rule's custom predicate, as Python's callable.
type Predicate = Arc<dyn Fn(&RoomEvent, &RoomContext, &ConversationState) -> bool + Send + Sync>;

/// A rule's scripted predicate: each call answers the next of its
/// `answers`, the last repeating, once recorded under its name.
fn scripted_predicate(run: &Arc<Run>, spec: &Value) -> Result<Predicate, Unreadable> {
    let name = field::<String>(spec, "name")?;
    let answers = field::<Vec<bool>>(spec, "answers")?;
    let last = *answers.last().ok_or("custom: no answers")?;
    let (recorder, asked) = (Arc::downgrade(run), AtomicUsize::new(0));
    Ok(Arc::new(move |event, context, state| {
        if let Some(run) = recorder.upgrade() {
            let call = json!({
                "event_id": event.id,
                "room_id": context.room.id,
                "phase": state.phase,
                "active_agent_id": state.active_agent_id,
            });
            run.record_custom_condition(&name, call);
        }
        let n = asked.fetch_add(1, Ordering::SeqCst);
        answers.get(n).copied().unwrap_or(last)
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
            "organization_id",
        ]),
        "addressing_of.normalize" => Some(&["channel_type", "address", "country_code"]),
        _ => None,
    }
}

/// Makes the routing call `name`.
pub(crate) async fn call(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    match name {
        "default_router.route" => default_route(run, name, spec).await,
        "addressing_of.normalize" => normalize(spec),
        _ => save_state(run, name, spec).await,
    }
}

/// Makes `save_conversation_state`.
async fn save_state(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
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
async fn default_route(run: &Run, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let channel_id = field::<ChannelId>(spec, "channel_id")?;
    let channel_type = field::<ChannelType>(spec, "channel_type")?;
    let identity = optional::<String>(spec, "identity_id")?
        .map(|id| serde_json::from_value(json!({ "id": id })))
        .transpose()
        .map_err(|error| format!("identity_id: {error}"))?;
    let options = RouteOptions {
        participant_id: optional(spec, "participant_id")?,
        organization_id: optional(spec, "organization_id")?,
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

/// Makes `addressing_of.normalize`: `address` written by the addressing a
/// channel of `channel_type` takes by default, with `country_code`, none
/// when absent (RFC §5.7). A code `CountryCode::new` refuses is the call's
/// `invalid_argument`, as Python's `normalize` raising `ValueError` is.
fn normalize(spec: &Value) -> Result<Called, Unreadable> {
    let channel_type = field::<ChannelType>(spec, "channel_type")?;
    let address = field::<String>(spec, "address")?;
    let country_code = match optional::<String>(spec, "country_code")? {
        None => None,
        Some(code) => match CountryCode::new(&code) {
            Ok(code) => Some(code),
            Err(_) => return Ok(Err(named("invalid_argument"))),
        },
    };
    let written =
        addressing_of(&channel_type).normalize(&address, NormalizeOptions { country_code });
    Ok(Ok(json!(written)))
}
