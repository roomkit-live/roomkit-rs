//! The handoff (roomkit-specs `conformance/README.md`, "Calls"; RFC §19.6):
//! `handoff_handler` builds the implementation's `HandoffHandler`,
//! `install_router` the one a router's `install` returns and
//! `install_pipeline` the one a `ConversationPipeline`'s `install` returns, each
//! under a name the later calls use; `conversation_pipeline` builds a pipeline
//! and reads it; `setup_handoff` serves the tool on an agent; `handoff.handle`,
//! `handoff.set_language` and `handoff.send_greeting` are the handler's own
//! calls.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use roomkit::model::{ChannelId, Metadata, RoomId};
use roomkit::orchestration::{
    ConversationPipelineOptions, HandoffHandlerOptions, InstallOptions, SendGreetingOptions,
    StrategyAgent, pipeline,
};
use roomkit::{ConversationPipeline, HandoffHandler, PipelineStage, RoomKitError};
use roomkit_ai::orchestration::build_handoff_tool;
use roomkit_ai::{SetupHandoffOptions, setup_handoff};
use serde_json::{Value, json};

use crate::calls::{Called, Unreadable, field, finish, optional};
use crate::run::{Run, lock};

/// The arguments of the handoff calls, for the shape check.
pub(crate) fn arguments(name: &str) -> Option<&'static [&'static str]> {
    match name {
        "handoff_handler" => Some(&[
            "handler",
            "router",
            "agent_aliases",
            "phase_map",
            "allowed_transitions",
            "known_agents",
            "greeting_map",
            "event_channel_id",
        ]),
        "install_router" => Some(&[
            "handler",
            "router",
            "agents",
            "agent_aliases",
            "phase_map",
            "hook_priority",
        ]),
        "conversation_pipeline" => Some(&["stages", "default_phase", "supervisor_id"]),
        "install_pipeline" => Some(&[
            "handler",
            "stages",
            "default_phase",
            "supervisor_id",
            "agents",
            "agent_aliases",
            "hook_priority",
        ]),
        "setup_handoff" => Some(&["agent_id", "handler", "targets", "room_id"]),
        "handoff.handle" => Some(&["handler", "room_id", "calling_agent_id", "arguments"]),
        "handoff.set_language" => Some(&["handler", "room_id", "language"]),
        "handoff.send_greeting" => Some(&["handler", "room_id", "channel_id"]),
        _ => None,
    }
}

/// The handler the run keeps under the name the call's `handler` gives.
fn handler(run: &Run, spec: &Value) -> Result<HandoffHandler<crate::Store>, Unreadable> {
    let name = field::<String>(spec, "handler")?;
    lock(&run.handoff_handlers)
        .get(&name)
        .cloned()
        .ok_or_else(|| format!("no handoff handler {name:?}"))
}

/// Keeps `built` under the call's `handler`, on success.
fn keep(
    run: &Run,
    name: &str,
    spec: &Value,
    built: Result<HandoffHandler<crate::Store>, RoomKitError>,
) -> Result<Called, Unreadable> {
    let named = field::<String>(spec, "handler")?;
    Ok(finish(run, name, built, |handler| {
        lock(&run.handoff_handlers).insert(named, handler.clone());
        Value::Null
    }))
}

/// The `ConversationPipeline` a call's `stages` describe.
fn conversation_pipeline(
    spec: &Value,
) -> Result<Result<ConversationPipeline, RoomKitError>, Unreadable> {
    let options = ConversationPipelineOptions {
        default_phase: optional(spec, "default_phase")?,
        supervisor_id: optional(spec, "supervisor_id")?,
    };
    let stages = field::<Vec<PipelineStage>>(spec, "stages")?;
    Ok(ConversationPipeline::new(stages, options))
}

/// What the `conversation_pipeline` call reads of `pipeline`.
fn read_pipeline(pipeline: &ConversationPipeline) -> Value {
    let allowed: BTreeMap<String, BTreeSet<String>> = pipeline
        .get_allowed_transitions()
        .into_iter()
        .map(|(phase, to)| (phase, to.into_iter().collect()))
        .collect();
    json!({
        "stages": pipeline.stages(),
        "phase_map": pipeline.get_phase_map(),
        "allowed_transitions": allowed,
    })
}

/// Makes the handoff call `name`.
pub(crate) async fn call(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    match name {
        "handoff_handler" => {
            let allowed: Option<HashMap<String, HashSet<String>>> =
                optional(spec, "allowed_transitions")?;
            let options = HandoffHandlerOptions {
                agent_aliases: optional(spec, "agent_aliases")?,
                phase_map: optional(spec, "phase_map")?,
                allowed_transitions: allowed,
                known_agents: optional(spec, "known_agents")?,
                event_channel_id: optional(spec, "event_channel_id")?,
                ..HandoffHandlerOptions::default()
            };
            let router = crate::routing_calls::router(&spec["router"])?;
            let greeting_map: Option<HashMap<ChannelId, String>> = optional(spec, "greeting_map")?;
            let handler = HandoffHandler::new(run.kit(), router, options);
            if let Some(greeting_map) = greeting_map {
                handler.set_greeting_map(greeting_map);
            }
            keep(run, name, spec, Ok(handler))
        }
        "install_router" => {
            let agents = field::<Vec<ChannelId>>(spec, "agents")?;
            let agents: Vec<StrategyAgent> = agents.into_iter().map(StrategyAgent::from).collect();
            let defaults = InstallOptions::default();
            let options = InstallOptions {
                agent_aliases: optional(spec, "agent_aliases")?,
                phase_map: optional(spec, "phase_map")?,
                hook_priority: optional(spec, "hook_priority")?.unwrap_or(defaults.hook_priority),
            };
            let router = crate::routing_calls::router(&spec["router"])?;
            let installed = router.install(run.kit(), &agents, options);
            keep(run, name, spec, installed)
        }
        "conversation_pipeline" => {
            let built = conversation_pipeline(spec)?;
            Ok(finish(run, name, built, read_pipeline))
        }
        "install_pipeline" => {
            let agents = field::<Vec<ChannelId>>(spec, "agents")?;
            let agents: Vec<StrategyAgent> = agents.into_iter().map(StrategyAgent::from).collect();
            let defaults = pipeline::InstallOptions::default();
            let options = pipeline::InstallOptions {
                agent_aliases: optional(spec, "agent_aliases")?,
                hook_priority: optional(spec, "hook_priority")?.unwrap_or(defaults.hook_priority),
            };
            let installed = conversation_pipeline(spec)?
                .and_then(|built| built.install(run.kit(), &agents, options))
                .map(|(_router, handler)| handler);
            keep(run, name, spec, installed)
        }
        "setup_handoff" => {
            let agent = field::<ChannelId>(spec, "agent_id")?;
            let targets: Option<Vec<(String, Option<String>)>> = optional(spec, "targets")?;
            let options = SetupHandoffOptions {
                tool: targets.map(|targets| build_handoff_tool(&targets)),
                room_id: optional(spec, "room_id")?,
            };
            let set_up = setup_handoff(agent, &handler(run, spec)?, options);
            Ok(finish(run, name, set_up, |()| Value::Null))
        }
        "handoff.handle" => {
            let room_id = field::<RoomId>(spec, "room_id")?;
            let calling = field::<ChannelId>(spec, "calling_agent_id")?;
            let arguments = field::<Metadata>(spec, "arguments")?;
            let result = handler(run, spec)?
                .handle(&room_id, &calling, arguments)
                .await;
            Ok(finish(run, name, result, |result| {
                serde_json::to_value(result).unwrap_or(Value::Null)
            }))
        }
        "handoff.set_language" => {
            let room_id = field::<RoomId>(spec, "room_id")?;
            let language = field::<String>(spec, "language")?;
            let set = handler(run, spec)?.set_language(&room_id, language).await;
            Ok(finish(run, name, set, |()| Value::Null))
        }
        "handoff.send_greeting" => {
            let room_id = field::<RoomId>(spec, "room_id")?;
            let options = SendGreetingOptions {
                channel_id: optional(spec, "channel_id")?,
            };
            let said = handler(run, spec)?.send_greeting(&room_id, options).await;
            Ok(finish(run, name, said, |()| Value::Null))
        }
        other => Err(format!("no handoff call {other:?}")),
    }
}
