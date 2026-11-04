//! A discussion built and read (roomkit-specs `conformance/README.md`,
//! "Calls"; RFC §19.7.5): `discussion` builds the implementation's
//! `Discussion` from its keywords, each agent a channel id or an agent given
//! live, and reads it, installed nowhere.

use std::collections::BTreeMap;
use std::sync::Arc;

use roomkit::Discussion;
use roomkit::model::ChannelId;
use roomkit::orchestration::{DiscussionOptions, StrategyAgent};
use roomkit_ai::testing::ScriptedProvider;
use roomkit_ai::{Agent, AgentOptions};
use serde_json::{Value, json};

use crate::calls::{Called, Unreadable, field, finish, optional};
use crate::run::Run;

/// The arguments the call `name` takes, if it is a discussion's.
pub(crate) fn arguments(name: &str) -> Option<&'static [&'static str]> {
    match name {
        "discussion" => Some(&[
            "agents",
            "handles",
            "people",
            "addressed_only",
            "everyone",
            "max_turns",
            "max_depth",
            "silent_token",
            "rules",
        ]),
        _ => None,
    }
}

/// An agent of a `discussion` call: its channel id, or an agent given live.
fn agent(spec: &Value) -> Result<StrategyAgent, Unreadable> {
    if spec.is_string() {
        let id = spec.as_str().unwrap_or_default();
        return Ok(ChannelId::new(id).map_err(|e| e.to_string())?.into());
    }
    let id = field::<ChannelId>(spec, "id")?;
    let options = AgentOptions {
        name: optional::<String>(spec, "name")?,
        ..AgentOptions::default()
    };
    let live =
        Agent::new(id, ScriptedProvider::new(Vec::new()), options).map_err(|e| e.to_string())?;
    Ok(StrategyAgent::channel(live))
}

/// The `discussion` call `spec` describes: built, the refusal the call's.
fn built(spec: &Value) -> Result<Result<Discussion, roomkit::RoomKitError>, Unreadable> {
    let agents = spec["agents"]
        .as_array()
        .ok_or("discussion names no agents")?
        .iter()
        .map(agent)
        .collect::<Result<Vec<_>, _>>()?;
    let defaults = DiscussionOptions::default();
    let options = DiscussionOptions {
        handles: optional::<BTreeMap<ChannelId, String>>(spec, "handles")?,
        people: optional(spec, "people")?,
        addressed_only: optional(spec, "addressed_only")?.unwrap_or(defaults.addressed_only),
        everyone: optional(spec, "everyone")?,
        max_turns: optional(spec, "max_turns")?,
        max_depth: optional(spec, "max_depth")?,
        silent_token: optional(spec, "silent_token")?.unwrap_or(defaults.silent_token),
        rules: optional(spec, "rules")?,
        ..DiscussionOptions::default()
    };
    Ok(Discussion::new(agents, options))
}

/// Makes the discussion's call `name`.
pub(crate) fn call(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let outcome = built(spec)?;
    Ok(finish(run, name, outcome, |discussion| {
        let agents: Vec<String> = discussion
            .agents()
            .iter()
            .map(|agent| agent.channel_id().to_string())
            .collect();
        json!({ "agents": agents, "handles": discussion.handles() })
    }))
}
