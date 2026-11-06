//! A discussion in the runner (roomkit-specs `conformance/README.md`,
//! "Calls", "Setup"; RFC §19.7.5): `discussion` builds the implementation's
//! `Discussion` from its keywords, each agent a channel id or an agent given
//! live, and reads it, installed nowhere. A setup's strategy with a
//! `discussion` is one installed through the kit, its `done` read from the
//! `discussion.done` call; `speak_queue` reads its queue, `listen_only` and
//! `talk_again` change it, and an `ON_SPEAK_QUEUE` hook records each change
//! it is handed.

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
        "speak_queue" => Some(&["room_id", "organization_id"]),
        "listen_only" | "talk_again" => Some(&["room_id", "channel_ids", "organization_id"]),
        "discussion.done" => Some(&["strategy", "holds"]),
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
    Ok(Discussion::new(agents, options_of(spec)?))
}

/// The keywords of a discussion `spec` describes.
fn options_of(spec: &Value) -> Result<DiscussionOptions, Unreadable> {
    let defaults = DiscussionOptions::default();
    Ok(DiscussionOptions {
        handles: optional::<BTreeMap<ChannelId, String>>(spec, "handles")?,
        people: optional(spec, "people")?,
        addressed_only: optional(spec, "addressed_only")?.unwrap_or(defaults.addressed_only),
        everyone: optional(spec, "everyone")?,
        max_turns: optional(spec, "max_turns")?,
        max_depth: optional(spec, "max_depth")?,
        silent_token: optional(spec, "silent_token")?.unwrap_or(defaults.silent_token),
        rules: optional(spec, "rules")?,
        ..DiscussionOptions::default()
    })
}

/// The members a setup's strategy with a `discussion` may give.
pub(crate) const DISCUSSION: &[&str] = &[
    "agents",
    "handles",
    "people",
    "addressed_only",
    "everyone",
    "max_turns",
    "max_depth",
    "silent_token",
    "rules",
    "done",
];

/// The Discussion a setup's strategy describes: its agents by id, its `done`
/// what the last `discussion.done` call named, false until one.
pub(crate) fn installed(
    id: &str,
    spec: &Value,
    slot: &crate::identity::RunSlot,
) -> Result<Discussion, Unreadable> {
    let mut options = options_of(spec)?;
    if spec.get("done").and_then(Value::as_bool) == Some(true) {
        let (id, slot) = (id.to_owned(), slot.clone());
        options.done = Some(Arc::new(move |_room| {
            let holds = slot.run().is_some_and(|run| run.discussion_done(&id));
            Box::pin(async move { holds })
        }));
    }
    let agents = field::<Vec<ChannelId>>(spec, "agents")?;
    let agents = agents.into_iter().map(StrategyAgent::from).collect();
    Discussion::new(agents, options).map_err(|error| error.to_string())
}

/// A speak queue as a scenario reads it.
fn queue_record(queue: &roomkit::SpeakQueue) -> Value {
    json!({
        "speaking": queue.speaking,
        "queue": queue.queue,
        "listening": queue.listening,
        "asked": queue.asked,
        "waiting": queue.waiting,
        "over": queue.over,
    })
}

/// What an `ON_SPEAK_QUEUE` hook is handed, as a scenario reads it.
pub(crate) fn speak_queue_event(event: &roomkit::SpeakQueueEvent) -> Value {
    let change = serde_json::to_value(event.change).unwrap_or_default();
    json!({
        "room_id": event.room_id,
        "change": change,
        "channel_ids": event.channel_ids,
        "event_id": event.event_id,
        "queue": queue_record(&event.queue),
    })
}

/// Makes the discussion's call `name`.
pub(crate) async fn call(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    if name == "speak_queue" {
        let room: roomkit::model::RoomId = field(spec, "room_id")?;
        let options = roomkit::SpeakQueueOptions {
            organization_id: optional(spec, "organization_id")?,
        };
        let queue = run.kit().speak_queue(&room, options);
        let result = queue.as_ref().map_or(Value::Null, queue_record);
        return Ok(finish(
            run,
            name,
            Ok::<_, roomkit::RoomKitError>(result),
            Clone::clone,
        ));
    }
    if name == "listen_only" || name == "talk_again" {
        return listening(run, name, spec).await;
    }
    if name == "discussion.done" {
        let id: String = field(spec, "strategy")?;
        run.set_discussion_done(id, field(spec, "holds")?);
        return Ok(finish(
            run,
            name,
            Ok::<_, roomkit::RoomKitError>(Value::Null),
            Clone::clone,
        ));
    }
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

/// Makes `listen_only` or `talk_again`, as `name` says.
async fn listening(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let room: roomkit::model::RoomId = field(spec, "room_id")?;
    let agents: Vec<ChannelId> = field(spec, "channel_ids")?;
    let organization_id: Option<String> = optional(spec, "organization_id")?;
    let kit = run.kit();
    let outcome = if name == "listen_only" {
        let options = roomkit::ListenOnlyOptions { organization_id };
        kit.listen_only(&room, &agents, options).await
    } else {
        let options = roomkit::TalkAgainOptions { organization_id };
        kit.talk_again(&room, &agents, options).await
    };
    Ok(finish(
        run,
        name,
        outcome.map(|()| Value::Null),
        Clone::clone,
    ))
}
