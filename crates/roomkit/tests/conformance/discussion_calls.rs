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
use std::time::Duration;

use roomkit::Discussion;
use roomkit::model::ChannelId;
use roomkit::orchestration::{
    DiscussionOptions, DispatchDecision, DispatchFuture, DispatchPolicy, DispatchTurn,
    MockDispatchPolicy, MockDispatchPolicyOptions, StrategyAgent,
};
use roomkit_ai::testing::ScriptedProvider;
use roomkit_ai::{Agent, AgentOptions};
use serde_json::{Value, json};

use crate::calls::{Called, Unreadable, field, finish, optional};
use crate::identity::RunSlot;
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
            "dispatch",
            "dispatch_timeout_ms",
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
    Ok(Discussion::new(agents, options_of(spec, None)?))
}

/// The keywords of a discussion `spec` describes; its scripted dispatch
/// policy records each turn on `recorder`, the run's slot and the
/// strategy's id.
fn options_of(
    spec: &Value,
    recorder: Option<(RunSlot, String)>,
) -> Result<DiscussionOptions, Unreadable> {
    let defaults = DiscussionOptions::default();
    let dispatch = match spec.get("dispatch") {
        Some(dispatch) => Some(ScriptedDispatch::of(dispatch, recorder)?),
        None => None,
    };
    let dispatch_timeout = optional::<u64>(spec, "dispatch_timeout_ms")?
        .map_or(defaults.dispatch_timeout, Duration::from_millis);
    Ok(DiscussionOptions {
        dispatch,
        dispatch_timeout,
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
    "dispatch",
    "dispatch_timeout_ms",
];

/// The members a scripted dispatch policy may give.
pub(crate) const DISPATCH: &[&str] = &["decisions", "delay_ms", "error", "gate"];

/// A setup's scripted dispatch policy (README, "Setup"): the mock, held at
/// its gate, each turn it is handed recorded as its strategy's.
struct ScriptedDispatch {
    mock: MockDispatchPolicy,
    gate: Option<String>,
    recorder: Option<(RunSlot, String)>,
}

impl ScriptedDispatch {
    fn of(
        spec: &Value,
        recorder: Option<(RunSlot, String)>,
    ) -> Result<Arc<dyn DispatchPolicy>, Unreadable> {
        let decisions = spec["decisions"]
            .as_array()
            .into_iter()
            .flatten()
            .map(decision_of)
            .collect::<Result<Vec<_>, _>>()?;
        let options = MockDispatchPolicyOptions {
            decisions,
            delay: Duration::from_millis(optional::<u64>(spec, "delay_ms")?.unwrap_or(0)),
            error: optional(spec, "error")?,
        };
        Ok(Arc::new(Self {
            mock: MockDispatchPolicy::new(options),
            gate: optional(spec, "gate")?,
            recorder,
        }))
    }
}

impl DispatchPolicy for ScriptedDispatch {
    fn decide(&self, turn: DispatchTurn) -> DispatchFuture<'_> {
        let run = self.recorder.as_ref().and_then(|(slot, _)| slot.run());
        if let (Some(run), Some((_, strategy))) = (&run, &self.recorder) {
            run.record_dispatch_turn(strategy, dispatch_turn_record(&turn));
        }
        Box::pin(async move {
            if let (Some(gate), Some(run)) = (&self.gate, run) {
                run.gates.hold(gate).await;
            }
            self.mock.decide(turn).await
        })
    }
}

/// A scripted decision: a list of channel ids, reason `scripted`, or a
/// decision whole.
fn decision_of(spec: &Value) -> Result<DispatchDecision, Unreadable> {
    if spec.is_array() {
        return Ok(DispatchDecision {
            agents: serde_json::from_value(spec.clone()).map_err(|e| e.to_string())?,
            reason: "scripted".to_owned(),
            ..DispatchDecision::default()
        });
    }
    Ok(DispatchDecision {
        agents: optional(spec, "agents")?.unwrap_or_default(),
        reason: optional(spec, "reason")?.unwrap_or_default(),
        judgments: optional(spec, "judgments")?.unwrap_or_default(),
    })
}

/// What a scripted dispatch policy was handed, as a scenario reads it.
fn dispatch_turn_record(turn: &DispatchTurn) -> Value {
    // In the order of `recent`, then the message, as the README reads them.
    let speakers: Vec<Value> = turn
        .recent
        .iter()
        .chain(std::iter::once(&turn.event))
        .filter_map(|event| Some(json!([event.id, turn.speakers.get(&event.id)?])))
        .collect();
    let candidates: Vec<Value> = turn
        .candidates
        .iter()
        .map(|candidate| {
            json!({
                "channel_id": candidate.channel_id,
                "name": candidate.name,
                "role": candidate.role,
                "description": candidate.description,
                "handle": candidate.handle,
            })
        })
        .collect();
    json!({
        "room_id": turn.room_id,
        "event": turn.event,
        "recent": turn.recent,
        "speakers": speakers,
        "candidates": candidates,
        "asked": turn.asked,
    })
}

/// What an `ON_DISPATCH_DECISION` hook is handed, as a scenario reads it.
pub(crate) fn dispatch_decision_event(event: &roomkit::DispatchDecisionEvent) -> Value {
    json!({
        "room_id": event.room_id,
        "event": event.event,
        "candidates": event.candidates,
        "decision": {
            "agents": event.decision.agents,
            "reason": event.decision.reason,
            "judgments": event.decision.judgments,
        },
        "duration_ms": event.duration_ms,
    })
}

/// The Discussion a setup's strategy describes: its agents by id, its `done`
/// what the last `discussion.done` call named, false until one.
pub(crate) fn installed(id: &str, spec: &Value, slot: &RunSlot) -> Result<Discussion, Unreadable> {
    let mut options = options_of(spec, Some((slot.clone(), id.to_owned())))?;
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
