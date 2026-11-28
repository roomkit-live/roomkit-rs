//! Scripted public speaking policies and thinkers with their actual call records.
#[path = "speaking/classifier.rs"]
mod classifier;
#[path = "speaking/scripts.rs"]
mod scripts;
#[path = "speaking/shape.rs"]
mod shape;
#[cfg(test)]
#[path = "speaking/tests.rs"]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use roomkit_ai::speaking::{
    LlmThinker, LlmThinkerOptions, SpeakDecision, SpeakDecisionEvent, SpeakPolicy, SpeakTurn,
    Thinker, ThoughtEvent,
};
use roomkit_ai::{AiChannelOptions, AiContext};
use serde_json::{Value, json};

use crate::ai_hooks::Scripted;
use crate::run::{Run, lock};
use roomkit_ai::testing::ScriptedProvider;
pub(crate) use shape::{check, hook};

/// A scripted model under the name `expect.models` gives it.
type NamedModel = (String, ScriptedProvider);

/// Installs the channel's speak policy and thinker; returns the scripted model
/// of its LLM thinker, under the name `expect.models` gives it, when it has one.
pub(crate) fn install(
    channel: &Value,
    run: &Arc<Run>,
    options: &mut AiChannelOptions,
    shared: Option<ScriptedProvider>,
) -> Result<Option<NamedModel>, String> {
    let mut problems = Vec::new();
    check(channel, "channel", &mut problems);
    if !problems.is_empty() {
        return Err(problems.join("; "));
    }
    let id = channel["id"].as_str().ok_or("speaking channel id")?;
    {
        let mut observed = lock(&run.observed);
        observed.speak_turns.entry(id.to_owned()).or_default();
        observed.thinker_calls.entry(id.to_owned()).or_default();
    }
    if let Some(spec) = channel.get("speak_policy") {
        let scripted = || -> Result<Arc<dyn SpeakPolicy>, String> {
            Ok(Arc::new(scripts::Policy::new(spec, id, run)?))
        };
        options.speak_policy = Some(classifier::policy(spec, id, run, scripted)?);
    }
    let mut thinker_model = None;
    if let Some(llm) = channel["thinker"].get("llm") {
        let (thinker, model) = llm_thinker(llm, id, run, shared)?;
        options.thinker = Some(thinker);
        thinker_model = Some(model);
    } else if let Some(spec) = channel.get("thinker") {
        options.thinker = Some(Arc::new(scripts::Thinking::new(spec, id, run)?));
    }
    if let Some(value) = channel.get("speak_timeout_ms").and_then(Value::as_f64) {
        // Nonpositive speak bounds reach the public constructor's refusal.
        options.speak_timeout = if value <= 0.0 {
            Duration::ZERO
        } else {
            Duration::try_from_secs_f64(value / 1000.0).map_err(|e| e.to_string())?
        };
    }
    if let Some(value) = channel.get("think_wait_ms").and_then(Value::as_f64) {
        options.think_wait =
            Duration::try_from_secs_f64(value / 1000.0).map_err(|e| e.to_string())?;
    }
    Ok(thinker_model)
}

/// The implementation's LLM thinker over the scripted model `spec` names
/// (README, `thinker.llm`), and that model under `<agent>/thinker`.
fn llm_thinker(
    spec: &Value,
    id: &str,
    run: &Arc<Run>,
    shared: Option<ScriptedProvider>,
) -> Result<(Arc<dyn Thinker>, NamedModel), String> {
    let name = format!("{id}/thinker");
    let (model, paced) = crate::agents::scripted_model(&spec["model"], run, &name, shared)?;
    let mut options = LlmThinkerOptions::default();
    if let Some(instructions) = spec.get("instructions").and_then(Value::as_str) {
        options.instructions = Some(instructions.to_owned());
    }
    if let Some(bound) = spec.get("max_tokens").and_then(Value::as_u64) {
        options.max_tokens = Some(u32::try_from(bound).map_err(|e| e.to_string())?);
    }
    if let Some(effort) = spec.get("reasoning_effort").and_then(Value::as_str) {
        options.reasoning_effort = Some(effort.to_owned());
    }
    if let Some(bound) = spec.get("timeout_ms").and_then(Value::as_f64) {
        options.timeout = Some(bound / 1000.0);
    }
    let thinker = LlmThinker::new(paced, options).map_err(|e| e.to_string())?;
    Ok((Arc::new(thinker), (name, model)))
}

fn decision(value: &Value) -> Result<SpeakDecision, String> {
    shape::decision(value)?;
    let mut full = json!({"reason":"", "notes":[], "judgments":{}, "final":false});
    full.as_object_mut()
        .ok_or("decision defaults")?
        .extend(value.as_object().ok_or("decision object")?.clone());
    serde_json::from_value(full).map_err(|e| e.to_string())
}

fn turn_record(turn: &SpeakTurn) -> Value {
    json!({"event":turn.event.snapshot(), "recent":turn.recent.iter().map(roomkit_ai::speaking::SharedRoomEvent::snapshot).collect::<Vec<_>>(),
        "people":turn.people,"channel_id":turn.channel_id,"speakers":turn.speakers,"thought":turn.thought,"cut":turn.cut})
}

fn context_record(context: &AiContext) -> Value {
    json!({"messages":crate::agents::messages(context),"system_prompt":context.system_prompt,
        "tools":context.tools.iter().map(|tool| tool.name.as_str()).collect::<Vec<_>>(),
        "room":context.room,"target_capabilities":context.target_capabilities,"target_media_types":context.target_media_types})
}

impl Scripted for SpeakDecisionEvent {
    const MEMBER: &'static str = "speak_decision";
    fn record(&self) -> Value {
        json!({"room_id":self.room_id,"channel_id":self.channel_id,"event":self.event.snapshot(),
            "decision":self.decision,"duration_ms":self.duration_ms,"asked_again":self.asked_again,
            "late":self.late,"thought":self.thought})
    }
    /// The patch's decision, and the event decided on (a patch on it), the
    /// room and the channel it names written in too (README, `speak_policy`).
    fn patched(mut self, patch: &Value) -> Result<Self, String> {
        shape::patch(patch)?;
        self.decision = decision(&patch["decision"])?;
        if let Some(members) = patch["event"].as_object() {
            let mut event =
                serde_json::to_value(self.event.snapshot()).map_err(|e| e.to_string())?;
            for (member, value) in members {
                event[member] = value.clone();
            }
            let event = serde_json::from_value(event).map_err(|e| e.to_string())?;
            self.event = roomkit_ai::speaking::SharedRoomEvent::new(event);
        }
        for (member, field) in [
            ("room_id", &mut self.room_id),
            ("channel_id", &mut self.channel_id),
        ] {
            if let Some(value) = patch[member].as_str() {
                value.clone_into(field);
            }
        }
        Ok(self)
    }
}

impl Scripted for ThoughtEvent {
    const MEMBER: &'static str = "thought";
    fn record(&self) -> Value {
        json!(self)
    }
    fn patched(self, _patch: &Value) -> Result<Self, String> {
        Err("an ON_THOUGHT hook is observed: it patches nothing".into())
    }
}
