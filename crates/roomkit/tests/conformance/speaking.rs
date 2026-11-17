//! Scripted public speaking policies and thinkers with their actual call records.
#[path = "speaking/scripts.rs"]
mod scripts;
#[path = "speaking/shape.rs"]
mod shape;
#[cfg(test)]
#[path = "speaking/tests.rs"]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use roomkit_ai::speaking::{SpeakDecision, SpeakDecisionEvent, SpeakTurn, ThoughtEvent};
use roomkit_ai::{AiChannelOptions, AiContext};
use serde_json::{Value, json};

use crate::ai_hooks::Scripted;
use crate::run::{Run, lock};
pub(crate) use shape::{check, hook};

pub(crate) fn install(
    channel: &Value,
    run: &Arc<Run>,
    options: &mut AiChannelOptions,
) -> Result<(), String> {
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
        options.speak_policy = Some(Arc::new(scripts::Policy::new(spec, id, run)?));
    }
    if let Some(spec) = channel.get("thinker") {
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
    Ok(())
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
        "people":turn.people,"channel_id":turn.channel_id,"speakers":turn.speakers,"thought":turn.thought})
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
    fn patched(mut self, patch: &Value) -> Result<Self, String> {
        shape::patch(patch)?;
        self.decision = decision(&patch["decision"])?;
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
