//! The scripted hooks on an agent's loop triggers (roomkit-specs
//! `conformance/README.md`, "Scripted agents" and "Expectations"):
//! `before_ai_generation` records its payload as `generation`,
//! `after_tool_round` as `tool_round`, `on_ai_response` as `response`; a
//! modify's `event` is a patch on the payload. And the framework events of
//! an agent's turn, which `expect.ai_events` compares.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};

use roomkit::model::hook::AllowOptions;
use roomkit::model::{HookAction, HookResult, RoomContext};
use roomkit::{HookError, TurnPayload};
use roomkit_ai::{
    AiContext, AiGenerationEvent, AiMessage, AiResponseEvent, AiRole, AiTool, ToolRoundEvent,
};
use serde_json::{Map, Value, json};

use crate::run::{Run, Scope};
use crate::scripted::{Reactions, perform};

/// The framework events of an agent's turn (RFC §8.2, Level 1), recorded
/// apart from the §26.1 table's, under `ai_events`.
pub(crate) const AI_EVENTS: [&str; 2] = ["before_ai_generation", "ai_response"];

/// A loop trigger's payload as a scenario reads it and patches it.
pub(crate) trait Scripted: TurnPayload {
    /// The member a hook's call records it under.
    const MEMBER: &'static str;

    /// The payload as the scenario reads it.
    fn record(&self) -> Value;

    /// The payload a modify's `patch` makes of it.
    fn patched(self, patch: &Value) -> Result<Self, String>;
}

impl Scripted for AiGenerationEvent {
    const MEMBER: &'static str = "generation";

    fn record(&self) -> Value {
        json!({
            "channel_id": self.channel_id.as_str(),
            "provider_name": self.provider_name,
            "purpose": self.purpose,
            "trigger": self.trigger.as_ref().map(|event| event.id.as_str()),
            "system_prompt": self.ai_context.system_prompt,
            "messages": messages(&self.ai_context),
            "tools": self.ai_context.tools.iter().map(|tool| tool.name.as_str()).collect::<Vec<_>>(),
        })
    }

    /// The replacement event: its context's `system_prompt` and `messages`
    /// the patch's, its `tools` those of the context the patch names, and
    /// each tool `define` gives in place of the tool of its name, or after
    /// them.
    fn patched(mut self, patch: &Value) -> Result<Self, String> {
        for (member, value) in patch.as_object().into_iter().flatten() {
            let context = &mut self.ai_context;
            match member.as_str() {
                "system_prompt" => context.system_prompt = value.as_str().map(str::to_owned),
                "messages" => context.messages = patched_messages(value)?,
                "tools" => {
                    let kept: Vec<&str> = value
                        .as_array()
                        .ok_or("a generation's tools are a list")?
                        .iter()
                        .filter_map(Value::as_str)
                        .collect();
                    context
                        .tools
                        .retain(|tool| kept.contains(&tool.name.as_str()));
                }
                // Applied after the others, as Python's runner does.
                "define" => {}
                other => return Err(format!("{other}: no generation member the harness patches")),
            }
        }
        for definition in patch["define"].as_array().into_iter().flatten() {
            let tool = AiTool {
                parameters: definition
                    .get("parameters")
                    .cloned()
                    .unwrap_or_else(|| json!({ "type": "object" })),
                ..AiTool::new(
                    definition["name"].as_str().unwrap_or_default(),
                    definition["description"].as_str().unwrap_or_default(),
                )
                .map_err(|e| e.to_string())?
            };
            let tools = &mut self.ai_context.tools;
            match tools.iter_mut().find(|own| own.name == tool.name) {
                Some(own) => *own = tool,
                None => tools.push(tool),
            }
        }
        Ok(self)
    }
}

impl Scripted for ToolRoundEvent {
    const MEMBER: &'static str = "tool_round";

    fn record(&self) -> Value {
        let calls: Vec<Value> = self
            .calls
            .iter()
            .map(|call| json!({ "tool_call_id": call.id, "name": call.name, "arguments": call.arguments }))
            .collect();
        let results: Vec<Value> = self
            .results
            .iter()
            .map(|result| {
                json!({
                    "tool_call_id": result.tool_call_id,
                    "content": result.result,
                    "is_error": result.is_error,
                })
            })
            .collect();
        json!({
            "channel_id": self.channel_id.as_str(),
            "round_index": self.round_index,
            "calls": calls,
            "results": results,
            "tools": self.tools,
        })
    }

    /// What the hook does to the round in place: each name of `withdrawn`
    /// withdrawn, each text of `messages` added.
    fn patched(mut self, patch: &Value) -> Result<Self, String> {
        for (member, value) in patch.as_object().into_iter().flatten() {
            let texts = value
                .as_array()
                .ok_or_else(|| format!("{member}: a list"))?
                .iter()
                .filter_map(Value::as_str);
            match member.as_str() {
                "withdrawn" => self.withdraw(texts),
                "messages" => texts.for_each(|text| self.add_message(text)),
                other => return Err(format!("{other}: no round member the harness patches")),
            }
        }
        Ok(self)
    }
}

impl Scripted for AiResponseEvent {
    const MEMBER: &'static str = "response";

    fn record(&self) -> Value {
        let declared: Map<String, Value> = self
            .declared_tools
            .iter()
            .enumerate()
            .map(|(position, tool)| {
                let entry = json!({
                    "position": position,
                    "description": tool.description,
                    "parameters": tool.parameters,
                    "origin": tool.origin,
                });
                (tool.name.clone(), entry)
            })
            .collect();
        json!({
            "channel_id": self.channel_id.as_str(),
            "response_content": self.response_content,
            "segments": self.segments,
            "tool_calls_count": self.tool_calls_count,
            "round_count": self.round_count,
            "loop_end_reason": self.loop_end_reason.map(|reason| reason.to_string()),
            "usage": self.usage,
            "streaming": self.streaming,
            "thinking": self.thinking,
            "declared_tools": declared,
        })
    }

    fn patched(self, _patch: &Value) -> Result<Self, String> {
        Err("a response's hook observes: no patch".to_owned())
    }
}

/// The messages `context` carries as `models` reads them: each its role
/// and its text parts joined.
pub(crate) fn messages(context: &AiContext) -> Vec<Value> {
    context
        .messages
        .iter()
        .map(|message| json!({ "role": message.role.as_str(), "text": message.content.text() }))
        .collect()
}

/// A patch's `messages`, each `{role, text}`.
fn patched_messages(value: &Value) -> Result<Vec<AiMessage>, String> {
    value
        .as_array()
        .ok_or("a generation's messages are a list")?
        .iter()
        .map(|message| {
            let role = match message["role"].as_str() {
                Some("system") => AiRole::System,
                Some("user") => AiRole::User,
                Some("assistant") => AiRole::Assistant,
                Some("tool") => AiRole::Tool,
                other => return Err(format!("{other:?}: no message role")),
            };
            Ok(AiMessage::new(
                role,
                message["text"].as_str().unwrap_or_default(),
            ))
        })
        .collect()
}

/// What a scripted hook on a loop trigger gives.
type HookFuture<P> = Pin<Box<dyn Future<Output = Result<HookResult<P>, HookError>> + Send>>;

/// The handler of the scripted hook `spec` on a loop trigger: it records
/// its call, its payload under its member, then reacts; its `result` is
/// the hook's, a modify's `event` a patch on the payload.
pub(crate) fn turn_hook_handler<P: Scripted>(
    spec: &Value,
    run: &Arc<Run>,
) -> impl Fn(P, RoomContext) -> HookFuture<P> + Send + Sync + 'static {
    let reactions = Arc::new(Reactions::new(spec.get("reactions")));
    let name = spec["name"].as_str().unwrap_or_default().to_owned();
    let trigger = spec["trigger"].as_str().unwrap_or_default().to_owned();
    let run = Arc::downgrade(run);
    move |payload: P, context: RoomContext| {
        let reactions = Arc::clone(&reactions);
        let (name, trigger, run) = (name.clone(), trigger.clone(), Weak::clone(&run));
        Box::pin(async move {
            let allow = HookResult::allow(AllowOptions::default());
            let Some(run) = run.upgrade() else {
                return Ok(allow);
            };
            let room_id = context.room.id.to_string();
            run.record_payload_hook(&name, &trigger, &room_id, P::MEMBER, payload.record());
            let Some(reaction) = reactions.pick(&json!({ "room_id": room_id })) else {
                return Ok(allow);
            };
            perform(&run, &reaction, &Scope::in_room(&room_id)).await?;
            match reaction.get("result") {
                Some(result) => Ok(turn_hook_result(result, payload)?),
                None => Ok(allow),
            }
        })
    }
}

/// A scenario's hook result on a loop trigger's payload.
fn turn_hook_result<P: Scripted>(spec: &Value, payload: P) -> Result<HookResult<P>, String> {
    let action: HookAction = serde_json::from_value(spec["action"].clone())
        .map_err(|error| format!("action: {error}"))?;
    let mut result = HookResult::new(action);
    result.reason = spec["reason"].as_str().map(str::to_owned);
    if let Some(patch) = spec.get("event") {
        result.event = Some(payload.patched(patch)?);
    }
    Ok(result)
}
