//! The scripted agent of a scenario (roomkit-specs `conformance/README.md`,
//! "Scripted agents"): roomkit-ai's own AI channel over a scripted model
//! and scripted tools, and the hooks on the tool triggers, which register
//! through the kit's typed registrations.

use std::future::pending;
use std::sync::{Arc, Weak};
use std::time::Duration;

use roomkit::model::{ChannelId, Metadata, RoomContext, ToolCall};
use roomkit::transcript::Role;
use roomkit::{
    HookExecution, HookTrigger, KitError, ToolCallEvent, ToolHookSpec, ToolUse, ToolVerdict,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiAgent, AiRequest, ToolContext, ToolDefinition, ToolError, ToolOutput, ToolPolicy, Tools,
};
use serde_json::{Value, json};

use crate::run::{Run, Scope};
use crate::scripted::{Reactions, perform};

/// The members a tool hook of this runner cannot apply, which the tool
/// registrations do not take.
const NOT_FOR_TOOL_HOOKS: [&str; 5] = [
    "channel_types",
    "channel_ids",
    "directions",
    "fail_closed",
    "needs_lock",
];

/// The scripted agent `spec` describes, and its model.
pub(crate) fn scripted_agent(
    spec: &Value,
) -> Result<(AiAgent<ScriptedProvider>, ScriptedProvider), String> {
    let id = ChannelId::new(spec["id"].as_str().unwrap_or_default()).map_err(|e| e.to_string())?;
    let rounds = spec["model"]["rounds"]
        .as_array()
        .into_iter()
        .flatten()
        .map(round)
        .collect::<Result<Vec<_>, _>>()?;
    let model = ScriptedProvider::new(rounds);
    let mut agent = AiAgent::new(id, model.clone())
        .with_tools(tools(spec))
        .map_err(|e| e.to_string())?;
    if let Some(policy) = spec.get("tool_policy") {
        agent = agent.with_tool_policy(tool_policy(policy));
    }
    if let Some(rounds) = spec.get("max_tool_rounds") {
        let rounds = rounds.as_u64().ok_or("max_tool_rounds: not a count")?;
        agent = agent.with_max_rounds(u32::try_from(rounds).map_err(|e| e.to_string())?);
    }
    Ok((agent, model))
}

/// One round of the model's script.
fn round(spec: &Value) -> Result<ScriptedRound, String> {
    let calls: Vec<(String, String, String)> = spec["calls"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|call| {
            // A cut call's arguments never arrived (README: `cut`).
            let arguments = if call["cut"] == true {
                String::new()
            } else {
                call.get("arguments")
                    .map_or_else(|| "{}".to_owned(), Value::to_string)
            };
            let text = |key: &str| call[key].as_str().unwrap_or_default().to_owned();
            (text("id"), text("name"), arguments)
        })
        .collect();
    let finish = spec["finish_reason"]
        .as_str()
        .unwrap_or(if calls.is_empty() {
            "stop"
        } else {
            "tool_calls"
        });
    let made = calls
        .iter()
        .map(|(id, name, arguments)| (id.as_str(), name.as_str(), arguments.as_str()));
    let usage: Metadata = match spec.get("usage") {
        Some(usage) => serde_json::from_value(usage.clone()).map_err(|e| e.to_string())?,
        None => Metadata::new(),
    };
    let mut round = ScriptedRound::calling(spec["text"].as_str().unwrap_or_default(), made)
        .with_finish_reason(finish)
        .with_usage(usage);
    if let Some(deltas) = spec.get("deltas") {
        let deltas = deltas.as_array().ok_or("deltas: not a list")?;
        round = round.in_pieces(deltas.iter().filter_map(Value::as_str));
    }
    if let Some(failure) = spec.get("fails") {
        round = round.failing(failure.as_str().ok_or("fails: not a text")?);
    }
    Ok(round)
}

/// The scripted tools of `spec`, each answering every call one way.
fn tools(spec: &Value) -> Tools {
    spec["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .fold(Tools::new(), |tools, tool| {
            let definition = ToolDefinition::new(
                tool["name"].as_str().unwrap_or_default(),
                tool["description"].as_str().unwrap_or_default(),
                tool.get("parameters")
                    .cloned()
                    .unwrap_or_else(|| json!({ "type": "object" })),
            );
            let behaviour = tool.clone();
            tools.tool(definition, move |_call: ToolCall, _context: ToolContext| {
                let behaviour = behaviour.clone();
                async move { answer(&behaviour).await }
            })
        })
}

/// What a scripted tool answers.
async fn answer(tool: &Value) -> Result<ToolOutput, ToolError> {
    let words = |key: &str| tool[key].as_str().map(str::to_owned);
    if let Some(text) = words("returns") {
        return Ok(ToolOutput::Text(text));
    }
    if let Some(raised) = tool.get("raises") {
        let field = |key: &str| raised[key].as_str().unwrap_or_default().to_owned();
        return Err(ToolError::raised_as(field("kind"), field("message")));
    }
    if let Some(failure) = words("fails") {
        return Err(ToolError::failed(failure));
    }
    if let Some(refusal) = words("refuses") {
        return Err(ToolError::refused(refusal));
    }
    pending().await
}

fn tool_policy(spec: &Value) -> ToolPolicy {
    let patterns = |key: &str| {
        spec[key]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect::<Vec<_>>()
    };
    let allowed = patterns("allow")
        .into_iter()
        .fold(ToolPolicy::new(), ToolPolicy::allow);
    patterns("deny").into_iter().fold(allowed, ToolPolicy::deny)
}

/// Each request `model` received, as the scenario reads it: the tool
/// results it carries (README, `models`).
pub(crate) fn requests(model: &ScriptedProvider) -> Vec<Value> {
    model.requests().iter().map(request).collect()
}

fn request(request: &AiRequest) -> Value {
    let results: Vec<Value> = request
        .messages
        .iter()
        .filter(|message| message.role == Role::Tool)
        .map(|message| json!({ "tool_call_id": message.tool_call_id, "content": message.text }))
        .collect();
    json!({ "tool_results": results })
}

/// Registers the hook `spec` on the tool trigger `trigger`, run as
/// `execution`, through the kit's typed registrations.
pub(crate) fn register_tool_hook(
    run: &Arc<Run>,
    spec: &Value,
    trigger: HookTrigger,
    execution: HookExecution,
) -> Result<Result<(), KitError>, String> {
    if let Some(member) = NOT_FOR_TOOL_HOOKS.iter().find(|m| spec.get(**m).is_some()) {
        return Err(format!("{member}: a tool hook takes none"));
    }
    let mut hook = ToolHookSpec::new(spec["name"].as_str().unwrap_or_default());
    hook = hook.with_priority(
        spec.get("priority")
            .and_then(Value::as_i64)
            .and_then(|p| i32::try_from(p).ok())
            .unwrap_or_default(),
    );
    if let Some(timeout) = spec.get("timeout_ms").and_then(Value::as_u64) {
        hook = hook.with_timeout(Duration::from_millis(timeout));
    }
    let scripted = ToolHook::new(spec, run);
    let kit = run.kit();
    let registered = match (trigger, execution) {
        (HookTrigger::BeforeToolUse, HookExecution::Sync) => {
            kit.before_tool_use(hook, move |event, context| {
                let scripted = scripted.clone();
                async move { scripted.gate(event, context).await }
            })
        }
        (HookTrigger::OnToolCall, HookExecution::Sync) => {
            kit.on_tool_call(hook, move |event, context| {
                let scripted = scripted.clone();
                async move { scripted.verdict(event, context).await }
            })
        }
        (HookTrigger::OnToolCall, HookExecution::Async) => {
            kit.observe_tool_calls(hook, move |event, context| {
                let scripted = scripted.clone();
                async move {
                    // An observer's failure is logged by the kit's rule; here
                    // its reaction runs, and what it raises is dropped.
                    let _observed = scripted.react(&event, &context).await;
                }
            })
        }
        (trigger, execution) => {
            return Err(format!("no tool hook on {trigger:?} runs {execution:?}"));
        }
    };
    Ok(registered.map(drop))
}

/// A scripted hook on a tool trigger: it records its call, then reacts.
#[derive(Clone)]
struct ToolHook {
    name: String,
    trigger: String,
    reactions: Arc<Reactions>,
    run: Weak<Run>,
}

type HookOutcome<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

impl ToolHook {
    fn new(spec: &Value, run: &Arc<Run>) -> Self {
        Self {
            name: spec["name"].as_str().unwrap_or_default().to_owned(),
            trigger: spec["trigger"].as_str().unwrap_or_default().to_owned(),
            reactions: Arc::new(Reactions::new(spec.get("reactions"))),
            run: Arc::downgrade(run),
        }
    }

    /// Records the call, runs its reaction, and gives the reaction's
    /// `result`, if any.
    async fn react(&self, event: &ToolCallEvent, context: &RoomContext) -> HookOutcome<Value> {
        let Some(run) = self.run.upgrade() else {
            return Ok(Value::Null);
        };
        let room_id = context.room.id.to_string();
        run.record_tool_hook(
            &self.name,
            &self.trigger,
            &room_id,
            &tool_call_record(event),
        );
        let Some(reaction) = self.reactions.pick(&json!({ "room_id": room_id })) else {
            return Ok(Value::Null);
        };
        perform(&run, &reaction, &Scope::in_room(&room_id)).await?;
        Ok(reaction.get("result").cloned().unwrap_or(Value::Null))
    }

    async fn gate(&self, event: ToolCallEvent, context: RoomContext) -> HookOutcome<ToolUse> {
        let result = self.react(&event, &context).await?;
        Ok(match result["action"].as_str() {
            Some("block") => ToolUse::Block(result["reason"].as_str().map(str::to_owned)),
            Some("modify") => match result["event"]["arguments"].as_object() {
                Some(arguments) => ToolUse::Rewrite(arguments.clone()),
                None => return Err("a modify of a tool call patches its arguments".into()),
            },
            Some(_) | None => ToolUse::Allow,
        })
    }

    async fn verdict(
        &self,
        event: ToolCallEvent,
        context: RoomContext,
    ) -> HookOutcome<ToolVerdict> {
        let result = self.react(&event, &context).await?;
        Ok(match result["action"].as_str() {
            Some("block") => {
                ToolVerdict::Block(result["reason"].as_str().unwrap_or_default().to_owned())
            }
            Some("modify") => match result["event"].get("result") {
                Some(replaced) => ToolVerdict::Replace(replaced.clone()),
                None => return Err("a modify of a tool call patches its result".into()),
            },
            Some(_) | None => ToolVerdict::Keep,
        })
    }
}

/// A tool hook's payload as a scenario reads it (README, "Expectations").
fn tool_call_record(event: &ToolCallEvent) -> Value {
    json!({
        "name": event.call.name,
        "tool_call_id": event.call.id,
        "arguments": event.call.arguments,
        "result": event.result,
        "is_error": event.is_error(),
        "refused": event.refused(),
        "cancelled": event.cancelled(),
        "error_detail": event.error_detail,
    })
}
