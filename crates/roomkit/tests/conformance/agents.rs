//! The scripted agent of a scenario (roomkit-specs `conformance/README.md`,
//! "Scripted agents"): roomkit-ai's own AI channel over a scripted model
//! and scripted tools, and the hooks on the tool triggers, which register
//! through the kit's typed registrations.

use std::future::{Future, pending};
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll, ready};

use futures_core::Stream;

use roomkit::model::hook::AllowOptions;
use roomkit::model::{ChannelId, HookAction, HookResult, Metadata, RoomContext, ToolCall};
use roomkit::{HookError, ToolCallEvent};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiProvider, AiResponse, AiRole, AiTool, ProviderError,
    ProviderStream, StreamEvent, ToolContext, ToolError, ToolPolicy, ToolResult, Tools,
};
use serde_json::{Value, json};

use crate::run::{Run, Scope};
use crate::scripted::{Reactions, perform};

/// A scenario's scripted model: `ScriptedProvider`'s script, the model
/// waiting between two of a round's deltas, so that a reader is handed
/// each apart (deltas yielded without waiting may cross as one, RFC
/// §10.6.4; `conformance/README.md`, "Scripted agents").
pub(crate) struct Paced(ScriptedProvider);

impl AiProvider for Paced {
    fn model_name(&self) -> &str {
        self.0.model_name()
    }

    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        self.0.generate(context).await
    }

    fn generate_structured_stream<'a>(&'a self, context: &'a AiContext) -> ProviderStream<'a> {
        Box::pin(PacedStream {
            inner: self.0.generate_structured_stream(context),
            held: None,
            after_text: false,
        })
    }
}

/// A scripted generation's events, each text delta that follows another
/// held for one poll.
struct PacedStream<'a> {
    inner: ProviderStream<'a>,
    held: Option<Result<StreamEvent, ProviderError>>,
    after_text: bool,
}

impl Stream for PacedStream<'_> {
    type Item = Result<StreamEvent, ProviderError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(held) = this.held.take() {
            return Poll::Ready(Some(held));
        }
        let next = ready!(this.inner.as_mut().poll_next(cx));
        let text = matches!(next, Some(Ok(StreamEvent::TextDelta(_))));
        if text && this.after_text {
            // The model waits before this delta: it goes on the next poll.
            this.held = next;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        this.after_text = text;
        Poll::Ready(next)
    }
}

/// The scripted agent `spec` describes, and its model.
pub(crate) fn scripted_agent(spec: &Value) -> Result<(AiChannel<Paced>, ScriptedProvider), String> {
    let id = ChannelId::new(spec["id"].as_str().unwrap_or_default()).map_err(|e| e.to_string())?;
    let rounds = spec["model"]["rounds"]
        .as_array()
        .into_iter()
        .flatten()
        .map(round)
        .collect::<Result<Vec<_>, _>>()?;
    let model = ScriptedProvider::new(rounds);
    let mut options = AiChannelOptions {
        tools: tools(spec).into(),
        ..AiChannelOptions::default()
    };
    if let Some(policy) = spec.get("tool_policy") {
        options.tool_policy = Some(tool_policy(policy));
    }
    if let Some(rounds) = spec.get("max_tool_rounds") {
        let rounds = rounds.as_u64().ok_or("max_tool_rounds: not a count")?;
        options.max_tool_rounds = u32::try_from(rounds).map_err(|e| e.to_string())?;
    }
    let agent = AiChannel::new(id, Paced(model.clone()), options).map_err(|e| e.to_string())?;
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
            let definition = AiTool {
                parameters: tool
                    .get("parameters")
                    .cloned()
                    .unwrap_or_else(|| json!({ "type": "object" })),
                ..AiTool::new(
                    tool["name"].as_str().unwrap_or_default(),
                    tool["description"].as_str().unwrap_or_default(),
                )
                .unwrap()
            };
            let behaviour = tool.clone();
            tools.tool(definition, move |_call: ToolCall, _context: ToolContext| {
                let behaviour = behaviour.clone();
                async move { answer(&behaviour).await }
            })
        })
}

/// What a scripted tool answers.
async fn answer(tool: &Value) -> Result<ToolResult, ToolError> {
    let words = |key: &str| tool[key].as_str().map(str::to_owned);
    if let Some(text) = words("returns") {
        return Ok(ToolResult::Text(text));
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
    if tool["declines"].as_bool() == Some(true) {
        return Err(ToolError::unserved("the call is not this tool's"));
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
    ToolPolicy {
        allow: patterns("allow"),
        deny: patterns("deny"),
    }
}

/// Each request `model` received, as the scenario reads it: the tool
/// results it carries (README, `models`).
pub(crate) fn requests(model: &ScriptedProvider) -> Vec<Value> {
    model.requests().iter().map(request).collect()
}

fn request(request: &AiContext) -> Value {
    let results: Vec<Value> = request
        .messages
        .iter()
        .filter(|message| message.role == AiRole::Tool)
        .map(|message| {
            let call = message
                .tool_result()
                .map(|result| result.tool_call_id.clone());
            json!({ "tool_call_id": call, "content": message.text() })
        })
        .collect();
    json!({ "tool_results": results })
}

/// The handler of the scripted hook `spec` on a tool trigger: it records
/// its call, then reacts.
pub(crate) fn tool_hook_handler(
    spec: &Value,
    run: &Arc<Run>,
) -> impl Fn(ToolCallEvent, RoomContext) -> Pin<Box<dyn Future<Output = ToolHookReturn> + Send>>
+ Send
+ Sync
+ 'static {
    let scripted = ToolHook::new(spec, run);
    move |event: ToolCallEvent, context: RoomContext| {
        let scripted = scripted.clone();
        Box::pin(async move { scripted.react(event, context).await })
    }
}

type ToolHookReturn = Result<HookResult<ToolCallEvent>, HookError>;

/// A scripted hook on a tool trigger: it records its call, then reacts.
#[derive(Clone)]
struct ToolHook {
    name: String,
    trigger: String,
    reactions: Arc<Reactions>,
    run: Weak<Run>,
}

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
    /// `result` as a hook's result.
    async fn react(&self, event: ToolCallEvent, context: RoomContext) -> ToolHookReturn {
        let allow = || HookResult::allow(AllowOptions::default());
        let Some(run) = self.run.upgrade() else {
            return Ok(allow());
        };
        let room_id = context.room.id.to_string();
        run.record_tool_hook(
            &self.name,
            &self.trigger,
            &room_id,
            &tool_call_record(&event),
        );
        let Some(reaction) = self.reactions.pick(&json!({ "room_id": room_id })) else {
            return Ok(allow());
        };
        perform(&run, &reaction, &Scope::in_room(&room_id)).await?;
        match reaction.get("result") {
            Some(result) => Ok(tool_hook_result(result, event)?),
            None => Ok(allow()),
        }
    }
}

/// A scenario's hook result on a tool call, a modify's `event` read as a
/// patch on the call, as the Python harness reads it.
fn tool_hook_result(
    spec: &Value,
    event: ToolCallEvent,
) -> Result<HookResult<ToolCallEvent>, String> {
    let action: HookAction = serde_json::from_value(spec["action"].clone())
        .map_err(|error| format!("action: {error}"))?;
    let mut result = HookResult::new(action);
    result.reason = spec["reason"].as_str().map(str::to_owned);
    if let Some(metadata) = spec["metadata"].as_object() {
        result.metadata.clone_from(metadata);
    }
    if let Some(patch) = spec.get("event") {
        result.event = Some(patched_tool_call(event, patch)?);
    }
    Ok(result)
}

/// `event` with the fields `patch` names replaced.
fn patched_tool_call(mut event: ToolCallEvent, patch: &Value) -> Result<ToolCallEvent, String> {
    for (field, value) in patch.as_object().into_iter().flatten() {
        match field.as_str() {
            "arguments" => {
                event.arguments = value
                    .as_object()
                    .cloned()
                    .ok_or("a tool call's arguments are an object")?;
            }
            "result" => event.result = (!value.is_null()).then(|| value.clone()),
            "structured_content" => {
                event.structured_content = (!value.is_null()).then(|| value.clone());
            }
            other => return Err(format!("{other}: no tool call field the harness patches")),
        }
    }
    Ok(event)
}

/// A tool hook's payload as a scenario reads it (README, "Expectations").
fn tool_call_record(event: &ToolCallEvent) -> Value {
    json!({
        "name": event.name,
        "tool_call_id": event.tool_call_id,
        "arguments": event.arguments,
        "result": event.result,
        "is_error": event.is_error,
        "refused": event.refused,
        "cancelled": event.cancelled,
        "error_detail": event.error_detail,
    })
}
