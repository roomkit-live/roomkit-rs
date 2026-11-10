//! The scripted agent of a scenario (roomkit-specs `conformance/README.md`,
//! "Scripted agents"): roomkit-ai's own AI channel over a scripted model
//! and scripted tools, and the hooks on the tool triggers, which register
//! through the kit's typed registrations.

use std::collections::HashMap;
use std::future::{Future, pending};
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures_core::Stream;

use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelBinding, ChannelId, HookAction, HookResult, Metadata, RetryPolicy, RoomContext, ToolCall,
};
use roomkit::{ChannelError, HookError, ToolCallEvent};
use roomkit_ai::tasks::{
    CancelTaskTool, CancelTaskToolOptions, TaskStatusTool, TaskStatusToolOptions,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    Agent, AgentOptions, AiChannel, AiChannelOptions, AiChannelTurnConfig, AiContentPart,
    AiContext, AiImagePart, AiMessage, AiMessageContent, AiProvider, AiResponse, AiRole,
    AiTextPart, AiThinkingPart, AiTool, AiToolCall, HumanInputToolHandler,
    HumanInputToolHandlerOptions, ModelInfo, ModelPricing, ProviderError, ProviderStream,
    StreamDone, StreamEvent, StreamTextDelta, StreamThinkingDelta, StreamToolCallDelta, Tool,
    ToolContext, ToolEntry, ToolError, ToolHandler, ToolPolicy, ToolResult, ToolResultPart, Tools,
    check_schema_answer, checked_stream, schema_for_generate,
};
use serde_json::{Value, json};

use crate::memory::scripted_memory;
use crate::run::{Run, Scope};
use crate::scripted::{Reactions, perform};

/// A scenario's scripted model: `ScriptedProvider`'s script, the model
/// waiting between two of a round's deltas, so that a reader is handed
/// each apart (deltas yielded without waiting may cross as one, RFC
/// §10.6.4; `conformance/README.md`, "Scripted agents"), and holding a
/// round with a `gate` there, before it writes anything.
pub(crate) struct Paced {
    whole: WholeModel,
    streaming: bool,
    /// Whether the model sees images (README: `model.vision`).
    vision: bool,
    deferred_tools: bool,
    /// The model's catalogue price (README: `model.pricing`).
    pricing: Option<ModelPricing>,
    /// The model's context window (README: `model.context_window`).
    context_window: Option<u64>,
}

impl AiProvider for Paced {
    fn name(&self) -> &str {
        &self.whole.provider_name
    }

    fn model_name(&self) -> &str {
        &self.whole.model_name
    }

    fn supports_deferred_tools(&self) -> bool {
        self.deferred_tools
    }

    fn supports_vision(&self) -> bool {
        self.vision
    }

    fn supports_response_schema(&self) -> bool {
        self.whole.schema.0
    }

    fn supports_response_schema_with_tools(&self) -> bool {
        self.whole.schema.1
    }

    /// The model's catalogue entry, with the price and the context window
    /// the scenario gives it; none for a model the scenario gives neither.
    fn catalog_entry(&self) -> Option<ModelInfo> {
        if self.pricing.is_none() && self.context_window.is_none() {
            return None;
        }
        Some(ModelInfo {
            pricing: self.pricing.clone(),
            context_window: self.context_window,
            ..ModelInfo::new(self.model_name())
        })
    }

    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        self.whole.generate(context).await
    }

    fn supports_structured_streaming(&self) -> bool {
        self.streaming
    }

    /// The round's events, under a response schema refused before the
    /// request is kept when the model takes none, and checked before its
    /// done event (README, `model.response_schema`).
    fn generate_structured_stream<'a>(&'a self, context: &'a AiContext) -> ProviderStream<'a> {
        if !self.streaming {
            // Use AiProvider's real default adapter over WholeModel::generate.
            return self.whole.generate_structured_stream(context);
        }
        let (supported, with_tools) = self.whole.schema;
        if let Err(error) =
            schema_for_generate(context, supported, with_tools, &self.whole.provider_name)
        {
            return Box::pin(Refused(Some(error)));
        }
        let events = self.scripted_stream(context);
        checked_stream(events, context, &self.whole.provider_name, refused)
    }
}

impl Paced {
    /// The script's events for `context`, held at its round's gate first.
    fn scripted_stream<'a>(&'a self, context: &'a AiContext) -> ProviderStream<'a> {
        let (inner, gate) = {
            let mut methods = crate::run::lock(&self.whole.methods);
            let position = self.whole.script.requests().len();
            let gate = self.whole.gates.get(position).cloned().flatten();
            let inner = self.whole.script.generate_structured_stream(context);
            methods.push("generate_structured_stream");
            (inner, gate)
        };
        let run = Weak::clone(&self.whole.run);
        let holding: Option<Pin<Box<dyn Future<Output = ()> + Send>>> = gate.map(|gate| {
            let held: Pin<Box<dyn Future<Output = ()> + Send>> = Box::pin(async move {
                if let Some(run) = run.upgrade() {
                    run.gates.hold(&gate).await;
                }
            });
            held
        });
        Box::pin(PacedStream {
            inner,
            holding,
            held: None,
            after_text: false,
            run: Weak::clone(&self.whole.run),
        })
    }
}

/// A whole-response provider. Its inherited structured stream is the real
/// AiProvider adapter, so the non-streaming fixture cannot bypass generate().
struct WholeModel {
    script: ScriptedProvider,
    gates: Vec<Option<String>>,
    run: Weak<Run>,
    model_name: String,
    provider_name: String,
    schema: (bool, bool),
    methods: GenerationMethods,
}

pub(crate) type GenerationMethods = Arc<Mutex<Vec<&'static str>>>;

impl AiProvider for WholeModel {
    fn model_name(&self) -> &str {
        &self.model_name
    }

    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        let (supported, with_tools) = self.schema;
        schema_for_generate(context, supported, with_tools, &self.provider_name)?;
        let (response, gate) = {
            // The script reserves synchronously. Keep that reservation and its
            // gate together, even when several rooms use the same provider.
            let mut methods = crate::run::lock(&self.methods);
            let position = self.script.requests().len();
            let gate = self.gates.get(position).cloned().flatten();
            let response = self.script.generate(context);
            methods.push("generate");
            (response, gate)
        };
        if let Some(gate) = gate
            && let Some(run) = self.run.upgrade()
        {
            run.gates.hold(&gate).await;
        }
        self.checked_response(context, response.await?)
    }
}

impl WholeModel {
    fn checked_response(
        &self,
        context: &AiContext,
        mut response: AiResponse,
    ) -> Result<AiResponse, ProviderError> {
        for call in &mut response.tool_calls {
            labelled(&self.run, &mut call.arguments);
        }
        let finish = response.finish_reason.as_deref();
        if let Some(schema) = &context.response_schema
            && response.tool_calls.is_empty()
            && !finish.is_some_and(malformed_call)
        {
            let refusal = (finish == Some("refusal")).then_some("refusal");
            check_schema_answer(
                &response.content,
                schema,
                &self.provider_name,
                refusal,
                finish,
            )?;
        }
        Ok(response)
    }
}

/// Whether a round ended on a call its provider would not hand over.
fn malformed_call(finish_reason: &str) -> bool {
    ["malformed_function_call", "unexpected_tool_call"]
        .iter()
        .any(|malformed| finish_reason.eq_ignore_ascii_case(malformed))
}

/// Why a scripted round declined to answer: its `refusal` finish reason.
fn refused(done: &StreamDone) -> Option<String> {
    (done.finish_reason.as_deref() == Some("refusal")).then(|| "refusal".to_owned())
}

/// A stream failing once, with the error a schema's check gave before the
/// request was made.
struct Refused(Option<ProviderError>);

impl Stream for Refused {
    type Item = Result<StreamEvent, ProviderError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.get_mut().0.take().map(Err))
    }
}

/// A call's `arguments` as the model writes it, each `$<label>` read as
/// what the run's call of that label made (README, "Scripted agents").
fn labelled(run: &Weak<Run>, arguments: &mut Metadata) {
    let Some(run) = run.upgrade() else {
        return;
    };
    if let Value::Object(read) = run.labelled(Value::Object(std::mem::take(arguments))) {
        *arguments = read;
    }
}

/// A scripted generation's events, held at its round's gate first, each
/// text delta that follows another held for one poll, each call's
/// arguments read as the model writes them.
struct PacedStream<'a> {
    inner: ProviderStream<'a>,
    holding: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    held: Option<Result<StreamEvent, ProviderError>>,
    after_text: bool,
    run: Weak<Run>,
}

impl Stream for PacedStream<'_> {
    type Item = Result<StreamEvent, ProviderError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(holding) = this.holding.as_mut() {
            ready!(holding.as_mut().poll(cx));
            this.holding = None;
        }
        if let Some(held) = this.held.take() {
            return Poll::Ready(Some(held));
        }
        let mut next = ready!(this.inner.as_mut().poll_next(cx));
        if let Some(Ok(StreamEvent::ToolCall(call))) = next.as_mut() {
            labelled(&this.run, &mut call.arguments);
        }
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

/// A scripted agent's models by the name `expect.models` gives them: its
/// own under its id, its memory's summarizer under `<id>/summarizer`, its
/// fallback under `<id>/fallback`.
pub(crate) type Models = Vec<(String, ScriptedProvider)>;

/// The scripted agent `spec` describes, and its models; its gates are
/// `run`'s.
pub(crate) fn scripted_agent(spec: &Value, run: &Arc<Run>) -> Result<(Scripted, Models), String> {
    scripted_agent_of(spec, run, false)
}

/// [`scripted_agent`], for another process of the setup when `process`: it
/// answers from the models the run's own agent of its id has (README,
/// `processes`).
pub(crate) fn scripted_agent_of(
    spec: &Value,
    run: &Arc<Run>,
    process: bool,
) -> Result<(Scripted, Models), String> {
    let id = ChannelId::new(spec["id"].as_str().unwrap_or_default()).map_err(|e| e.to_string())?;
    let shared = |name: &str| {
        process
            .then(|| crate::run::lock(&run.models).get(name).cloned())
            .flatten()
    };
    let (model, paced) = scripted_model(&spec["model"], run, id.as_str(), shared(id.as_str()))?;
    let mut options = AiChannelOptions {
        tools: tools(spec, run).into(),
        serialize_room_turns: spec["serialize_room_turns"] == true,
        steer_room_turns: spec["steer_room_turns"] == true,
        ..AiChannelOptions::default()
    };
    if let Some(policy) = spec.get("tool_policy") {
        options.tool_policy = Some(tool_policy(policy));
    }
    bounds(spec, &mut options)?;
    agent_settings(spec, &mut options)?;
    tool_search_settings(spec, &mut options)?;
    if let Some(by_room) = spec.get("config_provider") {
        options.config_provider = Some(config_provider(by_room, run)?);
    }
    if let Some(skills) = spec.get("skills") {
        options.skills = Some(run.skills.registry(skills)?);
    }
    if let Some(listed) = spec.get("skills_in_prompt") {
        options.skills_in_prompt = listed.as_bool().ok_or("skills_in_prompt: not a flag")?;
    }
    if let Some(bound) = spec.get("max_author_images") {
        options.max_author_images = if bound.is_null() {
            None
        } else {
            let bound = bound.as_u64().ok_or("max_author_images: not a count")?;
            Some(usize::try_from(bound).map_err(|e| e.to_string())?)
        };
    }
    if let Some(threshold) = spec.get("evict_threshold_tokens") {
        options.evict_threshold_tokens = threshold
            .as_u64()
            .ok_or("evict_threshold_tokens: not a count")?;
    }
    if let Some(describer) = spec.get("describe_empty_event") {
        let says = describer["says"].as_str().map(str::to_owned);
        options.describe_empty_event = Some(Arc::new(move |_| says.clone()));
    }
    for (name, bound) in spec["tool_timeouts_ms"].as_object().into_iter().flatten() {
        let bound = milliseconds(bound, "tool_timeouts_ms")?;
        options.tool_timeouts.insert(name.clone(), bound);
    }
    if let Some(policy) = spec.get("retry_policy") {
        options.retry_policy = Some(retry_policy(policy)?);
    }
    let mut models = vec![(id.to_string(), model)];
    keep_chat_dialect(&spec["model"], id.to_string(), run);
    if let Some(fallback) = spec.get("fallback_model") {
        keep_chat_dialect(fallback, format!("{id}/fallback"), run);
        let (fallback, paced) = scripted_model(
            fallback,
            run,
            &format!("{id}/fallback"),
            shared(&format!("{id}/fallback")),
        )?;
        models.push((format!("{id}/fallback"), fallback));
        options.fallback_provider = Some(Arc::new(paced));
    }
    if let Some(memory) = spec.get("memory") {
        let memory = agent_memory(memory, (&id, run), options.max_context_events, &mut models)?;
        options.memory = Some(memory);
    }
    options.tool_handler = undeclared_tools(spec, run);
    crate::audits::audit_tools(spec, run, &mut options);
    if let Some(human) = spec.get("human_input") {
        options.human_input_handler = Some(human_input(human)?);
    }
    options.tools.extend(kit_tools(spec, run)?);
    // An identity makes the scripted agent an Agent (README, "Scripted agents").
    let Some(identity) = spec.get("agent") else {
        let agent = AiChannel::new(id, paced, options).map_err(|e| e.to_string())?;
        return Ok((Scripted::Channel(Box::new(agent)), models));
    };
    let text = |key: &str| identity[key].as_str().map(str::to_owned);
    let agent_options = AgentOptions {
        name: text("name"),
        role: text("role"),
        description: text("description"),
        purpose: text("purpose"),
        scope: text("scope"),
        language: text("language"),
        identity_in_prompt: identity["identity_in_prompt"] != false,
        greeting: text("greeting"),
        auto_greet: identity["auto_greet"] != false,
        ai: options,
        ..AgentOptions::default()
    };
    let agent = Agent::new(id, paced, agent_options).map_err(|e| e.to_string())?;
    Ok((Scripted::Agent(Box::new(agent)), models))
}

/// The scripted memory `memory` of the agent `id`, recorded on `run`, its
/// summarizer among `models`.
fn agent_memory(
    memory: &Value,
    (id, run): (&ChannelId, &Arc<Run>),
    max_context_events: usize,
    models: &mut Models,
) -> Result<Arc<dyn roomkit_ai::memory::MemoryProvider>, String> {
    let summarizer = match memory.get("summarizer") {
        Some(summarizer) => {
            let rounds = summarizer["rounds"].as_array().into_iter().flatten();
            let summarizer =
                ScriptedProvider::new(rounds.map(round).collect::<Result<Vec<_>, _>>()?);
            models.push((format!("{id}/summarizer"), summarizer.clone()));
            Some(summarizer)
        }
        None => None,
    };
    scripted_memory(memory, max_context_events, summarizer, (id.clone(), run))
}

/// A scripted agent's bounds and budgets (README, "Scripted agents").
fn bounds(spec: &Value, options: &mut AiChannelOptions) -> Result<(), String> {
    if let Some(rounds) = spec.get("max_tool_rounds") {
        let rounds = rounds.as_u64().ok_or("max_tool_rounds: not a count")?;
        options.max_tool_rounds = u32::try_from(rounds).map_err(|e| e.to_string())?;
    }
    if let Some(tries) = spec.get("max_empty_retries") {
        let tries = tries.as_u64().ok_or("max_empty_retries: not a count")?;
        options.max_empty_retries = u32::try_from(tries).map_err(|e| e.to_string())?;
    }
    if let Some(window) = spec.get("max_context_events") {
        let window = window.as_u64().ok_or("max_context_events: not a count")?;
        options.max_context_events = usize::try_from(window).map_err(|e| e.to_string())?;
    }
    if let Some(bound) = spec.get("tool_timeout_ms") {
        options.tool_timeout = milliseconds(bound, "tool_timeout_ms")?;
    }
    if let Some(budget) = spec.get("turn_budget_tokens") {
        let budget = budget.as_u64().ok_or("turn_budget_tokens: not a count")?;
        options.turn_budget_tokens = Some(budget);
    }
    if let Some(budget) = spec.get("turn_budget_usd") {
        let budget = budget.as_f64().ok_or("turn_budget_usd: not a number")?;
        options.turn_budget_usd = Some(budget);
    }
    Ok(())
}

/// The Chat Completions rendering the model `spec` names, if any, under the
/// name `expect.models` gives the model (README, `model.chat_dialect`).
fn keep_chat_dialect(spec: &Value, model: String, run: &Arc<Run>) {
    if let Some(dialect) = spec["chat_dialect"].as_str() {
        crate::run::lock(&run.chat_dialects).insert(model, dialect.to_owned());
    }
}

/// The scripted model `spec` describes, and the provider reading it, its
/// gates `run`'s.
fn scripted_model(
    spec: &Value,
    run: &Arc<Run>,
    name: &str,
    shared: Option<ScriptedProvider>,
) -> Result<(ScriptedProvider, Paced), String> {
    let scripted = spec["rounds"].as_array().into_iter().flatten();
    let rounds = scripted.clone().map(round).collect::<Result<Vec<_>, _>>()?;
    let gates = scripted
        .map(|round| round["gate"].as_str().map(str::to_owned))
        .collect();
    let pricing = spec
        .get("pricing")
        .map(|pricing| serde_json::from_value(pricing.clone()))
        .transpose()
        .map_err(|e| format!("model.pricing: {e}"))?;
    let methods = generation_methods(run, name, shared.is_some());
    let model = shared.unwrap_or_else(|| ScriptedProvider::new(rounds));
    let paced = Paced {
        whole: WholeModel {
            script: model.clone(),
            gates,
            run: Arc::downgrade(run),
            model_name: spec["name"].as_str().unwrap_or("scripted").to_owned(),
            provider_name: spec["provider"].as_str().unwrap_or("scripted").to_owned(),
            schema: (
                spec["response_schema"] == true,
                spec["response_schema_with_tools"] == true,
            ),
            methods,
        },
        streaming: spec["streaming"] != false,
        vision: spec["vision"] == true,
        deferred_tools: spec["deferred_tools"] == true,
        pricing,
        context_window: spec["context_window"].as_u64(),
    };
    Ok((model, paced))
}

/// A replacement starts a new log; another process shares the original's.
fn generation_methods(run: &Arc<Run>, name: &str, shared: bool) -> GenerationMethods {
    let mut models = crate::run::lock(&run.model_methods);
    if shared {
        return models.entry(name.to_owned()).or_default().clone();
    }
    let methods = GenerationMethods::default();
    models.insert(name.to_owned(), methods.clone());
    methods
}

/// A scripted agent's retry policy, its delays given in milliseconds
/// (README, "Scripted agents").
fn retry_policy(spec: &Value) -> Result<RetryPolicy, String> {
    let defaults = RetryPolicy::default();
    let seconds = |key: &str, default: f64| match spec.get(key) {
        Some(value) => value
            .as_f64()
            .map(|milliseconds| milliseconds / 1000.0)
            .ok_or(format!("retry_policy.{key}: not a number")),
        None => Ok(default),
    };
    let max_retries = match spec.get("max_retries") {
        Some(count) => {
            let count = count
                .as_u64()
                .ok_or("retry_policy.max_retries: not a count")?;
            u32::try_from(count).map_err(|e| e.to_string())?
        }
        None => defaults.max_retries,
    };
    Ok(RetryPolicy {
        max_retries,
        base_delay_seconds: seconds("base_delay_ms", defaults.base_delay_seconds)?,
        max_delay_seconds: seconds("max_delay_ms", defaults.max_delay_seconds)?,
        exponential_base: match spec.get("exponential_base") {
            Some(base) => base
                .as_f64()
                .ok_or("retry_policy.exponential_base: not a number")?,
            None => defaults.exponential_base,
        },
        ..defaults
    })
}

/// A scripted agent: an AI channel, or an Agent when it has an identity.
pub(crate) enum Scripted {
    Channel(Box<AiChannel<Paced>>),
    Agent(Box<Agent<Paced>>),
}

/// The tools the kit makes that a scripted agent is given as its own
/// (README, "Scripted agents").
fn kit_tools(spec: &Value, run: &Arc<Run>) -> Result<Vec<ToolEntry>, String> {
    spec["kit_tools"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|name| {
            let tool: Arc<dyn Tool> = match name.as_str() {
                Some("task_status") => Arc::new(TaskStatusTool::new(
                    run.kit(),
                    TaskStatusToolOptions::default(),
                )),
                Some("cancel_task") => Arc::new(CancelTaskTool::new(
                    run.kit(),
                    CancelTaskToolOptions::default(),
                )),
                _ => return Err(format!("kit_tools: no tool {name}")),
            };
            Ok(ToolEntry::Tool(tool))
        })
        .collect()
}

/// A scripted agent's human-input tools (README, "Scripted agents").
fn human_input(spec: &Value) -> Result<HumanInputToolHandler, String> {
    let names = spec["tool_names"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str);
    let timeout = spec["timeout_ms"].as_f64().unwrap_or(300_000.0);
    let tool_definitions = spec["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|tool| {
            Ok(AiTool {
                parameters: tool
                    .get("parameters")
                    .cloned()
                    .unwrap_or_else(|| json!({ "type": "object" })),
                ..AiTool::new(
                    tool["name"].as_str().unwrap_or_default(),
                    tool["description"].as_str().unwrap_or_default(),
                )
                .map_err(|e| e.to_string())?
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let options = HumanInputToolHandlerOptions {
        timeout: Duration::from_secs_f64(timeout / 1000.0),
        tool_definitions,
        ..HumanInputToolHandlerOptions::default()
    };
    HumanInputToolHandler::new(names, options).map_err(|e| e.to_string())
}

/// A round's `stream`, in its order, then its end (README: `stream`).
fn streaming_round(spec: &Value, events: &[Value]) -> Result<ScriptedRound, String> {
    let mut called = false;
    let mut read = Vec::with_capacity(events.len() + 1);
    for event in events {
        read.push(if let Some(call) = event.get("call") {
            called = true;
            StreamEvent::ToolCall(tool_call(call)?.into())
        } else if let Some(text) = event["text"].as_str() {
            StreamEvent::TextDelta(StreamTextDelta::new(text))
        } else if let Some(delta) = event.get("tool_call_delta") {
            StreamEvent::ToolCallDelta(StreamToolCallDelta {
                id: delta["id"].as_str().ok_or("tool_call_delta: no id")?.into(),
                name: delta["name"]
                    .as_str()
                    .ok_or("tool_call_delta: no name")?
                    .into(),
                index: delta["index"]
                    .as_u64()
                    .and_then(|n| u32::try_from(n).ok())
                    .unwrap_or(0),
                arguments_delta: delta["arguments_delta"].as_str().unwrap_or_default().into(),
            })
        } else {
            let text = |key: &str| event[key].as_str().map(str::to_owned);
            StreamEvent::ThinkingDelta(StreamThinkingDelta {
                thinking: text("thinking").ok_or("stream: no known event")?,
                signature: text("signature"),
                redacted: text("redacted"),
                block: event["block"].as_u64().and_then(|n| u32::try_from(n).ok()),
            })
        });
    }
    read.push(StreamEvent::Done(StreamDone {
        finish_reason: Some(finish_reason(spec, called)),
        usage: usage(spec)?,
        ..StreamDone::default()
    }));
    with_failure(ScriptedRound::streaming(read), spec)
}

/// A round read whole, its reasoning as `whole` reads (README:
/// `whole_response`).
fn whole_round(spec: &Value, whole: &Value) -> Result<ScriptedRound, String> {
    let calls = spec["calls"]
        .as_array()
        .into_iter()
        .flatten()
        .map(tool_call)
        .collect::<Result<Vec<_>, _>>()?;
    let text = |value: &Value, key: &str| value[key].as_str().map(str::to_owned);
    let parts = whole["thinking_parts"].as_array().map(|parts| {
        parts
            .iter()
            .map(|part| AiThinkingPart {
                signature: text(part, "signature"),
                redacted: text(part, "redacted"),
                ..AiThinkingPart::new(part["thinking"].as_str().unwrap_or_default())
            })
            .collect()
    });
    Ok(ScriptedRound::whole_response(AiResponse {
        finish_reason: Some(finish_reason(spec, !calls.is_empty())),
        usage: usage(spec)?,
        tool_calls: calls,
        thinking: text(whole, "thinking"),
        thinking_signature: text(whole, "thinking_signature"),
        thinking_parts: parts,
        ..AiResponse::new(spec["text"].as_str().unwrap_or_default())
    }))
}

/// A scripted call as a provider hands it.
fn tool_call(call: &Value) -> Result<AiToolCall, String> {
    let text = |key: &str| call[key].as_str().unwrap_or_default().to_owned();
    let read = |key: &str| -> Result<Metadata, String> {
        call.get(key).map_or(Ok(Metadata::new()), |value| {
            serde_json::from_value(value.clone()).map_err(|e| e.to_string())
        })
    };
    Ok(AiToolCall {
        arguments: read("arguments")?,
        metadata: read("metadata")?,
        partial: call["cut"] == true,
        ..AiToolCall::new(text("id"), text("name"))
    })
}

/// How a round ends, `tool_calls` by default when it calls, else `stop`.
fn finish_reason(spec: &Value, called: bool) -> String {
    let default = if called { "tool_calls" } else { "stop" };
    spec["finish_reason"].as_str().unwrap_or(default).to_owned()
}

/// What a round used.
fn usage(spec: &Value) -> Result<Metadata, String> {
    match spec.get("usage") {
        Some(usage) => serde_json::from_value(usage.clone()).map_err(|e| e.to_string()),
        None => Ok(Metadata::new()),
    }
}

/// One round of the model's script.
pub(crate) fn round(spec: &Value) -> Result<ScriptedRound, String> {
    if let Some(events) = spec.get("stream") {
        return streaming_round(spec, events.as_array().ok_or("stream: not a list")?);
    }
    if let Some(whole) = spec.get("whole_response") {
        return whole_round(spec, whole);
    }
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
    let mut round = ScriptedRound::calling(spec["text"].as_str().unwrap_or_default(), made)
        .with_finish_reason(finish)
        .with_usage(usage(spec)?);
    if let Some(deltas) = spec.get("deltas") {
        let deltas = deltas.as_array().ok_or("deltas: not a list")?;
        round = round.in_pieces(deltas.iter().filter_map(Value::as_str));
    }
    if let Some(thinking) = spec.get("thinking") {
        let thinking = thinking.as_array().ok_or("thinking: not a list")?;
        round = round.reasoning(thinking.iter().filter_map(Value::as_str));
    }
    round = with_failure(round, spec)?;
    for call in spec["calls"].as_array().into_iter().flatten() {
        if let (Some(id), Some(metadata)) = (call["id"].as_str(), call.get("metadata")) {
            let metadata = serde_json::from_value(metadata.clone()).map_err(|e| e.to_string())?;
            round = round.with_call_metadata(id, metadata);
        }
    }
    Ok(round)
}

/// The scripted tools of `spec` the agent does not declare, served by its
/// tool handler all the same (README: a tool's `declared`); `None` when
/// every tool is declared.
fn undeclared_tools(spec: &Value, run: &Arc<Run>) -> Option<ToolHandler> {
    let undeclared: Vec<Value> = spec["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|tool| tool["declared"] == false)
        .cloned()
        .collect();
    if undeclared.is_empty() {
        return None;
    }
    Some(scripted_handler(undeclared, run))
}

/// A handler serving `tools`, scripted tools, each answering every call
/// one way, held at its `gate` first, `run`'s; a name it does not hold is
/// unserved.
fn scripted_handler(tools: Vec<Value>, run: &Arc<Run>) -> ToolHandler {
    let run = Arc::downgrade(run);
    ToolHandler::new(move |name, _arguments| {
        let behaviour = tools.iter().find(|tool| tool["name"] == name).cloned();
        let run = Weak::clone(&run);
        async move {
            let Some(behaviour) = behaviour else {
                return Err(ToolError::unserved(format!("{name} is not this handler's")));
            };
            write_record(&behaviour);
            if let (Some(gate), Some(run)) = (behaviour["gate"].as_str(), run.upgrade()) {
                run.gates.hold(gate).await;
            }
            answer(&behaviour).await
        }
    })
}

/// A scripted agent's Tool Search (README, "Scripted agents").
fn tool_search_settings(spec: &Value, options: &mut AiChannelOptions) -> Result<(), String> {
    options.tool_search = spec["tool_search"].as_bool();
    options.tool_search_pinned = spec["tool_search_pinned"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect();
    if let Some(threshold) = spec["tool_search_threshold"].as_u64() {
        options.tool_search_threshold = usize::try_from(threshold).map_err(|e| e.to_string())?;
    }
    if let Some(share) = spec["tool_search_threshold_pct"].as_f64() {
        options.tool_search_threshold_pct = share;
    }
    options.tool_search_miss_hint = spec["tool_search_miss_hint"].as_str().map(str::to_owned);
    if let Some(cap) = spec.get("tool_search_threshold_tokens") {
        options.tool_search_threshold_tokens = cap.as_u64();
    }
    Ok(())
}

/// A scripted agent's own `system_prompt`, `temperature` and `max_tokens`
/// (README, "Scripted agents").
fn agent_settings(spec: &Value, options: &mut AiChannelOptions) -> Result<(), String> {
    if let Some(effort) = spec
        .get("reasoning_effort")
        .filter(|effort| !effort.is_null())
    {
        let effort = effort.as_str().ok_or("reasoning_effort: not a text")?;
        options.reasoning_effort = Some(effort.to_owned());
    }
    if let Some(schema) = spec
        .get("response_schema")
        .filter(|schema| !schema.is_null())
    {
        let schema = schema.as_object().ok_or("response_schema: not an object")?;
        options.response_schema = Some(schema.clone());
    }
    if let Some(prompt) = spec.get("system_prompt") {
        let prompt = prompt.as_str().ok_or("system_prompt: not a text")?;
        options.system_prompt = Some(prompt.to_owned());
    }
    if let Some(temperature) = spec.get("temperature") {
        options.temperature = temperature.as_f64().ok_or("temperature: not a number")?;
    }
    if let Some(bound) = spec.get("max_tokens") {
        let bound = bound.as_u64().ok_or("max_tokens: not a count")?;
        options.max_tokens = Some(u32::try_from(bound).map_err(|e| e.to_string())?);
    }
    if let Some(flush) = spec.get("thinking_coalesce_ms") {
        options.thinking_coalesce_ms = flush.as_f64().ok_or("thinking_coalesce_ms: no number")?;
    }
    if let Some(flush) = spec.get("thinking_coalesce_chars") {
        let flush = flush
            .as_u64()
            .ok_or("thinking_coalesce_chars: not a count")?;
        options.thinking_coalesce_chars = usize::try_from(flush).map_err(|e| e.to_string())?;
    }
    options.publish_thinking_text = spec["publish_thinking_text"] == true;
    options.tool_call_intents = spec["tool_call_intents"] == true;
    if let Some(flush) = spec.get("text_coalesce_ms") {
        options.text_coalesce_ms = flush.as_f64().ok_or("text_coalesce_ms: no number")?;
    }
    if let Some(flush) = spec.get("text_coalesce_chars") {
        let flush = flush.as_u64().ok_or("text_coalesce_chars: not a count")?;
        options.text_coalesce_chars = usize::try_from(flush).map_err(|e| e.to_string())?;
    }
    options.publish_text_deltas = spec["publish_text_deltas"] == true;
    if let Some(budget) = spec.get("thinking_budget") {
        options.thinking_budget = Some(budget.as_i64().ok_or("thinking_budget: not a count")?);
    }
    if let Some(enabled) = spec.get("enable_thinking") {
        options.enable_thinking = Some(enabled.as_bool().ok_or("enable_thinking: not a flag")?);
    }
    Ok(())
}

/// What a scripted agent's `config_provider` answers a turn.
type TurnConfigFuture =
    Pin<Box<dyn Future<Output = Result<Option<AiChannelTurnConfig>, ChannelError>> + Send>>;

/// A scripted agent's per-turn configuration (README, "Scripted agents"):
/// the one `by_room` gives the binding's room, none for a room it does not
/// name; a list gives that room's turns, one each, its last standing for the
/// turns after. Called with a context of another room, the turn fails.
fn config_provider(
    by_room: &Value,
    run: &Arc<Run>,
) -> Result<Arc<dyn Fn(ChannelBinding, RoomContext) -> TurnConfigFuture + Send + Sync>, String> {
    let mut configs: HashMap<String, Vec<Option<AiChannelTurnConfig>>> = HashMap::new();
    for (room, spec) in by_room
        .as_object()
        .ok_or("config_provider: not an object")?
    {
        let turns = match spec.as_array() {
            Some(turns) => turns.iter().collect(),
            None => vec![spec],
        };
        let turns = turns
            .into_iter()
            .map(|turn| {
                (!turn.is_null())
                    .then(|| turn_config(turn, run))
                    .transpose()
            })
            .collect::<Result<_, _>>()?;
        configs.insert(room.clone(), turns);
    }
    let taken: Mutex<HashMap<String, usize>> = Mutex::default();
    Ok(Arc::new(
        move |binding: ChannelBinding, context: RoomContext| {
            let room = binding.room_id.as_str();
            let turn = {
                let mut taken = crate::run::lock(&taken);
                let turn = taken.entry(room.to_owned()).or_default();
                *turn += 1;
                *turn - 1
            };
            let config = configs
                .get(room)
                .and_then(|turns| turns.get(turn).or_else(|| turns.last()).cloned().flatten());
            Box::pin(async move {
                if context.room.id != binding.room_id {
                    let called = format!("called for {} in {}", binding.room_id, context.room.id);
                    return Err(called.into());
                }
                Ok(config)
            })
        },
    ))
}

/// One turn's configuration a scenario's `config_provider` gives (README,
/// "Scripted agents"), its handler's tools held at their gates, `run`'s.
fn turn_config(spec: &Value, run: &Arc<Run>) -> Result<AiChannelTurnConfig, String> {
    let text = |key: &str| spec[key].as_str().map(str::to_owned);
    let tools = spec["tools"].as_array().map(|tools| {
        tools
            .iter()
            .map(|tool| AiTool {
                name: tool["name"].as_str().unwrap_or_default().to_owned(),
                description: tool["description"].as_str().unwrap_or_default().to_owned(),
                parameters: tool.get("parameters").cloned().unwrap_or_else(|| json!({})),
                tags: Vec::new(),
                changes_state: false,
                defer_loading: false,
            })
            .collect()
    });
    let max_tokens = spec["max_tokens"].as_u64().map(u32::try_from).transpose();
    Ok(AiChannelTurnConfig {
        system_prompt: text("system_prompt"),
        tools,
        temperature: spec["temperature"].as_f64(),
        max_tokens: max_tokens.map_err(|e| e.to_string())?,
        thinking_budget: spec["thinking_budget"].as_i64(),
        enable_thinking: spec["enable_thinking"].as_bool(),
        reasoning_effort: text("reasoning_effort"),
        response_schema: spec["response_schema"].as_object().cloned(),
        turn_budget_tokens: spec["turn_budget_tokens"].as_u64(),
        turn_budget_usd: spec["turn_budget_usd"].as_f64(),
        tool_handler: spec["tool_handler"]
            .as_array()
            .map(|tools| scripted_handler(tools.clone(), run)),
        skills: spec
            .get("skills")
            .map(|skills| run.skills.registry(skills))
            .transpose()?,
    })
}

/// The scripted tools of `spec` the agent declares, each answering every
/// call one way, held at its `gate` first, `run`'s.
fn tools(spec: &Value, run: &Arc<Run>) -> Tools {
    spec["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|tool| tool["declared"] != false)
        .fold(Tools::new(), |tools, tool| {
            let definition = AiTool {
                parameters: tool
                    .get("parameters")
                    .cloned()
                    .unwrap_or_else(|| json!({ "type": "object" })),
                tags: tool["tags"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect(),
                changes_state: tool["changes_state"] == true,
                ..AiTool::new(
                    tool["name"].as_str().unwrap_or_default(),
                    tool["description"].as_str().unwrap_or_default(),
                )
                .unwrap()
            };
            let behaviour = tool.clone();
            let run = Arc::downgrade(run);
            tools.tool(definition, move |_call: ToolCall, _context: ToolContext| {
                let (behaviour, run) = (behaviour.clone(), Weak::clone(&run));
                async move {
                    write_record(&behaviour);
                    if let (Some(gate), Some(run)) = (behaviour["gate"].as_str(), run.upgrade()) {
                        run.gates.hold(gate).await;
                    }
                    answer(&behaviour).await
                }
            })
        })
}

/// Writes a tool's, a memory's or a hook's `writes_record` on the turn's
/// record, when it runs under one.
pub(crate) fn write_record(spec: &Value) {
    if let (Some(keys), Some(record)) = (
        spec.get("writes_record").and_then(Value::as_object),
        roomkit_ai::current_response_metadata(),
    ) {
        record.update(keys.clone());
    }
}

/// What a scripted tool answers.
pub(crate) async fn answer(tool: &Value) -> Result<ToolResult, ToolError> {
    let words = |key: &str| tool[key].as_str().map(str::to_owned);
    if let Some(text) = words("returns") {
        return Ok(ToolResult::Text(text));
    }
    if let Some(value) = tool.get("returns_json") {
        return Ok(ToolResult::Json(value.clone()));
    }
    if let Some(parts) = tool.get("returns_parts").and_then(Value::as_array) {
        return Ok(ToolResult::Parts(parts.iter().map(returned_part).collect()));
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
        return Err(ToolError::unserved(format!(
            "{} is not this handler's",
            tool["name"].as_str().unwrap_or_default()
        )));
    }
    pending().await
}

/// A part a tool that `returns_parts` answers: `{text}` or `{image,
/// mime_type}`.
fn returned_part(part: &Value) -> ToolResultPart {
    if let Some(text) = part["text"].as_str() {
        return ToolResultPart::Text(AiTextPart::new(text));
    }
    ToolResultPart::Image(AiImagePart {
        url: part["image"].as_str().unwrap_or_default().to_owned(),
        mime_type: part["mime_type"].as_str().map(str::to_owned),
    })
}

/// A result as the model reads it: its text, or its text and image parts
/// in order, written as a message's `parts` are (README, `models`).
pub(crate) fn result_content(result: &ToolResult) -> Value {
    match result {
        ToolResult::Text(text) => Value::String(text.clone()),
        ToolResult::Json(value) => value.clone(),
        ToolResult::Parts(parts) => parts
            .iter()
            .map(|part| match part {
                ToolResultPart::Text(text) => json!({ "text": text.text }),
                ToolResultPart::Image(image) => {
                    json!({ "image": image.url, "mime_type": image.mime_type })
                }
            })
            .collect(),
    }
}

/// A tool hook's result as a scenario reads it: parts, each a mapping
/// `{type, text}` or `{type, url, mime_type}` as the channel hands them,
/// written as a message's `parts` are; any other value as it is.
fn hook_result(result: &Value) -> Value {
    let parts = result.as_array().filter(|parts| {
        !parts.is_empty()
            && parts
                .iter()
                .all(|part| matches!(part["type"].as_str(), Some("text" | "image")))
    });
    let Some(parts) = parts else {
        return result.clone();
    };
    parts
        .iter()
        .map(|part| match part["type"].as_str() {
            Some("text") => json!({ "text": part["text"] }),
            _ => json!({ "image": part["url"], "mime_type": part["mime_type"] }),
        })
        .collect()
}

/// A bound in milliseconds, `null` bounding nothing (README, "Scripted
/// agents").
fn milliseconds(bound: &Value, member: &str) -> Result<Option<Duration>, String> {
    if bound.is_null() {
        return Ok(None);
    }
    let bound = bound
        .as_u64()
        .ok_or_else(|| format!("{member}: not milliseconds"))?;
    Ok(Some(Duration::from_millis(bound)))
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

/// Each request `model` received, as the scenario reads it: the messages
/// it carries, each its role and its text parts joined, and the tool
/// results, with whether the model reads each call as failed (README,
/// `models`).
pub(crate) fn requests(model: &ScriptedProvider) -> Vec<Value> {
    model.requests().iter().map(request).collect()
}

/// Each request `model` received with what `dialect` writes of it on the wire
/// beside it (README, `models`, `chat_messages`).
pub(crate) async fn requests_on_the_wire(
    model: &ScriptedProvider,
    dialect: &str,
) -> Result<Vec<Value>, String> {
    let mut requests = requests(model);
    for (read, context) in requests.iter_mut().zip(model.requests()) {
        read["chat_messages"] = crate::chat_wire::chat_messages(dialect, &context).await?;
    }
    Ok(requests)
}

/// The messages `request` carries, as `models` reads them.
pub(crate) fn messages(request: &AiContext) -> Vec<Value> {
    message_records(&request.messages)
}

/// `messages` as `models` reads a request's ([`messages`]).
pub(crate) fn message_records(messages: &[AiMessage]) -> Vec<Value> {
    messages
        .iter()
        .map(|message| {
            json!({
                "role": message.role.as_str(),
                "text": message.content.text(),
                "parts": parts(&message.content),
                "round": round_of(&message.content),
            })
        })
        .collect()
}

fn request(request: &AiContext) -> Value {
    let messages = messages(request);
    // Every result of each tool message: a round's results are one message.
    let results: Vec<Value> = request
        .messages
        .iter()
        .filter(|message| message.role == AiRole::Tool)
        .flat_map(|message| match &message.content {
            AiMessageContent::Parts(parts) => parts.as_slice(),
            AiMessageContent::Text(_) => &[],
        })
        .filter_map(|part| match part {
            AiContentPart::ToolResult(result) => Some(json!({
                "tool_call_id": result.tool_call_id,
                "name": result.name,
                "content": result_content(&result.result),
                "is_error": result.is_error,
                "references": result.references,
            })),
            AiContentPart::Text(_)
            | AiContentPart::Image(_)
            | AiContentPart::Thinking(_)
            | AiContentPart::ToolCall(_) => None,
        })
        .collect();
    let tools: serde_json::Map<String, Value> = request
        .tools
        .iter()
        .enumerate()
        .map(|(position, tool)| (tool.name.clone(), Value::from(position)))
        .collect();
    json!({
        "messages": messages,
        "tool_results": results,
        "system_prompt": request.system_prompt,
        "temperature": request.temperature,
        "max_tokens": request.max_tokens,
        "reasoning_effort": request.reasoning_effort,
        "thinking_budget": request.thinking_budget,
        "enable_thinking": request.enable_thinking,
        "response_schema": request.response_schema,
        "tools": tools,
        "deferred_tools": request.tools.iter().filter(|tool| tool.defer_loading).map(|tool| &tool.name).collect::<Vec<_>>(),
        "reopened_tools": request.messages.iter().filter(|message| message.metadata.get("reopening") == Some(&Value::Bool(true))).flat_map(AiMessage::tool_results).flat_map(|result| result.references.iter()).collect::<Vec<_>>(),
    })
}

/// A message's text and image parts in order, `null` for a text content
/// (README, `models`).
fn parts(content: &AiMessageContent) -> Value {
    let AiMessageContent::Parts(parts) = content else {
        return Value::Null;
    };
    parts
        .iter()
        .filter_map(|part| match part {
            AiContentPart::Text(text) => Some(json!({ "text": text.text })),
            AiContentPart::Image(image) => {
                Some(json!({ "image": image.url, "mime_type": image.mime_type }))
            }
            AiContentPart::Thinking(_)
            | AiContentPart::ToolCall(_)
            | AiContentPart::ToolResult(_) => None,
        })
        .collect()
}

/// A message's reasoning, text and call parts in order, `null` for a text
/// content (README, `models`).
fn round_of(content: &AiMessageContent) -> Value {
    let AiMessageContent::Parts(parts) = content else {
        return Value::Null;
    };
    parts
        .iter()
        .filter_map(|part| match part {
            AiContentPart::Thinking(part) => Some(json!({
                "thinking": part.thinking,
                "signature": part.signature,
                "redacted": part.redacted,
            })),
            AiContentPart::Text(text) => Some(json!({ "text": text.text })),
            AiContentPart::ToolCall(call) => Some(json!({
                "call": call.id,
                "arguments": call.arguments,
                "metadata": call.metadata,
            })),
            AiContentPart::Image(_) | AiContentPart::ToolResult(_) => None,
        })
        .collect()
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
        write_record(&reaction);
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
        "result": event.result.as_ref().map(hook_result),
        "is_error": event.is_error,
        "refused": event.refused,
        "cancelled": event.cancelled,
        "error_detail": event.error_detail,
    })
}

/// Attach the scripted refusal after an explicit stream or a conventional round.
fn with_failure(mut round: ScriptedRound, spec: &Value) -> Result<ScriptedRound, String> {
    if let Some(failure) = spec.get("fails") {
        round = round.failing(failure.as_str().ok_or("fails: not a text")?);
    }
    if let Some(refusal) = spec.get("refuses") {
        let status = refusal["status"]
            .as_u64()
            .and_then(|status| u16::try_from(status).ok());
        let message = refusal["message"].as_str();
        let (Some(status), Some(message)) = (status, message) else {
            return Err("refuses: no status and message".into());
        };
        round = round.refusing(status, message);
        if let Some(overflow) = refusal["context_overflow"].as_bool() {
            round = round.with_context_overflow(overflow);
        }
        if let Some(provider) = refusal["provider"].as_str() {
            round = round.refused_by(provider);
        }
        if refusal["retryable"] == true {
            round = round.retrying();
        }
    }
    if spec.get("done") == Some(&Value::Bool(false)) {
        round = round.without_done();
    }
    if spec.get("failure_after_calls") == Some(&Value::Bool(true)) {
        round = round.failing_after_calls();
    }
    if spec.get("failure_after_done") == Some(&Value::Bool(true)) {
        round = round.failing_after_done();
    }
    Ok(round)
}

#[cfg(test)]
#[path = "agent_model_tests.rs"]
mod model_tests;
