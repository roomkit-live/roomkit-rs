//! The scripted parts of a scenario: gates, reactions, channels and hooks
//! (roomkit-specs `conformance/README.md`, "Scripted channels", "Scripted
//! hooks"), built on the tokio runtime.
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelDirection, ChannelId,
    ChannelOutput, ChannelType, DeliveryStatus, HookResult, InboundMessage, Metadata, RoomContext,
    RoomEvent, RoomId,
};
use roomkit::{
    Channel, ChannelError, CountryCode, DeliveryStream, HookError, HookFuture, SessionStartedEvent,
    StreamItem, TransportChannel, TransportChannelOptions, default_inbound_event,
};
use roomkit_core::ops::ChannelConfig;
use serde_json::{Value, json};
use tokio::sync::watch;

use crate::run::{Run, Scope};
use crate::templates::{
    complete_event, complete_observation, complete_task, hook_result, invalid_hook_result,
};

/// How long a step waits for a call to reach a gate before the scenario fails.
pub(crate) const GATE_BOUND: Duration = Duration::from_secs(5);

tokio::task_local! {
    /// The payload and the context of the scripted hook whose actions run,
    /// which `sender_verification` reads (identity verification, §11.7.3).
    pub(crate) static HOOKED: (RoomEvent, RoomContext);
}

/// What a reaction's `fail` raises.
#[derive(Debug)]
pub(crate) struct ScriptedFailure(pub(crate) String);

impl std::fmt::Display for ScriptedFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ScriptedFailure {}

/// Named gates: a call held at one waits until a step opens it.
#[derive(Debug, Default)]
pub(crate) struct Gates {
    gates: Mutex<HashMap<String, Gate>>,
    order: Mutex<HashMap<String, usize>>,
}

#[derive(Clone, Debug)]
struct Gate {
    opened: watch::Sender<bool>,
    reached: watch::Sender<bool>,
}

impl Gates {
    fn gate(&self, name: &str) -> Gate {
        self.gates
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(name.to_owned())
            .or_insert_with(|| Gate {
                opened: watch::channel(false).0,
                reached: watch::channel(false).0,
            })
            .clone()
    }

    pub(crate) async fn hold(&self, name: &str) {
        let gate = self.gate(name);
        self.reach(name);
        let mut opened = gate.opened.subscribe();
        while !*opened.borrow_and_update() {
            // The sender lives in the map for the whole run.
            if opened.changed().await.is_err() {
                break;
            }
        }
    }

    /// Waits until a call is held at `name`; `false` when none came in time.
    pub(crate) async fn wait_for(&self, name: &str) -> bool {
        let mut reached = self.gate(name).reached.subscribe();
        tokio::time::timeout(GATE_BOUND, reached.wait_for(|held| *held))
            .await
            .is_ok_and(|result| result.is_ok())
    }

    pub(crate) fn reach(&self, name: &str) {
        let mut order = self.order.lock().unwrap_or_else(PoisonError::into_inner);
        let position = order.len();
        order.entry(name.to_owned()).or_insert(position);
        self.gate(name).reached.send_replace(true);
    }

    pub(crate) fn reached_after(&self, name: &str, earlier: &str) -> bool {
        let order = self.order.lock().unwrap_or_else(PoisonError::into_inner);
        order
            .get(earlier)
            .zip(order.get(name))
            .is_some_and(|(a, b)| a < b)
    }

    pub(crate) fn open(&self, name: &str) {
        self.gate(name).opened.send_replace(true);
    }
}

/// A list of reactions, each used once unless it repeats.
#[derive(Debug, Default)]
pub(crate) struct Reactions {
    items: Vec<Value>,
    used: Mutex<Vec<bool>>,
}

impl Reactions {
    pub(crate) fn new(reactions: Option<&Value>) -> Self {
        let items = reactions
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let used = Mutex::new(vec![false; items.len()]);
        Self { items, used }
    }

    pub(crate) fn pick(&self, subject: &Value) -> Option<Value> {
        let mut used = self.used.lock().unwrap_or_else(PoisonError::into_inner);
        for (position, reaction) in self.items.iter().enumerate() {
            if used[position] || !holds(reaction.get("when"), subject) {
                continue;
            }
            if reaction.get("repeat") != Some(&Value::Bool(true)) {
                used[position] = true;
            }
            return Some(reaction.clone());
        }
        None
    }
}

fn holds(condition: Option<&Value>, subject: &Value) -> bool {
    condition
        .and_then(Value::as_object)
        .is_none_or(|condition| {
            condition
                .iter()
                .all(|(key, value)| subject.get(key) == Some(value))
        })
}

/// What a reaction's `when` reads of the call's event.
pub(crate) fn event_subject(event: &RoomEvent, room_id: &str) -> Value {
    let dumped = serde_json::to_value(event).unwrap_or_default();
    json!({
        "room_id": room_id,
        "text": dumped["content"]["body"],
        "type": dumped["type"],
        "source_channel_id": dumped["source"]["channel_id"],
        "chain_depth": dumped["chain_depth"],
    })
}

fn message_subject(message: &InboundMessage, room_id: &str) -> Value {
    let dumped = serde_json::to_value(message).unwrap_or_default();
    json!({
        "room_id": room_id,
        "text": dumped["content"]["body"],
        "type": dumped["event_type"],
        "source_channel_id": dumped["channel_id"],
        "chain_depth": dumped["chain_depth"],
    })
}

/// Starts the reaction's task, holds at its gate, waits its delay, runs its
/// actions, then fails. Cut, its future dropped, it makes its `on_cut`
/// actions as its cleanup, on a task its guard starts in its context.
pub(crate) async fn perform(
    run: &Arc<Run>,
    reaction: &Value,
    scope: &Scope,
) -> Result<(), ScriptedFailure> {
    let on_cut = reaction
        .get("on_cut")
        .and_then(Value::as_array)
        .map(|actions| OnCut::new(run, actions.clone(), scope.clone()));
    let performed = perform_reaction(run, reaction, scope).await;
    if let Some(on_cut) = on_cut {
        on_cut.disarm();
    }
    performed
}

/// A reaction's `on_cut` cleanup: its actions, in the reaction's context,
/// made on a task of their own if the reaction is dropped before its end.
/// Rust runs no async cleanup in a dropped future, so the cleanup is built
/// while the reaction runs and started as the guard drops.
struct OnCut {
    run: Arc<Run>,
    cleanup: Option<std::pin::Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl OnCut {
    fn new(run: &Arc<Run>, actions: Vec<Value>, scope: Scope) -> Self {
        let acting = Arc::clone(run);
        let cleanup = roomkit::keep_context(async move {
            for action in actions {
                acting.run_action(action, scope.clone()).await;
            }
        });
        Self {
            run: Arc::clone(run),
            cleanup: Some(Box::pin(cleanup)),
        }
    }

    /// The reaction ended uncut: no cleanup.
    fn disarm(mut self) {
        self.cleanup = None;
    }
}

impl Drop for OnCut {
    fn drop(&mut self) {
        // A guard dropped outside any runtime (the run's own teardown) has
        // nowhere to run its cleanup: the cut it answers never came.
        if let (Some(cleanup), Ok(runtime)) =
            (self.cleanup.take(), tokio::runtime::Handle::try_current())
        {
            self.run.hold_spawned(runtime.spawn(cleanup));
        }
    }
}

async fn perform_reaction(
    run: &Arc<Run>,
    reaction: &Value,
    scope: &Scope,
) -> Result<(), ScriptedFailure> {
    if let Some(spawn) = reaction.get("spawn") {
        run.spawn(spawn.clone(), scope.clone());
    }
    if let Some(gate) = reaction.get("gate").and_then(Value::as_str) {
        run.gates.hold(gate).await;
    }
    if let Some(delay) = reaction.get("delay_ms").and_then(Value::as_u64)
        && delay > 0
    {
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }
    for action in reaction
        .get("do")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        run.run_action(action.clone(), scope.clone()).await;
    }
    if let Some(held) = reaction.get("holds_cpu_ms").and_then(Value::as_u64) {
        hold_cpu(Duration::from_millis(held));
    }
    if let Some(gate) = reaction.get("after_do_gate").and_then(Value::as_str) {
        run.gates.hold(gate).await;
    }
    if let Some(later) = reaction.get("after_turn").and_then(Value::as_array) {
        run.leave_after_turn(later.clone(), scope.clone());
    }
    match reaction.get("fail").and_then(Value::as_str) {
        Some(message) => Err(ScriptedFailure(message.to_owned())),
        None => Ok(()),
    }
}

/// Keeps the calling thread busy for `held`, yielding nothing to its runtime
/// meanwhile, as a turn busy with its CPU does (RFC §10.6.8).
#[expect(
    clippy::disallowed_methods,
    reason = "a busy thread reads the wall clock: a runtime's may be paused"
)]
fn hold_cpu(held: Duration) {
    let until = std::time::Instant::now() + held;
    while std::time::Instant::now() < until {
        std::hint::spin_loop();
    }
}

/// The provider's explicit refusal a reaction's `refuse` names, Python's
/// `ProviderDeliveryError` of its `ProviderResult`.
fn refusal(reaction: &Value) -> Option<roomkit::RoomKitError> {
    let refuse = reaction.get("refuse")?;
    let mut metadata = Metadata::new();
    for key in ["code", "retryable"] {
        if let Some(value) = refuse.get(key) {
            metadata.insert(key.to_owned(), value.clone());
        }
    }
    let result = roomkit::model::ProviderResult {
        success: false,
        provider_message_id: refuse["provider_message_id"].as_str().map(str::to_owned),
        error: refuse["error"].as_str().map(str::to_owned),
        metadata,
    };
    Some(roomkit::RoomKitError::ProviderDelivery { result })
}

/// The built-in error a reaction's `raises` names (README, Reactions),
/// raised under its Python class, as roomkit's runner raises the class
/// itself, with its message.
fn raised(reaction: &Value) -> Option<ChannelError> {
    let raises = reaction.get("raises")?;
    let kind = raises["kind"].as_str().unwrap_or_default();
    let message = raises["message"].as_str().unwrap_or_default().to_owned();
    Some(Box::new(roomkit::ClassedError::new(
        kind,
        ScriptedFailure(message),
    )))
}

/// `event` with a parse reaction's `parsed` patch: its members replace the
/// event's, those of its `source` the event's source's.
fn parsed_event(event: RoomEvent, patch: &Value) -> Result<RoomEvent, ScriptedFailure> {
    let mut data = serde_json::to_value(event).map_err(|e| ScriptedFailure(e.to_string()))?;
    for (key, value) in patch.as_object().into_iter().flatten() {
        match (key.as_str(), value) {
            ("source", Value::Object(source)) => {
                for (member, item) in source {
                    data["source"][member] = item.clone();
                }
            }
            _ => data[key] = value.clone(),
        }
    }
    serde_json::from_value(data).map_err(|e| ScriptedFailure(format!("a parsed patch: {e}")))
}

/// The implementation's transport channel of a scripted channel's type,
/// whose address methods the channel answers with (README, `addressing`):
/// its type's addressing, with the `country_code` the scenario declares;
/// `None` for a channel without `addressing`. A code `CountryCode::new`
/// refuses is the registration's `invalid_argument`, checked before
/// (`country_code_refused`).
fn addressed_transport(
    spec: &Value,
    declaration: &ChannelConfig,
) -> Result<Option<TransportChannel>, String> {
    let Some(addressing) = spec.get("addressing") else {
        return Ok(None);
    };
    let country_code = addressing["country_code"]
        .as_str()
        .map(CountryCode::new)
        .transpose()
        .map_err(|error| error.to_string())?;
    Ok(Some(TransportChannel::new(
        declaration.channel_id.clone(),
        declaration.channel_type.clone(),
        TransportChannelOptions {
            country_code,
            ..TransportChannelOptions::default()
        },
    )))
}

/// Whether the implementation refuses the country code a scripted channel's
/// `addressing` declares (RFC §5.7): the registration's `invalid_argument`.
pub(crate) fn country_code_refused(spec: &Value) -> bool {
    spec["addressing"]["country_code"]
        .as_str()
        .is_some_and(|code| CountryCode::new(code).is_err())
}

/// A channel whose parse, turn and delivery follow the scenario's reactions.
pub(crate) struct ScriptedChannel {
    declaration: ChannelConfig,
    /// Whether its `deliver_stream` records what each generation told
    /// (README: `reads_usage`).
    reads_usage: bool,
    /// Whether its `deliver_stream` records the turn's end the stream hands
    /// last (README: `reads_loop_end`).
    reads_loop_end: bool,
    run: Weak<Run>,
    parse: Reactions,
    on_event: Reactions,
    deliver: Reactions,
    deliver_stream: Reactions,
    /// The reactions of its `deliver_cut` (README, `deliver_cut`).
    deliver_cut: Reactions,
    /// What it declares of its room turns (README, `steers_room_turns`):
    /// whether it steers them, and whether it serializes them.
    room_turns: (bool, bool),
    on_room_attached: Reactions,
    on_room_detached: Reactions,
    close: Reactions,
    binding_update: Option<Value>,
    verbs: crate::channel_verbs::Verbs,
    /// What its own address methods answer: the defaults when `None`.
    addresses: Option<Value>,
    /// The implementation's transport channel of its type, whose address
    /// methods it answers with (README, `addressing`), in place of
    /// `addresses` and of the defaults.
    transport: Option<TransportChannel>,
    /// The rooms it streams in: every room when `None`.
    streams_in: Option<Vec<String>>,
    /// How many of the room's latest events it declares it reads.
    recent_events_window: usize,
    /// Whether it names its own senders (RFC §11.6).
    sender_is_participant: bool,
}

impl ScriptedChannel {
    pub(crate) fn new(spec: &Value, run: &Arc<Run>) -> Result<Self, String> {
        let parse_as = |key: &str, default: &str| -> Result<Value, String> {
            Ok(spec
                .get(key)
                .cloned()
                .unwrap_or_else(|| Value::String(default.to_owned())))
        };
        let channel_type: ChannelType =
            serde_json::from_value(spec["type"].clone()).map_err(|e| e.to_string())?;
        let default_category = if spec["type"] == "ai" {
            "intelligence"
        } else {
            "transport"
        };
        let category: ChannelCategory =
            serde_json::from_value(parse_as("category", default_category)?)
                .map_err(|e| e.to_string())?;
        let direction: ChannelDirection =
            serde_json::from_value(parse_as("direction", "bidirectional")?)
                .map_err(|e| e.to_string())?;
        let id =
            ChannelId::new(spec["id"].as_str().unwrap_or_default()).map_err(|e| e.to_string())?;
        let mut declaration = ChannelConfig::new(id, channel_type, category, direction);
        if let Some(capabilities) = spec.get("capabilities") {
            declaration.capabilities =
                serde_json::from_value::<ChannelCapabilities>(capabilities.clone())
                    .map_err(|e| e.to_string())?;
        }
        let transport = addressed_transport(spec, &declaration)?;
        declaration.recipient_key = match &transport {
            Some(transport) => transport.recipient_key(),
            None => spec
                .get("recipient_key")
                .and_then(Value::as_str)
                .map(str::to_owned),
        };
        declaration.chat_key = None;
        declaration.supports_streaming_delivery = spec.get("streams") == Some(&Value::Bool(true));
        Ok(Self {
            declaration,
            reads_usage: spec.get("reads_usage") == Some(&Value::Bool(true)),
            reads_loop_end: spec.get("reads_loop_end") == Some(&Value::Bool(true)),
            run: Arc::downgrade(run),
            parse: Reactions::new(spec.get("parse")),
            on_event: Reactions::new(spec.get("on_event")),
            deliver: Reactions::new(spec.get("deliver")),
            deliver_stream: Reactions::new(spec.get("deliver_stream")),
            deliver_cut: Reactions::new(spec.get("deliver_cut")),
            room_turns: (
                spec["steers_room_turns"] == true,
                spec["serializes_room_turns"] == true,
            ),
            on_room_attached: Reactions::new(spec.get("on_room_attached")),
            on_room_detached: Reactions::new(spec.get("on_room_detached")),
            close: Reactions::new(spec.get("close")),
            binding_update: spec.get("update_binding").cloned(),
            verbs: crate::channel_verbs::Verbs::new(spec["verbs"].clone()),
            addresses: spec.get("addresses").cloned(),
            transport,
            streams_in: spec
                .get("streams_in")
                .map(|rooms| serde_json::from_value(rooms.clone()))
                .transpose()
                .map_err(|e| e.to_string())?,
            recent_events_window: spec
                .get("recent_events_window")
                .and_then(Value::as_u64)
                .and_then(|window| usize::try_from(window).ok())
                .unwrap_or(0),
            sender_is_participant: spec.get("sender_is_participant") == Some(&Value::Bool(true)),
        })
    }

    /// The recipient key its own reply and recipient methods write under,
    /// when the scenario gives its address methods.
    fn reply_key(&self) -> Option<&str> {
        self.addresses.as_ref()?;
        self.declaration.recipient_key.as_deref()
    }

    fn source(&self) -> Value {
        json!({
            "channel_id": self.declaration.channel_id,
            "channel_type": self.declaration.channel_type,
            "direction": "outbound",
        })
    }

    /// A room callback (RFC §6.1): recorded, then its reaction's gate, its
    /// actions and its failure.
    async fn room_callback(
        &self,
        reactions: &Reactions,
        room_id: &RoomId,
        call: &str,
        binding: Option<&ChannelBinding>,
    ) -> Result<(), ChannelError> {
        let Some(run) = self.run.upgrade() else {
            return Ok(());
        };
        let room_id = room_id.to_string();
        run.record_room_callback(&room_id, &self.declaration.channel_id, call, binding);
        let Some(reaction) = reactions.pick(&serde_json::json!({ "room_id": room_id })) else {
            return Ok(());
        };
        perform(&run, &reaction, &Scope::in_room(&room_id)).await?;
        Ok(())
    }

    async fn react(
        &self,
        reactions: &Reactions,
        event: &RoomEvent,
        respond: bool,
    ) -> Result<ChannelOutput, ChannelError> {
        let Some(run) = self.run.upgrade() else {
            return Ok(ChannelOutput::default());
        };
        let room_id = event.room_id.to_string();
        let Some(reaction) = reactions.pick(&event_subject(event, &room_id)) else {
            return Ok(ChannelOutput::default());
        };
        let scope = Scope::with_event(&room_id, event.id.as_ref());
        perform(&run, &reaction, &scope).await?;
        if let Some(refusal) = refusal(&reaction) {
            return Err(Box::new(refusal));
        }
        if let Some(raised) = raised(&reaction) {
            return Err(raised);
        }
        let output = reaction.get("output").cloned().unwrap_or_default();
        let channel_id = self.declaration.channel_id.to_string();
        let responses: Vec<Value> = if respond {
            reaction
                .get("respond")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .map(|template| {
                    let mut template = template.clone();
                    if template.get("chain_depth").is_none() {
                        template["chain_depth"] = json!(event.chain_depth + 1);
                    }
                    complete_event(&template, &room_id, &self.source())
                })
                .collect()
        } else {
            Vec::new()
        };
        let mut dumped = side_effects(&output, &room_id, &channel_id);
        // Whether it answered the event, its responses re-entering only then
        // (README, `output.responded`).
        let answered = output["responded"]
            .as_bool()
            .unwrap_or_else(|| reaction.get("respond").is_some());
        dumped["responded"] = json!(respond && answered);
        dumped["response_events"] = json!(responses);
        Ok(serde_json::from_value(dumped)?)
    }
}

/// The output a reaction's `output` gives, in the serialized form: its
/// tasks, observations and room metadata updates, completed for `room_id`,
/// the record its turn writes, and the error the channel met, a
/// `RuntimeError` of its message.
fn side_effects(output: &Value, room_id: &str, channel_id: &str) -> Value {
    let list = |key: &str| {
        output
            .get(key)
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
    };
    json!({
        "tasks": list("tasks").iter().map(|t| complete_task(t, room_id)).collect::<Vec<_>>(),
        "observations": list("observations")
            .iter()
            .map(|o| complete_observation(o, room_id, channel_id))
            .collect::<Vec<_>>(),
        "metadata_updates": output.get("metadata_updates").cloned().unwrap_or_else(|| json!({})),
        "response_metadata": output.get("response_metadata").cloned().unwrap_or_else(|| json!({})),
        "provider_result": output.get("provider_result"),
        "error": output.get("error").and_then(Value::as_str).map(|message| {
            json!({ "error_type": "RuntimeError", "message": message })
        }),
    })
}

impl Channel for ScriptedChannel {
    fn channel_id(&self) -> ChannelId {
        self.declaration.channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        self.declaration.channel_type.clone()
    }

    fn category(&self) -> ChannelCategory {
        self.declaration.category
    }

    fn direction(&self) -> ChannelDirection {
        self.declaration.direction
    }

    fn capabilities(&self) -> ChannelCapabilities {
        self.declaration.capabilities.clone()
    }

    fn provider_name(&self) -> Option<String> {
        self.declaration.provider_name.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        self.declaration.recipient_key.clone()
    }

    fn supports_streaming_delivery(&self) -> bool {
        self.declaration.supports_streaming_delivery
    }

    fn recent_events_window(&self) -> usize {
        self.recent_events_window
    }

    fn sender_is_participant(&self) -> bool {
        self.sender_is_participant
    }

    fn steers_room_turns(&self) -> bool {
        self.room_turns.0
    }

    /// Declared with `steers_room_turns` (README, `steers_room_turns`).
    fn serializes_room_turns(&self) -> bool {
        self.room_turns.1
    }

    fn supports_streaming_delivery_for(&self, room_id: &RoomId) -> bool {
        // Asked whatever the channel declares (README, `streams_in`).
        match &self.streams_in {
            Some(rooms) => rooms.iter().any(|room| room == room_id.as_str()),
            None => self.declaration.supports_streaming_delivery,
        }
    }

    fn normalize_address(&self, address: &str) -> String {
        if let Some(transport) = &self.transport {
            return transport.normalize_address(address);
        }
        self.addresses
            .as_ref()
            .and_then(|addresses| addresses["normalizes"].get(address))
            .and_then(Value::as_str)
            .unwrap_or(address)
            .to_owned()
    }

    fn conversation_address(&self, message: &InboundMessage) -> String {
        if let Some(transport) = &self.transport {
            return transport.conversation_address(message);
        }
        let key = self
            .addresses
            .as_ref()
            .and_then(|addresses| addresses["conversation_key"].as_str());
        key.and_then(|key| message.metadata.get(key))
            .and_then(Value::as_str)
            .filter(|chat| !chat.trim().is_empty())
            .map_or_else(|| message.sender_id.clone(), str::to_owned)
    }

    fn reply_metadata(&self, message: &InboundMessage) -> BTreeMap<String, String> {
        if let Some(transport) = &self.transport {
            return transport.reply_metadata(message);
        }
        self.reply_key()
            .map(|key| (key.to_owned(), self.conversation_address(message)))
            .into_iter()
            .collect()
    }

    fn recipient_metadata(&self, address: &str) -> BTreeMap<String, String> {
        if let Some(transport) = &self.transport {
            return transport.recipient_metadata(address);
        }
        self.reply_key()
            .map(|key| (key.to_owned(), address.to_owned()))
            .into_iter()
            .collect()
    }

    fn recipient_address(&self, binding: &ChannelBinding) -> Option<String> {
        if let Some(transport) = &self.transport {
            return transport.recipient_address(binding);
        }
        let declared = self.reply_key()?;
        let key = self
            .addresses
            .as_ref()
            .and_then(|addresses| addresses["recipient_at"].as_str())
            .unwrap_or(declared);
        let held = binding.metadata.get(key)?.as_str()?;
        (!held.trim().is_empty()).then(|| self.normalize_address(held))
    }

    async fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> Result<RoomEvent, ChannelError> {
        let room_id = context.room.id.to_string();
        if let Some(run) = self.run.upgrade() {
            run.record_context("parse", &context, &self.declaration.channel_id);
        }
        let event = default_inbound_event(self, &message, &context);
        let (Some(run), Some(reaction)) = (
            self.run.upgrade(),
            self.parse.pick(&message_subject(&message, &room_id)),
        ) else {
            return Ok(event);
        };
        let scope = Scope::in_room(&room_id);
        perform(&run, &reaction, &scope).await?;
        let Some(patch) = reaction.get("parsed") else {
            return Ok(event);
        };
        let patch = run.resolve(patch.clone(), &scope).await;
        Ok(parsed_event(event, &patch)?)
    }

    async fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        if self.declaration.category == ChannelCategory::Intelligence
            && let Some(run) = self.run.upgrade()
        {
            run.record("turns", &event, &self.declaration.channel_id);
            run.record_context("on_event", &context, &self.declaration.channel_id);
        }
        self.react(&self.on_event, &event, true).await
    }

    async fn channel_call(
        &self,
        room_id: RoomId,
        verb: String,
        arguments: Metadata,
    ) -> Result<Value, ChannelError> {
        self.verbs
            .call(
                self.run.upgrade(),
                &self.declaration.channel_id,
                room_id,
                verb,
                arguments,
            )
            .await
    }

    fn room_state(&self, room_id: &RoomId) -> Result<Metadata, ChannelError> {
        Ok(self.verbs.state(room_id))
    }

    fn update_binding(
        &self,
        room_id: &RoomId,
        binding: &ChannelBinding,
    ) -> Result<(), ChannelError> {
        let Some(control) = &self.binding_update else {
            return Ok(());
        };
        if let Some(run) = self.run.upgrade() {
            run.record_room_callback(
                room_id.as_str(),
                &self.declaration.channel_id,
                "update_binding",
                Some(binding),
            );
        }
        if let Some(message) = control["fail"].as_str() {
            return Err(Box::new(ScriptedFailure(message.to_owned())));
        }
        Ok(())
    }

    fn restore_room_state(&self, room_id: &RoomId, state: Metadata) -> Result<(), ChannelError> {
        self.verbs.restore(room_id, state);
        Ok(())
    }

    async fn on_room_attached(
        &self,
        room_id: RoomId,
        binding: ChannelBinding,
    ) -> Result<(), ChannelError> {
        let reactions = &self.on_room_attached;
        self.room_callback(reactions, &room_id, "on_room_attached", Some(&binding))
            .await
    }

    /// Its close, recorded under `channel_closes`, then its reaction
    /// (README, `close`): its `fail` panics, Rust's close returning
    /// nothing.
    #[expect(
        clippy::panic,
        reason = "a channel's close fails by panicking: it returns nothing"
    )]
    async fn close(&self) {
        let Some(run) = self.run.upgrade() else {
            return;
        };
        run.record_channel_close(self.declaration.channel_id.as_str());
        let Some(reaction) = self.close.pick(&json!({})) else {
            return;
        };
        if let Err(ScriptedFailure(message)) = perform(&run, &reaction, &Scope::default()).await {
            std::panic::panic_any(message);
        }
    }

    async fn on_room_detached(&self, room_id: RoomId) -> Result<(), ChannelError> {
        self.verbs.forget(&room_id);
        let reactions = &self.on_room_detached;
        self.room_callback(reactions, &room_id, "on_room_detached", None)
            .await
    }

    async fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        if let Some(run) = self.run.upgrade() {
            run.record("deliveries", &event, &self.declaration.channel_id);
            run.record_context("deliver", &context, &self.declaration.channel_id);
        }
        self.react(&self.deliver, &event, false).await
    }

    /// Reads the stream it is handed, recording each delta and row, to its
    /// end, or until the delta its reaction's `stop_after` names, or the
    /// row its `stop_after_rows` counts.
    async fn deliver_stream(
        &self,
        mut stream: DeliveryStream,
        _event: RoomEvent,
        binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let Some(run) = self.run.upgrade() else {
            return Ok(ChannelOutput::default());
        };
        let room_id = binding.room_id.to_string();
        let reaction = self
            .deliver_stream
            .pick(&json!({ "room_id": room_id }))
            .unwrap_or_default();
        let mut held = reaction.clone();
        if let Some(held) = held.as_object_mut() {
            held.shift_remove("fail");
        }
        perform(&run, &held, &Scope::in_room(&room_id)).await?;
        let stop_after = reaction.get("stop_after").and_then(Value::as_str);
        let mut rows_left = reaction.get("stop_after_rows").and_then(Value::as_u64);
        let channel_id = &self.declaration.channel_id;
        while let Some(item) = stream.next().await {
            match item {
                Ok(StreamItem::Text(text)) => {
                    run.record_streamed(&room_id, channel_id, json!({ "text": text }));
                    if stop_after == Some(text.as_str()) {
                        break;
                    }
                    if reaction.get("hold_after").and_then(Value::as_str) == Some(text.as_str()) {
                        let at_gate = json!({ "gate": reaction["hold_gate"] });
                        perform(&run, &at_gate, &Scope::in_room(&room_id)).await?;
                    }
                }
                Ok(StreamItem::Usage(marker)) => {
                    if self.reads_usage {
                        let told = json!({
                            "usage": marker.usage,
                            "partial": marker.partial,
                            "model": marker.model,
                            "provider": marker.provider,
                        });
                        run.record_streamed(&room_id, channel_id, told);
                    }
                }
                Ok(StreamItem::LoopEnd(marker)) => {
                    if self.reads_loop_end {
                        let ended = json!({
                            "reason": marker.reason.to_string(),
                            "rounds": marker.rounds,
                            "usage": marker.usage,
                        });
                        run.record_streamed(&room_id, channel_id, json!({ "loop_end": ended }));
                    }
                }
                Ok(StreamItem::Thinking(thinking)) => {
                    let stops = stop_after == Some(thinking.as_str());
                    let piece = json!({ "thinking": thinking });
                    run.record_streamed(&room_id, channel_id, piece);
                    if stops {
                        break;
                    }
                }
                Ok(StreamItem::Row(row)) => {
                    let row = serde_json::to_value(&*row).unwrap_or_default();
                    run.record_streamed(&room_id, channel_id, json!({ "row": row }));
                    if row["type"] == "tool_call_start"
                        && let Some(abandon) = reaction.get("close_while_reading")
                    {
                        let running = abandon["running"].as_str().unwrap_or_default();
                        tokio::select! {
                            biased;
                            _ = stream.next() => run.problem(format!("tool read finished before {running} opened")),
                            reached = run.gates.wait_for(running) => {
                                if !reached {
                                    run.problem(format!("no tool reached {running}"));
                                }
                            }
                        }
                        drop(stream);
                        run.gates
                            .reach(abandon["closed"].as_str().unwrap_or_default());
                        break;
                    }
                    if let Some(left) = rows_left.as_mut() {
                        *left = left.saturating_sub(1);
                        if *left == 0 {
                            break;
                        }
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    run.record_streamed(&room_id, channel_id, json!({ "failed": true }));
                    return Err(error);
                }
            }
        }
        if let Some(message) = reaction.get("fail").and_then(Value::as_str) {
            return Err(ScriptedFailure(message.to_owned()).into());
        }
        let output = reaction.get("output").cloned().unwrap_or_default();
        let dumped = side_effects(&output, &room_id, channel_id.as_str());
        Ok(serde_json::from_value(dumped)?)
    }

    /// Records the end of a stream cut from outside, the row it is handed,
    /// then reacts (README, `deliver_cut`).
    async fn deliver_cut(
        &self,
        _event: RoomEvent,
        row: Option<RoomEvent>,
        binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<(), ChannelError> {
        let Some(run) = self.run.upgrade() else {
            return Ok(());
        };
        let row = row.map_or(Value::Null, |row| {
            serde_json::to_value(&row).unwrap_or_default()
        });
        let room_id = binding.room_id.to_string();
        run.record_streamed(
            &room_id,
            &self.declaration.channel_id,
            json!({ "cut": row }),
        );
        if let Some(reaction) = self.deliver_cut.pick(&json!({ "room_id": room_id })) {
            perform(&run, &reaction, &Scope::in_room(&room_id)).await?;
        }
        Ok(())
    }
}

/// The name a scripted hook records its calls under: its `as` label for
/// one registered with no name (README, Setup), else its name.
pub(crate) fn recorded_name(spec: &Value) -> String {
    spec.get("as")
        .or_else(|| spec.get("name"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// The handler of a scripted hook: it records its call, then reacts.
/// What a scripted session hook gives.
type SessionHookFuture = std::pin::Pin<
    Box<dyn Future<Output = Result<HookResult<SessionStartedEvent>, HookError>> + Send>,
>;

/// The handler of the scripted hook `spec` on `ON_SESSION_STARTED`: it
/// records its call, its payload no event, then reacts.
pub(crate) fn session_hook_handler(
    spec: &Value,
    run: &Arc<Run>,
) -> impl Fn(SessionStartedEvent, RoomContext) -> SessionHookFuture + Send + Sync + 'static {
    let reactions = Arc::new(Reactions::new(spec.get("reactions")));
    let name = recorded_name(spec);
    let trigger = spec["trigger"].as_str().unwrap_or_default().to_owned();
    let run = Arc::downgrade(run);
    move |_payload: SessionStartedEvent, context: RoomContext| {
        let reactions = Arc::clone(&reactions);
        let (name, trigger, run) = (name.clone(), trigger.clone(), Weak::clone(&run));
        Box::pin(async move {
            let allow = HookResult::allow(AllowOptions::default());
            let Some(run) = run.upgrade() else {
                return Ok(allow);
            };
            let room_id = context.room.id.to_string();
            run.record_hook(&name, &trigger, &room_id, None, &context);
            if let Some(reaction) = reactions.pick(&json!({ "room_id": room_id })) {
                perform(&run, &reaction, &Scope::in_room(&room_id)).await?;
            }
            Ok(allow)
        })
    }
}

/// What a scripted delivery status hook gives.
type StatusHookFuture =
    std::pin::Pin<Box<dyn Future<Output = Result<HookResult<DeliveryStatus>, HookError>> + Send>>;

/// The handler of the scripted hook `spec` on `ON_DELIVERY_STATUS`: it
/// records its call, its payload the status, then reacts.
pub(crate) fn status_hook_handler(
    spec: &Value,
    run: &Arc<Run>,
) -> impl Fn(DeliveryStatus, RoomContext) -> StatusHookFuture + Send + Sync + 'static {
    let reactions = Arc::new(Reactions::new(spec.get("reactions")));
    let name = recorded_name(spec);
    let trigger = spec["trigger"].as_str().unwrap_or_default().to_owned();
    let run = Arc::downgrade(run);
    move |payload: DeliveryStatus, context: RoomContext| {
        let reactions = Arc::clone(&reactions);
        let (name, trigger, run) = (name.clone(), trigger.clone(), Weak::clone(&run));
        Box::pin(async move {
            let allow = HookResult::allow(AllowOptions::default());
            let Some(run) = run.upgrade() else {
                return Ok(allow);
            };
            let room_id = context.room.id.to_string();
            let status = serde_json::to_value(&payload).unwrap_or_default();
            run.record_status_hook(&name, &trigger, &room_id, &status);
            if let Some(reaction) = reactions.pick(&json!({ "room_id": room_id })) {
                perform(&run, &reaction, &Scope::in_room(&room_id)).await?;
            }
            Ok(allow)
        })
    }
}

/// Registers the scripted framework-event handler `spec` on the run's kit
/// (README, `event_handlers`): it reacts to each event of its type, in the
/// context the kit runs it in.
pub(crate) fn register_event_handler(run: &Arc<Run>, spec: &Value) -> Result<(), String> {
    let event_type = spec["event"]
        .as_str()
        .ok_or_else(|| format!("{spec}: no event"))?
        .to_owned();
    let reactions = Arc::new(Reactions::new(spec.get("reactions")));
    let weak = Arc::downgrade(run);
    run.kit()
        .on(event_type, move |event: roomkit::FrameworkEvent| {
            let reactions = Arc::clone(&reactions);
            let run = Weak::clone(&weak);
            async move {
                let Some(run) = run.upgrade() else {
                    return;
                };
                let room = event.room_id.map(|id| id.as_str().to_owned());
                let Some(reaction) = reactions.pick(&json!({ "room_id": room })) else {
                    return;
                };
                let scope = room.as_deref().map(Scope::in_room).unwrap_or_default();
                // Nothing to raise: the shape refuses `fail` on these
                // reactions, and `run_action` keeps an action's error.
                drop(perform(&run, &reaction, &scope).await);
            }
        })
        .map_err(|error| format!("{spec}: {error}"))
}

pub(crate) fn hook_handler(
    spec: &Value,
    run: &Arc<Run>,
) -> impl Fn(RoomEvent, RoomContext) -> HookFuture + Send + Sync + 'static {
    // Invalid fixture data fails before the callback can turn it into a hook error.
    for reaction in spec["reactions"].as_array().into_iter().flatten() {
        if let Some(invalid) = reaction.get("invalid_result") {
            invalid_hook_result(invalid, "fixture").unwrap();
        }
    }
    let reactions = Arc::new(Reactions::new(spec.get("reactions")));
    let name = recorded_name(spec);
    let trigger = spec["trigger"].as_str().unwrap_or_default().to_owned();
    let run = Arc::downgrade(run);
    move |payload: RoomEvent, context: RoomContext| {
        let reactions = Arc::clone(&reactions);
        let (name, trigger, run) = (name.clone(), trigger.clone(), Weak::clone(&run));
        Box::pin(async move {
            let Some(run) = run.upgrade() else {
                return Ok(Some(HookResult::allow(AllowOptions::default())));
            };
            let room_id = context.room.id.to_string();
            run.record_hook(&name, &trigger, &room_id, Some(&payload), &context);
            let Some(reaction) = reactions.pick(&event_subject(&payload, &room_id)) else {
                return Ok(Some(HookResult::allow(AllowOptions::default())));
            };
            let scope = Scope::with_event(&room_id, payload.id.as_ref());
            let hooked = (payload.clone(), context);
            HOOKED
                .scope(hooked, perform(&run, &reaction, &scope))
                .await?;
            if let Some(invalid) = reaction.get("invalid_result") {
                return Ok(invalid_hook_result(invalid, &room_id)?);
            }
            let result = run.resolve(reaction["result"].clone(), &scope).await;
            Ok(Some(hook_result(
                (!result.is_null()).then_some(&result),
                &payload,
                &room_id,
            )?))
        })
    }
}
