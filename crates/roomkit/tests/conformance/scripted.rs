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
    ChannelOutput, ChannelType, DeliveryStatus, HookResult, InboundMessage, RoomContext, RoomEvent,
    RoomId,
};
use roomkit::{
    Channel, ChannelError, DeliveryStream, HookError, SessionStartedEvent, StreamItem,
    default_inbound_event,
};
use roomkit_core::ops::ChannelConfig;
use serde_json::{Value, json};
use tokio::sync::watch;

use crate::run::{Run, Scope};
use crate::templates::{complete_event, complete_observation, complete_task, hook_result};

/// How long a step waits for a call to reach a gate before the scenario fails.
const GATE_BOUND: Duration = Duration::from_secs(5);

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
        gate.reached.send_replace(true);
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

/// Holds at the reaction's gate, runs its actions, then raises its failure.
pub(crate) async fn perform(
    run: &Arc<Run>,
    reaction: &Value,
    scope: &Scope,
) -> Result<(), ScriptedFailure> {
    if let Some(gate) = reaction.get("gate").and_then(Value::as_str) {
        run.gates.hold(gate).await;
    }
    for action in reaction
        .get("do")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        run.run_action(action.clone(), scope.clone()).await;
    }
    match reaction.get("fail").and_then(Value::as_str) {
        Some(message) => Err(ScriptedFailure(message.to_owned())),
        None => Ok(()),
    }
}

/// The provider's explicit refusal a reaction's `refuse` names, Python's
/// `ProviderDeliveryError` of its `ProviderResult`.
fn refusal(reaction: &Value) -> Option<roomkit::RoomKitError> {
    let refuse = reaction.get("refuse")?;
    let mut metadata = roomkit::model::Metadata::new();
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

/// A channel whose parse, turn and delivery follow the scenario's reactions.
pub(crate) struct ScriptedChannel {
    declaration: ChannelConfig,
    run: Weak<Run>,
    parse: Reactions,
    on_event: Reactions,
    deliver: Reactions,
    deliver_stream: Reactions,
    /// What its own address methods answer: the defaults when `None`.
    addresses: Option<Value>,
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
        declaration.recipient_key = spec
            .get("recipient_key")
            .and_then(Value::as_str)
            .map(str::to_owned);
        declaration.chat_key = None;
        declaration.supports_streaming_delivery = spec.get("streams") == Some(&Value::Bool(true));
        Ok(Self {
            declaration,
            run: Arc::downgrade(run),
            parse: Reactions::new(spec.get("parse")),
            on_event: Reactions::new(spec.get("on_event")),
            deliver: Reactions::new(spec.get("deliver")),
            deliver_stream: Reactions::new(spec.get("deliver_stream")),
            addresses: spec.get("addresses").cloned(),
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
        let list = |key: &str| {
            output
                .get(key)
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        };
        let dumped = json!({
            "responded": respond && reaction.get("respond").is_some(),
            "response_events": responses,
            "tasks": list("tasks").iter().map(|t| complete_task(t, &room_id)).collect::<Vec<_>>(),
            "observations": list("observations")
                .iter()
                .map(|o| complete_observation(o, &room_id, &channel_id))
                .collect::<Vec<_>>(),
            "metadata_updates": output.get("metadata_updates").cloned().unwrap_or_else(|| json!({})),
        });
        Ok(serde_json::from_value(dumped)?)
    }
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

    fn supports_streaming_delivery_for(&self, room_id: &RoomId) -> bool {
        match &self.streams_in {
            Some(rooms) => {
                self.declaration.supports_streaming_delivery
                    && rooms.iter().any(|room| room == room_id.as_str())
            }
            None => self.declaration.supports_streaming_delivery,
        }
    }

    fn normalize_address(&self, address: &str) -> String {
        self.addresses
            .as_ref()
            .and_then(|addresses| addresses["normalizes"].get(address))
            .and_then(Value::as_str)
            .unwrap_or(address)
            .to_owned()
    }

    fn conversation_address(&self, message: &InboundMessage) -> String {
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
        self.reply_key()
            .map(|key| (key.to_owned(), self.conversation_address(message)))
            .into_iter()
            .collect()
    }

    fn recipient_metadata(&self, address: &str) -> BTreeMap<String, String> {
        self.reply_key()
            .map(|key| (key.to_owned(), address.to_owned()))
            .into_iter()
            .collect()
    }

    fn recipient_address(&self, binding: &ChannelBinding) -> Option<String> {
        let held = binding.metadata.get(self.reply_key()?)?.as_str()?;
        (!held.trim().is_empty()).then(|| self.normalize_address(held))
    }

    fn address_config(&self) -> (roomkit::ChannelAddressing, Option<roomkit::CountryCode>) {
        let config = &self.declaration;
        let addressing = roomkit::ChannelAddressing {
            addressing: config.addressing,
            recipient_key: config.recipient_key.clone(),
            chat_key: config.chat_key.clone(),
            replies_to_sender: false,
        };
        (addressing, config.country_code.clone())
    }

    async fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> Result<RoomEvent, ChannelError> {
        let room_id = context.room.id.to_string();
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
    /// end, or until the delta its reaction's `stop_after` names.
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
            held.remove("fail");
        }
        perform(&run, &held, &Scope::in_room(&room_id)).await?;
        let stop_after = reaction.get("stop_after").and_then(Value::as_str);
        let channel_id = &self.declaration.channel_id;
        while let Some(item) = stream.next().await {
            match item {
                Ok(StreamItem::Text(text)) => {
                    run.record_streamed(&room_id, channel_id, json!({ "text": text }));
                    if stop_after == Some(text.as_str()) {
                        break;
                    }
                }
                Ok(StreamItem::Row(row)) => {
                    let row = serde_json::to_value(&*row).unwrap_or_default();
                    run.record_streamed(&room_id, channel_id, json!({ "row": row }));
                }
                Ok(_) => {}
                Err(error) => {
                    run.record_streamed(&room_id, channel_id, json!({ "failed": true }));
                    return Err(error);
                }
            }
        }
        match reaction.get("fail").and_then(Value::as_str) {
            Some(message) => Err(ScriptedFailure(message.to_owned()).into()),
            None => Ok(ChannelOutput::default()),
        }
    }
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
    let name = spec["name"].as_str().unwrap_or_default().to_owned();
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
            run.record_hook(&name, &trigger, &room_id, None);
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
    let name = spec["name"].as_str().unwrap_or_default().to_owned();
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

pub(crate) fn hook_handler(
    spec: &Value,
    run: &Arc<Run>,
) -> impl Fn(
    RoomEvent,
    RoomContext,
) -> std::pin::Pin<Box<dyn Future<Output = Result<HookResult, HookError>> + Send>>
+ Send
+ Sync
+ 'static {
    let reactions = Arc::new(Reactions::new(spec.get("reactions")));
    let name = spec["name"].as_str().unwrap_or_default().to_owned();
    let trigger = spec["trigger"].as_str().unwrap_or_default().to_owned();
    let run = Arc::downgrade(run);
    move |payload: RoomEvent, context: RoomContext| {
        let reactions = Arc::clone(&reactions);
        let (name, trigger, run) = (name.clone(), trigger.clone(), Weak::clone(&run));
        Box::pin(async move {
            let Some(run) = run.upgrade() else {
                return Ok(HookResult::allow(AllowOptions::default()));
            };
            let room_id = context.room.id.to_string();
            run.record_hook(&name, &trigger, &room_id, Some(&payload));
            let Some(reaction) = reactions.pick(&event_subject(&payload, &room_id)) else {
                return Ok(HookResult::allow(AllowOptions::default()));
            };
            let scope = Scope::with_event(&room_id, payload.id.as_ref());
            perform(&run, &reaction, &scope).await?;
            Ok(hook_result(reaction.get("result"), &payload, &room_id)?)
        })
    }
}
