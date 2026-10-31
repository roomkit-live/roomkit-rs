//! The scripted parts of a scenario: gates, reactions, channels and hooks
//! (roomkit-specs `conformance/README.md`, "Scripted channels", "Scripted
//! hooks"), built on the tokio runtime.
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelDirection, ChannelId,
    ChannelOutput, ChannelType, HookResult, InboundMessage, RoomContext, RoomEvent,
};
use roomkit::{
    Channel, ChannelDeclaration, ChannelError, DeliveryStream, EventDraft, HookError,
    SessionStartedEvent, StreamItem, default_inbound_event,
};
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

/// A channel whose parse, turn and delivery follow the scenario's reactions.
pub(crate) struct ScriptedChannel {
    declaration: ChannelDeclaration,
    run: Weak<Run>,
    parse: Reactions,
    on_event: Reactions,
    deliver: Reactions,
    deliver_stream: Reactions,
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
        let mut declaration = ChannelDeclaration::new(id, channel_type, category, direction);
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
        })
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
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    async fn handle_inbound(
        &self,
        message: InboundMessage,
        _context: RoomContext,
        draft: EventDraft,
    ) -> Result<RoomEvent, ChannelError> {
        let room_id = draft.room_id.to_string();
        if let (Some(run), Some(reaction)) = (
            self.run.upgrade(),
            self.parse.pick(&message_subject(&message, &room_id)),
        ) {
            perform(&run, &reaction, &Scope::in_room(&room_id)).await?;
        }
        Ok(default_inbound_event(
            &message,
            &self.declaration,
            draft.room_id,
            draft.id,
            draft.now,
        ))
    }

    async fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        if self.declaration.category == ChannelCategory::Intelligence
            && let Some(run) = self.run.upgrade()
        {
            run.record("turns", &event, &self.declaration.channel_id);
        }
        self.react(&self.on_event, &event, true).await
    }

    async fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        if let Some(run) = self.run.upgrade() {
            run.record("deliveries", &event, &self.declaration.channel_id);
        }
        self.react(&self.deliver, &event, false).await
    }

    /// Reads the stream it is handed, recording each delta and row, to its
    /// end, or until the delta its reaction's `stop_after` names.
    async fn deliver_stream(
        &self,
        mut stream: DeliveryStream,
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
