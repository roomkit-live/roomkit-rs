//! One run of one scenario: the kit, what the run records of it, and the
//! calls the steps and the scripted actions make into it, their references
//! resolved (roomkit-specs `conformance/README.md`, "Steps", "Calls").

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use roomkit::model::{ChannelBinding, ChannelId, EventId, RoomContext, RoomEvent, RoomId};
use roomkit::testing::EventTap;
use roomkit::{ConversationStore, FrameworkEvent};
use roomkit_ai::testing::ScriptedProvider;
use serde_json::{Map, Value, json};
use tokio::task::JoinHandle;

use crate::calls;
use crate::harness::literal_rooms;
use crate::normalize::{Observed, is_lifecycle_record};
use crate::scripted::{GATE_BOUND, Gates};

/// A call that has not returned by then fails the scenario instead of
/// hanging it; so does a kit that does not go idle.
const CALL_BOUND: Duration = Duration::from_secs(10);

/// The framework events of the RFC §26.1 table, and Processing timeout's
/// (RFC §8.2).
const LEVEL_0_EVENTS: [&str; 21] = [
    "room_created",
    "room_paused",
    "room_closed",
    "room_archived",
    "room_refused_event",
    "channel_registered",
    "channel_unregistered",
    "room_channel_attached",
    "room_channel_detached",
    "event_processed",
    "event_blocked",
    "delivery_succeeded",
    "delivery_failed",
    "broadcast_partial_failure",
    "chain_depth_exceeded",
    "hook_error",
    "hook_timeout",
    "session_started",
    "process_timeout",
    // Identity resolution's (Level 1, RFC §8.2).
    "identity_resolved",
    "identity_timeout",
];

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Where a scripted action runs: the room in its context, and the event it
/// runs for, which `$room` and `$event` name.
#[derive(Clone, Debug, Default)]
pub(crate) struct Scope {
    room: Option<String>,
    event: Option<String>,
    /// The human-input request a hook on `on_user_input_required` was
    /// handed, which `$request` names.
    request: Option<String>,
}

impl Scope {
    pub(crate) fn in_room(room: &str) -> Self {
        Self {
            room: Some(room.to_owned()),
            event: None,
            request: None,
        }
    }

    pub(crate) fn with_event(room: &str, event: &str) -> Self {
        Self {
            room: Some(room.to_owned()),
            event: Some(event.to_owned()),
            request: None,
        }
    }

    pub(crate) fn with_request(room: &str, request: &str) -> Self {
        Self {
            room: Some(room.to_owned()),
            event: None,
            request: Some(request.to_owned()),
        }
    }
}

/// A check of an outcome against its expectation, made once the run is over.
pub(crate) struct Check {
    pub(crate) path: String,
    pub(crate) expect: Option<Value>,
    pub(crate) outcome: Value,
}

/// One run of one scenario.
pub(crate) struct Run {
    kit: crate::Kit,
    /// What each discussion's `done` answers, by its strategy's id.
    discussion_done: Mutex<HashMap<String, bool>>,
    /// The kit's store, shared: what a router of the run's own is built over.
    store: Arc<crate::Store>,
    /// The labels whose outcomes the scenario's `expect.actions` checks.
    asserted_actions: BTreeSet<String>,
    pub(crate) gates: Gates,
    pub(crate) recording: AtomicBool,
    pub(crate) observed: Mutex<Observed>,
    labels: Mutex<HashMap<String, Option<String>>>,
    pub(crate) checks: Mutex<Vec<Check>>,
    pub(crate) problems: Mutex<Vec<String>>,
    /// The framework events of `LEVEL_0_EVENTS` the kit emitted, and how
    /// many of them were read.
    events: EventTap,
    events_read: Mutex<usize>,
    pub(crate) background: Mutex<HashMap<String, JoinHandle<Value>>>,
    /// The tasks `delegate` returned, by their id.
    pub(crate) tasks: Mutex<HashMap<String, roomkit::tasks::DelegatedTask>>,
    /// The completion handles of deferred calls, by their call's label.
    pub(crate) handles: Mutex<HashMap<String, roomkit::DeliveryHandle>>,
    /// The work scripted turns left for after them (`after_turn`).
    after_turn: Mutex<Vec<JoinHandle<()>>>,
    /// The scripted agents' models, by channel.
    pub(crate) models: Mutex<BTreeMap<String, ScriptedProvider>>,
    /// The Chat Completions rendering of a model that names one, by the
    /// model's name (README, `model.chat_dialect`).
    pub(crate) chat_dialects: Mutex<BTreeMap<String, String>>,
    /// The in-memory delivery backend the kit was built with, whose queue
    /// and worker the idle wait waits for (README, "Setup").
    pub(crate) backend: Mutex<Option<roomkit::InMemoryDeliveryBackend>>,
    /// The scenario's scripted strategies, by id (README, "Setup").
    pub(crate) strategies: Mutex<HashMap<String, crate::strategy_calls::Strategy>>,
    /// The handoff handlers the run built, by the name its calls give them.
    pub(crate) handoff_handlers: Mutex<HashMap<String, roomkit::HandoffHandler<crate::Store>>>,
    pub(crate) delegate_handlers:
        Mutex<HashMap<String, roomkit::tasks::DelegateHandler<crate::Store>>>,
    /// The run's tool and session auditors (README, "Setup").
    pub(crate) auditors: crate::audits::Auditors,
    /// The directories of the scripted agents' skills.
    pub(crate) skills: crate::skills::SkillDirectories,
}

impl Run {
    /// What the discussion of the strategy `id`'s `done` answers now.
    pub(crate) fn discussion_done(&self, id: &str) -> bool {
        lock(&self.discussion_done)
            .get(id)
            .copied()
            .unwrap_or(false)
    }

    /// What the discussion of the strategy `id`'s `done` answers from now on.
    pub(crate) fn set_discussion_done(&self, id: String, holds: bool) {
        lock(&self.discussion_done).insert(id, holds);
    }

    /// A run of `scenario` on `kit`, recording nothing yet.
    pub(crate) fn new(kit: crate::Kit, store: Arc<crate::Store>, scenario: &Value) -> Self {
        let tapped: Vec<&str> = LEVEL_0_EVENTS
            .iter()
            .chain(&crate::task_calls::LEVEL_2_EVENTS)
            .chain(&crate::input_calls::INPUT_EVENTS)
            .chain(&crate::verification_calls::VERIFICATION_EVENTS)
            .chain(&crate::ai_hooks::AI_EVENTS)
            .chain(&crate::locks::LOCK_EVENTS)
            .copied()
            .collect();
        let events = EventTap::on(&kit, &tapped);
        let asserted_actions = scenario["expect"]["actions"]
            .as_object()
            .map(|actions| actions.keys().cloned().collect())
            .unwrap_or_default();
        Self {
            kit,
            store,
            asserted_actions,
            gates: Gates::default(),
            recording: AtomicBool::new(false),
            observed: Mutex::new(Observed {
                literal_rooms: literal_rooms(scenario),
                ..Observed::default()
            }),
            labels: Mutex::default(),
            checks: Mutex::default(),
            problems: Mutex::default(),
            events,
            events_read: Mutex::new(0),
            discussion_done: Mutex::default(),
            background: Mutex::default(),
            tasks: Mutex::default(),
            handles: Mutex::default(),
            after_turn: Mutex::default(),
            models: Mutex::default(),
            chat_dialects: Mutex::default(),
            backend: Mutex::default(),
            strategies: Mutex::default(),
            handoff_handlers: Mutex::default(),
            delegate_handlers: Mutex::default(),
            auditors: crate::audits::Auditors::default(),
            skills: crate::skills::SkillDirectories::default(),
        }
    }

    pub(crate) fn kit(&self) -> &crate::Kit {
        &self.kit
    }

    pub(crate) fn store(&self) -> Arc<crate::Store> {
        Arc::clone(&self.store)
    }

    /// Labels `value` as `label`, beside the labels the calls' `as` name:
    /// a verification request's secret.
    pub(crate) fn label(&self, label: String, value: String) {
        lock(&self.labels).insert(label, Some(value));
    }

    /// `value` with each `$<label>` a call's `as` named read as what it
    /// names: a scripted model's call arguments, as the model writes the
    /// call (README, "Scripted agents").
    pub(crate) fn labelled(&self, value: Value) -> Value {
        match value {
            Value::Object(object) => Value::Object(
                object
                    .into_iter()
                    .map(|(key, item)| (key, self.labelled(item)))
                    .collect(),
            ),
            Value::Array(items) => {
                Value::Array(items.into_iter().map(|item| self.labelled(item)).collect())
            }
            Value::String(text) => {
                let named = text
                    .strip_prefix('$')
                    .and_then(|label| lock(&self.labels).get(label).cloned().flatten());
                Value::String(named.unwrap_or(text))
            }
            scalar @ (Value::Null | Value::Bool(_) | Value::Number(_)) => scalar,
        }
    }

    pub(crate) fn problem(&self, problem: String) {
        lock(&self.problems).push(problem);
    }

    /// The context a scripted channel's `deliver` or `on_event` (`call`)
    /// was handed: the ids of its events, lifecycle records left out.
    pub(crate) fn record_context(&self, call: &str, context: &RoomContext, channel_id: &ChannelId) {
        if !self.recording.load(Ordering::SeqCst) {
            return;
        }
        let recent: Vec<Value> = context
            .recent_events
            .iter()
            .map(|event| serde_json::to_value(event).unwrap_or_default())
            .filter(|event| !is_lifecycle_record(event))
            .map(|event| event["id"].clone())
            .collect();
        lock(&self.observed)
            .contexts
            .entry(context.room.id.to_string())
            .or_default()
            .entry(channel_id.to_string())
            .or_default()
            .push(json!({ "call": call, "recent_events": recent }));
    }

    pub(crate) fn record(&self, kind: &str, event: &RoomEvent, channel_id: &ChannelId) {
        if !self.recording.load(Ordering::SeqCst) {
            return;
        }
        let mut observed = lock(&self.observed);
        let per_kind = if kind == "turns" {
            &mut observed.turns
        } else {
            &mut observed.deliveries
        };
        per_kind
            .entry(event.room_id.to_string())
            .or_default()
            .entry(channel_id.to_string())
            .or_default()
            .push(serde_json::to_value(event).unwrap_or_default());
    }

    /// One room callback a scripted channel was called with (RFC §6.1),
    /// also named in the hooks' order as `<channel_id>.<call>`.
    pub(crate) fn record_room_callback(
        &self,
        room_id: &str,
        channel_id: &ChannelId,
        call: &str,
        binding: Option<&ChannelBinding>,
    ) {
        if !self.recording.load(Ordering::SeqCst) {
            return;
        }
        let mut item = serde_json::json!({ "call": call });
        if let Some(binding) = binding {
            item["binding"] = serde_json::to_value(binding).unwrap_or_default();
        }
        let mut observed = lock(&self.observed);
        observed
            .room_callbacks
            .entry(room_id.to_owned())
            .or_default()
            .entry(channel_id.to_string())
            .or_default()
            .push(item);
        observed.hook_order.push(format!("{channel_id}.{call}"));
    }

    /// One item a streaming channel's `deliver_stream` read, as the scenario
    /// reads it: `{"text": ...}`, `{"row": event}`, or `{"failed": true}`
    /// where the stream raised.
    pub(crate) fn record_streamed(&self, room_id: &str, channel_id: &ChannelId, item: Value) {
        if !self.recording.load(Ordering::SeqCst) {
            return;
        }
        lock(&self.observed)
            .streamed
            .entry(room_id.to_owned())
            .or_default()
            .entry(channel_id.to_string())
            .or_default()
            .push(item);
    }

    /// One thinking event of `room_id`'s subscribers (README: `thinking_events`).
    pub(crate) fn record_thinking_event(&self, room_id: &str, event: Value) {
        lock(&self.observed)
            .thinking_events
            .entry(room_id.to_owned())
            .or_default()
            .push(event);
    }

    pub(crate) fn record_hook(
        &self,
        name: &str,
        trigger: &str,
        room_id: &str,
        payload: Option<&RoomEvent>,
    ) {
        if !self.recording.load(Ordering::SeqCst) {
            return;
        }
        let event = payload.map_or(Value::Null, |payload| {
            serde_json::to_value(payload).unwrap_or_default()
        });
        let call = json!({
            "trigger": trigger,
            "room_id": room_id,
            "event": event,
        });
        let mut observed = lock(&self.observed);
        observed
            .hooks
            .entry(name.to_owned())
            .or_default()
            .push(call);
        observed.hook_order.push(name.to_owned());
    }

    /// Records a call of the hook `name` whose payload is no event, a
    /// status entry or an observation, under `member` (README,
    /// "Expectations").
    /// Records a call of the hook `name` on `on_user_input_required`, its
    /// payload under `request` (README, "Expectations"): the request is
    /// the run's next `$p<n>` when it is new.
    pub(crate) fn record_request_hook(
        &self,
        name: &str,
        trigger: &str,
        room_id: &str,
        request: Value,
    ) {
        if let Some(pending_id) = request["pending_id"].as_str() {
            saw_request(&mut lock(&self.observed), pending_id);
        }
        self.record_payload_hook(name, trigger, room_id, "request", request);
    }

    pub(crate) fn record_payload_hook(
        &self,
        name: &str,
        trigger: &str,
        room_id: &str,
        member: &str,
        payload: Value,
    ) {
        if !self.recording.load(Ordering::SeqCst) {
            return;
        }
        let mut call = json!({
            "trigger": trigger,
            "room_id": room_id,
            "event": null,
        });
        call[member] = payload;
        let mut observed = lock(&self.observed);
        observed
            .hooks
            .entry(name.to_owned())
            .or_default()
            .push(call);
        observed.hook_order.push(name.to_owned());
    }

    /// Records a call of the hook `name` on a tool trigger, whose payload
    /// is the tool call `tool_call` (README, "Expectations").
    pub(crate) fn record_tool_hook(
        &self,
        name: &str,
        trigger: &str,
        room_id: &str,
        tool_call: &Value,
    ) {
        if !self.recording.load(Ordering::SeqCst) {
            return;
        }
        let call = json!({
            "trigger": trigger,
            "room_id": room_id,
            "event": null,
            "tool_call": tool_call,
        });
        let mut observed = lock(&self.observed);
        observed
            .hooks
            .entry(name.to_owned())
            .or_default()
            .push(call);
        observed.hook_order.push(name.to_owned());
    }

    /// Records a call of the hook `name` on `ON_DELIVERY_STATUS`, whose
    /// payload is the delivery status `status` (README, "Expectations").
    pub(crate) fn record_status_hook(
        &self,
        name: &str,
        trigger: &str,
        room_id: &str,
        status: &Value,
    ) {
        if !self.recording.load(Ordering::SeqCst) {
            return;
        }
        let call = json!({
            "trigger": trigger,
            "room_id": room_id,
            "event": null,
            "delivery_status": status,
        });
        let mut observed = lock(&self.observed);
        observed
            .hooks
            .entry(name.to_owned())
            .or_default()
            .push(call);
        observed.hook_order.push(name.to_owned());
    }

    /// Reads the framework events emitted since the last read.
    fn drain_events(&self) {
        let mut read = lock(&self.events_read);
        let events = self.events.events();
        for event in events.iter().skip(*read) {
            self.on_framework_event(event);
        }
        *read = events.len();
    }

    /// Records `event` as Python's harness does: the members of its data
    /// flat, its data whole too, and its ids (`_harness.py`
    /// `_on_framework_event`).
    fn on_framework_event(&self, event: &FrameworkEvent) {
        let mut observed = lock(&self.observed);
        if event.r#type == "room_created"
            && let Some(room) = &event.room_id
        {
            observed.created_rooms.push(room.as_str().to_owned());
        }
        if event.r#type == "user_input_required"
            && let Some(pending_id) = event.data.get("pending_id").and_then(Value::as_str)
        {
            saw_request(&mut observed, pending_id);
        }
        if !self.recording.load(Ordering::SeqCst) {
            return;
        }
        let mut flat = Map::new();
        flat.insert("event".to_owned(), Value::String(event.r#type.clone()));
        flat.extend(event.data.clone());
        flat.insert("data".to_owned(), Value::Object(event.data.clone()));
        let ids = [
            ("room_id", event.room_id.as_ref().map(RoomId::as_str)),
            (
                "channel_id",
                event.channel_id.as_ref().map(ChannelId::as_str),
            ),
            ("event_id", event.event_id.as_ref().map(EventId::as_str)),
        ];
        for (name, id) in ids {
            if let Some(id) = id {
                flat.insert(name.to_owned(), Value::String(id.to_owned()));
            }
        }
        if crate::ai_hooks::AI_EVENTS.contains(&event.r#type.as_str()) {
            observed.ai_events.push(Value::Object(flat));
        } else {
            observed.framework_events.push(Value::Object(flat));
        }
    }

    /// Starts the work a turn leaves for after it: a task holding the
    /// turn's context, which makes `actions` in order once the turn ended.
    pub(crate) fn leave_after_turn(self: &Arc<Self>, actions: Vec<Value>, scope: Scope) {
        let Some(host) = roomkit::current_turn_host() else {
            self.problem("an after_turn reaction outside a turn: no turn's host".to_owned());
            return;
        };
        let run = Arc::clone(self);
        let run_kit = self.kit.clone();
        let work = tokio::spawn(roomkit::keep_context(async move {
            let ended = async {
                while host.open() {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            };
            let ended = async {
                ended.await;
                // The owner reads the turn's frames on its own: work after
                // the turn is made once the kit's own work for it is over.
                roomkit::testing::idle(&run_kit).await;
            };
            if tokio::time::timeout(GATE_BOUND, ended).await.is_err() {
                run.problem(format!(
                    "a turn did not end within {GATE_BOUND:?} for its after_turn"
                ));
                return;
            }
            for action in actions {
                run.run_action(action, scope.clone()).await;
            }
        }));
        lock(&self.after_turn).push(work);
    }

    /// A call a hook or a channel makes while it runs; its outcome is kept.
    pub(crate) fn run_action(
        self: &Arc<Self>,
        action: Value,
        scope: Scope,
    ) -> BoxFuture<'static, ()> {
        let run = Arc::clone(self);
        Box::pin(async move {
            let outcome = run.invoke(action.clone(), scope).await;
            let label = action.get("as").and_then(Value::as_str);
            if let Some(label) = label {
                lock(&run.observed)
                    .actions
                    .entry(label.to_owned())
                    .or_default()
                    .push(outcome.clone());
            }
            // An action's outcome is checked by its own expect; with none, it
            // must not fail, unless the scenario's expect.actions checks its
            // label.
            let asserted = label.is_some_and(|label| run.asserted_actions.contains(label));
            if action.get("expect").is_some() || !asserted {
                let path = format!(
                    "action {}",
                    label.unwrap_or_else(|| action["call"].as_str().unwrap_or_default())
                );
                lock(&run.checks).push(Check {
                    path,
                    expect: action.get("expect").cloned(),
                    outcome,
                });
            }
        })
    }

    /// Makes one call; returns its outcome, its error named, never failing.
    pub(crate) fn invoke(self: &Arc<Self>, spec: Value, scope: Scope) -> BoxFuture<'static, Value> {
        let run = Arc::clone(self);
        Box::pin(async move {
            let resolved = run.resolve(spec.clone(), &scope).await;
            let name = spec["call"].as_str().unwrap_or_default().to_owned();
            match tokio::time::timeout(CALL_BOUND, calls::call(&run, &resolved)).await {
                Err(_) => {
                    run.problem(format!("{name} did not return within {CALL_BOUND:?}"));
                    json!({ "result": null, "error": { "name": "<hung>" } })
                }
                Ok(Err(unreadable)) => {
                    run.problem(format!(
                        "{name}: the runner cannot read the call: {unreadable}"
                    ));
                    json!({ "result": null, "error": { "name": "<unreadable>" } })
                }
                Ok(Ok((called, event_id))) => {
                    if let Some(label) = spec.get("as").and_then(Value::as_str) {
                        lock(&run.labels).insert(label.to_owned(), event_id);
                    }
                    match called {
                        Ok(result) => json!({ "result": result, "error": null }),
                        Err(error) => json!({ "result": null, "error": error }),
                    }
                }
            }
        })
    }

    pub(crate) fn resolve<'a>(&'a self, value: Value, scope: &'a Scope) -> BoxFuture<'a, Value> {
        Box::pin(async move {
            match value {
                Value::Object(object) => {
                    let mut resolved = Map::new();
                    for (key, item) in object {
                        resolved.insert(key, self.resolve(item, scope).await);
                    }
                    Value::Object(resolved)
                }
                Value::Array(items) => {
                    let mut resolved = Vec::new();
                    for item in items {
                        resolved.push(self.resolve(item, scope).await);
                    }
                    Value::Array(resolved)
                }
                Value::String(text) => Value::String(self.reference(&text, scope).await),
                scalar @ (Value::Null | Value::Bool(_) | Value::Number(_)) => scalar,
            }
        })
    }

    fn reference<'a>(&'a self, text: &'a str, scope: &'a Scope) -> BoxFuture<'a, String> {
        Box::pin(async move {
            match text {
                "$room" => return scope.room.clone().unwrap_or_default(),
                "$event" => return scope.event.clone().unwrap_or_default(),
                "$request" if scope.request.is_some() => {
                    return scope.request.clone().unwrap_or_default();
                }
                _ => {}
            }
            self.drain_events();
            if let Some(number) = text
                .strip_prefix("$p")
                .and_then(|n| n.parse::<usize>().ok())
            {
                let observed = lock(&self.observed);
                return number
                    .checked_sub(1)
                    .and_then(|position| observed.requests.get(position))
                    .map_or_else(|| text.to_owned(), Clone::clone);
            }
            if let Some(number) = text
                .strip_prefix("$r")
                .and_then(|n| n.parse::<usize>().ok())
            {
                let observed = lock(&self.observed);
                return observed
                    .generated_rooms()
                    .get(number - 1)
                    .map_or_else(|| text.to_owned(), |room| (*room).clone());
            }
            if let Some(label) = text.strip_prefix('$')
                && let Some(Some(id)) = lock(&self.labels).get(label)
            {
                return id.clone();
            }
            if let Some((room, index)) = text.rsplit_once('#')
                && let Ok(index) = index.parse::<i64>()
            {
                let room = self.reference(room, scope).await;
                if let Some(id) = self.event_at(&room, index).await {
                    return id;
                }
            }
            text.to_owned()
        })
    }

    pub(crate) async fn timeline(&self, room: &str) -> Vec<Value> {
        let Ok(room_id) = RoomId::new(room) else {
            return Vec::new();
        };
        let mut events: Vec<Value> = self
            .kit
            .store()
            .list_events(&room_id, every_row())
            .await
            .unwrap_or_default()
            .iter()
            .map(|event| serde_json::to_value(event).unwrap_or_default())
            .collect();
        events.sort_by_key(|event| event["index"].as_u64());
        events
    }

    /// The id of the room's event at `index`, read without lifecycle
    /// records.
    async fn event_at(&self, room: &str, index: i64) -> Option<String> {
        let mut records = 0;
        for event in self.timeline(room).await {
            if is_lifecycle_record(&event) {
                records += 1;
            } else if event["index"].as_i64().map(|raw| raw - records) == Some(index) {
                return event["id"].as_str().map(str::to_owned);
            }
        }
        None
    }

    pub(crate) async fn idle(&self) {
        let waiting = async {
            loop {
                roomkit::testing::idle(&self.kit).await;
                let running = lock(&self.background)
                    .values()
                    .any(|handle| !handle.is_finished())
                    || lock(&self.after_turn)
                        .iter()
                        .any(|work| !work.is_finished());
                if !running && !self.backend_busy().await {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        if tokio::time::timeout(CALL_BOUND, waiting).await.is_err() {
            self.problem(format!("the kit did not go idle within {CALL_BOUND:?}"));
        }
        self.drain_events();
    }
}

impl Run {
    /// Whether the kit's in-memory delivery backend still holds work: an
    /// item queued, waiting out its retry delay, or claimed by its worker.
    async fn backend_busy(&self) -> bool {
        let Some(backend) = lock(&self.backend).clone() else {
            return false;
        };
        let depth = roomkit::DeliveryBackend::<crate::Store>::get_queue_depth(&backend).await;
        depth.unwrap_or_default() > 0 || backend.in_flight() > 0
    }
}

/// Every row of a room's timeline, the refused ones included.
fn every_row() -> roomkit::store::ListEventsOptions {
    roomkit::store::ListEventsOptions {
        limit: u64::MAX,
        event_filter: Some(roomkit::EventFilter {
            include_blocked: true,
            ..roomkit::EventFilter::default()
        }),
        ..roomkit::store::ListEventsOptions::default()
    }
}

/// A human-input request the run shows: the next `$p<n>` when it is new.
fn saw_request(observed: &mut Observed, pending_id: &str) {
    if !observed.requests.iter().any(|seen| seen == pending_id) {
        observed.requests.push(pending_id.to_owned());
    }
}
