//! One run of one scenario: the kit, what the run records of it, and the
//! calls the steps and the scripted actions make into it, their references
//! resolved (roomkit-specs `conformance/README.md`, "Steps", "Calls").

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use roomkit::model::{ChannelId, RoomEvent, RoomId};
use roomkit::{ConversationStore, FrameworkEvent, RoomKit};
use serde_json::{Map, Value, json};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::calls;
use crate::harness::literal_rooms;
use crate::normalize::{Observed, is_lifecycle_record};
use crate::scripted::Gates;

/// A call that has not returned by then fails the scenario instead of
/// hanging it; so does a kit that does not go idle.
const CALL_BOUND: Duration = Duration::from_secs(10);

/// The framework events of the RFC §26.1 table, and Processing timeout's
/// (RFC §8.2).
const LEVEL_0_EVENTS: [&str; 19] = [
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
}

impl Scope {
    pub(crate) fn in_room(room: &str) -> Self {
        Self {
            room: Some(room.to_owned()),
            event: None,
        }
    }

    pub(crate) fn with_event(room: &str, event: &str) -> Self {
        Self {
            room: Some(room.to_owned()),
            event: Some(event.to_owned()),
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
    kit: RoomKit,
    /// The labels whose outcomes the scenario's `expect.actions` checks.
    asserted_actions: BTreeSet<String>,
    pub(crate) gates: Gates,
    pub(crate) recording: AtomicBool,
    pub(crate) observed: Mutex<Observed>,
    labels: Mutex<HashMap<String, Option<String>>>,
    pub(crate) checks: Mutex<Vec<Check>>,
    pub(crate) problems: Mutex<Vec<String>>,
    events: Mutex<broadcast::Receiver<FrameworkEvent>>,
    pub(crate) background: Mutex<HashMap<String, JoinHandle<Value>>>,
}

impl Run {
    /// A run of `scenario` on `kit`, recording nothing yet.
    pub(crate) fn new(kit: RoomKit, scenario: &Value) -> Self {
        let events = kit.subscribe();
        let asserted_actions = scenario["expect"]["actions"]
            .as_object()
            .map(|actions| actions.keys().cloned().collect())
            .unwrap_or_default();
        Self {
            kit,
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
            events: Mutex::new(events),
            background: Mutex::default(),
        }
    }

    pub(crate) fn kit(&self) -> &RoomKit {
        &self.kit
    }

    pub(crate) fn problem(&self, problem: String) {
        lock(&self.problems).push(problem);
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

    pub(crate) fn record_hook(
        &self,
        name: &str,
        trigger: &str,
        room_id: &str,
        payload: &RoomEvent,
    ) {
        if !self.recording.load(Ordering::SeqCst) {
            return;
        }
        let call = json!({
            "trigger": trigger,
            "room_id": room_id,
            "event": serde_json::to_value(payload).unwrap_or_default(),
        });
        let mut observed = lock(&self.observed);
        observed
            .hooks
            .entry(name.to_owned())
            .or_default()
            .push(call);
        observed.hook_order.push(name.to_owned());
    }

    /// Reads the framework events emitted so far.
    fn drain_events(&self) {
        let mut receiver = lock(&self.events);
        loop {
            match receiver.try_recv() {
                Ok(event) => self.on_framework_event(&event),
                Err(broadcast::error::TryRecvError::Lagged(missed)) => {
                    self.problem(format!("{missed} framework events were missed"));
                }
                Err(_) => break,
            }
        }
    }

    fn on_framework_event(&self, event: &FrameworkEvent) {
        let flat = serde_json::to_value(event).unwrap_or_default();
        let mut observed = lock(&self.observed);
        if flat["event"] == "room_created"
            && let Some(room) = flat["room_id"].as_str()
        {
            observed.created_rooms.push(room.to_owned());
        }
        let level_0 = flat["event"]
            .as_str()
            .is_some_and(|name| LEVEL_0_EVENTS.contains(&name));
        if self.recording.load(Ordering::SeqCst) && level_0 {
            observed.framework_events.push(flat);
        }
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

    fn resolve<'a>(&'a self, value: Value, scope: &'a Scope) -> BoxFuture<'a, Value> {
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
                _ => {}
            }
            self.drain_events();
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
            .list_events(&room_id, true)
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
                self.kit.idle().await;
                let running = lock(&self.background)
                    .values()
                    .any(|handle| !handle.is_finished());
                if !running {
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
