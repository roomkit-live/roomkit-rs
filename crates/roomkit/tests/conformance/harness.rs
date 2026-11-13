//! Runs one conformance scenario on the tokio runtime and compares what it
//! did (roomkit-specs `conformance/README.md`): the setup is made before
//! anything is recorded, the steps run in order, the kit is left to go idle,
//! and what the run produced is normalized and matched against the
//! scenario's expectations.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use roomkit::model::{ChannelId, ChannelType, RoomId};
use roomkit::realtime::{
    EphemeralCallback, EphemeralEvent, EphemeralEventType, InMemoryRealtime, RealtimeBackend,
    RealtimeFuture,
};
use roomkit::{
    ConversationStore, InboundRoomRouter, RoomKitOptions, RouteOptions, RouterFuture,
    SubscribeRoomOptions,
};
use serde_json::{Value, json};
use tokio::task::JoinHandle;

use crate::calls;
use crate::compare::compare;
use crate::identity::{self, RunSlot};
use crate::normalize::Observed;
use crate::run::{Check, Run, Scope, lock};

/// A router answering every routed message with one room (README,
/// `kit.router`), once held at its gate when it has one.
struct ScriptedRouter {
    room: Option<RoomId>,
    gate: Option<String>,
    run: RunSlot,
}

impl InboundRoomRouter for ScriptedRouter {
    fn route<'a>(
        &'a self,
        channel_id: &'a ChannelId,
        channel_type: &'a ChannelType,
        options: RouteOptions,
    ) -> RouterFuture<'a> {
        let room = self.room.clone();
        Box::pin(async move {
            if let Some(run) = self
                .run
                .run()
                .filter(|run| run.recording.load(Ordering::SeqCst))
            {
                lock(&run.observed).router_calls.push(json!({
                    "channel_id": channel_id,
                    "channel_type": channel_type,
                    "participant_id": options.participant_id,
                    "channel_data": options.channel_data,
                    "organization_id": options.organization_id,
                    "identity_id": options.identity.map(|identity| identity.id),
                }));
            }
            if let (Some(gate), Some(run)) = (&self.gate, self.run.run()) {
                run.gates.hold(gate).await;
            }
            Ok(room)
        })
    }
}

/// The in-memory delivery backend a scenario's `setup.kit` names (README,
/// "Setup"), its bounds in milliseconds and counts.
fn delivery_backend(kit: &Value) -> Option<roomkit::InMemoryDeliveryBackend> {
    let spec = kit.get("delivery_backend")?;
    let defaults = roomkit::InMemoryDeliveryBackendOptions::default();
    let millis = |key: &str| spec[key].as_u64().map(Duration::from_millis);
    let count = |key: &str| spec[key].as_u64().and_then(|n| usize::try_from(n).ok());
    let options = roomkit::InMemoryDeliveryBackendOptions {
        retry_delay: millis("retry_delay_ms").unwrap_or(defaults.retry_delay),
        max_retry_delay: millis("max_retry_delay_ms").unwrap_or(defaults.max_retry_delay),
        max_queue_size: count("max_queue_size").unwrap_or(defaults.max_queue_size),
        max_dead_letter_size: count("max_dead_letter_size")
            .unwrap_or(defaults.max_dead_letter_size),
    };
    roomkit::InMemoryDeliveryBackend::new(options).ok()
}

/// The kit's options a scenario's setup names, over `options`: its
/// chain-depth limit, its `process_timeout`, its router, its inbound rate
/// limit and its persistence policy.
fn kit_options<S>(
    mut options: RoomKitOptions<S>,
    kit: &Value,
    slot: &RunSlot,
) -> RoomKitOptions<S> {
    if let Some(router) = kit.get("router") {
        // A malformed answer is a broken fixture, never the default router.
        let answer = router.get("room_id").unwrap();
        let room: Option<RoomId> = serde_json::from_value(answer.clone()).unwrap();
        options.inbound_router = Some(Arc::new(ScriptedRouter {
            room,
            gate: kit["router"]["gate"].as_str().map(str::to_owned),
            run: slot.clone(),
        }));
    }
    if let Some(depth) = kit["max_chain_depth"].as_u64() {
        options.max_chain_depth = u32::try_from(depth).unwrap_or(u32::MAX);
    }
    if let Some(timeout) = kit["process_timeout_ms"].as_u64() {
        options.process_timeout = Duration::from_millis(timeout);
    }
    if let Some(limit) = kit.get("inbound_rate_limit") {
        // A limit the model refuses is a broken fixture, as Python's runner
        // raises on it.
        options.inbound_rate_limit = Some(serde_json::from_value(limit.clone()).unwrap());
    }
    if let Some(policy) = kit.get("persistence_policy") {
        // A policy naming a type the model lacks is a broken fixture, as
        // Python's runner raises on it.
        options.persistence_policy = Some(serde_json::from_value(policy.clone()).unwrap());
    }
    options
}

/// Per room, the answer's text its realtime was handed.
type TextEvents = Arc<Mutex<BTreeMap<String, Vec<Value>>>>;

/// The answer's text a kit's realtime is handed, a room minted during the
/// scenario included, which nothing could subscribe to ahead (README:
/// `text_events`).
struct TextRecorder {
    realtime: InMemoryRealtime,
    recorded: TextEvents,
}

impl RealtimeBackend for TextRecorder {
    fn publish<'a>(&'a self, channel: &'a str, event: EphemeralEvent) -> RealtimeFuture<'a, ()> {
        if event.r#type == EphemeralEventType::TextDelta {
            lock(&self.recorded)
                .entry(event.room_id.to_string())
                .or_default()
                .push(Value::Object(event.data.clone()));
        }
        self.realtime.publish(channel, event)
    }

    fn subscribe<'a>(
        &'a self,
        channel: &'a str,
        callback: EphemeralCallback,
    ) -> RealtimeFuture<'a, String> {
        self.realtime.subscribe(channel, callback)
    }

    fn unsubscribe<'a>(&'a self, subscription_id: &'a str) -> RealtimeFuture<'a, bool> {
        self.realtime.unsubscribe(subscription_id)
    }
}

/// Runs `scenario`; returns every mismatch with its expectations.
pub(crate) async fn run(scenario: &Value) -> Vec<String> {
    // The store's file, when it has one, goes once the run is over; another
    // process opens it too.
    let setup = scenario.get("setup").cloned().unwrap_or_default();
    let slot = RunSlot::default();
    let backend = delivery_backend(&setup["kit"]);
    let queue = backend.clone();
    let (strategies, default) = match crate::strategy_calls::strategies(&setup, &slot) {
        Ok(read) => read,
        Err(problem) => return vec![format!("setup: {problem}")],
    };
    let mut shared_store = None;
    let text_events = TextEvents::default();
    let (kit, store_file) = crate::new_kit(|mut options| {
        shared_store = Some(Arc::clone(&options.store));
        if scenario["expect"].get("text_events").is_some() {
            options.realtime = Some(Arc::new(TextRecorder {
                realtime: InMemoryRealtime::default(),
                recorded: Arc::clone(&text_events),
            }));
        }
        let options = identity::kit_options(
            kit_options(options, &setup["kit"], &slot),
            &setup["kit"],
            &slot,
        );
        let options = crate::verification_calls::kit_options(options, &setup["kit"]);
        let options = crate::media::kit_options(options, &setup["kit"], &slot);
        let mut options = crate::locks::kit_options(options, &setup["kit"]);
        if let Some(backend) = queue {
            options.delivery_backend = Some(Arc::new(backend));
        }
        options.orchestration = default;
        options
    })
    .await;
    let Some(store) = shared_store else {
        return vec!["setup: the kit took no store".to_owned()];
    };
    let snapshot_store = crate::observer_store(&store, &store_file).await;
    let run = Arc::new(Run::new(kit, store, snapshot_store, scenario));
    slot.set(&run);
    *lock(&run.backend) = backend;
    *lock(&run.strategies) = strategies;
    if let Err(problem) = set_up(&run, &setup).await {
        return vec![format!("setup: {problem}")];
    }
    for process in setup["processes"].as_array().into_iter().flatten() {
        let id = process["id"].as_str().unwrap_or_default().to_owned();
        let kit = crate::process_kit(&run.store(), &store_file, run.kit().lock_manager()).await;
        for channel in setup["channels"].as_array().into_iter().flatten() {
            if let Err(problem) = calls::register_process_channel(&run, &kit, channel) {
                return vec![format!("setup: process {id}: {problem}")];
            }
        }
        run.add_process(id, kit);
    }
    if records_ephemerals(scenario) {
        for room in setup["rooms"].as_array().into_iter().flatten() {
            let room = room["id"].as_str().unwrap_or_default();
            if let Err(problem) = subscribe_thinking(&run, room).await {
                return vec![format!("setup: {problem}")];
            }
        }
    }
    run.recording.store(true, Ordering::SeqCst);
    let mut step_outcomes = Vec::new();
    for (position, step) in scenario["steps"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
    {
        step_outcomes.extend(run_step(&run, step, &format!("steps[{position}]")).await);
    }
    run.idle().await;
    if records_ephemerals(scenario) {
        // The realtime backend's own tasks hand what it was published.
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }
    let mut observed = observe(&run, step_outcomes).await;
    for kit in run.open_kits() {
        kit.close().await.unwrap();
    }
    run.observer_store().close().await;
    observed.text_events = std::mem::take(&mut *lock(&text_events));
    let mut found = lock(&run.problems).clone();
    found.extend(compare(&run, scenario, &observed));
    found
}

async fn set_up(run: &Arc<Run>, setup: &Value) -> Result<(), String> {
    run.auditors.set_up(&setup["session_auditor"], run.kit())?;
    for channel in setup["channels"].as_array().into_iter().flatten() {
        calls::register_channel(run, channel)?
            .map_err(|error| format!("register {channel}: {error}"))?;
    }
    crate::member_calls::store_identities(run, &setup["identities"]).await?;
    for room in setup["rooms"].as_array().into_iter().flatten() {
        if let Err(error) = calls::create_room(run, room, None).await? {
            return Err(format!("create {room}: {error}"));
        }
        calls::patch_store_metadata(run, room).await?;
        for binding in room["bindings"].as_array().into_iter().flatten() {
            calls::attach_binding(run, room, binding).await?;
        }
        for participant in room["participants"].as_array().into_iter().flatten() {
            calls::add_participant(run, room, participant).await?;
        }
        crate::room_calls::seed_status(run, room).await?;
    }
    for hook in setup["hooks"].as_array().into_iter().flatten() {
        calls::register_hook(run, hook)?.map_err(|error| format!("register {hook}: {error}"))?;
    }
    for hook in setup["identity_hooks"].as_array().into_iter().flatten() {
        identity::register_identity_hook(run, hook)
            .map_err(|error| format!("register {hook}: {error}"))?;
    }
    // Entered, as Python's `async with` starts its delivery backend.
    run.kit()
        .start()
        .await
        .map_err(|error| format!("start: {error}"))?;
    run.idle().await;
    Ok(())
}

/// Runs `work`, telling `started` once its first poll returned: it ran to
/// its first wait, or to its end.
async fn first_polled<T>(
    work: impl Future<Output = T>,
    started: tokio::sync::oneshot::Sender<()>,
) -> T {
    let mut work = std::pin::pin!(work);
    let mut started = Some(started);
    std::future::poll_fn(move |cx| {
        let polled = work.as_mut().poll(cx);
        if let Some(started) = started.take() {
            let _ = started.send(());
        }
        polled
    })
    .await
}

/// Runs one step; returns the outcomes of the calls it made, in order.
async fn run_step(run: &Arc<Run>, step: &Value, path: &str) -> Vec<Value> {
    let mut outcomes = Vec::new();
    let mut check = |path: String, expect: Option<&Value>, outcome: Value| {
        outcomes.push(outcome.clone());
        lock(&run.checks).push(Check {
            path,
            expect: expect.cloned(),
            outcome,
        });
    };
    if step.get("call").is_some() {
        if step.get("background") == Some(&Value::Bool(true)) {
            let label = step["as"].as_str().unwrap_or_default().to_owned();
            let (started, starting) = tokio::sync::oneshot::channel();
            let handle = tokio::spawn(first_polled(
                run.invoke(step.clone(), Scope::default()),
                started,
            ));
            lock(&run.background).insert(label, handle);
            // The call runs to its first wait before the next step (README,
            // "Steps"), as a task Python's loop started runs its first step
            // before what the next step wakes.
            let _ = starting.await;
            return outcomes;
        }
        let outcome = run.invoke(step.clone(), Scope::default()).await;
        check(path.to_owned(), step.get("expect"), outcome);
    } else if let Some(label) = step["await"].as_str() {
        let handle = lock(&run.background).remove(label);
        let outcome = match handle {
            Some(handle) => handle
                .await
                .unwrap_or_else(|_| json!({ "result": null, "error": { "name": "<panicked>" } })),
            None => json!({ "result": null, "error": { "name": "<no such call>" } }),
        };
        check(path.to_owned(), step.get("expect"), outcome);
    } else if let Some(calls) = step["concurrent"].as_array() {
        let handles: Vec<JoinHandle<Value>> = calls
            .iter()
            .map(|call| tokio::spawn(run.invoke(call.clone(), Scope::default())))
            .collect();
        for (position, (call, handle)) in calls.iter().zip(handles).enumerate() {
            let outcome = handle
                .await
                .unwrap_or_else(|_| json!({ "result": null, "error": { "name": "<panicked>" } }));
            check(
                format!("{path}.concurrent[{position}]"),
                call.get("expect"),
                outcome,
            );
        }
    } else if let Some(gate) = step["wait_for"].as_str() {
        if !run.gates.wait_for(gate).await {
            run.problem(format!("{path}: no call reached the gate {gate}"));
        }
    } else if step["wait_for_lock"].is_string() {
        let room = run
            .resolve(step["wait_for_lock"].clone(), &Scope::default())
            .await;
        let room_id = room.as_str().unwrap_or_default();
        let waiters = step["waiters"]
            .as_u64()
            .map_or(1, |waiters| usize::try_from(waiters).unwrap_or(usize::MAX));
        if !wait_for_lock(run, room_id, waiters).await {
            run.problem(format!(
                "{path}: {waiters} calls never waited for {room_id}'s lock"
            ));
        }
    } else if let Some(releases) = step["lock_releases"].as_str() {
        if !crate::locks::set_lock_releases(run.kit(), releases) {
            run.problem(format!(
                "{path}: a lock_releases step needs setup.kit.lock_manager"
            ));
        }
    } else if let Some(gate) = step["open"].as_str() {
        run.gates.open(gate);
    } else if let Some(channel) = step["acp_disconnect"].as_str() {
        let pipe = lock(&run.acp_pipes).get(channel).cloned();
        if let Some(pipe) = pipe {
            pipe.close().await;
        } else {
            run.problem(format!("{path}: no scripted ACP transport for {channel}"));
        }
    } else if step.get("idle").is_some() {
        run.idle().await;
    }
    outcomes
}

/// Waits until `waiters` calls wait for the lock of the room `room_id`:
/// whether they did within the gates' bound.
async fn wait_for_lock(run: &Arc<Run>, room_id: &str, waiters: usize) -> bool {
    let Ok(room_id) = RoomId::new(room_id) else {
        return false;
    };
    let kit = run.kit();
    let waited = async {
        while roomkit::testing::room_lock_waiters(kit, &room_id) < waiters {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(crate::scripted::GATE_BOUND, waited)
        .await
        .is_ok()
}

/// Whether the scenario expects what a room's subscribers read of a turn's
/// reasoning or of its tool calls (README: `thinking_events`, `tool_events`).
fn records_ephemerals(scenario: &Value) -> bool {
    ["thinking_events", "tool_events"]
        .iter()
        .any(|kind| scenario["expect"].get(kind).is_some())
}

/// Records the room's thinking and tool call events, for a scenario that
/// expects them.
async fn subscribe_thinking(run: &Arc<Run>, room: &str) -> Result<(), String> {
    let id = RoomId::new(room).map_err(|e| e.to_string())?;
    let (recorder, room) = (Arc::downgrade(run), room.to_owned());
    let record = move |event: EphemeralEvent| {
        let (recorder, room) = (Weak::clone(&recorder), room.clone());
        async move {
            let tool = matches!(
                event.r#type,
                EphemeralEventType::ToolCallStart
                    | EphemeralEventType::ToolCallEnd
                    | EphemeralEventType::ToolCallDelta
            );
            let thinking = matches!(
                event.r#type,
                EphemeralEventType::ThinkingStart
                    | EphemeralEventType::ThinkingDelta
                    | EphemeralEventType::ThinkingEnd
            );
            let Some(run) = recorder.upgrade().filter(|_| thinking || tool) else {
                return;
            };
            let mut read = event.data;
            read.insert("type".to_owned(), json!(event.r#type));
            if tool {
                run.record_tool_event(&room, Value::Object(read));
            } else {
                run.record_thinking_event(&room, Value::Object(read));
            }
        }
    };
    let options = SubscribeRoomOptions::default();
    let subscribed = run.kit().subscribe_room(&id, record, options).await;
    subscribed.map(drop).map_err(|e| e.to_string())
}

async fn observe(run: &Arc<Run>, step_outcomes: Vec<Value>) -> Observed {
    let store = run.observer_store();
    let rooms = crate::room_queries::all_rooms(run).await.unwrap();
    let mut timelines = Vec::new();
    for room in &rooms {
        timelines.push((room.id.to_string(), run.timeline(room.id.as_str()).await));
    }
    let mut observed = std::mem::take(&mut *lock(&run.observed));
    observed.steps = step_outcomes;
    let mut problems = Vec::new();
    let models = lock(&run.models).clone();
    let dialects = lock(&run.chat_dialects).clone();
    for (channel, model) in &models {
        let mut requests = match dialects.get(channel) {
            None => crate::agents::requests(model),
            Some(dialect) => crate::agents::requests_on_the_wire(model, dialect)
                .await
                .unwrap_or_else(|problem| {
                    problems.push(format!("models.{channel}: {problem}"));
                    Vec::new()
                }),
        };
        if let Some(methods) = lock(&run.model_methods).get(channel) {
            for (request, method) in requests.iter_mut().zip(lock(methods).iter()) {
                request["method"] = json!(method);
            }
        }
        observed.models.insert(channel.clone(), requests);
    }
    for (channel, pipe) in lock(&run.acp_pipes).iter() {
        observed
            .acp_prompts
            .insert(channel.clone(), lock(&pipe.attempts).clone());
    }
    collect_acp_permissions(run, &mut observed);
    collect_acp_mcp(run, &mut observed);
    for (channel, wire) in lock(&run.acp_wires).iter() {
        let received = lock(wire).clone();
        observed.acp_wire.insert(channel.clone(), received);
    }
    (observed.tool_audit, observed.session_audit) = run.auditors.observe(&mut problems);
    lock(&run.problems).extend(problems);
    for ((room_key, timeline), room) in timelines.into_iter().zip(&rooms) {
        observed.timelines.insert(room_key.clone(), timeline);
        observed.rooms.insert(
            room_key.clone(),
            serde_json::to_value(room).unwrap_or_default(),
        );
        let bindings = store.list_bindings(&room.id).await.unwrap();
        observed.bindings.insert(
            room_key.clone(),
            bindings
                .iter()
                .map(|binding| {
                    (
                        binding.channel_id.to_string(),
                        serde_json::to_value(binding).unwrap_or_default(),
                    )
                })
                .collect(),
        );
        let observations = store.list_observations(&room.id).await.unwrap();
        observed.observations.insert(
            room_key.clone(),
            observations
                .iter()
                .map(|o| serde_json::to_value(o).unwrap_or_default())
                .collect(),
        );
        let participants = store.list_participants(&room.id).await.unwrap();
        observed.participants.insert(
            room_key.clone(),
            participants
                .iter()
                .map(|p| serde_json::to_value(p).unwrap_or_default())
                .collect(),
        );
        let tasks = store
            .list_tasks(&room.id, roomkit::store::ListTasksOptions::default())
            .await
            .unwrap();
        observed.tasks.insert(
            room_key,
            tasks
                .iter()
                .map(|t| serde_json::to_value(t).unwrap_or_default())
                .collect(),
        );
    }
    observed
}

/// The room ids a scenario writes itself; any other room a run creates is
/// aliased.
pub(crate) fn literal_rooms(scenario: &Value) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    for room in scenario["setup"]["rooms"].as_array().into_iter().flatten() {
        if let Some(id) = room["id"].as_str() {
            found.insert(id.to_owned());
        }
    }
    for part in [
        &scenario["steps"],
        &scenario["setup"]["hooks"],
        &scenario["setup"]["channels"],
    ] {
        walk_rooms(part, &mut found);
    }
    found
}

fn walk_rooms(value: &Value, found: &mut BTreeSet<String>) {
    match value {
        Value::Object(object) => {
            for (key, item) in object {
                if key == "room_id"
                    && let Some(room) = item.as_str()
                    && !room.starts_with('$')
                {
                    found.insert(room.to_owned());
                }
                if let (true, Some(room)) = (key == "room", item.get("id").and_then(Value::as_str))
                {
                    found.insert(room.to_owned());
                }
                walk_rooms(item, found);
            }
        }
        Value::Array(items) => items.iter().for_each(|item| walk_rooms(item, found)),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn collect_acp_permissions(run: &Run, observed: &mut Observed) {
    for (channel, pipe) in lock(&run.acp_pipes).iter() {
        observed
            .acp_permissions
            .insert(channel.clone(), lock(&pipe.permissions.received).clone());
    }
}

fn collect_acp_mcp(run: &Run, observed: &mut Observed) {
    for (channel, requests) in lock(&run.acp_mcp_requests).iter() {
        observed
            .acp_mcp_requests
            .insert(channel.clone(), lock(requests).clone());
    }
}
