//! Runs one conformance scenario on the tokio runtime and compares what it
//! did (roomkit-specs `conformance/README.md`): the setup is made before
//! anything is recorded, the steps run in order, the kit is left to go idle,
//! and what the run produced is normalized and matched against the
//! scenario's expectations.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use roomkit::model::{ChannelId, ChannelType, RoomId};
use roomkit::{
    ArchiveRoomOptions, CloseRoomOptions, ConversationStore, InboundRoomRouter, RoomKitOptions,
    RouteOptions, RouterFuture,
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
    room: RoomId,
    gate: Option<String>,
    run: RunSlot,
}

impl InboundRoomRouter for ScriptedRouter {
    fn route<'a>(
        &'a self,
        _channel_id: &'a ChannelId,
        _channel_type: &'a ChannelType,
        _options: RouteOptions,
    ) -> RouterFuture<'a> {
        let room = self.room.clone();
        Box::pin(async move {
            if let (Some(gate), Some(run)) = (&self.gate, self.run.run()) {
                run.gates.hold(gate).await;
            }
            Ok(Some(room))
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
/// chain-depth limit, its `process_timeout` and its router.
fn kit_options<S>(
    mut options: RoomKitOptions<S>,
    kit: &Value,
    slot: &RunSlot,
) -> RoomKitOptions<S> {
    if let Some(room) = kit["router"]["room_id"]
        .as_str()
        .and_then(|id| RoomId::new(id).ok())
    {
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
    options
}

/// Runs `scenario`; returns every mismatch with its expectations.
pub(crate) async fn run(scenario: &Value) -> Vec<String> {
    // The store's file, when it has one, goes once the run is over.
    let setup = scenario.get("setup").cloned().unwrap_or_default();
    let slot = RunSlot::default();
    let backend = delivery_backend(&setup["kit"]);
    let queue = backend.clone();
    let (kit, _store_file) = crate::new_kit(|options| {
        let options = identity::kit_options(
            kit_options(options, &setup["kit"], &slot),
            &setup["kit"],
            &slot,
        );
        let mut options = crate::verification_calls::kit_options(options, &setup["kit"]);
        if let Some(backend) = queue {
            options.delivery_backend = Some(Arc::new(backend));
        }
        options
    })
    .await;
    let run = Arc::new(Run::new(kit, scenario));
    slot.set(&run);
    *lock(&run.backend) = backend;
    if let Err(problem) = set_up(&run, &setup).await {
        return vec![format!("setup: {problem}")];
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
    let observed = observe(&run, step_outcomes).await;
    run.kit().close().await.unwrap();
    let mut found = lock(&run.problems).clone();
    found.extend(compare(&run, scenario, &observed));
    found
}

async fn set_up(run: &Arc<Run>, setup: &Value) -> Result<(), String> {
    for channel in setup["channels"].as_array().into_iter().flatten() {
        calls::register_channel(run, channel)?
            .map_err(|error| format!("register {channel}: {error}"))?;
    }
    crate::member_calls::store_identities(run, &setup["identities"]).await?;
    for room in setup["rooms"].as_array().into_iter().flatten() {
        if let Err(error) = calls::create_room(run, room).await? {
            return Err(format!("create {room}: {error}"));
        }
        for binding in room["bindings"].as_array().into_iter().flatten() {
            calls::attach_binding(run, room, binding).await?;
        }
        for participant in room["participants"].as_array().into_iter().flatten() {
            calls::add_participant(run, room, participant).await?;
        }
        let status = room["status"].as_str();
        let id = RoomId::new(room["id"].as_str().unwrap_or_default()).map_err(|e| e.to_string())?;
        if matches!(status, Some("closed" | "archived")) {
            run.kit()
                .close_room(&id, CloseRoomOptions::default())
                .await
                .map_err(|e| e.to_string())?;
        }
        if status == Some("archived") {
            run.kit()
                .archive_room(&id, ArchiveRoomOptions::default())
                .await
                .map_err(|e| e.to_string())?;
        }
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
            let handle = tokio::spawn(run.invoke(step.clone(), Scope::default()));
            lock(&run.background).insert(label, handle);
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
    } else if let Some(room_id) = step["wait_for_lock"].as_str() {
        let waiters = step["waiters"]
            .as_u64()
            .map_or(1, |waiters| usize::try_from(waiters).unwrap_or(usize::MAX));
        if !wait_for_lock(run, room_id, waiters).await {
            run.problem(format!(
                "{path}: {waiters} calls never waited for {room_id}'s lock"
            ));
        }
    } else if let Some(gate) = step["open"].as_str() {
        run.gates.open(gate);
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
        while roomkit::testing::room_lock_waiters(&kit, &room_id) < waiters {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(crate::scripted::GATE_BOUND, waited)
        .await
        .is_ok()
}

async fn observe(run: &Arc<Run>, step_outcomes: Vec<Value>) -> Observed {
    let store = run.kit().store();
    let rooms = store
        .list_rooms(roomkit::store::ListRoomsOptions::default())
        .await
        .unwrap_or_default();
    let mut timelines = Vec::new();
    for room in &rooms {
        timelines.push((room.id.to_string(), run.timeline(room.id.as_str()).await));
    }
    let mut observed = std::mem::take(&mut *lock(&run.observed));
    observed.steps = step_outcomes;
    observed.models = lock(&run.models)
        .iter()
        .map(|(channel, model)| (channel.clone(), crate::agents::requests(model)))
        .collect();
    for ((room_key, timeline), room) in timelines.into_iter().zip(&rooms) {
        observed.timelines.insert(room_key.clone(), timeline);
        observed.rooms.insert(
            room_key.clone(),
            serde_json::to_value(room).unwrap_or_default(),
        );
        let bindings = store.list_bindings(&room.id).await.unwrap_or_default();
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
        let observations = store.list_observations(&room.id).await.unwrap_or_default();
        observed.observations.insert(
            room_key.clone(),
            observations
                .iter()
                .map(|o| serde_json::to_value(o).unwrap_or_default())
                .collect(),
        );
        let participants = store.list_participants(&room.id).await.unwrap_or_default();
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
            .unwrap_or_default();
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
