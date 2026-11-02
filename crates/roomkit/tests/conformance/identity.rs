//! The scripted parts of identity resolution (roomkit-specs
//! `conformance/README.md`, "Identity resolution"): the kit's resolver and
//! its identity hooks (RFC §11.1 to §11.3), built on the tokio runtime.

use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use roomkit::identity::{IdentityFuture, IdentityResolver};
use roomkit::model::{
    ChannelId, ChannelType, EventDirection, IdentityHookResult, IdentityResult, InboundMessage,
    RoomContext, RoomEvent,
};
use roomkit::{HookError, HookTrigger, IdentityHookOptions, RoomKitOptions};
use serde_json::{Value, json};

use crate::run::{Run, Scope, lock};
use crate::scripted::{Reactions, ScriptedFailure, event_subject, perform};
use crate::templates::complete_event;

/// The run a scripted resolver records into, set once the run exists: the
/// kit, which holds the resolver, is built before it.
#[derive(Clone, Debug, Default)]
pub(crate) struct RunSlot(Arc<OnceLock<Weak<Run>>>);

impl RunSlot {
    pub(crate) fn set(&self, run: &Arc<Run>) {
        // Set once, by the run that built the kit.
        drop(self.0.set(Arc::downgrade(run)));
    }

    pub(crate) fn run(&self) -> Option<Arc<Run>> {
        self.0.get().and_then(Weak::upgrade)
    }
}

/// A resolver that records each call, then answers with the first answer
/// whose `when` holds, `{"status": "unknown"}` with none.
struct ScriptedResolver {
    answers: Reactions,
    run: RunSlot,
}

impl IdentityResolver for ScriptedResolver {
    fn resolve<'a>(
        &'a self,
        message: &'a InboundMessage,
        context: &'a RoomContext,
    ) -> IdentityFuture<'a> {
        Box::pin(async move {
            let room_id = context.room.id.to_string();
            let run = self.run.run();
            if let Some(run) = &run {
                record_resolver(run, &room_id, message);
            }
            let subject = json!({
                "room_id": room_id,
                "sender_id": message.sender_id,
                "channel_id": message.channel_id,
                "text": serde_json::to_value(&message.content).unwrap_or_default()["body"],
            });
            let answer = self.answers.pick(&subject).unwrap_or_default();
            if let (Some(gate), Some(run)) = (answer.get("gate").and_then(Value::as_str), &run) {
                run.gates.hold(gate).await;
            }
            if let Some(failure) = answer.get("fail").and_then(Value::as_str) {
                return Err(ScriptedFailure(failure.to_owned()).into());
            }
            let result = answer
                .get("result")
                .cloned()
                .unwrap_or_else(|| json!({ "status": "unknown" }));
            Ok(serde_json::from_value::<IdentityResult>(result)?)
        })
    }
}

/// The kit's identity options a scenario's `setup.kit` names, over
/// `options`: its resolver, recording into the run `slot` holds, its
/// channel types and its timeout.
pub(crate) fn kit_options<S>(
    mut options: RoomKitOptions<S>,
    kit: &Value,
    slot: &RunSlot,
) -> RoomKitOptions<S> {
    if let Some(resolver) = kit.get("identity_resolver") {
        options.identity_resolver = Some(Arc::new(ScriptedResolver {
            answers: Reactions::new(resolver.get("answers")),
            run: slot.clone(),
        }));
    }
    if let Some(types) = kit.get("identity_channel_types") {
        options.identity_channel_types = serde_json::from_value(types.clone()).ok();
    }
    if let Some(timeout) = kit["identity_timeout_ms"].as_u64() {
        options.identity_timeout = Duration::from_millis(timeout);
    }
    options
}

fn record_resolver(run: &Run, room_id: &str, message: &InboundMessage) {
    if !run.recording.load(Ordering::SeqCst) {
        return;
    }
    lock(&run.observed).resolver.push(json!({
        "room_id": room_id,
        "channel_id": message.channel_id,
        "sender_id": message.sender_id,
    }));
}

fn record_identity_hook(
    run: &Run,
    name: &str,
    trigger: &str,
    event: &RoomEvent,
    result: &IdentityResult,
) {
    if !run.recording.load(Ordering::SeqCst) {
        return;
    }
    let call = json!({
        "trigger": trigger,
        "room_id": event.room_id,
        "event": event,
        "identity_result": result,
    });
    let mut observed = lock(&run.observed);
    observed
        .hooks
        .entry(name.to_owned())
        .or_default()
        .push(call);
    observed.hook_order.push(name.to_owned());
}

/// Registers the scripted identity hook `hook` (README, "Setup") on the
/// run's kit.
pub(crate) fn register_identity_hook(run: &Arc<Run>, hook: &Value) -> Result<(), String> {
    let trigger: HookTrigger =
        serde_json::from_value(hook["trigger"].clone()).map_err(|e| e.to_string())?;
    let options = IdentityHookOptions {
        channel_types: set::<ChannelType>(hook, "channel_types")?,
        channel_ids: set::<ChannelId>(hook, "channel_ids")?,
        directions: set::<EventDirection>(hook, "directions")?,
    };
    let reactions = Arc::new(Reactions::new(hook.get("reactions")));
    let name = hook["name"].as_str().unwrap_or_default().to_owned();
    let trigger_name = hook["trigger"].as_str().unwrap_or_default().to_owned();
    let weak = Arc::downgrade(run);
    run.kit().identity_hook(
        trigger,
        move |event: RoomEvent, _context: RoomContext, result: IdentityResult| {
            let reactions = Arc::clone(&reactions);
            let (name, trigger_name, run) =
                (name.clone(), trigger_name.clone(), Weak::clone(&weak));
            async move {
                let Some(run) = run.upgrade() else {
                    return Ok(None);
                };
                record_identity_hook(&run, &name, &trigger_name, &event, &result);
                let room_id = event.room_id.to_string();
                let Some(reaction) = reactions.pick(&event_subject(&event, &room_id)) else {
                    return Ok(None);
                };
                let scripted =
                    json!({ "gate": reaction.get("gate"), "fail": reaction.get("fail") });
                perform(&run, &scripted, &Scope::in_room(&room_id)).await?;
                answer(reaction.get("result"), &room_id)
            }
        },
        options,
    );
    Ok(())
}

/// The members of `hook`'s `key` as a set, `None` when it names none.
fn set<T: serde::de::DeserializeOwned + Ord>(
    hook: &Value,
    key: &str,
) -> Result<Option<BTreeSet<T>>, String> {
    hook.get(key)
        .map(|items| serde_json::from_value(items.clone()))
        .transpose()
        .map_err(|e| e.to_string())
}

/// An identity hook's answer from a scenario's `result`, its `inject`'s
/// event completed as a hook's injected event; null answers none.
fn answer(result: Option<&Value>, room_id: &str) -> Result<Option<IdentityHookResult>, HookError> {
    let Some(result) = result.filter(|result| !result.is_null()) else {
        return Ok(None);
    };
    let mut result = result.clone();
    if let Some(event) = result.get("inject").and_then(|inject| inject.get("event")) {
        let system =
            json!({ "channel_id": "system", "channel_type": "system", "direction": "inbound" });
        result["inject"]["event"] = complete_event(event, room_id, &system);
    }
    Ok(Some(serde_json::from_value(result)?))
}
