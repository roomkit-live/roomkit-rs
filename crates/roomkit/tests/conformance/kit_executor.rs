//! Public calls on the kit's own executor (RFC §10.6.6).
use roomkit::turns::{
    HookOptions as ExecutorHookOptions, RoomHookOptions as ExecutorRoomHookOptions,
};

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use roomkit::model::{RoomContext, RoomId};
use roomkit::{
    DisableRoomHookOptions, EnableRoomHookOptions, HookPayload, HookTrigger, IntoHookReturn,
    RoomKitError,
};
use roomkit_ai::{
    AiGenerationEvent, AiResponseEvent, PlanUpdatedEvent, ThinkingEvent, ToolRoundEvent,
};
use serde_json::Value;

use crate::calls::{Called, Unreadable, field, finish, optional};
use crate::run::Run;

/// The arguments of each call the pass's executor fixture makes, beside
/// `call`: none for a call that is not its own.
pub(crate) fn fixture_arguments(call: &str) -> Option<&'static [&'static str]> {
    Some(match call {
        "owner.kill" => &[],
        "executor.pause" | "executor.resume" | "executor.start" | "executor.close"
        | "executor.kill" | "executor.calls" => &["executor"],
        "executor.reserve" => &["executor", "process"],
        "turn_backend.fault" => &["operation", "enabled", "gate"],
        _ => return None,
    })
}

/// An executor's lifecycle change, or the kit killed as its turns' owner,
/// which the pass's executor fixture makes.
async fn change_workers(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let id: String = if name == "owner.kill" {
        String::new()
    } else {
        field(spec, "executor")?
    };
    let workers = run
        .workers
        .as_ref()
        .ok_or("this pass has no executor fixture")?;
    Ok(finish(run, name, workers.change(name, &id).await, |()| {
        Value::Null
    }))
}

/// An executor reserved for the kit's turns, or none again (README:
/// `executor.reserve`).
fn reserve(run: &Arc<Run>, spec: &Value) -> Result<Called, Unreadable> {
    if spec.get("executor").is_none() {
        return Err("executor.reserve: executor is missing".into());
    }
    let executor = optional::<Option<String>>(spec, "executor")?.flatten();
    let process: Option<String> = optional(spec, "process")?;
    let workers = run
        .workers
        .as_ref()
        .ok_or("this pass has no executor fixture")?;
    let reserved = workers.reserve(executor.as_deref(), process.as_deref());
    Ok(finish(run, "executor.reserve", reserved, |()| Value::Null))
}

/// A step the pass's executor fixture makes: an executor reserved for the
/// kit's turns, else a lifecycle change.
async fn fixture(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    if name == "executor.reserve" {
        return reserve(run, spec);
    }
    change_workers(run, name, spec).await
}

/// Make the named-hook or executor registration call.
pub(crate) async fn call(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let kit = run.kit();
    match name {
        "turn_backend.fault" => fault(run, spec),
        "executor.calls" => {
            let id: String = field(spec, "executor")?;
            let calls = crate::run::lock(&run.observed)
                .executors
                .get(&id)
                .cloned()
                .unwrap_or_default();
            Ok(finish(run, name, Ok(calls), crate::calls::dump))
        }
        "executor.pause" | "executor.resume" | "executor.start" | "executor.close"
        | "executor.kill" | "owner.kill" | "executor.reserve" => fixture(run, name, spec).await,
        "kit_executor.executor_id" => Ok(finish(
            run,
            name,
            Ok(kit.turn_executor().executor_id().to_owned()),
            |id| Value::String(id.clone()),
        )),
        "executors" => Ok(finish(run, name, kit.executors().await, crate::calls::dump)),
        "agent_presence" => agent_presence(run, spec).await,
        "kit_executor.close" => {
            kit.turn_executor().close().await;
            Ok(finish(run, name, Ok(()), |()| Value::Null))
        }
        "kit_executor.closed" => Ok(finish(
            run,
            name,
            Ok(kit.turn_executor().closed()),
            |value| Value::Bool(*value),
        )),
        "enable_room_hook" => {
            let room: RoomId = field(spec, "room_id")?;
            let hook: String = field(spec, "name")?;
            let options = EnableRoomHookOptions {
                organization_id: optional(spec, "organization_id")?,
            };
            Ok(finish(
                run,
                name,
                kit.enable_room_hook(&room, &hook, options).await,
                |()| Value::Null,
            ))
        }
        "disable_room_hook" => {
            let room: RoomId = field(spec, "room_id")?;
            let hook: String = field(spec, "name")?;
            let options = DisableRoomHookOptions {
                organization_id: optional(spec, "organization_id")?,
            };
            Ok(finish(
                run,
                name,
                kit.disable_room_hook(&room, &hook, options).await,
                |value| Value::Bool(*value),
            ))
        }
        "kit_executor.remove_room_hook" => {
            let hook: String = field(spec, "name")?;
            Ok(finish(
                run,
                name,
                Ok(kit.turn_executor().remove_room_hook(&hook)),
                |value| Value::Bool(*value),
            ))
        }
        _ => Ok(finish(
            run,
            name,
            register(run, &kit.turn_executor(), name, &spec["hook"])?,
            |()| Value::Null,
        )),
    }
}

#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "owner triggers use a generic payload to exercise the executor-side refusal"
)]
pub(crate) fn register(
    run: &Arc<Run>,
    executor: &roomkit::TurnExecutor<impl roomkit::ConversationStore>,
    name: &str,
    hook: &Value,
) -> Result<Result<(), RoomKitError>, Unreadable> {
    use crate::ai_hooks::turn_hook_handler;
    let trigger: HookTrigger = field(hook, "trigger")?;
    match trigger {
        HookTrigger::BeforeToolUse | HookTrigger::OnToolCall => given(
            executor,
            name,
            hook,
            crate::agents::tool_hook_handler(hook, run),
        ),
        HookTrigger::BeforeAiGeneration => given(
            executor,
            name,
            hook,
            turn_hook_handler::<AiGenerationEvent>(hook, run),
        ),
        HookTrigger::AfterToolRound => given(
            executor,
            name,
            hook,
            turn_hook_handler::<ToolRoundEvent>(hook, run),
        ),
        HookTrigger::OnAiResponse => given(
            executor,
            name,
            hook,
            turn_hook_handler::<AiResponseEvent>(hook, run),
        ),
        HookTrigger::OnSpeakDecision => given(
            executor,
            name,
            hook,
            turn_hook_handler::<roomkit_ai::SpeakDecisionEvent>(hook, run),
        ),
        HookTrigger::OnThought => given(
            executor,
            name,
            hook,
            turn_hook_handler::<roomkit_ai::ThoughtEvent>(hook, run),
        ),
        HookTrigger::OnPlanUpdated => given(
            executor,
            name,
            hook,
            turn_hook_handler::<PlanUpdatedEvent>(hook, run),
        ),
        HookTrigger::OnAiThinking => given(
            executor,
            name,
            hook,
            turn_hook_handler::<ThinkingEvent>(hook, run),
        ),
        HookTrigger::OnUserInputRequired => given(
            executor,
            name,
            hook,
            crate::input_calls::input_hook_handler(hook, run),
        ),
        _ => given(
            executor,
            name,
            hook,
            crate::scripted::hook_handler(hook, run),
        ),
    }
}

fn given<P, F, Fut, R>(
    executor: &roomkit::TurnExecutor<impl roomkit::ConversationStore>,
    name: &str,
    hook: &Value,
    handler: F,
) -> Result<Result<(), RoomKitError>, Unreadable>
where
    P: HookPayload,
    F: Fn(P, RoomContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = R> + Send + 'static,
    R: IntoHookReturn<P>,
{
    let trigger: HookTrigger = field(hook, "trigger")?;
    let defaults = ExecutorHookOptions::default();
    let options = ExecutorHookOptions {
        name: field(hook, "name")?,
        execution: optional(hook, "execution")?.unwrap_or(defaults.execution),
        priority: optional(hook, "priority")?.unwrap_or(defaults.priority),
        timeout: optional::<u64>(hook, "timeout_ms")?
            .map_or(defaults.timeout, Duration::from_millis),
        fail_closed: optional(hook, "fail_closed")?.unwrap_or(false),
        channel_types: optional(hook, "channel_types")?,
        channel_ids: optional(hook, "channel_ids")?,
        directions: optional(hook, "directions")?,
        ..defaults
    };
    if name == "kit_executor.hook" {
        return Ok(executor.hook(trigger, handler, options).map(drop));
    }
    let named = ExecutorRoomHookOptions {
        execution: options.execution,
        priority: options.priority,
        timeout: options.timeout,
        fail_closed: options.fail_closed,
    };
    Ok(executor.room_hook(&options.name, trigger, handler, named))
}

fn fault(run: &Run, spec: &Value) -> Result<Called, Unreadable> {
    let workers = run.workers.as_ref().ok_or("no executor fixture")?;
    let result = workers.fault(
        &field::<String>(spec, "operation")?,
        field(spec, "enabled")?,
        optional(spec, "gate")?,
    );
    Ok(finish(run, "turn_backend.fault", result, |()| Value::Null))
}

async fn agent_presence(run: &Run, spec: &Value) -> Result<Called, Unreadable> {
    let kit = run.kit_of(spec)?;
    let room: RoomId = field(spec, "room_id")?;
    let options = roomkit::AgentPresenceOptions {
        organization_id: optional::<Option<String>>(spec, "organization_id")?.flatten(),
    };
    let target = optional::<String>(spec, "wait_for_online")?;
    let read = async {
        loop {
            let agents = kit.agent_presence(&room, options.clone()).await?;
            if target.as_ref().is_none_or(|id| {
                agents
                    .iter()
                    .any(|agent| agent.online && agent.channel_id.as_str() == id)
            }) {
                return Ok(agents);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    };
    let outcome = if target.is_some() {
        tokio::time::timeout(crate::scripted::GATE_BOUND, read)
            .await
            .unwrap_or(Err(RoomKitError::Timeout))
    } else {
        read.await
    };
    Ok(finish(run, "agent_presence", outcome, crate::calls::dump))
}
