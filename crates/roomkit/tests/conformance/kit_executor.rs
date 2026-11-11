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
use roomkit_ai::{AiGenerationEvent, AiResponseEvent, ThinkingEvent, ToolRoundEvent};
use serde_json::Value;

use crate::calls::{Called, Unreadable, field, finish, optional};
use crate::run::Run;

/// Make the named-hook or executor registration call.
pub(crate) async fn call(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let kit = run.kit();
    match name {
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
            register(run, name, &spec["hook"])?,
            |()| Value::Null,
        )),
    }
}

#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "owner triggers use a generic payload to exercise the executor-side refusal"
)]
fn register(
    run: &Arc<Run>,
    name: &str,
    hook: &Value,
) -> Result<Result<(), RoomKitError>, Unreadable> {
    use crate::ai_hooks::turn_hook_handler;
    let trigger: HookTrigger = field(hook, "trigger")?;
    match trigger {
        HookTrigger::BeforeToolUse | HookTrigger::OnToolCall => {
            given(run, name, hook, crate::agents::tool_hook_handler(hook, run))
        }
        HookTrigger::BeforeAiGeneration => given(
            run,
            name,
            hook,
            turn_hook_handler::<AiGenerationEvent>(hook, run),
        ),
        HookTrigger::AfterToolRound => given(
            run,
            name,
            hook,
            turn_hook_handler::<ToolRoundEvent>(hook, run),
        ),
        HookTrigger::OnAiResponse => given(
            run,
            name,
            hook,
            turn_hook_handler::<AiResponseEvent>(hook, run),
        ),
        HookTrigger::OnAiThinking => given(
            run,
            name,
            hook,
            turn_hook_handler::<ThinkingEvent>(hook, run),
        ),
        HookTrigger::OnUserInputRequired => given(
            run,
            name,
            hook,
            crate::input_calls::input_hook_handler(hook, run),
        ),
        _ => given(run, name, hook, crate::scripted::hook_handler(hook, run)),
    }
}

fn given<P, F, Fut, R>(
    run: &Arc<Run>,
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
    let executor = run.kit().turn_executor();
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
