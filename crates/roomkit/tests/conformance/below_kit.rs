//! Room hooks registered below the kit, which reads no room (RFC §9.1,
//! §17.2): on the kit's hook engine, or given straight to the kit's own
//! executor.

use std::sync::Arc;

use roomkit::model::{Observation, RoomId};
use roomkit::orchestration::StatusEntry;
use roomkit::{HookExecution, HookRegistration, HookTrigger};
use roomkit_ai::{
    AiGenerationEvent, AiResponseEvent, PlanUpdatedEvent, ThinkingEvent, ToolRoundEvent,
};
use serde_json::Value;

use crate::ai_hooks::turn_hook_handler;
use crate::calls::{Called, Unreadable, field, finish, optional};
use crate::run::Run;
use crate::scripted::{hook_handler, session_hook_handler, status_hook_handler};
use crate::task_calls::payload_hook_handler;

/// Makes `hook_engine.add_room_hook` or `kit_executor.add_owner_room_hook`.
pub(crate) fn call(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let room: RoomId = field(spec, "room_id")?;
    let hook = registration(run, &spec["hook"])?;
    let kit = run.kit();
    let registered = if name == "hook_engine.add_room_hook" {
        kit.hook_engine().add_room_hook(&room, hook)
    } else {
        kit.turn_executor().hooks().add_owner_room_hook(&room, hook)
    };
    Ok(finish(run, name, registered, |_| Value::Null))
}

/// A room hook as `add_room_hook` takes it: no filter, the default timeout.
fn registration(run: &Arc<Run>, hook: &Value) -> Result<HookRegistration, Unreadable> {
    let trigger: HookTrigger = field(hook, "trigger")?;
    let execution: HookExecution = optional(hook, "execution")?.unwrap_or(HookExecution::Sync);
    let registered = match hook.get("router") {
        Some(router) => {
            let router = crate::routing_calls::router(router)?;
            HookRegistration::new(trigger, execution, router.as_hook())
        }
        None => scripted(run, trigger, execution, hook),
    };
    Ok(HookRegistration {
        name: hook["name"].as_str().unwrap_or_default().to_owned(),
        priority: optional(hook, "priority")?.unwrap_or_default(),
        fail_closed: optional(hook, "fail_closed")?.unwrap_or(false),
        needs_lock: optional(hook, "needs_lock")?.unwrap_or(true),
        ..registered
    })
}

/// A scripted hook's registration, its handler the one its trigger's
/// payload takes, as `register_hook` picks it.
#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "every trigger whose payload is a room event takes the event hook"
)]
fn scripted(
    run: &Arc<Run>,
    trigger: HookTrigger,
    execution: HookExecution,
    hook: &Value,
) -> HookRegistration {
    macro_rules! on {
        ($handler:expr) => {
            HookRegistration::new(trigger, execution, $handler)
        };
    }
    match trigger {
        HookTrigger::BeforeToolUse | HookTrigger::OnToolCall => {
            on!(crate::agents::tool_hook_handler(hook, run))
        }
        HookTrigger::OnSessionStarted => on!(session_hook_handler(hook, run)),
        HookTrigger::OnDeliveryStatus => on!(status_hook_handler(hook, run)),
        HookTrigger::OnStatusPosted => on!(payload_hook_handler::<StatusEntry>(
            hook,
            run,
            "status_entry"
        )),
        HookTrigger::OnFeedback => on!(payload_hook_handler::<Observation>(
            hook,
            run,
            "observation"
        )),
        HookTrigger::OnUserInputRequired => {
            on!(crate::input_calls::input_hook_handler(hook, run))
        }
        HookTrigger::BeforeAiGeneration => on!(turn_hook_handler::<AiGenerationEvent>(hook, run)),
        HookTrigger::AfterToolRound => on!(turn_hook_handler::<ToolRoundEvent>(hook, run)),
        HookTrigger::OnAiResponse => on!(turn_hook_handler::<AiResponseEvent>(hook, run)),
        HookTrigger::OnSpeakQueue => on!(turn_hook_handler::<roomkit::SpeakQueueEvent>(hook, run)),
        HookTrigger::OnDispatchDecision => {
            on!(turn_hook_handler::<roomkit::DispatchDecisionEvent>(
                hook, run
            ))
        }
        HookTrigger::OnSpeakDecision => {
            on!(turn_hook_handler::<roomkit_ai::SpeakDecisionEvent>(
                hook, run
            ))
        }
        HookTrigger::OnThought => on!(turn_hook_handler::<roomkit_ai::ThoughtEvent>(hook, run)),
        HookTrigger::OnPlanUpdated => on!(turn_hook_handler::<PlanUpdatedEvent>(hook, run)),
        HookTrigger::OnAiThinking => on!(turn_hook_handler::<ThinkingEvent>(hook, run)),
        _ => on!(hook_handler(hook, run)),
    }
}
