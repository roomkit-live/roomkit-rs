//! Side effects and diagnostics visible in the returned synchronous pipeline result.
use crate::turns::SyncPipelineResult;
use roomkit_core::model::{HookAction, Metadata};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::HookSpec;

pub(super) fn collect_effects<P>(
    spec: &HookSpec,
    outcome: &mut HookOutcome<P>,
    target: &mut SyncPipelineResult<P>,
) {
    let (code, error) = match outcome {
        HookOutcome::Returned(result) => {
            target.tasks.append(&mut result.tasks);
            target.observations.append(&mut result.observations);
            target.metadata.append(&mut result.metadata);
            if result.action != HookAction::Modify || result.event.is_some() {
                target.injected_events.append(&mut result.injected_events);
                return;
            }
            ("hook_invalid_result", "modify returned NoneType".to_owned())
        }
        HookOutcome::Raised(error) => ("hook_error", error.message.clone()),
        HookOutcome::TimedOut => (
            "hook_timeout",
            format!("timeout ({:?}s)", spec.timeout.as_secs_f64()),
        ),
        HookOutcome::Invalid {
            what,
            tasks,
            observations,
        } => {
            target.tasks.append(tasks);
            target.observations.append(observations);
            ("hook_invalid_result", what.clone())
        }
    };
    if spec.fail_closed {
        target.reason = Some(format!("{code}:{}", spec.name));
    }
    target.hook_errors.push(Metadata::from_iter([
        ("hook".to_owned(), spec.name.clone().into()),
        ("error".to_owned(), error.into()),
    ]));
}
