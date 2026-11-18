//! Standalone errors an application handles on the tool and task paths.

use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use roomkit::{
    ChannelRefusalError, TaskCutShortError, TaskTurnFailedError, ToolTimeoutError,
    TurnCutShortError,
};
use roomkit_ai::ToolError;

#[test]
fn channel_refusal_carries_the_original_words_into_the_refusal_family() {
    let refusal = ChannelRefusalError::new("outside the turn");
    assert_eq!(refusal.message, "outside the turn");
    assert_eq!(refusal.to_string(), refusal.message);
    assert_eq!(
        ToolError::from(refusal),
        ToolError::refused("outside the turn")
    );
}

#[test]
fn a_tool_timeout_keeps_its_bound_and_formats_the_quoted_name() {
    for (seconds, rendered) in [
        (0.012_345_678, "0.0123457"),
        (1e-5, "1e-05"),
        (1e6, "1e+06"),
        (-0.0, "-0"),
        (f64::INFINITY, "inf"),
        (f64::NAN, "nan"),
    ] {
        let timeout = ToolTimeoutError::new("wait's turn", seconds);
        assert_eq!(timeout.name, "wait's turn");
        assert_eq!(timeout.timeout.to_bits(), seconds.to_bits());
        assert_eq!(
            timeout.to_string(),
            format!("tool \"wait's turn\" did not answer within {rendered} s")
        );
    }
}

#[test]
fn a_task_cut_exposes_its_open_reason_and_optional_narration() {
    let turn = TurnCutShortError::new("operator stopped the turn", None);
    assert_eq!(turn.reason, None);
    assert_eq!(turn.to_string(), "operator stopped the turn");
    for narration in [None, Some(""), Some("Searching")] {
        let cut = TaskCutShortError::new("custom stop", narration.map(str::to_owned));
        let base: &TurnCutShortError = cut.as_ref();
        assert_eq!(base.reason.as_deref(), Some("custom stop"));
        assert_eq!(cut.narration.as_deref(), narration);
        assert_eq!(
            base.to_string(),
            TaskCutShortError::message_for("custom stop")
        );
        assert_eq!(
            cut.to_string(),
            "The worker's turn ended custom stop before its answer"
        );
    }
}

#[derive(Debug)]
struct WorkerFailure {
    changed: AtomicBool,
}

impl fmt::Display for WorkerFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.changed.load(Ordering::Relaxed) {
            "updated"
        } else {
            "upstream failed"
        })
    }
}

impl Error for WorkerFailure {}

#[test]
fn a_failed_task_turn_keeps_the_original_cause_and_its_initial_message() {
    let error = Arc::new(WorkerFailure {
        changed: AtomicBool::new(false),
    });
    let failure = TaskTurnFailedError::new(
        Arc::<WorkerFailure>::clone(&error),
        "interrupted",
        Some("Searching".to_owned()),
    );
    assert_eq!(failure.reason, "interrupted");
    assert_eq!(failure.narration.as_deref(), Some("Searching"));
    let cause = failure
        .source()
        .and_then(|cause| cause.downcast_ref::<WorkerFailure>());
    assert!(cause.is_some_and(|cause| std::ptr::eq(error.as_ref(), cause)));
    error.changed.store(true, Ordering::Relaxed);
    assert_eq!(error.to_string(), "updated");
    assert_eq!(failure.to_string(), "upstream failed");
}
