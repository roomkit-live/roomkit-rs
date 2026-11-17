//! A loop reviewer's verdict (RFC §19.7.4): the `submit_review` tool it is
//! handed back through, and how it is read. Python's
//! `orchestration/strategies/_loop_verdict.py`.
//!
//! A reviewer judges the producer's output by calling `submit_review` once,
//! forced as a worker's `submit_result` and a supervisor's `submit_verdict`
//! are, its reminders included. The verdict fails closed: a review that
//! never called the tool, a reviewer channel that answered in text, or a
//! task that failed approves nothing, whatever words its text holds.

use std::sync::Arc;

use roomkit_core::model::Metadata;
use serde_json::{Value, json};

use super::supervisor::{python_str, truthy};
use crate::tasks::ResultTool;

/// A reviewer's verdict: whether it approves, and its feedback.
pub(super) struct ReviewVerdict {
    pub(super) approved: bool,
    pub(super) feedback: String,
}

impl ReviewVerdict {
    /// The verdict as the payload the orchestration hands back.
    fn payload(&self) -> Metadata {
        Metadata::from_iter([
            ("approved".to_owned(), json!(self.approved)),
            ("feedback".to_owned(), json!(self.feedback)),
        ])
    }
}

/// A `submit_review` call's arguments read as a verdict: approved only when
/// `approved` is the boolean true (Python's `_normalize_review`).
fn normalize_review(arguments: &Metadata) -> ReviewVerdict {
    ReviewVerdict {
        approved: arguments.get("approved") == Some(&Value::Bool(true)),
        feedback: arguments
            .get("feedback")
            .filter(|value| truthy(value))
            .map(python_str)
            .unwrap_or_default(),
    }
}

/// The tool a reviewer judges the producer's output by: Python's
/// `SUBMIT_REVIEW`. A reviewer that never calls it is not approving, its
/// last text kept as its feedback (Python's `_no_review`).
pub(super) fn submit_review() -> ResultTool {
    let tool = json!({
        "name": "submit_review",
        "description": "Submit your review of the content. You MUST call this exactly once; it \
                        is the ONLY way your judgement reaches the producer.",
        "parameters": {
            "type": "object",
            "properties": {
                "approved": {
                    "type": "boolean",
                    "description": "true only if the content meets the standard as it is.",
                },
                "feedback": {
                    "type": "string",
                    "description": "What to change, precisely, when not approved; else empty.",
                },
            },
            "required": ["approved", "feedback"],
        },
    });
    ResultTool::new(
        tool,
        Arc::new(|arguments| normalize_review(arguments).payload()),
        Arc::new(|_role, last_output, _attempts| {
            ReviewVerdict {
                approved: false,
                feedback: last_output.to_owned(),
            }
            .payload()
        }),
        "You did not submit your review. You MUST now call the `submit_review` tool with \
         approved and feedback. Do NOT reply with plain text.",
    )
}

/// The verdict a review's task handed back: its `submit_review` payload as
/// the orchestration wrote it. Anything else (a reviewer channel that
/// answered in text, a task that failed and handed back nothing) is no
/// approval, its text the feedback (Python's `read_review`).
pub(super) fn read_review(output: &str) -> ReviewVerdict {
    match serde_json::from_str::<Value>(output) {
        Ok(Value::Object(payload)) if payload.contains_key("approved") => {
            normalize_review(&payload)
        }
        _ => ReviewVerdict {
            approved: false,
            feedback: output.to_owned(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::read_review;

    /// The review's result: an approving `submit_review` payload.
    const APPROVAL: &str = r#"{"approved": true, "feedback": ""}"#;

    // Python: tests/orchestration/test_loop_strategy.py
    // TestReviewVerdict::test_only_an_approving_verdict_approves (RMK-597).
    #[test]
    fn only_an_approving_verdict_approves() {
        let cases = [
            (
                "Not approved: the intro is missing.",
                false,
                "Not approved: the intro is missing.",
            ),
            ("DISAPPROVED", false, "DISAPPROVED"),
            (
                "I cannot mark this APPROVED yet.",
                false,
                "I cannot mark this APPROVED yet.",
            ),
            ("APPROVED", false, "APPROVED"),
            (
                r#"{"approved": false, "feedback": "Add the intro."}"#,
                false,
                "Add the intro.",
            ),
            (r#"{"approved": "true", "feedback": ""}"#, false, ""),
            ("", false, ""),
            (APPROVAL, true, ""),
        ];
        for (output, approved, feedback) in cases {
            let verdict = read_review(output);
            assert_eq!(
                (verdict.approved, verdict.feedback.as_str()),
                (approved, feedback),
                "{output:?}"
            );
        }
    }
}
