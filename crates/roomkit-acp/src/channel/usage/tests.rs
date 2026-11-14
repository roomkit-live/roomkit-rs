//! Exact maps and owned snapshots complement the shared hook assertions.

use super::Usage;
use agent_client_protocol_schema::v1::{PromptResponse, StopReason};
use serde_json::json;

// Python: tests/test_channels/test_acp.py::TestEndOfTurnReport::test_an_agent_that_reports_nothing_reports_no_usage
#[test]
fn test_an_agent_that_reports_nothing_reports_no_usage() {
    let usage = Usage::new("s", "e", "test", false, None, None);
    assert!(
        usage
            .finish(&PromptResponse::new(StopReason::EndTurn))
            .counters
            .is_empty()
    );
}

// Python: ACPTurnMixin._run_prompt accepts a recovered model only when it is a string.
#[test]
fn recovered_model_only_accepts_a_string() {
    for model in [json!("original"), json!(null), json!(42), json!({})] {
        let usage = Usage::new("s", "e", "test", true, None, Some(json!("current")));
        let response: PromptResponse = serde_json::from_value(json!({
            "stopReason": "end_turn",
            "_meta": {"roomkit.live/usage": {"model": model}}
        }))
        .unwrap();
        let snapshot = usage.finish(&response);
        let prompt = &snapshot.metadata["prompt"];
        assert!(prompt.get("model_at_start").is_none());
        assert_eq!(prompt.get("model"), model.is_string().then_some(&model));
    }
}

// Python: tests/test_channels/test_acp_usage.py::TestUsageProvenance::test_recovered_snapshot_is_authoritative_and_repeatable
#[test]
fn recovered_reports_and_local_observations_are_owned_snapshots() {
    let usage = Usage::new("s", "e", "test", true, None, None);
    let mut update = json!({"sessionUpdate":"usage_update","used":0,"size":100});
    let notice = usage.observe(&update, Some(json!("opus")));
    update["used"] = json!(99);
    let first = usage.finish(&PromptResponse::new(StopReason::EndTurn));
    assert_eq!(first.counters["context_used"], 0);
    assert_eq!(first.metadata["usage_report"], notice["usage_report"]);
    assert_eq!(
        first.metadata["usage_report"]["model_at_observation"],
        "opus"
    );
    assert!(
        first.metadata["usage_report"]
            .get("source_result_id")
            .is_none()
    );
    let report = first.metadata["usage_report"].clone();
    let mut response: PromptResponse = serde_json::from_value(json!({"stopReason":"cancelled",
        "_meta":{"roomkit.live/usage":{"session_id":"original","usage_report":report}}
    }))
    .unwrap();
    let recovered = Usage::new("new-session", "second-event", "test", true, None, None);
    let second = recovered.finish(&response);
    response.meta.as_mut().unwrap()["roomkit.live/usage"]["session_id"] = json!("mutated");
    assert_eq!(second.metadata["session_id"], "original");
    assert_eq!(first.metadata["session_id"], "s");
}
