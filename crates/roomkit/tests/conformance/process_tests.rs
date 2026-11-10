//! A named process owns the call made through it, even when it is closed.

use serde_json::json;

// Python: tests/conformance/test_processes.py::test_regeneration_uses_the_named_kit_without_closing_the_other
#[tokio::test]
async fn regeneration_uses_the_named_kit() {
    let scenario = json!({
        "setup": {"rooms": [{"id": "r1"}], "processes": [{"id": "follower"}]},
        "steps": [
            {"call": "close", "kit": "follower"},
            {"call": "regenerate_response", "kit": "follower", "room_id": "r1",
             "expect": {"error": {"name": "kit_closed"}}},
            {"call": "get_room", "room_id": "r1", "expect": {"result": {"id": "r1"}}}
        ]
    });
    let problems = crate::harness::run(&scenario).await;
    assert!(problems.is_empty(), "{problems:?}");
}
