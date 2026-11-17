//! Fixture failures remain visible even when product errors are sanitized.

use serde_json::{Value, json};

fn scenario(name: &str) -> Value {
    let path = crate::scenarios::suite()
        .join("scenarios/served-skills")
        .join(format!("{name}.json"));
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

#[test]
fn malformed_approval_controls_are_not_truthy_host_decisions() {
    for approvals in [
        Value::Null,
        json!(true),
        json!([1]),
        json!([{}]),
        json!([{"error":1}]),
        json!([{"error":"x","extra":1}]),
    ] {
        let spec = json!({"servers":{"remote":{"replies":[],"approvals":approvals}},"operations":[{"method":"snapshot"}]});
        let mut problems = Vec::new();
        super::validate(&spec, "skill_registry_session", &mut problems);
        assert_ne!(problems, Vec::<String>::new());
    }
}

#[tokio::test]
async fn sanitized_read_errors_never_hide_a_wrong_server_request() {
    let mut scenario = scenario("load-refuses-size");
    scenario["steps"][0]["servers"]["remote"]["replies"][1]["method"] = json!("wrong/read");
    let problems = crate::harness::run(&scenario).await;
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("wrong request")),
        "{problems:?}"
    );
}

#[tokio::test]
async fn an_error_cannot_pass_as_an_expected_null_result() {
    let mut scenario = scenario("only-full-copies-fetch-unlisted");
    scenario["steps"][0]["expect"]["result"]["results"][3] = json!({"status":"ok","result":null});
    let problems = crate::harness::run(&scenario).await;
    assert!(
        problems.iter().any(|problem| problem.contains("status")),
        "{problems:?}"
    );
}

#[tokio::test]
async fn approval_exhaustion_is_not_an_expected_execution_refusal() {
    let mut scenario = scenario("execution-absent-approval");
    scenario["steps"][0]["servers"]["remote"]["approvals"] = json!([]);
    let problems = crate::harness::run(&scenario).await;
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("approval fixture exhausted")),
        "{problems:?}"
    );
}
