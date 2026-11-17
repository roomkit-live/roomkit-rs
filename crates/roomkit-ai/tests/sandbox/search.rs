//! Sandbox tools remain ordinary search candidates under the resolved policy.
use super::support::*;
use roomkit::Channel;
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions, ToolPolicy, ToolPolicyOptions};
use serde_json::json;
use std::collections::BTreeSet;
use std::sync::Arc;

// Python: test_tool_search_text.py::TestSandboxToolSearch (all three methods).
#[tokio::test]
async fn search_defers_and_reveals_only_policy_allowed_sandbox_names() {
    for streaming in [false, true] {
        for (search, allow) in [
            (true, vec!["send_sms", "sandbox_*"]),
            (true, vec!["send_sms"]),
            (false, vec!["send_sms", "sandbox_read"]),
        ] {
            let sandbox = Arc::new(Recording::new(json!([
                {"name":"sandbox_read","description":"Read file contents from the sandbox."},
                {"name":"sandbox_grep","description":"Run a regex pattern match across the workspace."}
            ])));
            let mut rounds = Vec::new();
            if search {
                rounds.push(ScriptedRound::calling(
                    "",
                    [("f", "find_tools", r#"{"query":"read file contents"}"#)],
                ));
            }
            rounds.push(ScriptedRound::answering("done"));
            let model = Model {
                inner: ScriptedProvider::new(rounds),
                streaming,
            };
            let policy = ToolPolicy::new(ToolPolicyOptions {
                allow: allow.iter().map(|name| (*name).to_owned()).collect(),
                ..ToolPolicyOptions::default()
            })
            .unwrap();
            let agent = AiChannel::new(
                "ai".parse().unwrap(),
                model.clone(),
                AiChannelOptions {
                    sandbox: Some(Arc::<Recording>::clone(&sandbox)),
                    tool_policy: Some(policy),
                    tool_search: Some(search),
                    ..AiChannelOptions::default()
                },
            )
            .unwrap();
            answer(&agent).await;
            agent.close().await;
            let requests = model.inner.requests();
            let names = |at: usize| -> BTreeSet<&str> {
                requests
                    .get(at)
                    .unwrap()
                    .tools
                    .iter()
                    .map(|tool| tool.name.as_str())
                    .collect()
            };
            if search {
                assert_eq!(
                    names(0),
                    BTreeSet::from(["find_tools", "list_tools", "read_stored_result"])
                );
                let result = requests
                    .get(1)
                    .unwrap()
                    .messages
                    .iter()
                    .flat_map(roomkit_ai::AiMessage::tool_results)
                    .find(|part| part.name == "find_tools")
                    .unwrap();
                let result: serde_json::Value = serde_json::from_str(&result.as_text()).unwrap();
                assert!(result.get("error").is_none());
                let matches: Vec<_> = result["matches"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|item| item["name"].as_str().unwrap())
                    .collect();
                if allow.len() == 2 {
                    assert_eq!(matches, ["sandbox_read"]);
                    assert!(names(1).contains("sandbox_read"));
                    assert!(!names(1).contains("sandbox_grep"));
                } else {
                    assert_eq!(matches, Vec::<&str>::new());
                    assert!(!names(1).iter().any(|name| name.starts_with("sandbox_")));
                }
            } else {
                assert!(names(0).contains("sandbox_read"));
                assert!(!names(0).contains("sandbox_grep"));
                assert!(!names(0).contains("find_tools"));
            }
            assert!(sandbox.calls.lock().unwrap().is_empty());
        }
    }
}
