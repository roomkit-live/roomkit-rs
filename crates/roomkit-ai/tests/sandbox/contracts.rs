use std::error::Error;
use std::sync::Arc;

use roomkit::Channel;
use roomkit::model::ToolCallOutcome;
use roomkit_ai::sandbox::{SANDBOX_PREAMBLE, SANDBOX_TOOL_SCHEMAS};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    Agent, AgentOptions, AiChannel, AiChannelOptions, AiError, AiTool, SandboxExecutor, ToolError,
};
use serde_json::json;

use super::support::*;

#[test]
fn catalogue_errors_and_exact_host_collisions_fail_before_execution() {
    let failed = Arc::new(Recording {
        failure: Some(ToolError::raised_as(
            "ConnectionError",
            "catalogue unavailable",
        )),
        ..Recording::new(json!([]))
    });
    let error = AiChannel::new(
        "ai".parse().unwrap(),
        ScriptedProvider::new([]),
        AiChannelOptions {
            sandbox: Some(Arc::<Recording>::clone(&failed)),
            ..AiChannelOptions::default()
        },
    )
    .unwrap_err();
    assert!(
        matches!(&error,AiError::SandboxDefinition{error:ToolError::Raised{kind,message}} if kind=="ConnectionError" && message=="catalogue unavailable")
    );
    assert_eq!(
        error.source().unwrap().to_string(),
        "ConnectionError: catalogue unavailable"
    );
    assert!(failed.calls.lock().unwrap().is_empty());
    for (definition, kind) in [
        (json!({}), "KeyError"),
        (json!({"name":null}), "AttributeError"),
    ] {
        let sandbox = Arc::new(Recording::new(json!([definition])));
        let error = AiChannel::new(
            "ai".parse().unwrap(),
            ScriptedProvider::new([]),
            AiChannelOptions {
                sandbox: Some(Arc::<Recording>::clone(&sandbox)),
                ..AiChannelOptions::default()
            },
        )
        .unwrap_err();
        assert!(
            matches!(error,AiError::SandboxDefinition{error:ToolError::Raised{kind:actual,..}} if actual==kind)
        );
        assert!(sandbox.calls.lock().unwrap().is_empty());
    }
    let sandbox = Arc::new(Recording::new(json!([{"name":"sandbox_bash"}])));
    let options = AiChannelOptions {
        sandbox: Some(Arc::<Recording>::clone(&sandbox)),
        tools: vec![AiTool::new("sandbox_bash", "host").unwrap().into()],
        ..AiChannelOptions::default()
    };
    assert!(matches!(
        AiChannel::new(
            "ai".parse().unwrap(),
            ScriptedProvider::new([]),
            options.clone()
        ),
        Err(AiError::DuplicateTool { .. })
    ));
    assert!(matches!(
        Agent::new(
            "ai".parse().unwrap(),
            ScriptedProvider::new([]),
            AgentOptions {
                ai: options,
                ..AgentOptions::default()
            }
        ),
        Err(AiError::DuplicateTool { .. })
    ));
    assert!(sandbox.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn optional_arguments_no_op_close_and_one_prefix_are_preserved() {
    let sandbox = Recording::new(json!([]));
    sandbox.execute("bash", None).await.unwrap();
    sandbox.close().await;
    assert_eq!(
        *sandbox.calls.lock().unwrap(),
        [json!({"command":"bash","arguments":null})]
    );
    for streaming in [false, true] {
        let sandbox = Arc::new(Recording::new(json!([{"name":"sandbox_sandbox_echo"}])));
        let model = model(streaming, "sandbox_sandbox_echo");
        let agent = Agent::new(
            "ai".parse().unwrap(),
            model.clone(),
            AgentOptions {
                ai: AiChannelOptions {
                    sandbox: Some(Arc::<Recording>::clone(&sandbox)),
                    ..AiChannelOptions::default()
                },
                ..AgentOptions::default()
            },
        )
        .unwrap();
        let output = answer(&agent).await;
        agent.close().await;
        assert_eq!(
            *sandbox.calls.lock().unwrap(),
            [json!({"command":"sandbox_echo","arguments":{}})]
        );
        let ends = ends(&output);
        assert_eq!(ends.len(), 1);
        assert_eq!(ends.first().unwrap().outcome, Some(ToolCallOutcome::Served));
        let result: serde_json::Value =
            serde_json::from_str(ends.first().unwrap().result.as_str().unwrap()).unwrap();
        assert_eq!(
            result,
            json!({"exit_code":7,"output":"résultat","error":"Unknown tool: frobnicate","metadata":{},"success":false})
        );
        let requests = model.inner.requests();
        let parts: Vec<_> = requests
            .get(1)
            .unwrap()
            .messages
            .iter()
            .flat_map(roomkit_ai::AiMessage::tool_results)
            .collect();
        assert_eq!(parts.len(), 1);
        assert!(!parts.first().unwrap().is_error);
        assert_eq!(
            parts.first().unwrap().outcome,
            Some(ToolCallOutcome::Served)
        );
    }
}

#[tokio::test]
async fn each_turn_reads_only_the_three_definition_fields() {
    for streaming in [false, true] {
        let sandbox = Arc::new(Recording::new(
            json!([{"name":"sandbox_first","tags":["ignored"],"changes_state":true,"defer_loading":true}]),
        ));
        let model = Model {
            inner: ScriptedProvider::new([
                ScriptedRound::answering("one"),
                ScriptedRound::answering("two"),
            ]),
            streaming,
        };
        let agent = channel(model.clone(), Arc::<Recording>::clone(&sandbox));
        answer(&agent).await;
        *sandbox.definitions.lock().unwrap() =
            serde_json::from_value(json!([{"name":"sandbox_second","description":"Second"}]))
                .unwrap();
        answer(&agent).await;
        let requests = model.inner.requests();
        let first = requests
            .first()
            .unwrap()
            .tools
            .iter()
            .find(|tool| tool.name == "sandbox_first")
            .unwrap();
        assert_eq!(first, &AiTool::new("sandbox_first", "").unwrap());
        let second = &requests.get(1).unwrap().tools;
        assert!(second.iter().any(|tool| tool.name == "sandbox_second"));
        assert!(!second.iter().any(|tool| tool.name == "sandbox_first"));
        assert!(sandbox.calls.lock().unwrap().is_empty());
        agent.close().await;
    }
}

#[test]
fn reference_catalogue_is_ordered_with_no_runtime_defaults() {
    let names: Vec<_> = SANDBOX_TOOL_SCHEMAS
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        [
            "sandbox_read",
            "sandbox_write",
            "sandbox_edit",
            "sandbox_ls",
            "sandbox_grep",
            "sandbox_find",
            "sandbox_git",
            "sandbox_diff",
            "sandbox_delete",
            "sandbox_bash"
        ]
    );
    let bash = SANDBOX_TOOL_SCHEMAS.last().unwrap();
    assert!(
        bash["parameters"]["properties"]["timeout"]
            .get("default")
            .is_none()
    );
    let mut copy = SANDBOX_TOOL_SCHEMAS.clone();
    copy.clear();
    assert_eq!(SANDBOX_TOOL_SCHEMAS.len(), 10);
    assert!(SANDBOX_PREAMBLE.contains("Do not assume it is isolated from the host machine."));
}

#[tokio::test]
async fn repeated_sandbox_declarations_do_not_reject_the_executor() {
    for streaming in [false, true] {
        let sandbox = Arc::new(Recording::new(json!([
            {"name": "sandbox_bash"}, {"name": "sandbox_bash"}
        ])));
        let agent = channel(
            model(streaming, "sandbox_bash"),
            Arc::<Recording>::clone(&sandbox),
        );
        let output = answer(&agent).await;
        agent.close().await;
        assert_eq!(
            *sandbox.calls.lock().unwrap(),
            [json!({"command":"bash","arguments":{}})]
        );
        let calls = ends(&output);
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls.first().unwrap().outcome,
            Some(ToolCallOutcome::Served)
        );
    }
}
