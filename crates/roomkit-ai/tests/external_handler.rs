//! Public handler behavior before a channel registers its callbacks.

use roomkit::model::Metadata;
use roomkit_ai::{
    ExternalToolHandler, FireBeforeHookOptions, FireOnToolHookOptions, OnToolResultOptions,
    PolicyExternalToolHandler, ProcessToolCallOptions, ToolPolicy,
};

// Python: tests/test_external_tool_handler.py::TestExternalToolHandlerABC::test_start_stop_are_noops
// Python: tests/test_external_tool_handler.py::TestExternalToolHandlerABC::test_fire_before_hook_no_callback
// Python: tests/test_external_tool_handler.py::TestExternalToolHandlerABC::test_fire_on_tool_hook_no_callback
// Python: tests/test_external_tool_handler.py::TestPolicyExternalToolHandler::test_on_tool_result_no_hook
#[tokio::test]
async fn an_unregistered_handler_needs_no_service_or_hook_callback() {
    let handler = PolicyExternalToolHandler::default();
    handler.start().await.unwrap();
    let decision = handler
        .state()
        .fire_before_hook("Read", &Metadata::new(), FireBeforeHookOptions::default())
        .await;
    assert!(decision.allowed);
    assert!(decision.arguments.is_none());
    handler
        .state()
        .fire_on_tool_hook(
            "Read",
            &Metadata::new(),
            "result",
            FireOnToolHookOptions::default(),
        )
        .await;
    handler
        .on_tool_result(
            "Read",
            &Metadata::new(),
            "result",
            OnToolResultOptions::default(),
        )
        .await
        .unwrap();
    handler.stop().await.unwrap();
}

// Python: tests/test_external_tool_handler.py::TestPolicyExternalToolHandler::test_allow_by_policy
// Python: tests/test_external_tool_handler.py::TestPolicyExternalToolHandler::test_allow_list_policy
#[tokio::test]
async fn a_policy_applies_before_registration() {
    for policy in [
        ToolPolicy {
            deny: vec!["Bash".into()],
            ..Default::default()
        },
        ToolPolicy {
            allow: vec!["Read".into(), "Write".into()],
            ..Default::default()
        },
    ] {
        let handler =
            PolicyExternalToolHandler::new(roomkit_ai::PolicyExternalToolHandlerOptions {
                policy: Some(policy),
            });
        assert!(
            handler
                .process_tool_call("Read", &Metadata::new(), ProcessToolCallOptions::default())
                .await
                .unwrap()
                .approved
        );
        let decision = handler
            .process_tool_call("Bash", &Metadata::new(), ProcessToolCallOptions::default())
            .await
            .unwrap();
        assert!(!decision.approved);
        assert_eq!(
            decision.reason,
            "Tool 'Bash' is not permitted by the agent's tool policy."
        );
    }
    // roomkit.tools.PolicyExternalToolHandler.__init__(policy) = None
    assert!(
        PolicyExternalToolHandler::default()
            .process_tool_call("Bash", &Metadata::new(), ProcessToolCallOptions::default())
            .await
            .unwrap()
            .approved
    );
}
