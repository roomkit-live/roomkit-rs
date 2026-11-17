//! A served skill's execution approval precedes the real sandbox boundary.
use std::future::{Future, ready};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use roomkit::Channel;
use roomkit::model::Metadata;
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AddMcpServerOptions, AiChannel, AiChannelOptions, ApproveExecution, SandboxExecutor,
    SandboxResult, SkillRegistry, ToolError,
};
use serde_json::json;

use super::script_execution::{Model, turn};
use super::served_registry::Server;

#[derive(Default)]
struct Executor(AtomicUsize);
impl SandboxExecutor for Executor {
    fn tool_definitions(&self) -> Result<Vec<Metadata>, ToolError> {
        Ok(vec![
            json!({"name":"sandbox_bash"}).as_object().unwrap().clone(),
        ])
    }
    fn execute<'a>(
        &'a self,
        command: &'a str,
        arguments: Option<&'a Metadata>,
    ) -> Pin<Box<dyn Future<Output = Result<SandboxResult, ToolError>> + Send + 'a>> {
        assert_eq!(command, "bash");
        assert_eq!(arguments, Some(&Metadata::new()));
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(ready(Ok(SandboxResult::new(0))))
    }
}

// Python: test_mcp_skills_execution_gate.py's sandbox absent/denied/failed/approved and no-active cases.
#[tokio::test]
async fn only_approval_or_no_active_served_skill_reaches_the_sandbox() {
    for streaming in [false, true] {
        for decision in ["absent", "denied", "failed", "approved", "inactive"] {
            let registry = SkillRegistry::default();
            let asked = Arc::new(Mutex::new(Vec::new()));
            let seen = Arc::clone(&asked);
            let callback = (decision != "absent").then(|| {
                ApproveExecution::new(move |request| {
                    seen.lock().unwrap().push(request);
                    async move {
                        if decision == "failed" {
                            Err(std::io::Error::other("approval failure").into())
                        } else {
                            Ok(decision == "approved")
                        }
                    }
                })
            });
            registry
                .add_mcp_server(
                    Arc::new(Server::new()),
                    "docs",
                    AddMcpServerOptions {
                        approve_execution: callback,
                        ..AddMcpServerOptions::default()
                    },
                )
                .await
                .unwrap();
            let mut rounds = Vec::new();
            if decision != "inactive" {
                rounds.push(ScriptedRound::calling(
                    "",
                    [("a", "activate_skill", r#"{"name":"docs:guide"}"#)],
                ));
            }
            rounds.extend([
                ScriptedRound::calling("", [("s", "sandbox_bash", "{}")]),
                ScriptedRound::answering("done"),
            ]);
            let model = Model {
                inner: ScriptedProvider::new(rounds),
                streaming,
            };
            let executor = Arc::new(Executor::default());
            let agent = AiChannel::new(
                "ai".parse().unwrap(),
                model.clone(),
                AiChannelOptions {
                    skills: Some(registry),
                    sandbox: Some(Arc::<Executor>::clone(&executor)),
                    ..AiChannelOptions::default()
                },
            )
            .unwrap();
            turn(agent.clone()).await;
            agent.close().await;
            let allowed = matches!(decision, "approved" | "inactive");
            assert_eq!(
                executor.0.load(Ordering::SeqCst),
                usize::from(allowed),
                "{streaming}/{decision}"
            );
            let requests = model.inner.requests();
            let result = requests
                .last()
                .unwrap()
                .messages
                .iter()
                .flat_map(roomkit_ai::AiMessage::tool_results)
                .find(|part| part.name == "sandbox_bash")
                .unwrap();
            assert_eq!(result.is_error, !allowed, "{streaming}/{decision}");
            if !allowed {
                assert!(result.as_text().contains("has not approved"));
            }
            let requests = asked.lock().unwrap();
            assert_eq!(
                requests.len(),
                usize::from(matches!(decision, "denied" | "failed" | "approved"))
            );
            if let Some(request) = requests.first() {
                assert_eq!(request.tool, "sandbox_bash");
                assert_eq!(request.name, "docs:guide");
                assert_eq!(request.label, "docs");
                assert_eq!(request.uri, "skill://guide/SKILL.md");
                assert!(request.fingerprint.is_some());
            }
        }
    }
}

// Python: test_mcp_skills_execution_gate.py::test_an_approval_lets_it_run_and_holds_for_the_fingerprint.
#[tokio::test]
async fn sandbox_approval_is_reused_until_the_room_detaches() {
    for streaming in [false, true] {
        let registry = SkillRegistry::default();
        let approvals = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&approvals);
        registry
            .add_mcp_server(
                Arc::new(Server::new()),
                "docs",
                AddMcpServerOptions {
                    approve_execution: Some(ApproveExecution::new(move |_| {
                        count.fetch_add(1, Ordering::SeqCst);
                        async { Ok(true) }
                    })),
                    ..AddMcpServerOptions::default()
                },
            )
            .await
            .unwrap();
        let activate =
            || ScriptedRound::calling("", [("a", "activate_skill", r#"{"name":"docs:guide"}"#)]);
        let execute = || ScriptedRound::calling("", [("s", "sandbox_bash", "{}")]);
        let model = Model {
            inner: ScriptedProvider::new([
                activate(),
                execute(),
                ScriptedRound::answering("one"),
                execute(),
                ScriptedRound::answering("two"),
                activate(),
                execute(),
                ScriptedRound::answering("three"),
            ]),
            streaming,
        };
        let executor = Arc::new(Executor::default());
        let agent = AiChannel::new(
            "ai".parse().unwrap(),
            model,
            AiChannelOptions {
                skills: Some(registry),
                sandbox: Some(Arc::<Executor>::clone(&executor)),
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        turn(agent.clone()).await;
        turn(agent.clone()).await;
        assert_eq!(approvals.load(Ordering::SeqCst), 1);
        assert_eq!(executor.0.load(Ordering::SeqCst), 2);
        agent.on_room_detached("r".parse().unwrap()).await.unwrap();
        turn(agent.clone()).await;
        assert_eq!(approvals.load(Ordering::SeqCst), 2);
        assert_eq!(executor.0.load(Ordering::SeqCst), 3);
        agent.close().await;
    }
}
