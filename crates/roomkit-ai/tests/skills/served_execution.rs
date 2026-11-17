//! A refused execution never reaches a host tool or the script executor (RFC §24.7).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use roomkit::Channel;
use roomkit::model::Metadata;
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AddMcpServerOptions, AiChannel, AiChannelOptions, AiTool, ApproveExecution, ScriptExecutor,
    ScriptResult, Skill, SkillRegistry, ToolError, ToolResult, Tools,
};

use super::script_execution::{Model, turn};
use super::served_registry::Server;

#[derive(Clone, Copy, Debug)]
enum Approval {
    Absent,
    Refused,
    Failed,
    Approved,
}

impl Approval {
    fn callback(self) -> Option<ApproveExecution> {
        if matches!(self, Self::Absent) {
            return None;
        }
        Some(ApproveExecution::new(move |_| async move {
            match self {
                Self::Failed => Err(std::io::Error::other("approval failed").into()),
                Self::Approved => Ok(true),
                Self::Absent | Self::Refused => Ok(false),
            }
        }))
    }
}

#[derive(Default)]
struct ScriptCalls(AtomicUsize);
impl ScriptExecutor for ScriptCalls {
    fn execute<'a>(
        &'a self,
        _: &'a Skill,
        _: &'a str,
        _: Option<&'a Metadata>,
    ) -> Pin<Box<dyn Future<Output = Result<ScriptResult, ToolError>> + Send + 'a>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(ScriptResult::new(0)) })
    }
}

fn tools(executed: Arc<AtomicUsize>, read: Arc<AtomicUsize>) -> Tools {
    Tools::new()
        .tool(
            AiTool::new("run_python", "Run code").unwrap(),
            move |_, _| {
                executed.fetch_add(1, Ordering::SeqCst);
                async { Ok(ToolResult::Text("ran".into())) }
            },
        )
        .tool(
            AiTool::new("weather", "Read weather").unwrap(),
            move |_, _| {
                read.fetch_add(1, Ordering::SeqCst);
                async { Ok(ToolResult::Text("sunny".into())) }
            },
        )
}

// Python: test_mcp_skills_execution_gate.py's absent/denied/failed and host-tool witnesses.
// Python: test_mcp_skills_text_door.py::test_a_served_script_is_never_run.
#[tokio::test]
async fn served_execution_reaches_only_approved_host_tools_and_never_a_script_executor() {
    for streaming in [false, true] {
        for approval in [
            Approval::Absent,
            Approval::Refused,
            Approval::Failed,
            Approval::Approved,
        ] {
            counters(streaming, approval).await;
        }
    }
}

async fn counters(streaming: bool, approval: Approval) {
    let registry = SkillRegistry::default();
    registry
        .add_mcp_server(
            Arc::new(Server::new()),
            "docs",
            AddMcpServerOptions {
                approve_execution: approval.callback(),
                execution_tools: vec!["run_*".into()],
                ..AddMcpServerOptions::default()
            },
        )
        .await
        .unwrap();
    let executed = Arc::new(AtomicUsize::new(0));
    let read = Arc::new(AtomicUsize::new(0));
    let scripts = Arc::new(ScriptCalls::default());
    let model = Model {
        inner: ScriptedProvider::new([
            ScriptedRound::calling("", [("a", "activate_skill", r#"{"name":"docs:guide"}"#)]),
            ScriptedRound::calling("", [("e", "run_python", "{}")]),
            ScriptedRound::calling(
                "",
                [(
                    "s",
                    "run_skill_script",
                    r#"{"skill_name":"docs:guide","script_name":"build.sh"}"#,
                )],
            ),
            ScriptedRound::calling("", [("r", "weather", "{}")]),
            ScriptedRound::answering("done"),
        ]),
        streaming,
    };
    let agent = AiChannel::new(
        "ai".parse().unwrap(),
        model.clone(),
        AiChannelOptions {
            skills: Some(registry),
            tools: tools(Arc::clone(&executed), Arc::clone(&read)).into(),
            script_executor: Some(Arc::<ScriptCalls>::clone(&scripts)),
            ..AiChannelOptions::default()
        },
    )
    .unwrap();
    turn(agent.clone()).await;
    agent.close().await;
    assert_eq!(read.load(Ordering::SeqCst), 1, "{streaming}/{approval:?}");
    assert_eq!(
        executed.load(Ordering::SeqCst),
        usize::from(matches!(approval, Approval::Approved)),
        "{streaming}/{approval:?}"
    );
    assert_eq!(
        scripts.0.load(Ordering::SeqCst),
        0,
        "{streaming}/{approval:?}"
    );
    let requests = model.inner.requests();
    assert_eq!(requests.len(), 5);
    let script_result = requests
        .get(3)
        .unwrap()
        .messages
        .iter()
        .flat_map(roomkit_ai::AiMessage::tool_results)
        .find(|result| result.name == "run_skill_script")
        .unwrap()
        .as_text();
    let expected = if matches!(approval, Approval::Approved) {
        "runs nothing"
    } else {
        "has not approved"
    };
    assert!(script_result.contains(expected), "{script_result}");
}
