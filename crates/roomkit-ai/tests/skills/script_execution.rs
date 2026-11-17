//! Native shutdown boundaries of the host's script executor.
use std::future::Future;
use std::pin::Pin;
#[cfg(unix)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelType, EventSource, Metadata, Room, RoomContext,
    RoomEvent, TextContent,
};
use roomkit::{Channel, RoomKit, RoomKitError, RoomKitOptions};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiProvider, AiResponse, ProviderError, ProviderStream,
    ScriptExecutor, ScriptResult, Skill, SkillMetadata, SkillRegistry, ToolError,
};
use tokio::sync::Notify;

use super::support::TempDir;

type ResultFuture<'a> = Pin<Box<dyn Future<Output = Result<ScriptResult, ToolError>> + Send + 'a>>;
type CloseFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

struct DefaultClose;
impl ScriptExecutor for DefaultClose {
    fn execute<'a>(
        &'a self,
        _: &'a Skill,
        _: &'a str,
        _: Option<&'a Metadata>,
    ) -> ResultFuture<'a> {
        Box::pin(std::future::ready(Ok(ScriptResult::new(7))))
    }
}

// Python: tests/skills/test_script_executor.py::test_script_result_and_optional_arguments_cross_the_tool_boundary
#[tokio::test]
async fn an_executor_inherits_a_no_op_close() {
    DefaultClose.close().await;
}

// Python: tests/skills/test_script_executor.py::test_script_result_and_optional_arguments_cross_the_tool_boundary
#[test]
fn script_results_require_a_code_and_preserve_independent_success() {
    let result: ScriptResult = serde_json::from_value(serde_json::json!({"exit_code": 7})).unwrap();
    assert_eq!(result, ScriptResult::new(7));
    assert!(serde_json::from_value::<ScriptResult>(serde_json::json!({})).is_err());
    let value = serde_json::json!({
        "exit_code": 0, "stdout": "été", "stderr": "warning",
        "success": false, "metadata": {"host": true}
    });
    let result: ScriptResult = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(result).unwrap(), value);
}

struct FailedClose;
impl ScriptExecutor for FailedClose {
    fn execute<'a>(
        &'a self,
        _: &'a Skill,
        _: &'a str,
        _: Option<&'a Metadata>,
    ) -> ResultFuture<'a> {
        Box::pin(std::future::ready(Ok(ScriptResult::new(0))))
    }
    #[expect(
        clippy::panic,
        reason = "the host's deliberate close failure is the test input"
    )]
    fn close(&self) -> CloseFuture<'_> {
        Box::pin(async { panic!("script executor cleanup failed") })
    }
}

fn failing_channel() -> AiChannel<ScriptedProvider> {
    AiChannel::new(
        "ai".parse().unwrap(),
        ScriptedProvider::new([]),
        AiChannelOptions {
            script_executor: Some(Arc::new(FailedClose)),
            ..AiChannelOptions::default()
        },
    )
    .unwrap()
}

// Python: tests/skills/test_script_executor.py::test_a_direct_channel_close_propagates_its_executor_failure
#[tokio::test]
async fn a_direct_channel_close_propagates_its_executor_failure() {
    let agent = failing_channel(); // An executor still closes without a registry.
    assert!(
        tokio::spawn(async move { agent.close().await })
            .await
            .unwrap_err()
            .is_panic()
    );
}

// Python: tests/skills/test_script_executor.py::test_a_kit_close_reports_its_script_executor_failure
#[tokio::test]
async fn a_kit_close_reports_its_script_executor_failure() {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(failing_channel()).unwrap();
    let error = kit.close().await.unwrap_err();
    assert!(matches!(error, RoomKitError::ExceptionGroup { failures }
        if failures.len() == 1 && failures.first().unwrap().1.contains("script executor cleanup failed")));
}

type Trace = Arc<Mutex<Vec<&'static str>>>;
struct Held {
    started: Arc<Notify>,
    trace: Trace,
}
struct Ended(Trace);
impl Drop for Ended {
    fn drop(&mut self) {
        self.0.lock().unwrap().push("call ended");
    }
}
impl ScriptExecutor for Held {
    fn execute<'a>(
        &'a self,
        _: &'a Skill,
        _: &'a str,
        _: Option<&'a Metadata>,
    ) -> ResultFuture<'a> {
        Box::pin(async move {
            let _ended = Ended(Arc::clone(&self.trace));
            self.started.notify_one();
            std::future::pending().await
        })
    }
    fn close(&self) -> CloseFuture<'_> {
        Box::pin(async {
            self.trace.lock().unwrap().push("executor closed");
        })
    }
}

#[derive(Clone)]
pub(super) struct Model {
    pub(super) inner: ScriptedProvider,
    pub(super) streaming: bool,
}
impl AiProvider for Model {
    fn model_name(&self) -> &'static str {
        "script witness"
    }
    fn supports_structured_streaming(&self) -> bool {
        self.streaming
    }
    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        self.inner.generate(context).await
    }
    fn generate_structured_stream<'a>(&'a self, context: &'a AiContext) -> ProviderStream<'a> {
        self.inner.generate_structured_stream(context)
    }
}

// Python: tests/skills/test_script_executor.py::test_a_channel_close_ends_its_script_before_closing_the_executor
#[tokio::test]
async fn a_channel_close_ends_its_script_before_closing_the_executor() {
    for streaming in [false, true] {
        tokio::time::timeout(Duration::from_secs(3), close_active(streaming))
            .await
            .unwrap();
    }
}

async fn close_active(streaming: bool) {
    let files = TempDir::new();
    files.file("local/scripts/build.sh", "source");
    let skills = script_registry(&files);
    let executor = Arc::new(Held {
        started: Arc::default(),
        trace: Arc::default(),
    });
    let model = script_model(streaming);
    let agent = AiChannel::new(
        "ai".parse().unwrap(),
        model.clone(),
        AiChannelOptions {
            skills: Some(skills),
            script_executor: Some(Arc::<Held>::clone(&executor)),
            ..AiChannelOptions::default()
        },
    )
    .unwrap();
    let task = tokio::spawn(turn(agent.clone()));
    executor.started.notified().await;
    agent.close().await;
    task.await.unwrap();
    assert_eq!(
        *executor.trace.lock().unwrap(),
        ["call ended", "executor closed"]
    );
    assert_eq!(model.inner.requests().len(), 1);
}

fn script_registry(files: &TempDir) -> SkillRegistry {
    let skills = SkillRegistry::default();
    skills
        .add(Skill {
            metadata: SkillMetadata::new("local", "Build"),
            instructions: "Build locally".into(),
            path: files.path().join("local"),
        })
        .unwrap();
    skills
}

#[cfg(unix)]
struct CountCalls(AtomicUsize);
#[cfg(unix)]
impl ScriptExecutor for CountCalls {
    fn execute<'a>(
        &'a self,
        _: &'a Skill,
        _: &'a str,
        _: Option<&'a Metadata>,
    ) -> ResultFuture<'a> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(std::future::ready(Ok(ScriptResult::new(0))))
    }
}

// Python: tests/test_skills_integration.py::TestRunScriptHandler::test_escaping_script_never_reaches_executor
#[cfg(unix)]
#[tokio::test]
async fn an_escaping_script_never_reaches_the_executor() {
    for streaming in [false, true] {
        let files = TempDir::new();
        let outside = files.file("outside", "private source");
        let scripts = files.dir("local/scripts");
        super::support::link(&outside, &scripts.join("build.sh"));
        let executor = Arc::new(CountCalls(AtomicUsize::new(0)));
        let model = script_model(streaming);
        let agent = AiChannel::new(
            "ai".parse().unwrap(),
            model.clone(),
            AiChannelOptions {
                skills: Some(script_registry(&files)),
                script_executor: Some(Arc::<CountCalls>::clone(&executor)),
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        turn(agent.clone()).await;
        agent.close().await;
        assert_eq!(executor.0.load(Ordering::SeqCst), 0);
        let requests = model.inner.requests();
        assert_eq!(requests.len(), 2);
        let result = requests[1]
            .messages
            .iter()
            .find_map(|message| message.tool_result())
            .unwrap();
        assert!(!result.is_error);
        assert_eq!(result.name, "run_skill_script");
        let value: serde_json::Value = serde_json::from_str(&result.as_text()).unwrap();
        assert!(value["error"].as_str().unwrap().contains("escapes"));
    }
}

fn script_model(streaming: bool) -> Model {
    Model {
        inner: ScriptedProvider::new([
            ScriptedRound::calling(
                "",
                [(
                    "s1",
                    "run_skill_script",
                    r#"{"skill_name":"local","script_name":"build.sh"}"#,
                )],
            ),
            ScriptedRound::answering("must not run"),
        ]),
        streaming,
    }
}

pub(super) async fn turn(agent: AiChannel<Model>) {
    let room = "r".parse().unwrap();
    let event = RoomEvent::new(
        room,
        EventSource::new("web".parse().unwrap(), ChannelType::WEBHOOK),
        TextContent::new("Build"),
    );
    let binding = ChannelBinding {
        category: ChannelCategory::Intelligence,
        ..ChannelBinding::new("ai".parse().unwrap(), "r".parse().unwrap(), ChannelType::AI)
    };
    agent
        .on_event(
            event,
            binding,
            RoomContext::new(Room::new("r".parse().unwrap())),
        )
        .await
        .unwrap();
}
