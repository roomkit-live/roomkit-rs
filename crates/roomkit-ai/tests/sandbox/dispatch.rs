//! Python's `test_sandbox_dispatch.py`: a live catalogue changes dispatch within one turn.
use std::future::{pending, ready};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use roomkit::Channel;
use roomkit::model::{Metadata, ToolCallOutcome};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, SandboxExecutor, SandboxResult, ToolError, ToolHandler, ToolResult,
};
use serde_json::{Value, json};

use super::support::*;

pub(super) struct Changing {
    pub(super) calls: Mutex<Vec<Value>>,
    fails: bool,
    pending: bool,
    pub(super) entered: AtomicBool,
    pub(super) ended: AtomicBool,
    pub(super) force_failure: AtomicBool,
}
impl SandboxExecutor for Changing {
    fn tool_definitions(&self) -> Result<Vec<Metadata>, ToolError> {
        if self.force_failure.load(Ordering::SeqCst) {
            return Err(ToolError::raised_as("ConnectionError", "catalogue changed"));
        }
        if self.calls.lock().unwrap().is_empty() {
            Ok(vec![
                json!({"name":"sandbox_bash"}).as_object().unwrap().clone(),
            ])
        } else if self.fails {
            Err(ToolError::raised_as("ConnectionError", "catalogue changed"))
        } else {
            Ok(Vec::new())
        }
    }
    fn execute<'a>(
        &'a self,
        command: &'a str,
        arguments: Option<&'a Metadata>,
    ) -> ResultFuture<'a> {
        self.calls
            .lock()
            .unwrap()
            .push(json!({"command":command,"arguments":arguments}));
        if self.pending {
            return Box::pin(async move {
                let _end = Ended(&self.ended);
                self.entered.store(true, Ordering::SeqCst);
                pending().await
            });
        }
        Box::pin(ready(Ok(SandboxResult {
            output: "sandbox".into(),
            ..SandboxResult::new(0)
        })))
    }
}
struct Ended<'a>(&'a AtomicBool);
impl Drop for Ended<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
pub(super) struct Case {
    pub(super) sandbox: Arc<Changing>,
    host: Arc<Mutex<Vec<Value>>>,
    pub(super) model: Model,
    pub(super) agent: AiChannel<Model>,
}
impl Case {
    pub(super) fn new(streaming: bool, fails: bool, pending: bool) -> Self {
        let sandbox = Arc::new(Changing {
            calls: Mutex::default(),
            fails,
            pending,
            entered: AtomicBool::new(false),
            ended: AtomicBool::new(false),
            force_failure: AtomicBool::new(false),
        });
        let host = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&host);
        let model = Model {
            streaming,
            inner: ScriptedProvider::new([
                ScriptedRound::calling("", [("one", "sandbox_bash", r#"{"n":1}"#)]),
                ScriptedRound::calling("", [("two", "sandbox_bash", r#"{"n":2}"#)]),
                ScriptedRound::answering("done"),
            ]),
        };
        let agent = AiChannel::new(
            "ai".parse().unwrap(),
            model.clone(),
            AiChannelOptions {
                sandbox: Some(Arc::<Changing>::clone(&sandbox)),
                tool_handler: Some(ToolHandler::new(move |name, arguments| {
                    seen.lock()
                        .unwrap()
                        .push(json!({"name":name,"arguments":arguments}));
                    ready(Ok(ToolResult::Text("host".into())))
                })),
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        Self {
            sandbox,
            host,
            model,
            agent,
        }
    }
}
#[tokio::test]
async fn a_command_removed_during_a_turn_reaches_the_host() {
    for streaming in [false, true] {
        let case = Case::new(streaming, false, false);
        let output = answer(&case.agent).await;
        case.agent.close().await;
        assert_eq!(
            *case.sandbox.calls.lock().unwrap(),
            [json!({"command":"bash","arguments":{"n":1}})]
        );
        assert_eq!(
            *case.host.lock().unwrap(),
            [json!({"name":"sandbox_bash","arguments":{"n":2}})]
        );
        assert_eq!(case.model.inner.requests().len(), 3);
        let ends = ends(&output);
        assert_eq!(ends.len(), 2);
        let second = ends.last().unwrap();
        assert_eq!(second.result, "host");
        assert_eq!(second.outcome, Some(ToolCallOutcome::Served));
    }
}
#[tokio::test]
async fn a_catalogue_failure_at_a_command_gate_ends_the_turn_without_another_result() {
    for streaming in [false, true] {
        let case = Case::new(streaming, true, false);
        let output = answer(&case.agent).await;
        case.agent.close().await;
        // Standalone Rust buffers rows and carries an error after them; the kit streams the error.
        let error = output.error.as_ref().unwrap();
        assert_eq!(error.error_type, "ConnectionError");
        assert_eq!(error.message, "catalogue changed");
        assert_eq!(
            *case.sandbox.calls.lock().unwrap(),
            [json!({"command":"bash","arguments":{"n":1}})]
        );
        assert_eq!(*case.host.lock().unwrap(), Vec::<Value>::new());
        assert_eq!(case.model.inner.requests().len(), 2);
        let ends = ends(&output);
        assert_eq!(ends.len(), 1);
        assert_eq!(ends.first().unwrap().tool_id, "one");
    }
}

#[tokio::test]
async fn a_catalogue_gate_failure_drops_its_actually_pending_sibling() {
    for streaming in [false, true] {
        let mut case = Case::new(streaming, true, true);
        case.model.inner = ScriptedProvider::new([ScriptedRound::calling(
            "",
            [
                ("one", "sandbox_bash", r#"{"n":1}"#),
                ("two", "sandbox_bash", r#"{"n":2}"#),
            ],
        )]);
        case.agent = AiChannel::new(
            "ai".parse().unwrap(),
            case.model.clone(),
            AiChannelOptions {
                sandbox: Some(Arc::<Changing>::clone(&case.sandbox)),
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        let output = answer(&case.agent).await;
        assert_eq!(output.error.as_ref().unwrap().error_type, "ConnectionError");
        assert!(case.sandbox.entered.load(Ordering::SeqCst));
        assert!(case.sandbox.ended.load(Ordering::SeqCst));
        assert_eq!(
            *case.sandbox.calls.lock().unwrap(),
            [json!({"command":"bash","arguments":{"n":1}})]
        );
        assert_eq!(case.model.inner.requests().len(), 1);
        case.agent.close().await;
    }
}
