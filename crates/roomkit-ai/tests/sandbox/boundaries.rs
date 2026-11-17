//! Catalogue errors and Bash's own bound also apply outside a declared sandbox call.
use std::future::ready;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use roomkit::Channel;
use roomkit::model::Metadata;
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiError, AiTool, SandboxExecutor, SandboxResult, ToolError,
    ToolHandler, ToolResult,
};
use serde_json::json;
use tokio::sync::Notify;

use super::support::*;

struct LaterFailure(AtomicBool);
impl SandboxExecutor for LaterFailure {
    fn tool_definitions(&self) -> Result<Vec<Metadata>, ToolError> {
        if self.0.load(Ordering::SeqCst) {
            Err(ToolError::raised_as(
                "ConnectionError",
                "catalogue unavailable",
            ))
        } else {
            Ok(Vec::new())
        }
    }
    fn execute<'a>(&'a self, _: &'a str, _: Option<&'a Metadata>) -> ResultFuture<'a> {
        Box::pin(ready(Ok(SandboxResult::new(0))))
    }
}
#[tokio::test]
async fn a_later_catalogue_failure_keeps_its_class_and_original_error() {
    for streaming in [false, true] {
        let sandbox = Arc::new(LaterFailure(AtomicBool::new(false)));
        let model = model(streaming, "unused");
        let agent = channel(model.clone(), Arc::<LaterFailure>::clone(&sandbox));
        sandbox.0.store(true, Ordering::SeqCst);
        let error = try_answer(&agent).await.unwrap_err();
        let classed = error.downcast_ref::<roomkit::ClassedError>().unwrap();
        assert_eq!(classed.class(), "ConnectionError");
        assert!(
            matches!(classed.error().downcast_ref::<AiError>(),Some(AiError::SandboxDefinition{error:ToolError::Raised{kind,message}}) if kind=="ConnectionError" && message=="catalogue unavailable")
        );
        assert_eq!(model.inner.requests(), Vec::<roomkit_ai::AiContext>::new());
        agent.close().await;
    }
}

#[tokio::test(start_paused = true)]
async fn an_attached_sandbox_keeps_bash_unbounded_even_when_the_host_serves_it() {
    for streaming in [false, true] {
        let sandbox = Arc::new(Recording::new(json!([])));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let started = Arc::clone(&entered);
        let open = Arc::clone(&release);
        let agent = AiChannel::new(
            "ai".parse().unwrap(),
            model(streaming, "sandbox_bash"),
            AiChannelOptions {
                sandbox: Some(Arc::<Recording>::clone(&sandbox)),
                tools: vec![AiTool::new("sandbox_bash", "Host bash").unwrap().into()],
                tool_timeout: Some(Duration::from_millis(20)),
                tool_handler: Some(ToolHandler::new(move |name, arguments| {
                    assert_eq!(name, "sandbox_bash");
                    assert_eq!(arguments, Metadata::new());
                    let started = Arc::clone(&started);
                    let open = Arc::clone(&open);
                    async move {
                        started.notify_one();
                        open.notified().await;
                        Ok(ToolResult::Text("host ran".into()))
                    }
                })),
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        let task_agent = agent.clone();
        let mut task = tokio::spawn(async move { answer(&task_agent).await });
        bounded(entered.notified()).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(60), &mut task)
                .await
                .is_err()
        );
        release.notify_one();
        let output = bounded(task).await.unwrap();
        let calls = ends(&output);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls.first().unwrap().result, "host ran");
        assert!(sandbox.calls.lock().unwrap().is_empty());
        agent.close().await;
    }
}
