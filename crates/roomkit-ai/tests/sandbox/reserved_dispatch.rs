//! A reserved tool returns before consulting the sandbox's dispatch a second time.

use std::future::ready;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use roomkit::Channel;
use roomkit::model::{Metadata, ToolCallOutcome};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiProvider, AiResponse, ProviderError, ProviderStream,
    SandboxExecutor, ToolError,
};
use serde_json::json;

use super::support::{Model, ResultFuture, answer, ends};

#[derive(Default)]
struct Catalogue {
    armed: AtomicBool,
    reads: AtomicUsize,
    executions: AtomicUsize,
}
impl SandboxExecutor for Catalogue {
    fn tool_definitions(&self) -> Result<Vec<Metadata>, ToolError> {
        if self.armed.load(Ordering::SeqCst) && self.reads.fetch_add(1, Ordering::SeqCst) >= 2 {
            return Err(ToolError::raised_as(
                "ConnectionError",
                "sandbox dispatch must not run",
            ));
        }
        Ok(vec![
            json!({"name":"sandbox_bash"}).as_object().unwrap().clone(),
        ])
    }
    fn execute<'a>(
        &'a self,
        _command: &'a str,
        _arguments: Option<&'a Metadata>,
    ) -> ResultFuture<'a> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        Box::pin(ready(Err(ToolError::raised_as(
            "AssertionError",
            "unexpected sandbox execution",
        ))))
    }
}
struct Arming {
    model: Model,
    catalogue: Arc<Catalogue>,
}
impl AiProvider for Arming {
    fn model_name(&self) -> &'static str {
        "reserved dispatch witness"
    }
    fn supports_structured_streaming(&self) -> bool {
        self.model.streaming
    }
    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        self.catalogue.armed.store(true, Ordering::SeqCst);
        self.model.generate(context).await
    }
    fn generate_structured_stream<'a>(&'a self, context: &'a AiContext) -> ProviderStream<'a> {
        self.catalogue.armed.store(true, Ordering::SeqCst);
        self.model.generate_structured_stream(context)
    }
}
// Python: test_sandbox_dispatch.py::test_a_reserved_tool_returns_before_sandbox_dispatch.
#[tokio::test]
async fn a_reserved_tool_returns_before_sandbox_dispatch() {
    for streaming in [false, true] {
        let catalogue = Arc::new(Catalogue::default());
        let model = Model {
            streaming,
            inner: ScriptedProvider::new([
                ScriptedRound::calling("", [("search", "find_tools", r#"{"query":"bash"}"#)]),
                ScriptedRound::answering("done"),
            ]),
        };
        let agent = AiChannel::new(
            "ai".parse().unwrap(),
            Arming {
                model: model.clone(),
                catalogue: Arc::clone(&catalogue),
            },
            AiChannelOptions {
                sandbox: Some(Arc::<Catalogue>::clone(&catalogue)),
                tool_search: Some(true),
                max_tool_rounds: 1,
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        let output = answer(&agent).await;
        agent.close().await;
        assert_eq!(catalogue.executions.load(Ordering::SeqCst), 0);
        let rows = ends(&output);
        assert_eq!(rows.len(), 1);
        let end = rows.first().unwrap();
        assert_eq!(end.outcome, Some(ToolCallOutcome::Served));
        assert!(end.result.to_string().contains("sandbox_bash"));
        assert_eq!(model.inner.requests().len(), 2);
        assert_eq!(catalogue.reads.load(Ordering::SeqCst), 2);
    }
}
