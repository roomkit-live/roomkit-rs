use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::Channel;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelOutput, ChannelType, EventSource, Metadata, Room,
    RoomContext, RoomEvent, TextContent, ToolCallContent,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiProvider, AiResponse, ProviderError, ProviderStream,
    SandboxExecutor, SandboxResult, ToolError,
};
use serde_json::{Value, json};

pub(super) type ResultFuture<'a> =
    Pin<Box<dyn Future<Output = Result<SandboxResult, ToolError>> + Send + 'a>>;
pub(super) type CloseFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

#[derive(Clone)]
pub(super) struct Model {
    pub(super) inner: ScriptedProvider,
    pub(super) streaming: bool,
}
impl AiProvider for Model {
    fn model_name(&self) -> &'static str {
        "sandbox witness"
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
pub(super) fn model(streaming: bool, name: &str) -> Model {
    Model {
        inner: ScriptedProvider::new([
            ScriptedRound::calling("", [("s1", name, "{}")]),
            ScriptedRound::answering("done"),
        ]),
        streaming,
    }
}
pub(super) fn answer<C: Channel>(
    channel: &C,
) -> Pin<Box<dyn Future<Output = ChannelOutput> + Send + '_>> {
    Box::pin(async move { try_answer(channel).await.unwrap() })
}
pub(super) fn try_answer<C: Channel>(
    channel: &C,
) -> Pin<Box<dyn Future<Output = Result<ChannelOutput, roomkit::ChannelError>> + Send + '_>> {
    Box::pin(async move {
        let room = roomkit::model::RoomId::new("r").unwrap();
        let event = RoomEvent::new(
            room.clone(),
            EventSource::new("web".parse().unwrap(), ChannelType::WEBHOOK),
            TextContent::new("go"),
        );
        let binding = ChannelBinding {
            category: ChannelCategory::Intelligence,
            ..ChannelBinding::new("ai".parse().unwrap(), room.clone(), ChannelType::AI)
        };
        bounded(channel.on_event(event, binding, RoomContext::new(Room::new(room)))).await
    })
}
pub(super) fn ends(output: &ChannelOutput) -> Vec<&ToolCallContent> {
    output
        .response_events
        .iter()
        .filter(|event| event.r#type == roomkit::model::EventType::ToolCallEnd)
        .filter_map(|event| {
            if let roomkit::model::EventContent::ToolCall(call) = &event.content {
                Some(call)
            } else {
                None
            }
        })
        .collect()
}
pub(super) async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .unwrap()
}
pub(super) struct Recording {
    pub(super) definitions: Mutex<Vec<Metadata>>,
    pub(super) calls: Mutex<Vec<Value>>,
    pub(super) failure: Option<ToolError>,
}
impl Recording {
    pub(super) fn new(definitions: Value) -> Self {
        Self {
            definitions: Mutex::new(serde_json::from_value(definitions).unwrap()),
            calls: Mutex::default(),
            failure: None,
        }
    }
}
impl SandboxExecutor for Recording {
    fn tool_definitions(&self) -> Result<Vec<Metadata>, ToolError> {
        match &self.failure {
            Some(error) => Err(error.clone()),
            None => Ok(self.definitions.lock().unwrap().clone()),
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
        Box::pin(std::future::ready(Ok(SandboxResult {
            output: "résultat".into(),
            error: "Unknown tool: frobnicate".into(),
            ..SandboxResult::new(7)
        })))
    }
}
pub(super) fn channel(model: Model, sandbox: Arc<dyn SandboxExecutor>) -> AiChannel<Model> {
    AiChannel::new(
        "ai".parse().unwrap(),
        model,
        AiChannelOptions {
            sandbox: Some(sandbox),
            tool_search: Some(false),
            ..AiChannelOptions::default()
        },
    )
    .unwrap()
}
