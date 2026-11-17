//! AI consumers of external handlers: native boundaries beyond the shared corpus.
#![expect(
    clippy::unwrap_used,
    reason = "test fixtures fail if their setup is invalid"
)]

#[path = "ai_external_handler/boundaries.rs"]
mod boundaries;
#[path = "ai_external_handler/errors.rs"]
mod errors;
#[path = "ai_external_handler/identity.rs"]
mod identity;
#[path = "ai_external_handler/recovery.rs"]
mod recovery;
#[path = "ai_external_handler/reentrant.rs"]
mod reentrant;
#[path = "ai_external_handler/reports.rs"]
mod reports;
#[path = "ai_external_handler/standalone_reports.rs"]
mod standalone_reports;
#[path = "../../roomkit/tests/support/warnings.rs"]
mod warnings;

use roomkit::Channel;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelOutput, ChannelType, EventContent, EventSource,
    EventType, Metadata, Room, RoomContext, RoomEvent, TextContent, ToolCallContent,
};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiResponse, AiToolCall, MockAiProvider, MockAiProviderOptions,
};
use roomkit_ai::{
    ExternalToolFuture, ExternalToolHandler, ExternalToolHandlerState, FireOnToolHookOptions,
    OnToolCancelledOptions, OnToolResultOptions, ProcessToolCallOptions, ToolDecision,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Default)]
struct Handler {
    state: ExternalToolHandlerState,
    decisions: AtomicUsize,
    starts: AtomicUsize,
    stops: AtomicUsize,
    reports: Mutex<Vec<String>>,
    result: String,
    detail: Option<String>,
    hold: bool,
    entered: Notify,
    cancelled: Notify,
    release: Notify,
    closed: Notify,
    close: Mutex<Option<std::pin::Pin<Box<dyn Future<Output = ()> + Send>>>>,
}
impl ExternalToolHandler for Handler {
    fn state(&self) -> &ExternalToolHandlerState {
        &self.state
    }
    fn start(&self) -> ExternalToolFuture<'_, ()> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Box::pin(std::future::ready(Ok(())))
    }
    fn stop(&self) -> ExternalToolFuture<'_, ()> {
        self.stops.fetch_add(1, Ordering::SeqCst);
        Box::pin(std::future::ready(Ok(())))
    }
    fn process_tool_call<'a>(
        &'a self,
        _: &'a str,
        _: &'a Metadata,
        _: ProcessToolCallOptions,
    ) -> ExternalToolFuture<'a, ToolDecision> {
        Box::pin(async move {
            self.decisions.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            if self.hold {
                std::future::pending::<()>().await;
            }
            Ok(ToolDecision {
                result: Some(self.result.clone()),
                ..ToolDecision::new(true)
            })
        })
    }
    fn on_tool_result<'a>(
        &'a self,
        name: &'a str,
        input: &'a Metadata,
        result: &'a str,
        options: OnToolResultOptions,
    ) -> ExternalToolFuture<'a, ()> {
        Box::pin(async move {
            self.reports.lock().unwrap().push(result.into());
            self.state
                .fire_on_tool_hook(
                    name,
                    input,
                    result,
                    FireOnToolHookOptions {
                        tool_call_id: options.tool_call_id,
                        room_id: options.room_id,
                        is_error: options.is_error || self.detail.is_some(),
                        error_detail: self.detail.clone(),
                        ..FireOnToolHookOptions::default()
                    },
                )
                .await;
            Ok(())
        })
    }
    fn on_tool_cancelled<'a>(
        &'a self,
        name: &'a str,
        input: &'a Metadata,
        options: OnToolCancelledOptions,
    ) -> ExternalToolFuture<'a, ()> {
        Box::pin(async move {
            self.cancelled.notify_one();
            let closer = self.close.lock().unwrap().take();
            if let Some(closer) = closer {
                closer.await;
                self.closed.notify_one();
            }
            self.release.notified().await;
            self.state.on_tool_cancelled(name, input, options).await;
            self.reports.lock().unwrap().push("cancelled".into());
            Ok(())
        })
    }
}

fn model(streaming: bool, call: AiToolCall) -> MockAiProvider {
    let mut response = AiResponse::new("done");
    response.tool_calls.push(call);
    MockAiProvider::new(MockAiProviderOptions {
        streaming,
        ai_responses: Some(vec![response]),
        ..MockAiProviderOptions::default()
    })
}

async fn turn<P: roomkit_ai::AiProvider + 'static>(agent: AiChannel<P>) -> ChannelOutput {
    let room = "r".parse().unwrap();
    let event = RoomEvent::new(
        room,
        EventSource::new("web".parse().unwrap(), ChannelType::WEBHOOK),
        TextContent::new("Go"),
    );
    let binding = ChannelBinding {
        category: ChannelCategory::Intelligence,
        ..ChannelBinding::new(
            "ai".parse().unwrap(),
            event.room_id.clone(),
            ChannelType::AI,
        )
    };
    let context = RoomContext::new(Room::new(event.room_id.clone()));
    agent.on_event(event, binding, context).await.unwrap()
}

fn ends(output: &ChannelOutput) -> Vec<ToolCallContent> {
    output
        .response_events
        .iter()
        .filter_map(|event| match (&event.r#type, &event.content) {
            (EventType::ToolCallEnd, EventContent::ToolCall(call)) => Some(call.clone()),
            _ => None,
        })
        .collect()
}

async fn bounded<F: Future>(work: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(3), work)
        .await
        .unwrap()
}
