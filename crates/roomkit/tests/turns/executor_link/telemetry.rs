//! A standalone worker records its own AI generation spans (RFC §10.6.9).
use std::sync::Arc;
use std::time::Duration;

use roomkit::telemetry::{
    EndSpanOptions, MockTelemetryProvider, SpanKind, StartSpanOptions, TelemetryProvider,
};
use roomkit::turns::wire::{Control, Frame};
use roomkit::{HookTrigger, RoomKitError, TurnExecutor, TurnExecutorOptions};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions, AiGenerationEvent};
use serde_json::{Value, json};

use super::{Link, link};

async fn finish_turn(
    link: &Link,
    mut frames: tokio::sync::mpsc::UnboundedReceiver<Frame>,
    controls: tokio::sync::mpsc::UnboundedSender<Control>,
) {
    let mut read = 0;
    while let Some(frame) = frames.recv().await {
        if matches!(frame, Frame::Item { .. }) {
            read += 1;
        }
        if matches!(
            frame,
            Frame::Output { streams: true, .. } | Frame::Item { .. }
        ) {
            controls.send(Control::Pull { count: 1, read }).unwrap();
        }
        if let Frame::HostCall {
            call_id, ref verb, ..
        } = frame
        {
            let result = match verb.as_str() {
                "room_tasks" => json!([]),
                "room_context" => serde_json::to_value(&link.request.context).unwrap(),
                other => {
                    assert_eq!(other, "room_vision");
                    Value::Null
                }
            };
            controls
                .send(Control::HostResult {
                    call_id,
                    result,
                    error: None,
                    record_changes: None,
                })
                .unwrap();
            continue;
        }
        if matches!(frame, Frame::End) {
            break;
        }
    }
    link.write.notify_one();
    link.released.notified().await;
}

async fn generation(parent: Option<&str>) {
    let (mut link, frames, controls) = link(false);
    Arc::get_mut(&mut link).unwrap().request.trace =
        parent.map(|id| [("span_id".to_owned(), id.to_owned())].into());
    let recorder = Arc::new(MockTelemetryProvider::default());
    let worker = TurnExecutor::new(
        |id| async move {
            let model = ScriptedProvider::new([ScriptedRound::writing(["Hello"])]);
            AiChannel::new(id, model, AiChannelOptions::default())
                .map(Some)
                .map_err(|err| RoomKitError::Value(err.to_string()))
        },
        TurnExecutorOptions {
            telemetry: Some(Arc::<MockTelemetryProvider>::clone(&recorder)),
            ..TurnExecutorOptions::default()
        },
    )
    .unwrap();
    worker
        .hook(
            HookTrigger::BeforeAiGeneration,
            |_: AiGenerationEvent, _| async {},
            roomkit::turns::HookOptions {
                name: "observe_generation".into(),
                ..roomkit::turns::HookOptions::default()
            },
        )
        .unwrap();
    worker.take(Arc::<Link>::clone(&link)).unwrap();
    tokio::time::timeout(Duration::from_secs(3), finish_turn(&link, frames, controls))
        .await
        .unwrap();
    worker.close().await;
    let spans = recorder.get_spans(SpanKind::LlmGenerate);
    assert_eq!(spans.len(), 1, "{spans:?}");
    let span = spans.first().unwrap();
    assert_eq!(span.parent_id.as_deref(), parent);
    assert_eq!(span.status, "ok");
    let hooks = recorder.get_spans(SpanKind::HookSync);
    assert!(
        hooks
            .iter()
            .any(|hook| hook.name == "hook.sync.observe_generation")
    );
}

#[tokio::test]
async fn external_worker_telemetry_continues_the_owners_broadcast() {
    let owner = MockTelemetryProvider::default();
    let broadcast = owner.start_span(
        SpanKind::Broadcast,
        "broadcast",
        StartSpanOptions::default(),
    );
    generation(Some(&broadcast)).await;
    owner.end_span(&broadcast, EndSpanOptions::default());
    assert_eq!(owner.get_spans(SpanKind::LlmGenerate), vec![]);
}

#[tokio::test]
async fn external_worker_telemetry_starts_a_generation_without_an_owner_span() {
    generation(None).await;
}
