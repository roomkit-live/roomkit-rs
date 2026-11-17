//! A cut owes one framework event, with the original outcome if one was known.
use super::*;
use roomkit::model::{HookResult, InboundMessage};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CancelOptions, CreateRoomOptions, HookExecution, HookOptions,
    HookTrigger, ProcessInboundOptions, RoomKit, RoomKitOptions, ToolCallEvent,
};
use roomkit_ai::ServedCall;
use serde_json::{Value, json};

// Python: test_external_call_cut.py::test_a_call_the_provider_ran_cut_while_reported_keeps_its_outcome
// and test_a_call_cut_while_its_handler_decides_is_reported_cancelled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_reports_one_original_framework_outcome() {
    for streaming in [false, true] {
        for known in [None, Some(false), Some(true)] {
            cut_report(streaming, known).await;
        }
    }
}

async fn cut_report(streaming: bool, known_error: Option<bool>) {
    let handler = Arc::new(Handler {
        hold: true,
        ..Handler::default()
    });
    // The cancellation callback may finish: it delegates to the real bridge.
    handler.release.notify_one();
    let mut call = AiToolCall::new("c1", "remote");
    call.served = known_error.map(|is_error| ServedCall {
        result: "original outcome".into(),
        is_error,
    });
    let agent = AiChannel::new(
        "ai".parse().unwrap(),
        model(streaming, call),
        AiChannelOptions {
            external_tool_handler: Some(Arc::<Handler>::clone(&handler)),
            ..AiChannelOptions::default()
        },
    )
    .unwrap();
    let kit = room(agent).await;
    let reporting = Arc::new(Notify::new());
    let entered = Arc::clone(&reporting);
    kit.hook(
        HookTrigger::OnToolCall,
        move |_: ToolCallEvent, _| {
            entered.notify_one();
            std::future::pending::<Result<HookResult<ToolCallEvent>, roomkit::HookError>>()
        },
        HookOptions {
            name: "held report".into(),
            timeout: Duration::from_secs(60),
            ..HookOptions::default()
        },
    )
    .unwrap();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let capture = Arc::clone(&observed);
    kit.hook(
        HookTrigger::OnToolCall,
        move |event: ToolCallEvent, _| {
            let claim =
                roomkit_ai::turn_report_claim(&event.tool_call_id, event.channel_id.as_ref())
                    .map(|claim| claim());
            capture.lock().unwrap().push((event, claim));
            std::future::ready(())
        },
        HookOptions {
            name: "audit".into(),
            execution: HookExecution::Async,
            ..HookOptions::default()
        },
    )
    .unwrap();
    let framework = Arc::new(Mutex::new(Vec::new()));
    let capture = Arc::clone(&framework);
    kit.on("tool_call", move |event| {
        capture.lock().unwrap().push(event.data);
        std::future::ready(())
    })
    .unwrap();
    let sent = kit
        .process_inbound(
            InboundMessage::new("sms".parse().unwrap(), "u", TextContent::new("Go")),
            ProcessInboundOptions {
                room_id: Some("r".parse().unwrap()),
                defer_delivery: true,
                ..ProcessInboundOptions::default()
            },
        )
        .await
        .unwrap();
    if known_error.is_some() {
        bounded(reporting.notified()).await;
    } else {
        bounded(handler.entered.notified()).await;
    }
    bounded(sent.delivery.unwrap().cancel(CancelOptions::default()))
        .await
        .unwrap();
    bounded(roomkit::testing::idle(&kit)).await;
    bounded(kit.close()).await.unwrap();
    assert_reports(
        &observed.lock().unwrap(),
        &framework.lock().unwrap(),
        known_error,
    );
}

fn assert_reports(
    observed: &[(ToolCallEvent, Option<bool>)],
    framework: &[Metadata],
    known_error: Option<bool>,
) {
    assert_eq!(observed.len(), 1);
    let (report, claim) = observed.first().unwrap();
    assert_eq!(
        *claim,
        Some(false),
        "the occurrence is claimed before observers run"
    );
    assert_eq!(report.tool_call_id, "c1");
    assert_eq!(report.is_error, known_error.unwrap_or(true));
    assert_eq!(report.cancelled, known_error.is_none());
    if known_error.is_some() {
        assert_eq!(report.result, Some(json!("original outcome")));
    } else {
        assert!(
            report
                .result
                .as_ref()
                .unwrap()
                .as_str()
                .unwrap()
                .contains("The turn ended before its result.")
        );
    }
    assert_eq!(framework.len(), 1);
    let event = framework.first().unwrap();
    assert_eq!(event.get("tool_call_id"), Some(&json!("c1")));
    assert_eq!(
        event
            .get("is_error")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        known_error.unwrap_or(true)
    );
    assert_eq!(
        event
            .get("cancelled")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        known_error.is_none()
    );
}

pub(super) async fn room<C: Channel + 'static>(agent: C) -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(agent).unwrap();
    kit.register_channel(RecordingTransport::new(
        "sms".parse().unwrap(),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    let room: roomkit::model::RoomId = "r".parse().unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    })
    .await
    .unwrap();
    for id in ["sms", "ai"] {
        kit.attach_channel(&room, &id.parse().unwrap(), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    kit
}
