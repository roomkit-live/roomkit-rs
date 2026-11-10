//! The exhausted named path refuses before looking up its channel, and
//! still reports the channel and room on its inbound span.

use super::*;
use pretty_assertions::assert_eq;
use roomkit::InboundResult;
use roomkit::telemetry::{MockTelemetryProvider, TelemetryConfig};
use std::sync::atomic::AtomicUsize;

fn looping(
    kit: &RoomKit,
    target: &'static str,
    refused: Arc<Mutex<Vec<InboundResult>>>,
) -> Scripted {
    let weak = kit.downgrade();
    let calls = Arc::new(AtomicUsize::new(0));
    Scripted::transport(
        "loop",
        Arc::new(move |_| {
            let (kit, calls, refused) = (
                weak.upgrade().unwrap(),
                Arc::clone(&calls),
                Arc::clone(&refused),
            );
            Box::pin(async move {
                // Bound a missing cap independently of the contract's ten passes.
                if calls.fetch_add(1, AtomicOrdering::SeqCst) >= 20 {
                    return Ok(ChannelOutput::empty());
                }
                let result = kit
                    .process_inbound(message("again"), in_room(room_id()))
                    .await
                    .unwrap();
                if result.reason.as_deref() == Some("reentry_loop_cap") {
                    let message = InboundMessage::new(channel(target), "u", TextContent::new("x"));
                    let result = kit
                        .process_inbound(message, in_room(room_id()))
                        .await
                        .unwrap();
                    refused.lock().unwrap().push(result);
                }
                Ok(ChannelOutput::empty())
            })
        }),
    )
}

// Python: tests/test_reentrance.py::TestACallFromADelivery::test_a_call_naming_its_room_past_the_budget_is_refused_before_anything_else
#[tokio::test]
async fn an_exhausted_named_call_refuses_before_channel_lookup_and_keeps_its_span() {
    for target in ["ghost", "late"] {
        let telemetry = Arc::new(MockTelemetryProvider::default());
        let (kit, _) = room_with(RoomKitOptions {
            max_chain_depth: 1,
            telemetry: Some(TelemetryConfig::from(Arc::clone(&telemetry))),
            ..Default::default()
        })
        .await;
        kit.register_channel(RecordingTransport::new(
            channel("late"),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        ))
        .unwrap();
        let refused = Arc::new(Mutex::new(Vec::new()));
        kit.register_channel(looping(&kit, target, Arc::clone(&refused)))
            .unwrap();
        kit.attach_channel(
            &room_id(),
            &channel("loop"),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
        send(&kit, "hello").await.unwrap();
        tokio::time::timeout(BOUND, roomkit::testing::idle(&kit))
            .await
            .unwrap();
        let answers = std::mem::take(&mut *refused.lock().unwrap());
        assert_eq!(answers.len(), 1, "{target}");
        let answer = &answers[0];
        assert!(answer.blocked, "{target}");
        assert_eq!(answer.reason.as_deref(), Some("reentry_loop_cap"));
        assert!(answer.event.is_none());
        assert!(
            kit.store()
                .get_binding(&room_id(), &channel(target))
                .await
                .unwrap()
                .is_none()
        );
        let expected: Vec<_> = std::iter::once("hello".to_owned())
            .chain(std::iter::repeat_n("again".to_owned(), 10))
            .collect();
        assert_eq!(message_texts(&kit).await, expected);
        let spans: Vec<_> = telemetry
            .completed_spans()
            .into_iter()
            .filter(|span| {
                span.name == "framework.inbound" && span.channel_id.as_deref() == Some(target)
            })
            .collect();
        assert_eq!(spans.len(), 1, "{target}: {spans:?}");
        assert_eq!(spans[0].room_id.as_deref(), Some(room_id().as_str()));
        assert_eq!(spans[0].attributes.get("blocked"), Some(&true.into()));
        kit.close().await.unwrap();
    }
}
