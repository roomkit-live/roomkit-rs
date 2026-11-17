//! Callback and observer teardown use their own tracked units.
use super::reports::room;
use super::*;
use roomkit::model::InboundMessage;
use roomkit::{
    CancelOptions, HookExecution, HookOptions, HookTrigger, ProcessInboundOptions, ToolCallEvent,
};

// The kit tracker gives recovery its own unit: closing from that callback must
// spare that live unit, rather than the dropped producer's old marks.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_recovery_callback_can_close_its_kit() {
    for streaming in [false, true] {
        let handler = Arc::new(Handler {
            hold: true,
            ..Handler::default()
        });
        handler.release.notify_one();
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let agent = AiChannel::new(
            "ai".parse().unwrap(),
            model(streaming, AiToolCall::new("c1", "remote")),
            AiChannelOptions {
                memory: Some(Arc::new(recovery::OwnedMemory(Arc::clone(&closed)))),
                external_tool_handler: Some(Arc::<Handler>::clone(&handler)),
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        let kit = room(agent).await;
        let own_kit = kit.clone();
        *handler.close.lock().unwrap() = Some(Box::pin(async move {
            own_kit.close().await.unwrap();
        }));
        let sent = kit
            .process_inbound(
                InboundMessage::new("sms".parse().unwrap(), "u", TextContent::new("go")),
                ProcessInboundOptions {
                    room_id: Some("r".parse().unwrap()),
                    defer_delivery: true,
                    ..ProcessInboundOptions::default()
                },
            )
            .await
            .unwrap();
        bounded(handler.entered.notified()).await;
        bounded(sent.delivery.unwrap().cancel(CancelOptions::default()))
            .await
            .unwrap();
        bounded(handler.cancelled.notified()).await;
        let returned = tokio::time::timeout(Duration::from_secs(1), handler.closed.notified())
            .await
            .is_ok();
        // Join teardown even on the broken baseline before asserting the oracle.
        tokio::time::timeout(Duration::from_secs(7), kit.close())
            .await
            .unwrap()
            .unwrap();
        assert!(
            returned,
            "the callback's close must not await its own recovery task"
        );
        assert!(
            closed.load(Ordering::SeqCst),
            "close returned only after releasing owned memory"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_external_report_observer_can_close_its_kit() {
    for streaming in [false, true] {
        let handler = Arc::new(Handler::default());
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let agent = AiChannel::new(
            "ai".parse().unwrap(),
            model(streaming, AiToolCall::new("c1", "remote")),
            AiChannelOptions {
                memory: Some(Arc::new(recovery::OwnedMemory(Arc::clone(&closed)))),
                external_tool_handler: Some(Arc::<Handler>::clone(&handler)),
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        let kit = room(agent).await;
        let entered = Arc::new(Notify::new());
        let returned = Arc::new(Notify::new());
        let own_kit = kit.clone();
        let listening = Arc::clone(&entered);
        let completed = Arc::clone(&returned);
        kit.hook(
            HookTrigger::OnToolCall,
            move |_: ToolCallEvent, _| {
                let (kit, listening, completed) = (
                    own_kit.clone(),
                    Arc::clone(&listening),
                    Arc::clone(&completed),
                );
                async move {
                    listening.notify_one();
                    kit.close().await.unwrap();
                    completed.notify_one();
                }
            },
            HookOptions {
                name: "close from observer".into(),
                execution: HookExecution::Async,
                ..HookOptions::default()
            },
        )
        .unwrap();
        let sent = kit
            .process_inbound(
                InboundMessage::new("sms".parse().unwrap(), "u", TextContent::new("go")),
                ProcessInboundOptions {
                    room_id: Some("r".parse().unwrap()),
                    defer_delivery: true,
                    ..ProcessInboundOptions::default()
                },
            )
            .await
            .unwrap();
        bounded(entered.notified()).await;
        let reentered = tokio::time::timeout(Duration::from_secs(1), returned.notified())
            .await
            .is_ok();
        tokio::time::timeout(Duration::from_secs(7), kit.close())
            .await
            .unwrap()
            .unwrap();
        bounded(sent.delivery.unwrap().wait()).await.unwrap();
        assert!(
            reentered,
            "the external observer must close without waiting for itself"
        );
        assert!(closed.load(Ordering::SeqCst));
    }
}
