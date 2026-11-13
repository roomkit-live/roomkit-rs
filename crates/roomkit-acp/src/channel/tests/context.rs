//! Host callback admission and cancellation through the public channel API.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use roomkit::Channel;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelType, EventType,
    RoomContext, RoomEvent, Timestamp,
};
use tokio::sync::{Notify, mpsc};

use super::{PATIENCE, Piped, test_input};
use crate::{AcpChannel, AcpChannelOptions, AcpContextContributor};

fn arguments() -> (RoomEvent, ChannelBinding, RoomContext) {
    let input = test_input("room-1");
    let binding = ChannelBinding::at(
        ChannelId::new("agent").unwrap(),
        input.context.room.id.clone(),
        ChannelType::AI,
        ChannelCategory::Intelligence,
        ChannelDirection::Bidirectional,
        Timestamp::UNIX_EPOCH,
    );
    (input.trigger, binding, input.context)
}

fn channel(options: AcpChannelOptions<Piped>) -> AcpChannel<Piped> {
    AcpChannel::new(ChannelId::new("agent").unwrap(), "/srv", options).unwrap()
}

fn transport() -> (Piped, mpsc::UnboundedReceiver<tokio::io::DuplexStream>) {
    let (agents, opened) = mpsc::unbounded_channel();
    (
        Piped {
            agents,
            closed: Arc::default(),
            log: Arc::default(),
        },
        opened,
    )
}

// Python: tests/test_channels/test_acp.py::TestRoomCatchUp::test_the_window_follows_room_history
#[test]
fn test_the_window_follows_room_history() {
    for count in [0, 20, 120] {
        let (transport, _opened) = transport();
        let channel = channel(AcpChannelOptions {
            transport: Some(transport),
            room_history: count,
            ..AcpChannelOptions::default()
        });
        assert_eq!(
            channel.recent_events_window(),
            usize::try_from(count).unwrap()
        );
    }
    assert_eq!(AcpChannelOptions::<Piped>::default().room_history, 20);
    assert!(
        AcpChannelOptions::<Piped>::default()
            .context_contributor
            .is_none()
    );
}

// Python: tests/test_channels/test_acp.py::TestRoomCatchUp::test_a_negative_window_is_a_mistake_not_an_off_switch
#[test]
fn test_a_negative_window_is_a_mistake_not_an_off_switch() {
    let (transport, _opened) = transport();
    let result = AcpChannel::new(
        ChannelId::new("agent").unwrap(),
        "/srv",
        AcpChannelOptions {
            transport: Some(transport),
            room_history: -1,
            ..AcpChannelOptions::default()
        },
    );
    assert!(result.unwrap_err().to_string().contains("room_history"));
}

// Python: tests/test_channels/test_acp.py::TestHostContext::test_a_skipped_event_never_reaches_it
#[tokio::test]
async fn test_a_skipped_event_never_reaches_it() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let contributor = AcpContextContributor::new(move |_, _| {
        counted.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(vec!["policy".to_owned()]))
    });
    let (transport, mut opened) = transport();
    let channel = channel(AcpChannelOptions {
        transport: Some(transport),
        context_contributor: Some(contributor),
        ..AcpChannelOptions::default()
    });
    let (mut own, binding, context) = arguments();
    own.source.channel_id = ChannelId::new("agent").unwrap();
    assert!(
        !channel
            .on_event(own, binding.clone(), context.clone())
            .await
            .unwrap()
            .responded
    );
    let (mut tool, _, _) = arguments();
    tool.r#type = EventType::ToolCallStart;
    assert!(
        !channel
            .on_event(tool, binding, context)
            .await
            .unwrap()
            .responded
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(opened.try_recv().is_err());
    channel.close().await;
}

// Python: tests/test_channels/test_acp.py::TestHostContext::test_a_cancelled_contributor_cancels_the_turn
#[tokio::test]
async fn test_a_cancelled_contributor_cancels_the_turn() {
    let entered = Arc::new(Notify::new());
    let signalled = Arc::clone(&entered);
    let contributor = AcpContextContributor::new(move |_, _| {
        let entered = Arc::clone(&signalled);
        async move {
            entered.notify_one();
            std::future::pending().await
        }
    });
    let (transport, mut opened) = transport();
    let channel = channel(AcpChannelOptions {
        transport: Some(transport),
        context_contributor: Some(contributor),
        ..AcpChannelOptions::default()
    });
    let running = channel.clone();
    let task = tokio::spawn(async move {
        let (event, binding, context) = arguments();
        running.on_event(event, binding, context).await
    });
    tokio::time::timeout(PATIENCE, entered.notified())
        .await
        .unwrap();
    assert!(opened.try_recv().is_err());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(opened.try_recv().is_err());
    channel.close().await;
}
