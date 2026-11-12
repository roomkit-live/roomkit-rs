//! Python: `tests/test_turns_async_steering.py`; public kit calls around a held write.

use crate::support::{channel, room};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CreateRoomOptions, ProcessInboundOptions, RoomKit,
    RoomKitOptions, SteerOptions,
    model::{
        Cancel, ChannelBinding, ChannelCategory, ChannelOutput, ChannelType, InboundMessage,
        RoomContext, RoomEvent, SteeringDirective, TextContent,
    },
    testing::{RecordingTransport, RecordingTransportOptions},
};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::sync::Notify;

const PATIENCE: Duration = Duration::from_secs(5);

#[derive(Default)]
struct State {
    started: Notify,
    advance: Notify,
    progressed: Notify,
    finish: Notify,
    steering: Notify,
    answer: Notify,
    stopped: Notify,
}
struct AwaitedSteering {
    state: Arc<State>,
    fails: bool,
}
struct Write(Arc<State>);
impl Drop for Write {
    fn drop(&mut self) {
        self.0.stopped.notify_one();
    }
}
impl Channel for AwaitedSteering {
    fn channel_id(&self) -> roomkit::model::ChannelId {
        channel("agent")
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }
    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }
    fn handle_inbound(
        &self,
        _: InboundMessage,
        _: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Err("no inbound".into()))
    }
    fn deliver(
        &self,
        _: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }
    async fn on_event(
        &self,
        _: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        self.state.started.notify_one();
        self.state.advance.notified().await;
        self.state.progressed.notify_one();
        self.state.finish.notified().await;
        Ok(ChannelOutput::empty())
    }
    async fn steer_turn_async(
        &self,
        _: &SteeringDirective,
        _: &str,
    ) -> Result<Option<usize>, ChannelError> {
        let _write = Write(Arc::clone(&self.state));
        self.state.steering.notify_one();
        self.state.answer.notified().await;
        if self.fails {
            return Err("steering write refused".into());
        }
        Ok(Some(1))
    }
}
async fn start(
    fails: bool,
) -> (
    Arc<RoomKit>,
    Arc<State>,
    tokio::task::JoinHandle<roomkit::InboundResult>,
) {
    let state = Arc::new(State::default());
    let kit = Arc::new(RoomKit::new(RoomKitOptions::default()));
    kit.register_channel(AwaitedSteering {
        state: Arc::clone(&state),
        fails,
    })
    .unwrap();
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "agent"] {
        kit.attach_channel(&room(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let running = tokio::spawn({
        let kit = Arc::clone(&kit);
        async move {
            kit.process_inbound(
                InboundMessage::new(channel("sms"), "alice", TextContent::new("hi")),
                ProcessInboundOptions {
                    room_id: Some(room()),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
        }
    });
    tokio::time::timeout(PATIENCE, state.started.notified())
        .await
        .unwrap();
    (kit, state, running)
}
fn steer(kit: &Arc<RoomKit>) -> tokio::task::JoinHandle<usize> {
    let kit = Arc::clone(kit);
    tokio::spawn(async move {
        kit.steer(
            &room(),
            &channel("agent"),
            Cancel::default(),
            SteerOptions::default(),
        )
        .await
        .unwrap()
    })
}

// Python: tests/test_turns_async_steering.py::test_awaited_steering_keeps_work_running_and_reports_its_result
#[tokio::test]
async fn test_awaited_steering_keeps_work_running_and_reports_its_result() {
    for fails in [false, true] {
        let (kit, state, running) = start(fails).await;
        let steering = steer(&kit);
        tokio::time::timeout(PATIENCE, state.steering.notified())
            .await
            .unwrap();
        assert!(!steering.is_finished());
        state.advance.notify_one();
        tokio::time::timeout(PATIENCE, state.progressed.notified())
            .await
            .unwrap();
        assert!(!steering.is_finished());
        state.answer.notify_one();
        assert_eq!(
            tokio::time::timeout(PATIENCE, steering)
                .await
                .unwrap()
                .unwrap(),
            usize::from(!fails)
        );
        state.finish.notify_one();
        assert!(
            tokio::time::timeout(PATIENCE, running)
                .await
                .unwrap()
                .unwrap()
                .error
                .is_none()
        );
        kit.close().await.unwrap();
    }
}

// Python: tests/test_turns_async_steering.py::test_executor_close_stops_an_awaited_steering_write
#[tokio::test]
async fn test_executor_close_stops_an_awaited_steering_write() {
    let (kit, state, running) = start(false).await;
    let steering = steer(&kit);
    tokio::time::timeout(PATIENCE, state.steering.notified())
        .await
        .unwrap();
    tokio::time::timeout(PATIENCE, kit.turn_executor().close())
        .await
        .unwrap();
    tokio::time::timeout(PATIENCE, state.stopped.notified())
        .await
        .unwrap();
    tokio::time::timeout(PATIENCE, running)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(PATIENCE, steering)
        .await
        .unwrap()
        .unwrap();
    kit.close().await.unwrap();
}
