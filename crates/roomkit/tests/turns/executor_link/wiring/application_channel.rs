//! A third-party intelligence channel can use the executor's response observer.
use super::{Env, link, worker};
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, InboundMessage,
    RoomContext, RoomEvent,
};
use roomkit::turns::{HookOptions, TurnWiring};
use roomkit::{Channel, ChannelError, HookExecution, HookTrigger};
use roomkit_ai::AiResponseEvent;
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct Agent {
    id: ChannelId,
    wiring: Mutex<Option<TurnWiring>>,
}

impl Channel for Agent {
    fn channel_id(&self) -> ChannelId {
        self.id.clone()
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::new("application").unwrap()
    }
    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }
    fn set_turn_wiring(&self, wiring: TurnWiring) {
        *self.wiring.lock().unwrap() = Some(wiring);
    }
    fn turn_wiring(&self) -> Option<TurnWiring> {
        self.wiring.lock().unwrap().clone()
    }
    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }
    fn deliver(
        &self,
        _: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }
    fn on_event(
        &self,
        event: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let mut response = AiResponseEvent::new(self.id.clone(), "custom response");
        response.room_id = Some(event.room_id.clone());
        let wiring = self.turn_wiring().unwrap();
        let hooks = wiring.channel_hooks(&event.room_id, &self.id, &self.channel_type());
        hooks.observe(&response, || {});
        std::future::ready(Ok(ChannelOutput::empty()))
    }
}

#[tokio::test]
async fn an_application_intelligence_channel_reports_its_response_after_wiring() {
    let (link, _, _) = link(false);
    let req = &link.request;
    let worker = worker();
    worker.use_wiring(Arc::new(Env::new(req.context.clone())));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    worker
        .hook(
            HookTrigger::OnAiResponse,
            move |response: AiResponseEvent, room: RoomContext| {
                let tx = tx.clone();
                async move {
                    tx.send((response, room.room.id)).unwrap();
                }
            },
            HookOptions {
                execution: HookExecution::Async,
                ..Default::default()
            },
        )
        .unwrap();
    let agent = Agent {
        id: req.channel_id.clone(),
        wiring: Mutex::new(None),
    };
    worker.wire(&agent).unwrap();
    agent
        .on_event(req.event.clone(), req.binding.clone(), req.context.clone())
        .await
        .unwrap();
    let (response, room) = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(room, req.room_id);
    assert_eq!(response.channel_id, req.channel_id);
    assert_eq!(response.response_content, "custom response");
    worker.close().await;
}
