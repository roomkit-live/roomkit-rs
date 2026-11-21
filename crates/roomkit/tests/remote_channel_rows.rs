//! The owner's private row query for a turn on a different executor (RFC §10.6.5).
#![expect(clippy::unwrap_used, reason = "invalid fixtures fail the test")]

#[path = "strategies/faulty_store.rs"]
mod faulty_store;

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, EventId, EventSource,
    EventType, InboundMessage, RoomContext, RoomEvent, TextContent, ToolCallContent,
};
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions,
};
use roomkit::turns::{TurnBackend, TurnBackendHandle};
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, ConversationStore,
    CreateRoomOptions, InMemoryTurnBackend, ProcessInboundOptions, RoomKit, RoomKitOptions,
    TurnExecutor, TurnExecutorOptions,
};
use std::sync::{Arc, Mutex, atomic::Ordering};

#[derive(Clone)]
struct Reader {
    id: ChannelId,
    seen: Arc<Mutex<Vec<Vec<EventId>>>>,
}

impl Channel for Reader {
    fn channel_id(&self) -> ChannelId {
        self.id.clone()
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }
    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }
    fn handle_inbound(
        &self,
        msg: InboundMessage,
        ctx: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(roomkit::default_inbound_event(self, &msg, &ctx)))
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
        event: RoomEvent,
        binding: ChannelBinding,
        ctx: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let host = roomkit::current_turn_host().unwrap();
        self.seen
            .lock()
            .unwrap()
            .push(host.channel_rows().iter().map(|r| r.id.clone()).collect());
        EchoAgent::new(self.id.clone(), "answer", EchoAgentOptions::default())
            .on_event(event, binding, ctx)
            .await
    }
}

async fn remote_turn(fail: bool) -> (usize, Vec<Vec<EventId>>, EventId) {
    let backend = Arc::new(InMemoryTurnBackend::new());
    let kit = RoomKit::new(RoomKitOptions {
        turn_backend: Some(TurnBackendHandle::new(Arc::<InMemoryTurnBackend>::clone(
            &backend,
        ))),
        ..RoomKitOptions::with_store(faulty_store::AbstractOnly::default())
    });
    let agent = ChannelId::new("agent").unwrap();
    let src = ChannelId::new("text").unwrap();
    kit.register_channel(ChannelDeclaration::new(agent.clone()))
        .unwrap();
    kit.register_channel(RecordingTransport::new(
        src.clone(),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    let seen = Arc::<Mutex<Vec<Vec<EventId>>>>::default();
    let reader = Reader {
        id: agent.clone(),
        seen: Arc::clone(&seen),
    };
    let worker = Arc::new(
        TurnExecutor::new(
            move |_| std::future::ready(Ok(Some(reader.clone()))),
            TurnExecutorOptions {
                hosts: Some(["agent".into()].into()),
                ..Default::default()
            },
        )
        .unwrap(),
    );
    backend
        .serve(Arc::<TurnExecutor>::clone(&worker))
        .await
        .unwrap();
    let room = kit
        .create_room(CreateRoomOptions::default())
        .await
        .unwrap()
        .id;
    kit.attach_channel(&room, &src, AttachChannelOptions::default())
        .await
        .unwrap();
    kit.attach_channel(
        &room,
        &agent,
        AttachChannelOptions {
            category: Some(ChannelCategory::Intelligence),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut row = RoomEvent::new(
        room.clone(),
        EventSource::new(agent, ChannelType::AI),
        ToolCallContent::new("lookup", "a"),
    );
    row.r#type = EventType::ToolCallStart;
    let id = kit
        .store()
        .add_event_auto_index(&room, row)
        .await
        .unwrap()
        .id;
    kit.store().tool_reads.store(0, Ordering::Relaxed);
    kit.store().fail_tool_read.store(fail, Ordering::Relaxed);
    let result = kit
        .process_inbound(
            InboundMessage::new(src, "user", TextContent::new("hi")),
            ProcessInboundOptions {
                room_id: Some(room),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(result.error, None);
    let count = kit.store().tool_reads.load(Ordering::Relaxed);
    let rows = seen.lock().unwrap().clone();
    kit.close().await.unwrap();
    worker.close().await;
    (count, rows, id)
}

#[tokio::test(start_paused = true)]
async fn the_remote_executor_gets_the_rows_from_one_read() {
    let (count, rows, id) = remote_turn(false).await;
    assert_eq!(rows, [vec![id]]);
    assert_eq!(count, 1);
}

#[tokio::test(start_paused = true)]
async fn one_failed_read_costs_the_remote_turn_its_rows() {
    let (count, rows, _) = remote_turn(true).await;
    assert_eq!(rows, [Vec::<EventId>::new()]);
    assert_eq!(count, 1);
}
