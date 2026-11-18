//! Public application wiring, including the independent worker's last room context.
use super::{Link, link};
use roomkit::model::{ChannelId, RoomContext, RoomEvent, RoomId};
use roomkit::testing::{EchoAgent, EchoAgentOptions};
use roomkit::turns::{ChannelControl, TurnFuture, WiringEnv};
use roomkit::{Channel, FrameworkEvent, RoomKit, RoomKitError, TurnExecutor, TurnExecutorOptions};
use roomkit_ai::testing::ScriptedProvider;
use roomkit_ai::{AiChannel, AiChannelOptions};
use std::sync::{Arc, Mutex};

fn worker() -> TurnExecutor {
    TurnExecutor::new(
        |id| async move {
            Ok(Some(EchoAgent::new(
                id,
                "done",
                EchoAgentOptions::default(),
            )))
        },
        TurnExecutorOptions::default(),
    )
    .unwrap()
}

async fn accepted(worker: &TurnExecutor, channel: &str, org: Option<&str>) -> RoomContext {
    let (mut link, mut frames, _controls) = link(false);
    let req = &mut Arc::get_mut(&mut link).unwrap().request;
    let id = ChannelId::new(channel).unwrap();
    req.turn_id = format!("turn-{channel}");
    req.channel_id = id.clone();
    req.binding.channel_id = id;
    req.context.room.organization_id = org.map(ToOwned::to_owned);
    req.context
        .room
        .metadata
        .insert("latest".into(), channel.into());
    let expected = req.context.clone();
    worker.take(Arc::<Link>::clone(&link)).unwrap();
    link.write.notify_one();
    while let Some(frame) = frames.recv().await {
        if matches!(frame, roomkit::turns::wire::Frame::End) {
            break;
        }
    }
    link.released.notified().await;
    expected
}

async fn detach(worker: &TurnExecutor, room: &RoomId, channel: &str) {
    worker
        .control(ChannelControl::RoomDetached {
            room_id: room.clone(),
            channel_id: ChannelId::new(channel).unwrap(),
            organization_id: None,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn wiring_retains_the_last_admitted_room_until_its_last_channel_detaches() {
    let worker = worker();
    let default = worker.wiring();
    let first = accepted(&worker, "one", None).await;
    let room = &first.room.id;
    assert_eq!(default.env.context(room, None).await.unwrap(), first);
    let second = accepted(&worker, "two", None).await;
    assert_eq!(default.env.context(room, None).await.unwrap(), second);
    accepted(&worker, "refused", Some("other")).await;
    assert_eq!(default.env.context(room, None).await.unwrap(), second);
    detach(&worker, room, "one").await;
    assert_eq!(default.env.context(room, None).await.unwrap(), second);
    detach(&worker, room, "two").await;
    assert!(matches!(
        default.env.context(room, None).await,
        Err(RoomKitError::RoomNotFound(_))
    ));
    worker.close().await;
}

struct Env {
    context: RoomContext,
    events: Mutex<Vec<String>>,
}

impl WiringEnv for Env {
    fn context<'a>(
        &'a self,
        _: &'a RoomId,
        _: Option<&'a RoomContext>,
    ) -> TurnFuture<'a, Result<RoomContext, RoomKitError>> {
        Box::pin(async { Ok(self.context.clone()) })
    }
    fn tool_rows<'a>(
        &'a self,
        _: &'a RoomId,
        _: &'a ChannelId,
        _: u64,
    ) -> TurnFuture<'a, Result<Vec<RoomEvent>, RoomKitError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
    fn history_rows<'a>(
        &'a self,
        _: &'a RoomId,
        _: &'a ChannelId,
        _: Option<u64>,
        _: u64,
    ) -> TurnFuture<'a, Result<Vec<RoomEvent>, RoomKitError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
    fn message_before<'a>(
        &'a self,
        _: &'a RoomId,
        _: u64,
    ) -> TurnFuture<'a, Result<Option<u64>, RoomKitError>> {
        Box::pin(async { Ok(None) })
    }
    fn emit(&self, event: FrameworkEvent) -> TurnFuture<'_, ()> {
        Box::pin(async move {
            self.events.lock().unwrap().push(event.r#type);
        })
    }
}

#[tokio::test]
async fn wiring_before_registration_retains_the_environment_selected_for_the_channel() {
    let (link, _, _) = link(false);
    let kit = RoomKit::new(roomkit::RoomKitOptions::default());
    let local = kit.turn_executor().wiring();
    let room = &link.request.room_id;
    kit.create_room(roomkit::CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(&local.env.context(room, None).await.unwrap().room.id, room);
    let select = || {
        Arc::new(Env {
            context: link.request.context.clone(),
            events: Mutex::default(),
        })
    };
    let first = select();
    let second = select();
    let channel = AiChannel::new(
        link.request.channel_id.clone(),
        ScriptedProvider::new([]),
        AiChannelOptions::default(),
    )
    .unwrap();
    kit.turn_executor().use_wiring(Arc::<Env>::clone(&first));
    kit.turn_executor().wire(&channel);
    kit.turn_executor().use_wiring(Arc::<Env>::clone(&second));
    channel
        .turn_wiring()
        .unwrap()
        .emit(FrameworkEvent::new("first"))
        .await;
    kit.register_channel(channel.clone()).unwrap();
    channel
        .turn_wiring()
        .unwrap()
        .emit(FrameworkEvent::new("second"))
        .await;
    assert_eq!(*first.events.lock().unwrap(), ["first"]);
    assert_eq!(*second.events.lock().unwrap(), ["second"]);
    kit.close().await.unwrap();
}

#[tokio::test]
async fn wiring_an_unregistered_ai_channel_activates_its_external_tool_gate() {
    use roomkit::model::{HookResult, Metadata};
    use roomkit::{HookTrigger, ToolCallEvent};
    use roomkit_ai::{ExternalToolHandler, PolicyExternalToolHandler, ProcessToolCallOptions};

    let (link, _, _) = link(false);
    let worker = worker();
    let env = Arc::new(Env {
        context: link.request.context.clone(),
        events: Mutex::default(),
    });
    worker.use_wiring(Arc::<Env>::clone(&env));
    worker
        .hook(
            HookTrigger::BeforeToolUse,
            |_: ToolCallEvent, ctx: RoomContext| async move {
                HookResult::block(
                    format!("approval in {}", ctx.room.id),
                    roomkit::model::hook::BlockOptions::default(),
                )
            },
            roomkit::turns::HookOptions::default(),
        )
        .unwrap();
    let handler = Arc::new(PolicyExternalToolHandler::default());
    let ai = AiChannel::new(
        link.request.channel_id.clone(),
        ScriptedProvider::new([]),
        AiChannelOptions {
            external_tool_handler: Some(Arc::<PolicyExternalToolHandler>::clone(&handler)),
            ..Default::default()
        },
    )
    .unwrap();
    let options = ProcessToolCallOptions {
        room_id: Some(link.request.room_id.clone()),
        ..Default::default()
    };
    assert!(
        handler
            .process_tool_call("search", &Metadata::new(), options.clone())
            .await
            .unwrap()
            .approved
    );
    worker.wire(&ai);
    let decision = handler
        .process_tool_call("search", &Metadata::new(), options)
        .await
        .unwrap();
    assert!(!decision.approved);
    assert!(decision.reason.contains("approval in r1"));
    assert_eq!(*env.events.lock().unwrap(), ["before_tool_use"]);
    worker.close().await;
}
