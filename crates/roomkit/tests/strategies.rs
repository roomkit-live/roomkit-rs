//! A strategy's agents given live (RFC §19.7): what the shared scenarios,
//! whose agents are named by id, cannot give.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::{Future, ready};
use std::sync::Arc;

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, InboundMessage,
    RoomContext, RoomEvent, RoomId,
};
use roomkit::orchestration::{StrategyAgent, StrategyFuture};
use roomkit::{
    Channel, ChannelError, CreateRoomOptions, ListBindingsOptions, Orchestration, RoomKit,
    RoomKitOptions, default_inbound_event,
};

/// An intelligence channel that answers nothing.
#[derive(Clone)]
struct Quiet(&'static str);

impl Channel for Quiet {
    fn channel_id(&self) -> ChannelId {
        ChannelId::new(self.0).unwrap()
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }
}

/// A strategy whose agents are given live, set up nothing else.
struct Team(Vec<Quiet>);

impl Orchestration for Team {
    fn agents(&self) -> Vec<StrategyAgent> {
        self.0.iter().cloned().map(StrategyAgent::channel).collect()
    }

    fn install<'call>(
        &'call self,
        _kit: &'call RoomKit,
        _room_id: &'call RoomId,
    ) -> StrategyFuture<'call> {
        Box::pin(async { Ok(()) })
    }
}

async fn bound(kit: &RoomKit, room: &str) -> Vec<String> {
    let room = RoomId::new(room).unwrap();
    let mut bound: Vec<String> = kit
        .list_bindings(&room, ListBindingsOptions::default())
        .await
        .unwrap()
        .into_iter()
        .map(|binding| binding.channel_id.as_str().to_owned())
        .collect();
    bound.sort();
    bound
}

// Python: tests/orchestration/test_framework_orchestration.py::TestFrameworkOrchestration::test_agents_registered_once
#[tokio::test]
async fn agents_registered_once() {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(Quiet("shared")).unwrap();
    let team: Arc<dyn Orchestration> = Arc::new(Team(vec![Quiet("shared"), Quiet("other")]));

    for room in ["r1", "r2"] {
        let options = CreateRoomOptions {
            room_id: Some(RoomId::new(room).unwrap()),
            orchestration: Some(Some(Arc::clone(&team))),
            ..Default::default()
        };
        kit.create_room(options).await.unwrap();
    }

    let held: Vec<String> = kit
        .list_channels()
        .iter()
        .map(|channel| channel.channel_id().as_str().to_owned())
        .collect();
    assert_eq!(held, ["shared", "other"]);
    assert_eq!(bound(&kit, "r1").await, ["other", "shared"]);
    assert_eq!(bound(&kit, "r2").await, ["other", "shared"]);
    kit.close().await.unwrap();
}

#[path = "strategies/faulty_store.rs"]
mod faulty_store;

use std::sync::atomic::Ordering;
use std::time::Duration;

use roomkit::model::Metadata;
use roomkit::{
    GetRoomOptions, InstallStrategyOptions, RoomKitError, RoomStrategyOptions,
    UninstallStrategyOptions, UpdateRoomMetadataOptions,
};

use crate::faulty_store::AbstractOnly as Faulty;

fn r1() -> RoomId {
    RoomId::new("r1").unwrap()
}

async fn with_room<S: roomkit::ConversationStore>(kit: &RoomKit<S>) {
    let options = CreateRoomOptions {
        room_id: Some(r1()),
        ..Default::default()
    };
    kit.create_room(options).await.unwrap();
}

/// A strategy that sets up nothing.
struct Plain;

impl<S: roomkit::ConversationStore> Orchestration<S> for Plain {
    fn agents(&self) -> Vec<StrategyAgent> {
        Vec::new()
    }

    fn install<'call>(
        &'call self,
        _kit: &'call RoomKit<S>,
        _room_id: &'call RoomId,
    ) -> StrategyFuture<'call> {
        Box::pin(async { Ok(()) })
    }
}

/// A strategy whose install adds a room hook, then never ends.
struct Endless;

impl Orchestration for Endless {
    fn agents(&self) -> Vec<StrategyAgent> {
        Vec::new()
    }

    fn install<'call>(
        &'call self,
        kit: &'call RoomKit,
        room_id: &'call RoomId,
    ) -> StrategyFuture<'call> {
        Box::pin(async move {
            let options = roomkit::AddRoomHookOptions {
                name: "endless".to_owned(),
                ..Default::default()
            };
            kit.add_room_hook(
                room_id,
                roomkit::HookTrigger::BeforeBroadcast,
                roomkit::HookExecution::Sync,
                |_event: RoomEvent, _context: RoomContext| async {
                    roomkit::model::HookResult::allow(roomkit::model::hook::AllowOptions::default())
                },
                options,
            )
            .await?;
            std::future::pending::<()>().await;
            Ok(())
        })
    }
}

// Python: core/mixins/strategies.py::StrategyMixin._install_strategy, whose
// `except BaseException` takes back an install cut short.
#[tokio::test]
async fn an_install_cut_short_is_taken_back_and_frees_the_room() {
    let kit = RoomKit::new(RoomKitOptions::default());
    with_room(&kit).await;
    let endless: Arc<dyn Orchestration> = Arc::new(Endless);
    let room = r1();
    let install = kit.install_strategy(&room, endless, InstallStrategyOptions::default());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), install)
            .await
            .is_err()
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while kit
        .room_strategy(&r1(), RoomStrategyOptions::default())
        .is_some()
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the room stayed claimed"
        );
        tokio::task::yield_now().await;
    }
    assert_eq!(
        kit.hook_engine().room_hook_names(&r1()),
        Vec::<String>::new()
    );
    let plain: Arc<dyn Orchestration> = Arc::new(Plain);
    kit.install_strategy(&r1(), plain, InstallStrategyOptions::default())
        .await
        .unwrap();
    kit.close().await.unwrap();
}

/// A strategy whose install succeeds, then makes the room's bindings
/// unreadable.
struct BreaksReads;

impl Orchestration<Faulty> for BreaksReads {
    fn agents(&self) -> Vec<StrategyAgent> {
        Vec::new()
    }

    fn install<'call>(
        &'call self,
        kit: &'call RoomKit<Faulty>,
        _room_id: &'call RoomId,
    ) -> StrategyFuture<'call> {
        Box::pin(async move {
            kit.store().fail_bindings.store(true, Ordering::SeqCst);
            Ok(())
        })
    }
}

// Python: core/mixins/strategies.py::StrategyMixin.uninstall_strategy, which
// takes back a strategy whose install could not be recorded whole.
#[tokio::test]
async fn an_install_the_room_could_not_be_read_after_is_uninstalled() {
    let kit = RoomKit::new(RoomKitOptions::with_store(Faulty::default()));
    with_room(&kit).await;
    let breaks: Arc<dyn Orchestration<Faulty>> = Arc::new(BreaksReads);
    let installed = kit
        .install_strategy(&r1(), breaks, InstallStrategyOptions::default())
        .await;
    kit.store().fail_bindings.store(false, Ordering::SeqCst);

    assert!(matches!(
        installed,
        Err(RoomKitError::Store(_) | RoomKitError::Value(_))
    ));
    assert!(
        kit.uninstall_strategy(&r1(), UninstallStrategyOptions::default())
            .await
            .unwrap()
    );
    let plain: Arc<dyn Orchestration<Faulty>> = Arc::new(Plain);
    kit.install_strategy(&r1(), plain, InstallStrategyOptions::default())
        .await
        .unwrap();
    kit.close().await.unwrap();
}

/// A strategy whose install writes a metadata key, makes room writes fail,
/// then refuses the room.
struct WritesThenRefuses;

impl Orchestration<Faulty> for WritesThenRefuses {
    fn agents(&self) -> Vec<StrategyAgent> {
        Vec::new()
    }

    fn install<'call>(
        &'call self,
        kit: &'call RoomKit<Faulty>,
        room_id: &'call RoomId,
    ) -> StrategyFuture<'call> {
        Box::pin(async move {
            let metadata = Metadata::from_iter([("k".to_owned(), 1.into())]);
            kit.update_room_metadata(room_id, metadata, UpdateRoomMetadataOptions::default())
                .await?;
            kit.store().fail_updates.store(true, Ordering::SeqCst);
            Err(RoomKitError::Value("refused".to_owned()))
        })
    }
}

// Python: core/mixins/strategies.py::StrategyMixin._undo_install, which keeps
// what a failed install could not take back for uninstall_strategy.
#[tokio::test]
async fn a_failed_install_s_keys_are_taken_back_by_the_uninstall() {
    let kit = RoomKit::new(RoomKitOptions::with_store(Faulty::default()));
    with_room(&kit).await;
    let strategy: Arc<dyn Orchestration<Faulty>> = Arc::new(WritesThenRefuses);
    let installed = kit
        .install_strategy(&r1(), strategy, InstallStrategyOptions::default())
        .await;
    kit.store().fail_updates.store(false, Ordering::SeqCst);

    assert!(matches!(installed, Err(RoomKitError::Value(_))));
    assert!(
        kit.room_strategy(&r1(), RoomStrategyOptions::default())
            .is_some()
    );
    assert!(
        kit.uninstall_strategy(&r1(), UninstallStrategyOptions::default())
            .await
            .unwrap()
    );
    let room = kit
        .get_room(&r1(), GetRoomOptions::default())
        .await
        .unwrap();
    assert_eq!(room.metadata.get("k"), Some(&serde_json::Value::Null));
    kit.close().await.unwrap();
}

/// A strategy with one agent, named by its id.
struct WithAgent;

impl Orchestration<Faulty> for WithAgent {
    fn agents(&self) -> Vec<StrategyAgent> {
        vec![ChannelId::new("sales").unwrap().into()]
    }

    fn install<'call>(
        &'call self,
        _kit: &'call RoomKit<Faulty>,
        _room_id: &'call RoomId,
    ) -> StrategyFuture<'call> {
        Box::pin(async { Ok(()) })
    }
}

// Python: tests/orchestration/test_strategy_install.py::test_an_uninstall_that_cannot_finish_keeps_the_room_claimed
#[tokio::test]
async fn an_uninstall_that_cannot_finish_keeps_the_room_claimed() {
    let kit = RoomKit::new(RoomKitOptions::with_store(Faulty::default()));
    kit.register_channel(Quiet("sales")).unwrap();
    with_room(&kit).await;
    let strategy: Arc<dyn Orchestration<Faulty>> = Arc::new(WithAgent);
    kit.install_strategy(&r1(), strategy, InstallStrategyOptions::default())
        .await
        .unwrap();

    kit.store().fail_removes.store(true, Ordering::SeqCst);
    let failed = kit
        .uninstall_strategy(&r1(), UninstallStrategyOptions::default())
        .await;
    assert!(
        matches!(&failed, Err(RoomKitError::Runtime(message)) if message.contains("still binds"))
    );
    // Part of the strategy stays: the room is still its, never open to another.
    assert!(
        kit.room_strategy(&r1(), RoomStrategyOptions::default())
            .is_some()
    );
    let other: Arc<dyn Orchestration<Faulty>> = Arc::new(Plain);
    let refused = kit
        .install_strategy(&r1(), other, InstallStrategyOptions::default())
        .await;
    assert!(
        matches!(&refused, Err(RoomKitError::Value(message)) if message.contains("holds a strategy already"))
    );

    kit.store().fail_removes.store(false, Ordering::SeqCst);
    assert!(
        kit.uninstall_strategy(&r1(), UninstallStrategyOptions::default())
            .await
            .unwrap()
    );
    assert!(
        kit.room_strategy(&r1(), RoomStrategyOptions::default())
            .is_none()
    );
    let bindings = kit
        .list_bindings(&r1(), ListBindingsOptions::default())
        .await
        .unwrap();
    assert_eq!(bindings.len(), 0, "the agent it attached is detached");
    kit.close().await.unwrap();
}
