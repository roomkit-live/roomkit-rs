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

// Python: core/mixins/strategies.py::StrategyMixin._install_strategy, whose
// `except BaseException` takes back an install cut short in the install's
// task, under the room's lock, once the cut call's decided write landed.
#[tokio::test(start_paused = true)]
async fn a_cut_install_is_taken_back_once_its_decided_write_landed() {
    let kit = RoomKit::new(RoomKitOptions::with_store(Faulty::default()));
    kit.register_channel(Quiet("sales")).unwrap();
    with_room(&kit).await;
    let strategy: Arc<dyn Orchestration<Faulty>> = Arc::new(WithAgent);
    let held = kit.store().binding_gate.write().await;
    let room = r1();
    let install = kit.install_strategy(&room, strategy, InstallStrategyOptions::default());
    // Cut as the agent's binding is written: the write runs to its end.
    tokio::select! {
        installed = install => panic!("the install ended: {installed:?}"),
        () = kit.store().binding_reached.notified() => {}
    }
    let undone = async {
        while kit
            .room_strategy(&r1(), RoomStrategyOptions::default())
            .is_some()
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(50), undone)
            .await
            .is_err(),
        "the undo read the room before the cut call's write landed"
    );

    drop(held);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while kit
        .room_strategy(&r1(), RoomStrategyOptions::default())
        .is_some()
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the room stayed claimed"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let bindings = kit
        .list_bindings(&r1(), ListBindingsOptions::default())
        .await
        .unwrap();
    assert_eq!(
        bindings.len(),
        0,
        "the agent the cut install attached is detached"
    );
    kit.close().await.unwrap();
}

/// A strategy whose install delegates a task in the background, then
/// never ends.
struct Delegates;

impl Orchestration for Delegates {
    fn agents(&self) -> Vec<StrategyAgent> {
        Vec::new()
    }

    fn install<'call>(
        &'call self,
        kit: &'call RoomKit,
        room_id: &'call RoomId,
    ) -> StrategyFuture<'call> {
        Box::pin(async move {
            let worker = ChannelId::new("worker").unwrap();
            let options = roomkit::DelegateOptions::default();
            kit.delegate(room_id, &worker, "Find it.", options).await?;
            std::future::pending::<()>().await;
            Ok(())
        })
    }
}

/// An agent whose turn waits until it is let go.
struct Slow(Arc<tokio::sync::Notify>);

impl Channel for Slow {
    fn channel_id(&self) -> ChannelId {
        ChannelId::new("worker").unwrap()
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

    fn on_event(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let gate = Arc::clone(&self.0);
        async move {
            gate.notified().await;
            Ok(ChannelOutput::empty())
        }
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

// Python: core/mixins/strategies.py::StrategyMixin._install_strategy: a
// cancelled install is undone at once in its task, and a task it delegated
// goes on by itself.
#[tokio::test]
async fn a_cut_install_frees_its_room_while_a_task_it_delegated_runs_on() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let turn = Arc::new(tokio::sync::Notify::new());
    kit.register_channel(Slow(Arc::clone(&turn))).unwrap();
    with_room(&kit).await;
    let delegates: Arc<dyn Orchestration> = Arc::new(Delegates);
    let room = r1();
    let install = kit.install_strategy(&room, delegates, InstallStrategyOptions::default());
    assert!(
        tokio::time::timeout(Duration::from_millis(100), install)
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
            "the room stayed claimed while the task it delegated ran"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    turn.notify_one();
    kit.close().await.unwrap();
}

/// A strategy whose install waits until it is let go, then attaches its
/// agent.
struct Gated(Arc<tokio::sync::Notify>);

impl Orchestration for Gated {
    fn agents(&self) -> Vec<StrategyAgent> {
        vec![ChannelId::new("sales").unwrap().into()]
    }

    fn install<'call>(
        &'call self,
        _kit: &'call RoomKit,
        _room_id: &'call RoomId,
    ) -> StrategyFuture<'call> {
        Box::pin(async move {
            self.0.notified().await;
            Ok(())
        })
    }
}

// Python: core/mixins/strategies.py::StrategyMixin.uninstall_strategy,
// which waits for an install under way (decided 2026-11-02, RMK-1247); the
// shared scenario orchestration/an-uninstall-during-an-install-takes-it-
// back-once-it-is-over says it without making sure the uninstall waits.
#[tokio::test(start_paused = true)]
async fn an_uninstall_during_an_install_waits_for_it_then_takes_it_back() {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(Quiet("sales")).unwrap();
    with_room(&kit).await;
    let gate = Arc::new(tokio::sync::Notify::new());
    let gated: Arc<dyn Orchestration> = Arc::new(Gated(Arc::clone(&gate)));
    let installing = kit.clone();
    let install = tokio::spawn(async move {
        let options = InstallStrategyOptions::default();
        installing.install_strategy(&r1(), gated, options).await
    });
    while kit
        .room_strategy(&r1(), RoomStrategyOptions::default())
        .is_none()
    {
        tokio::task::yield_now().await;
    }
    let uninstalling = kit.clone();
    let uninstall = tokio::spawn(async move {
        let options = UninstallStrategyOptions::default();
        uninstalling.uninstall_strategy(&r1(), options).await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !uninstall.is_finished(),
        "the uninstall answered while the install ran"
    );

    gate.notify_one();
    install.await.unwrap().unwrap();
    assert!(uninstall.await.unwrap().unwrap());
    assert!(
        kit.room_strategy(&r1(), RoomStrategyOptions::default())
            .is_none()
    );
    let bindings = kit
        .list_bindings(&r1(), ListBindingsOptions::default())
        .await
        .unwrap();
    assert_eq!(
        bindings.len(),
        0,
        "the agent the install attached is detached"
    );
    kit.close().await.unwrap();
}

/// A strategy whose install deletes its room from the store.
struct Deletes;

impl Orchestration for Deletes {
    fn agents(&self) -> Vec<StrategyAgent> {
        Vec::new()
    }

    fn install<'call>(
        &'call self,
        kit: &'call RoomKit,
        room_id: &'call RoomId,
    ) -> StrategyFuture<'call> {
        Box::pin(async move {
            roomkit::ConversationStore::delete_room(kit.store(), room_id).await?;
            Ok(())
        })
    }
}

// Python: core/mixins/helpers.py::_fire_lifecycle_hook, which runs no hook
// for a room it cannot read, and create_room, which announces the room
// and returns it all the same.
#[tokio::test]
async fn a_room_gone_before_its_announcement_is_created_all_the_same() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let deletes: Arc<dyn Orchestration> = Arc::new(Deletes);
    let created = kit
        .create_room(CreateRoomOptions {
            room_id: Some(r1()),
            orchestration: Some(Some(deletes)),
            ..CreateRoomOptions::default()
        })
        .await;
    assert_eq!(created.unwrap().id, r1());
    kit.close().await.unwrap();
}

/// A transport channel: what a live agent's id must not name.
struct Sms(&'static str);

impl Channel for Sms {
    fn channel_id(&self) -> ChannelId {
        ChannelId::new(self.0).unwrap()
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::SMS
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

// Python: tests/test_declared_strategies.py::test_a_live_agent_under_a_transport_s_id_is_no_agent
#[tokio::test]
async fn a_live_agent_under_a_transport_s_id_is_no_agent() {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(Sms("ana")).unwrap();
    let team: Arc<dyn Orchestration> = Arc::new(Team(vec![Quiet("ana")]));
    let options = CreateRoomOptions {
        room_id: Some(r1()),
        orchestration: Some(Some(team)),
        ..Default::default()
    };

    let refused = kit.create_room(options).await;

    assert!(
        matches!(&refused, Err(RoomKitError::Value(message)) if message == "Channel 'ana' is not an intelligence channel"),
        "{refused:?}"
    );
    let bindings = kit
        .list_bindings(&r1(), ListBindingsOptions::default())
        .await
        .unwrap();
    assert_eq!(bindings.len(), 0, "nothing is attached");
    kit.close().await.unwrap();
}
