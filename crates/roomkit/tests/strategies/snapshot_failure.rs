//! An install whose initial store read fails remains available to uninstall.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use roomkit::model::ChannelId;
use roomkit::orchestration::DiscussionOptions;
use roomkit::testing::{EchoAgent, EchoAgentOptions};
use roomkit::{
    Discussion, InstallStrategyOptions, ListBindingsOptions, Orchestration, RoomKit, RoomKitError,
    RoomKitOptions, RoomStrategyOptions, UninstallStrategyOptions,
};

use super::{Faulty, Plain, r1, with_room};

fn discussion(rules: &str) -> Arc<dyn Orchestration<Faulty>> {
    Arc::new(
        Discussion::new(
            vec![ChannelId::new("a").unwrap().into()],
            DiscussionOptions {
                rules: Some(rules.into()),
                ..Default::default()
            },
        )
        .unwrap(),
    )
}

async fn failed_snapshot(kit: &RoomKit<Faulty>, strategy: Arc<dyn Orchestration<Faulty>>) {
    kit.store().fail_bindings.store(true, Ordering::SeqCst);
    let installed = kit
        .install_strategy(
            &r1(),
            Arc::clone(&strategy),
            InstallStrategyOptions::default(),
        )
        .await;
    kit.store().fail_bindings.store(false, Ordering::SeqCst);
    assert!(
        matches!(installed, Err(RoomKitError::Value(message)) if message == "list_bindings fails")
    );
    let held = kit
        .room_strategy(&r1(), RoomStrategyOptions::default())
        .unwrap();
    assert!(Arc::ptr_eq(&held, &strategy));
}

async fn uninstall_and_retry(kit: &RoomKit<Faulty>, retry: Arc<dyn Orchestration<Faulty>>) {
    let uninstalled = tokio::time::timeout(
        Duration::from_secs(1),
        kit.uninstall_strategy(&r1(), UninstallStrategyOptions::default()),
    )
    .await
    .expect("a failed install must not leave an uninstall waiting forever")
    .unwrap();
    assert!(uninstalled, "the failed claim is released explicitly");
    assert!(
        kit.room_strategy(&r1(), RoomStrategyOptions::default())
            .is_none()
    );
    kit.install_strategy(&r1(), Arc::clone(&retry), InstallStrategyOptions::default())
        .await
        .unwrap();
    assert!(Arc::ptr_eq(
        &kit.room_strategy(&r1(), RoomStrategyOptions::default())
            .unwrap(),
        &retry
    ));
    kit.close().await.unwrap();
}

// Python: tests/orchestration/test_strategy_install.py::test_an_uninstall_after_an_install_the_store_cut_short_frees_the_room[new]
// Python: tests/orchestration/test_framework_orchestration.py::TestCreateRoomOrchestration::test_no_orchestration_is_noop
#[tokio::test]
async fn an_uninstall_after_an_install_the_store_cut_short_frees_the_room() {
    let kit = RoomKit::new(RoomKitOptions::with_store(Faulty::default()));
    with_room(&kit).await;
    assert!(
        kit.room_strategy(&r1(), RoomStrategyOptions::default())
            .is_none()
    );
    assert!(
        kit.list_bindings(&r1(), ListBindingsOptions::default())
            .await
            .unwrap()
            .is_empty()
    );
    failed_snapshot(&kit, Arc::new(Plain)).await;
    let refused = kit
        .install_strategy(&r1(), Arc::new(Plain), InstallStrategyOptions::default())
        .await;
    assert!(
        matches!(refused, Err(RoomKitError::Value(message)) if message.contains("holds a strategy already"))
    );
    uninstall_and_retry(&kit, Arc::new(Plain)).await;
}

// Python: tests/orchestration/test_strategy_install.py::test_an_uninstall_after_an_install_the_store_cut_short_frees_the_room[discussion-replacement]
#[tokio::test]
async fn a_failed_discussion_replacement_is_uninstalled_before_another() {
    let kit = RoomKit::new(RoomKitOptions::with_store(Faulty::default()));
    kit.register_channel(EchoAgent::new(
        ChannelId::new("a").unwrap(),
        "quiet",
        EchoAgentOptions::default(),
    ))
    .unwrap();
    with_room(&kit).await;
    kit.install_strategy(
        &r1(),
        discussion("Original rules"),
        InstallStrategyOptions::default(),
    )
    .await
    .unwrap();
    failed_snapshot(&kit, discussion("Replacement rules")).await;
    let retry = discussion("Third rules");
    let refused = kit
        .install_strategy(&r1(), Arc::clone(&retry), InstallStrategyOptions::default())
        .await;
    assert!(
        matches!(refused, Err(RoomKitError::Value(message)) if message.contains("another install is replacing"))
    );
    uninstall_and_retry(&kit, retry).await;
}
