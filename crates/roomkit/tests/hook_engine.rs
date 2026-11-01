//! The kit's hook engine and its channel registry reads (RFC §9.1,
//! §10.6.6): `RoomKit::hook_engine`, `RoomKit::hooks_for`,
//! `RoomKit::list_channels` and `RoomKit::channels`, each test named after
//! the Python test it translates. What a registration does in a room's
//! chains is the shared conformance suite's (`hooks/`).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use pretty_assertions::assert_eq;
use roomkit::model::hook::{AllowOptions, BlockOptions};
use roomkit::model::{
    ChannelId, ChannelType, HookResult, InboundMessage, RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, HasHooksOptions, HookExecution, HookOptions,
    HookRegistration, HookTrigger, ProcessInboundOptions, RoomKit, RoomKitError, RoomKitOptions,
    ToolCallEvent,
};

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room(id: &str) -> RoomId {
    RoomId::new(id).unwrap()
}

fn transport(id: &str) -> RecordingTransport {
    RecordingTransport::new(
        channel(id),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    )
}

fn kit() -> RoomKit {
    RoomKit::new(RoomKitOptions::default())
}

/// A kit with the room `r1`, binding the transport `sms`.
async fn kit_with_room() -> RoomKit {
    let kit = kit();
    kit.register_channel(transport("sms")).unwrap();
    let options = CreateRoomOptions {
        room_id: Some(room("r1")),
        ..Default::default()
    };
    kit.create_room(options).await.unwrap();
    kit.attach_channel(
        &room("r1"),
        &channel("sms"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    kit
}

fn blocking(name: &str) -> HookRegistration {
    HookRegistration {
        name: name.to_owned(),
        ..HookRegistration::new(
            HookTrigger::BeforeBroadcast,
            HookExecution::Sync,
            |_event: RoomEvent, _context: RoomContext| async {
                HookResult::block("held", BlockOptions::default())
            },
        )
    }
}

fn tool_observer(name: &str) -> HookRegistration {
    HookRegistration {
        name: name.to_owned(),
        ..HookRegistration::new(
            HookTrigger::OnToolCall,
            HookExecution::Async,
            |_call: ToolCallEvent, _context: RoomContext| async {},
        )
    }
}

async fn send(kit: &RoomKit, text: &str) -> bool {
    let message = InboundMessage::new(channel("sms"), "+15551234567", TextContent::new(text));
    let options = ProcessInboundOptions {
        room_id: Some(room("r1")),
        ..Default::default()
    };
    kit.process_inbound(message, options).await.unwrap().blocked
}

// Python: tests/test_framework_queries.py::TestListChannels::test_empty and
// test_returns_all
#[tokio::test]
async fn list_channels_returns_each_registered_channel_in_its_registration_order() {
    let kit = kit();
    let empty = kit.list_channels().is_empty();
    for id in ["ch2", "ch1", "ch3"] {
        kit.register_channel(transport(id)).unwrap();
    }
    kit.unregister_channel(&channel("ch3")).unwrap();
    kit.register_channel(transport("ch3")).unwrap();
    let listed: Vec<String> = kit
        .list_channels()
        .iter()
        .map(|registered| registered.channel_id().to_string())
        .collect();
    let by_id: Vec<String> = kit.channels().keys().map(ToString::to_string).collect();
    assert_eq!(
        (empty, listed, by_id),
        (
            true,
            vec!["ch2".to_owned(), "ch1".to_owned(), "ch3".to_owned()],
            vec!["ch1".to_owned(), "ch2".to_owned(), "ch3".to_owned()]
        )
    );
}

// Python: tests/test_kit_close.py::test_a_sync_read_answers_during_the_close_and_after_it
// (hooks_for, list_channels, get_channel)
#[tokio::test]
async fn the_sync_reads_answer_after_the_close() {
    let kit = kit();
    kit.register_channel(transport("sms")).unwrap();
    kit.close().await.unwrap();
    let engine = kit.hooks_for(HookTrigger::BeforeBroadcast);
    assert_eq!(
        (
            kit.list_channels().len(),
            kit.get_channel(&channel("sms")).is_some(),
            engine.has_hooks(HasHooksOptions::default()),
        ),
        (1, true, false)
    );
}

// Python: tests/test_turn_hooks.py::test_the_kit_s_engine_reads_the_turn_hooks_registered_through_it
#[tokio::test]
async fn the_kit_s_engine_reads_the_turn_hooks_registered_through_it() {
    let kit = kit();
    let options = HookOptions {
        name: "gate".to_owned(),
        fail_closed: true,
        ..Default::default()
    };
    kit.hook(
        HookTrigger::BeforeToolUse,
        |_call: ToolCallEvent, _context: RoomContext| async {},
        options,
    )
    .unwrap();
    let engine = kit.hook_engine();
    let on = |trigger| HasHooksOptions {
        trigger: Some(trigger),
    };
    assert_eq!(
        (
            engine.has_hooks(on(HookTrigger::BeforeToolUse)),
            engine.has_hooks(HasHooksOptions::default()),
            engine.has_hooks(on(HookTrigger::OnToolCall)),
        ),
        (true, true, false)
    );
}

// Python: tests/test_turn_hooks.py::test_one_registration_given_twice_is_refused_the_second_time
#[tokio::test]
async fn one_registration_given_twice_is_refused_the_second_time() {
    let kit = kit();
    let engine = kit.hooks_for(HookTrigger::OnToolCall);
    engine.register(tool_observer("dup")).unwrap();
    let again = engine.register(tool_observer("dup"));
    assert!(
        matches!(&again, Err(RoomKitError::Value(why)) if why.contains("'dup'")),
        "{again:?}"
    );
    assert_eq!(
        (
            engine.remove_global_hook("dup"),
            engine.remove_global_hook("dup")
        ),
        (true, false)
    );
}

// Python: tests/test_hook_rules.py::TestTheKitScope::test_an_unnamed_turn_hook_is_removed_by_its_function_s_name
// (a Rust closure has no name: an unnamed hook takes `hook`)
#[tokio::test]
async fn an_unnamed_turn_hook_is_removed_by_the_name_it_was_given() {
    let kit = kit();
    let held = kit.hook_engine().register(tool_observer("")).unwrap();
    assert_eq!(
        (
            held.name.clone(),
            kit.hook_engine().remove_global_hook(&held.name)
        ),
        ("hook".to_owned(), true)
    );
}

#[tokio::test]
async fn a_hook_the_engine_registers_runs_as_the_kit_s() {
    let kit = kit_with_room().await;
    let held = kit
        .hooks_for(HookTrigger::BeforeBroadcast)
        .register(blocking("hold"))
        .unwrap();
    let blocked = send(&kit, "first").await;
    kit.hook_engine().remove_global_hook(&held.name);
    let after_removal = send(&kit, "second").await;
    kit.close().await.unwrap();
    assert_eq!(
        (held.name, blocked, after_removal),
        ("hold".to_owned(), true, false)
    );
}

// Python: tests/test_room_org_scoping.py::TestScopedRoomHooks (the engine's
// room_hook_names; the scoped refusals are tenancy/another-organization-s-room-hooks-stand)
#[tokio::test]
async fn the_engine_s_room_hooks_read_no_room_and_are_named_per_room() {
    let kit = kit_with_room().await;
    let engine = kit.hook_engine();
    engine.add_room_hook(&room("r1"), blocking("hold")).unwrap();
    engine.add_room_hook(&room("r9"), blocking("hold")).unwrap();
    let names = engine.room_hook_names(&room("r1"));
    let blocked = send(&kit, "first").await;
    let removed = (
        engine.remove_room_hook(&room("r1"), "hold"),
        engine.remove_room_hook(&room("r1"), "hold"),
    );
    let after_removal = send(&kit, "second").await;
    kit.close().await.unwrap();
    assert_eq!(
        (names, blocked, removed, after_removal),
        (vec!["hold".to_owned()], true, (true, false), false)
    );
}

// Python: tests/test_hook_rules.py::TestReplacingARoomHook::test_replaces_the_room_s_hook_of_the_same_name
#[tokio::test]
async fn a_replaced_room_hook_runs_no_more() {
    let kit = kit_with_room().await;
    let engine = kit.hook_engine();
    engine
        .add_room_hook(&room("r1"), blocking("router"))
        .unwrap();
    let letting_through = HookRegistration {
        name: "router".to_owned(),
        ..HookRegistration::new(
            HookTrigger::BeforeBroadcast,
            HookExecution::Sync,
            |_event: RoomEvent, _context: RoomContext| async {},
        )
    };
    engine
        .replace_room_hook(&room("r1"), letting_through)
        .unwrap();
    let blocked = send(&kit, "hello").await;
    let names = engine.room_hook_names(&room("r1"));
    kit.close().await.unwrap();
    assert_eq!((blocked, names), (false, vec!["router".to_owned()]));
}

#[tokio::test]
async fn a_registration_whose_handler_takes_another_payload_is_refused() {
    let kit = kit();
    let wrong = HookRegistration::new(
        HookTrigger::OnToolCall,
        HookExecution::Async,
        |_event: RoomEvent, _context: RoomContext| async {},
    );
    let refused = kit.hook_engine().register(wrong);
    assert!(
        matches!(refused, Err(RoomKitError::Value(_))),
        "{refused:?}"
    );
}

// Python: tests/test_turn_hooks.py::test_an_executor_refuses_an_owners_trigger_on_every_door
#[tokio::test]
async fn the_turn_executor_s_engine_refuses_an_owner_s_trigger_and_room_hooks() {
    let kit = kit();
    let executor = kit.hooks_for(HookTrigger::OnToolCall);
    let owner_s = executor.register(blocking("hold"));
    let for_a_room = executor.add_room_hook(&room("r1"), tool_observer("audit"));
    assert!(
        matches!(&owner_s, Err(RoomKitError::Value(why)) if why.contains("kit.hook(...)")),
        "{owner_s:?}"
    );
    assert!(
        matches!(&for_a_room, Err(RoomKitError::Value(why)) if why.contains("kit.enable_room_hook(room_id")),
        "{for_a_room:?}"
    );
}

#[tokio::test]
async fn the_turn_executor_s_engine_reads_the_turn_s_hooks_alone() {
    let kit = kit();
    let engine = kit.hook_engine();
    engine.register(blocking("hold")).unwrap();
    engine
        .add_room_hook(&room("r1"), tool_observer("audit"))
        .unwrap();
    let executor = kit.hooks_for(HookTrigger::BeforeToolUse);
    let read = (
        executor.has_hooks(HasHooksOptions::default()),
        executor.has_hooks(HasHooksOptions {
            trigger: Some(HookTrigger::BeforeBroadcast),
        }),
        executor.room_hook_names(&room("r1")),
        executor.remove_global_hook("hold"),
        engine.room_hook_names(&room("r1")),
    );
    let removed = executor.remove_room_hook(&room("r1"), "audit");
    assert_eq!(
        (read, removed, engine.has_hooks(HasHooksOptions::default())),
        (
            (true, false, Vec::new(), false, vec!["audit".to_owned()]),
            true,
            true
        )
    );
}

/// A hook removed while an operation that read it runs: the operation runs
/// the hook it read, as Python's chain runs the list it took, and reports
/// no failure of it (the review of stream B, finding 1).
#[tokio::test]
async fn a_hook_removed_while_its_chain_runs_runs_as_it_was_read() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use roomkit::AddRoomHookOptions;
    use tokio::sync::{Notify, oneshot};

    let kit = kit_with_room().await;
    let (entered, held) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let (entered_by, held_by) = (Arc::clone(&entered), Arc::clone(&held));
    let slow = AddRoomHookOptions {
        name: "slow".to_owned(),
        ..Default::default()
    };
    kit.add_room_hook(
        &room("r1"),
        HookTrigger::BeforeBroadcast,
        HookExecution::Sync,
        move |_event: RoomEvent, _context: RoomContext| {
            let (entered, held) = (Arc::clone(&entered_by), Arc::clone(&held_by));
            async move {
                entered.notify_one();
                held.notified().await;
            }
        },
        slow,
    )
    .await
    .unwrap();
    let ran = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&ran);
    let guard = AddRoomHookOptions {
        name: "guard".to_owned(),
        priority: 1,
        fail_closed: true,
        ..Default::default()
    };
    kit.add_room_hook(
        &room("r1"),
        HookTrigger::BeforeBroadcast,
        HookExecution::Sync,
        move |_event: RoomEvent, _context: RoomContext| {
            counted.fetch_add(1, Ordering::SeqCst);
            async { HookResult::allow(AllowOptions::default()) }
        },
        guard,
    )
    .await
    .unwrap();
    let (sent, blocked) = oneshot::channel();
    let sender = kit.clone();
    tokio::spawn(async move {
        sent.send(send(&sender, "hello").await).ok();
    });
    entered.notified().await;
    let removed = kit
        .remove_room_hook(
            &room("r1"),
            "guard",
            roomkit::RemoveRoomHookOptions::default(),
        )
        .await
        .unwrap();
    held.notify_one();
    let blocked = blocked.await.unwrap();
    kit.close().await.unwrap();
    assert_eq!(
        (removed, blocked, ran.load(Ordering::SeqCst)),
        (true, false, 1)
    );
}
