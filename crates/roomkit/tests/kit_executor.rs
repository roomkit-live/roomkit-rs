//! Python: `tests/test_kit_executor_contract.py`; public contracts of RFC §10.6.6.
#![expect(clippy::unwrap_used, reason = "tests fail on invalid fixtures")]

use roomkit::model::{ChannelId, ChannelType, RoomContext, RoomEvent, RoomId};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::turns::{
    HookOptions as ExecutorHookOptions, RoomHookOptions as ExecutorRoomHookOptions, TakesOptions,
};
use roomkit::{
    DisableRoomHookOptions, EnableRoomHookOptions, HasHooksOptions, HookExecution,
    HookRegistration, HookTrigger, RemoveRoomHookOptions, RoomKit, RoomKitOptions, ToolCallEvent,
};

use std::collections::BTreeSet;

fn kit() -> RoomKit {
    RoomKit::new(RoomKitOptions::default())
}
fn room() -> RoomId {
    RoomId::new("room").unwrap()
}
fn named(name: &str) -> HookRegistration {
    HookRegistration {
        name: name.to_owned(),
        ..HookRegistration::new(
            HookTrigger::OnToolCall,
            HookExecution::Async,
            |_event: ToolCallEvent, _context: RoomContext| async {},
        )
    }
}
fn has(trigger: HookTrigger) -> HasHooksOptions {
    HasHooksOptions {
        trigger: Some(trigger),
    }
}

// Python: test_named_definitions_and_selection_are_separate_from_owner_hooks
#[tokio::test]
async fn named_definitions_and_selection_are_separate_from_owner_hooks() {
    let kit = kit();
    let engine = kit.turn_executor().hooks();
    engine.define_room_hook(named("audit")).unwrap();
    engine
        .define_room_hook(HookRegistration {
            name: "audit".to_owned(),
            ..HookRegistration::new(
                HookTrigger::OnAiResponse,
                HookExecution::Async,
                |_event: roomkit_ai::AiResponseEvent, _context: RoomContext| async {},
            )
        })
        .unwrap();
    assert_eq!(
        engine.defined_room_hooks(),
        BTreeSet::from(["audit".to_owned()])
    );
    assert!(engine.defines_room_hooks(&[]));
    assert!(!engine.defines_room_hooks(&["missing".to_owned()]));
    assert_eq!(engine.room_hook_names(&room()), Vec::<String>::new());
    engine.use_room_hooks(
        &room(),
        vec!["missing".to_owned(), "audit".to_owned(), "audit".to_owned()],
    );
    assert_eq!(engine.room_hook_names(&room()), ["audit", "audit"]);
    assert_eq!(
        kit.hook_engine().room_hook_names(&room()),
        Vec::<String>::new()
    );
    assert!(!kit.hook_engine().undefine_room_hook("audit"));
    assert!(kit.hook_engine().defined_room_hooks().is_empty());
    assert!(engine.remove_room_hook(&room(), "audit"));
    assert_eq!(engine.room_hook_names(&room()), Vec::<String>::new());
    assert!(engine.defines_room_hooks(&["audit".to_owned()]));
    assert!(!engine.remove_room_hook(&room(), "missing"));
    assert!(engine.undefine_room_hook("audit"));
    assert!(!engine.undefine_room_hook("audit"));
    assert!(!engine.has_hooks(HasHooksOptions::default()));
    kit.close().await.unwrap();
}

// Python: test_executor_and_owner_global_names_have_separate_removal_scopes
#[tokio::test]
async fn executor_and_owner_global_names_have_separate_removal_scopes() {
    let kit = kit();
    let owner = || HookRegistration {
        name: "shared".to_owned(),
        ..HookRegistration::new(
            HookTrigger::BeforeBroadcast,
            HookExecution::Sync,
            |_event: RoomEvent, _context: RoomContext| async {},
        )
    };
    kit.hook_engine().register(owner()).unwrap();
    let executor = kit.turn_executor();
    executor.hooks().register(named("shared")).unwrap();
    assert!(executor.hooks().remove_global_hook("shared"));
    assert!(
        kit.hook_engine()
            .has_hooks(has(HookTrigger::BeforeBroadcast))
    );
    assert!(kit.hook_engine().remove_global_hook("shared"));
    executor.hooks().register(named("only-executor")).unwrap();
    assert!(!kit.hook_engine().remove_global_hook("only-executor"));
    let mut own = owner();
    own.name = "only-executor".to_owned();
    kit.hook_engine().register(own).unwrap();
    assert!(kit.hook_engine().remove_global_hook("only-executor"));
    assert!(executor.hooks().has_hooks(has(HookTrigger::OnToolCall)));
    kit.close().await.unwrap();
}

// Python: test_an_owner_hook_given_to_executor_can_share_a_named_definition
#[tokio::test]
async fn an_owner_hook_given_to_executor_can_share_a_named_definition() {
    let kit = kit();
    let engine = kit.turn_executor().hooks();
    engine.define_room_hook(named("audit")).unwrap();
    kit.enable_room_hook(&room(), "audit", EnableRoomHookOptions::default())
        .await
        .unwrap();
    engine.add_owner_room_hook(&room(), named("audit")).unwrap();
    assert_eq!(engine.room_hook_names(&room()), ["audit"]);
    assert_eq!(
        kit.hook_engine().room_hook_names(&room()),
        Vec::<String>::new()
    );
    assert!(
        !kit.remove_room_hook(&room(), "audit", RemoveRoomHookOptions::default())
            .await
            .unwrap()
    );
    assert!(engine.remove_room_hook(&room(), "audit"));
    assert_eq!(engine.room_hook_names(&room()), ["audit"]);
    assert!(engine.remove_room_hook(&room(), "audit"));
    assert_eq!(engine.room_hook_names(&room()), Vec::<String>::new());
    assert!(
        kit.disable_room_hook(&room(), "audit", DisableRoomHookOptions::default())
            .await
            .unwrap()
    );
    kit.close().await.unwrap();
}

// Python: test_local_executor_queries_follow_registration_and_close
#[tokio::test]
async fn local_executor_queries_follow_registration_and_close() {
    let kit = kit();
    let executor = kit.turn_executor();
    let agent = ChannelId::new("agent").unwrap();
    assert!(executor.hosted().is_none());
    assert_eq!(executor.organizations(), BTreeSet::from(["*".to_owned()]));
    assert!(executor.serves(None) && executor.serves(Some("acme")));
    assert!(!executor.declares(&agent));
    kit.register_channel(RecordingTransport::new(
        agent.clone(),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    assert!(executor.hosts_channel(&agent) && executor.declares(&agent));
    assert!(executor.takes(&agent, TakesOptions::default()));
    let options = TakesOptions {
        room_hooks: vec!["audit".to_owned()],
        organization_id: Some("acme".to_owned()),
    };
    assert!(!executor.takes(&agent, options.clone()));
    executor.hooks().define_room_hook(named("audit")).unwrap();
    assert!(executor.takes(&agent, options));
    kit.close().await.unwrap();
    assert!(executor.closed());
    assert!(executor.hosts_channel(&agent));
    assert!(!executor.declares(&agent));
    assert!(!executor.takes(&agent, TakesOptions::default()));
}

// Python: test_refused_redefinition_keeps_the_original_and_default_options
#[tokio::test]
async fn refused_redefinition_keeps_the_original_and_default_options() {
    let kit = kit();
    let engine = kit.turn_executor().hooks();
    engine.define_room_hook(named("audit")).unwrap();
    let mut invalid = named("audit");
    invalid.fail_closed = true;
    assert!(engine.define_room_hook(invalid).is_err());
    assert_eq!(
        engine.defined_room_hooks(),
        BTreeSet::from(["audit".to_owned()])
    );
    assert!(engine.has_hooks(has(HookTrigger::OnToolCall)));
    assert!(!engine.has_hooks(has(HookTrigger::BeforeBroadcast)));
    let defaults = ExecutorHookOptions::default();
    assert_eq!(defaults.timeout, std::time::Duration::from_secs(30));
    assert_eq!(defaults.priority, 0);
    assert_eq!(defaults.execution, HookExecution::Sync);
    assert!(!defaults.fail_closed);
    kit.close().await.unwrap();
}

// Python: tests/test_turn_hooks.py::test_an_executor_refuses_an_owners_trigger_on_every_door
#[tokio::test]
async fn an_executor_refuses_an_owners_trigger_on_every_door() {
    let kit = kit();
    let snapshot: serde_json::Value = serde_json::from_str(include_str!(
        "../../../api/python/public_api_signatures.json"
    ))
    .unwrap();
    let triggers = snapshot
        .get("roomkit.HookTrigger")
        .unwrap()
        .as_object()
        .unwrap();
    let executor = kit.turn_executor();
    let mut checked = 0;
    for name in triggers.keys() {
        let Ok(trigger) =
            serde_json::from_value::<HookTrigger>(serde_json::Value::String(name.to_lowercase()))
        else {
            continue;
        };
        if trigger.is_turn_trigger() {
            continue;
        }
        checked += 1;
        assert!(
            executor
                .hook(
                    trigger,
                    |_event: RoomEvent, _ctx: RoomContext| async {},
                    ExecutorHookOptions::default()
                )
                .is_err()
        );
        assert!(
            executor
                .room_hook(
                    "audit",
                    trigger,
                    |_event: RoomEvent, _ctx: RoomContext| async {},
                    ExecutorRoomHookOptions::default()
                )
                .is_err()
        );
    }
    assert!(checked > 20);
    assert!(!executor.hooks().has_hooks(HasHooksOptions::default()));
    kit.close().await.unwrap();
}

// Python: tests/test_kit_executor_contract.py::test_add_owner_room_hook_returns_the_registered_name
#[tokio::test]
async fn add_owner_room_hook_returns_the_registered_name() {
    let kit = kit();
    let engine = kit.turn_executor().hooks();
    assert_eq!(
        engine
            .add_owner_room_hook(&room(), named("audit"))
            .unwrap()
            .name,
        "audit"
    );
    let generated = engine.add_owner_room_hook(&room(), named("")).unwrap();
    assert_eq!(generated.name, "hook");
    assert!(engine.remove_room_hook(&room(), &generated.name));
    kit.close().await.unwrap();
}
