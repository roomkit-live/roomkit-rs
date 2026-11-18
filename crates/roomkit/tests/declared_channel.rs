//! An owner holding an intelligence channel by declaration alone (§10.6.2/.6).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::collections::BTreeSet;
use std::sync::Arc;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelCategory, ChannelId, ChannelType, EventSource, InboundMessage, RoomContext, RoomEvent,
    RoomId, TextContent,
};
use roomkit::orchestration::{Discussion, DiscussionOptions, StrategyAgent};
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions,
};
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, CreateRoomOptions, DeclaredChannel,
    HookExecution, HookRegistration, HookTrigger, RoomKit, RoomKitError, RoomKitOptions,
    ToolCallEvent,
};

fn id(name: &str) -> ChannelId {
    ChannelId::new(name).unwrap()
}

fn declaration() -> ChannelDeclaration {
    ChannelDeclaration::new(id("meteo"))
}

fn kit() -> RoomKit {
    RoomKit::new(RoomKitOptions::default())
}

fn observer() -> HookRegistration {
    HookRegistration {
        name: "audit".into(),
        ..HookRegistration::new(
            HookTrigger::OnToolCall,
            HookExecution::Async,
            |_call: ToolCallEvent, _context: RoomContext| async {},
        )
    }
}

#[test]
fn declarations_and_live_channels_share_one_namespace() {
    let kit = kit();
    let invalid = ChannelDeclaration {
        category: ChannelCategory::Transport,
        ..declaration()
    };
    assert!(matches!(
        kit.register_channel(invalid),
        Err(RoomKitError::Value(_))
    ));
    assert!(kit.list_channels().is_empty());
    let live = || {
        RecordingTransport::new(
            id("meteo"),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        )
    };
    kit.register_channel(declaration()).unwrap();
    assert!(matches!(
        kit.register_channel(live()),
        Err(RoomKitError::ChannelAlreadyRegistered { .. })
    ));
    kit.unregister_channel(&id("meteo")).unwrap();
    kit.register_channel(live()).unwrap();
    assert!(matches!(
        kit.register_channel(declaration()),
        Err(RoomKitError::ChannelAlreadyRegistered { .. })
    ));
}

#[tokio::test]
async fn the_stub_binds_its_own_capabilities_and_refuses_all_three_execution_doors() {
    let kit = kit();
    let declared = ChannelDeclaration {
        recent_events_window: 7,
        steers_room_turns: true,
        thinks_while_listening: true,
        identity: Some(roomkit::AgentIdentity {
            name: Some("Meteo".into()),
            greeting: Some("Hello.".into()),
            ..Default::default()
        }),
        ..declaration()
    };
    let stub = DeclaredChannel::new(declared.clone()).unwrap();
    assert_eq!(stub.declaration(), declared);
    assert_eq!(stub.name(), Some("Meteo"));
    assert_eq!(stub.greeting(), Some("Hello."));
    assert!(stub.auto_greet() && stub.thinks_while_listening());
    assert!(!stub.is_config_only());
    assert_eq!(stub.recent_events_window(), 7);
    assert!(stub.serializes_room_turns() && stub.steers_room_turns());
    kit.register_channel(stub.clone()).unwrap();
    let room = kit
        .create_room(CreateRoomOptions {
            room_id: Some(RoomId::new("r").unwrap()),
            ..Default::default()
        })
        .await
        .unwrap();
    let transport = AttachChannelOptions {
        category: Some(ChannelCategory::Transport),
        ..Default::default()
    };
    let error = kit
        .attach_channel(&room.id, &id("meteo"), transport)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("an intelligence channel only"));
    let binding = kit
        .attach_channel(&room.id, &id("meteo"), AttachChannelOptions::default())
        .await
        .unwrap();
    assert_eq!(binding.capabilities, declared.capabilities);
    assert_eq!(binding.category, ChannelCategory::Intelligence);
    let context = RoomContext::new(room.clone());
    let message = InboundMessage::new(id("meteo"), "u", TextContent::new("Hi."));
    let event = RoomEvent::new(
        room.id,
        EventSource::new(id("meteo"), ChannelType::AI),
        TextContent::new("Hi."),
    );
    let refused = [
        stub.handle_inbound(message, context.clone())
            .await
            .unwrap_err(),
        stub.on_event(event.clone(), binding.clone(), context.clone())
            .await
            .unwrap_err(),
        stub.deliver(event, binding, context).await.unwrap_err(),
    ];
    for error in refused {
        assert!(
            matches!(error.downcast_ref::<RoomKitError>(), Some(RoomKitError::Type(message)) if message.contains("by its declaration alone"))
        );
    }
    kit.close().await.unwrap();
}

#[test]
fn matching_kit_turn_hooks_and_declarations_refuse_each_other_in_either_order() {
    let first = kit();
    first.hook_engine().register(observer()).unwrap();
    assert!(
        first
            .register_channel(declaration())
            .unwrap_err()
            .to_string()
            .contains("'audit'")
    );
    assert!(first.list_channels().is_empty());
    assert!(first.turn_executor().hooks().remove_global_hook("audit"));
    first.register_channel(declaration()).unwrap();
    let second = kit();
    second.register_channel(declaration()).unwrap();
    assert!(
        second
            .hook_engine()
            .register(observer())
            .unwrap_err()
            .to_string()
            .contains("'meteo'")
    );
    let mut other = observer();
    other.channel_ids = Some(BTreeSet::from([id("other")]));
    second.hook_engine().register(other.clone()).unwrap();
    let room = RoomId::new("r").unwrap();
    assert!(second.hook_engine().add_room_hook(&room, other).is_err());
    second
        .turn_executor()
        .hooks()
        .register(HookRegistration {
            name: "on-executor".into(),
            ..observer()
        })
        .unwrap();
    let third = kit();
    third.turn_executor().hooks().register(observer()).unwrap();
    third.register_channel(declaration()).unwrap();
}

#[test]
fn typed_kit_hooks_filtered_to_a_different_channel_type_are_compatible() {
    let global = kit();
    let restricted = HookRegistration {
        channel_types: Some(BTreeSet::from([ChannelType::SMS])),
        ..observer()
    };
    global.hook_engine().register(restricted.clone()).unwrap();
    global.register_channel(declaration()).unwrap();
    let scoped = kit();
    let room = RoomId::new("r").unwrap();
    scoped
        .hook_engine()
        .add_room_hook(&room, restricted)
        .unwrap();
    assert!(scoped.register_channel(declaration()).is_err());
    assert!(scoped.hook_engine().remove_room_hook(&room, "audit"));
    scoped.register_channel(declaration()).unwrap();
}

#[tokio::test]
async fn a_strategy_given_a_live_object_refuses_the_id_of_an_owners_declaration() {
    let kit = kit();
    kit.register_channel(declaration()).unwrap();
    let room = kit.create_room(CreateRoomOptions::default()).await.unwrap();
    let agents = ["new", "meteo"].map(|name| {
        StrategyAgent::channel(EchoAgent::new(
            id(name),
            "hello",
            EchoAgentOptions::default(),
        ))
    });
    let strategy = Discussion::new(agents.to_vec(), DiscussionOptions::default()).unwrap();
    let error = kit
        .install_strategy(
            &room.id,
            Arc::new(strategy),
            roomkit::InstallStrategyOptions::default(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("give the strategy its id"));
    assert_eq!(kit.list_channels().len(), 1);
    kit.close().await.unwrap();
}
