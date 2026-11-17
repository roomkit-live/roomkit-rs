//! Python: `tests/test_channels/test_ai_context_fields.py`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use roomkit::model::hook::ModifyOptions;
use roomkit::model::{
    Access, ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelDirection,
    ChannelMediaType, ChannelType, EventSource, HookResult, Participant, RoomContext, RoomEvent,
    TextContent,
};
use roomkit::{Channel, HookOptions, HookTrigger};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiGenerationEvent, AiResponse, AiTool, AiToolCall,
    MockAiProvider, MockAiProviderOptions, Tools,
};
use serde_json::json;

use super::support::{Room, channel, room_id};

fn target_caps() -> ChannelCapabilities {
    ChannelCapabilities {
        media_types: [ChannelMediaType::Text, ChannelMediaType::Rich].into(),
        max_length: Some(91),
        supports_buttons: true,
        custom: json!({"target": "snapshot"}).as_object().unwrap().clone(),
        ..Default::default()
    }
}

fn snapshot() -> RoomContext {
    let mut context = RoomContext::new(roomkit::model::Room::new(room_id()));
    context
        .room
        .metadata
        .clone_from(json!({"topic": "source"}).as_object().unwrap());
    let mut target = ChannelBinding::new(channel("out"), room_id(), ChannelType::WEBSOCKET);
    target.capabilities = target_caps();
    target.access = Access::None;
    target.muted = true;
    target.output_muted = true;
    target.visibility = roomkit::model::Visibility::Internal;
    let mut inbound = target.clone();
    inbound.channel_id = channel("in");
    inbound.direction = ChannelDirection::Inbound;
    inbound.capabilities = ChannelCapabilities {
        max_length: Some(1),
        ..Default::default()
    };
    let mut intelligence = inbound.clone();
    intelligence.channel_id = channel("other-ai");
    intelligence.direction = ChannelDirection::Bidirectional;
    intelligence.category = ChannelCategory::Intelligence;
    context.bindings = vec![
        ChannelBinding::new(channel("assistant"), room_id(), ChannelType::AI),
        target,
        inbound,
        intelligence,
    ];
    let mut person = Participant::new("alice".parse().unwrap(), room_id(), channel("in"));
    person.display_name = Some("Alice".into());
    person
        .metadata
        .clone_from(json!({"team": "support"}).as_object().unwrap());
    context.participants = vec![person];
    let mut earlier = input();
    earlier.index = 7;
    earlier.content = TextContent::new("earlier").into();
    context.recent_events = vec![earlier];
    context.history_start_index = Some(7);
    context.author_register = Some(
        json!({"version": 3, "names": {}})
            .as_object()
            .unwrap()
            .clone(),
    );
    context
}

fn input() -> RoomEvent {
    RoomEvent::new(
        room_id(),
        EventSource::new(channel("in"), ChannelType::WEBSOCKET),
        TextContent::new("read"),
    )
}

fn agent(streaming: bool) -> (AiChannel<MockAiProvider>, MockAiProvider) {
    let mut called = AiResponse::new("Searching.");
    called.tool_calls = vec![AiToolCall::new("tc1", "search")];
    let model = MockAiProvider::new(MockAiProviderOptions {
        streaming,
        ai_responses: Some(vec![called, AiResponse::new("Done.")]),
        ..Default::default()
    });
    let tools = Tools::new().tool(AiTool::new("search", "Search").unwrap(), |_, _| async {
        Ok("found".into())
    });
    let agent = AiChannel::new(
        channel("assistant"),
        model.clone(),
        AiChannelOptions {
            tools: tools.into(),
            ..Default::default()
        },
    )
    .unwrap();
    (agent, model)
}

fn check(
    requests: &[AiContext],
    room: &RoomContext,
    caps: &ChannelCapabilities,
    media: &[ChannelMediaType],
) {
    assert_eq!(requests.len(), 2);
    for request in requests {
        assert_eq!(request.room.as_ref(), Some(room));
        assert_eq!(request.target_capabilities.as_ref(), Some(caps));
        assert_eq!(
            request
                .target_media_types
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>(),
            media.iter().copied().collect()
        );
    }
}

// The supplied snapshot includes output-muted and disabled targets, an
// inbound-only transport, another intelligence channel and this channel
// marked as a transport. Selection reads the supplied bindings themselves.
#[tokio::test]
async fn full_room_and_target_fields_reach_every_round() {
    for streaming in [false, true] {
        let (agent, model) = agent(streaming);
        let context = snapshot();
        let own = context.bindings.first().unwrap().clone();
        agent.on_event(input(), own, context.clone()).await.unwrap();
        check(
            &model.calls(),
            &context,
            &target_caps(),
            &[ChannelMediaType::Text, ChannelMediaType::Rich],
        );
        agent.close().await;
    }
}

// The public hook can replace all three fields independently. Its values,
// including a complete hand-built snapshot, remain on the next tool round.
#[tokio::test]
async fn full_room_and_target_fields_reach_every_round_after_the_hook() {
    for streaming in [false, true] {
        let (agent, model) = agent(streaming);
        let room = Room::with_agent(agent).await;
        let replacement = snapshot();
        let captured = replacement.clone();
        let caps = ChannelCapabilities {
            media_types: [ChannelMediaType::Location].into(),
            max_length: Some(17),
            ..Default::default()
        };
        let captured_caps = caps.clone();
        let count = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&count);
        room.kit
            .hook(
                HookTrigger::BeforeAiGeneration,
                move |event: AiGenerationEvent, _: RoomContext| {
                    assert!(event.ai_context.snapshot().room.is_some());
                    counted.fetch_add(1, Ordering::SeqCst);
                    event.ai_context.update(|context| {
                        context.room = Some(captured.clone());
                        context.target_capabilities = Some(captured_caps.clone());
                        context.target_media_types = vec![ChannelMediaType::Audio];
                    });
                    async move { HookResult::modify(event, ModifyOptions::default()) }
                },
                HookOptions {
                    name: "replace context".into(),
                    ..Default::default()
                },
            )
            .unwrap();
        room.says("read").await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        check(
            &model.calls(),
            &replacement,
            &caps,
            &[ChannelMediaType::Audio],
        );
        room.kit.close().await.unwrap();
    }
}
