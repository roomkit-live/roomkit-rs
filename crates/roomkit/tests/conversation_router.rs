//! The conversation router's selection (RFC §19.4): the rules a router
//! tries, which the shared scenarios show through the agent asked, and the
//! host's own predicate, which a scenario cannot write.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::collections::BTreeSet;
use std::sync::Arc;

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelType, EventSource, HookAction, Room,
    RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit::orchestration::ConversationRouterOptions;
use roomkit::{
    ConversationRouter, ConversationState, HookExecution, HookOptions, HookRegistration,
    HookTrigger, RoomKit, RoomKitOptions, RoutingConditions, RoutingRule, set_conversation_state,
};
use serde_json::{Value, json};

fn id(value: &str) -> ChannelId {
    ChannelId::new(value).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("r1").unwrap()
}

fn ai(channel_id: &str) -> ChannelBinding {
    ChannelBinding {
        category: ChannelCategory::Intelligence,
        ..ChannelBinding::new(id(channel_id), room_id(), ChannelType::AI)
    }
}

fn transport(channel_id: &str, channel_type: ChannelType) -> ChannelBinding {
    ChannelBinding::new(id(channel_id), room_id(), channel_type)
}

fn sms(channel_id: &str) -> ChannelBinding {
    transport(channel_id, ChannelType::SMS)
}

fn context(bindings: Vec<ChannelBinding>) -> RoomContext {
    RoomContext {
        bindings,
        ..RoomContext::new(Room::new(room_id()))
    }
}

fn event_from(channel_id: &str, channel_type: ChannelType) -> RoomEvent {
    let source = EventSource::new(id(channel_id), channel_type);
    RoomEvent::new(room_id(), source, TextContent::new("hello"))
}

fn sms_event() -> RoomEvent {
    event_from("sms1", ChannelType::SMS)
}

fn with_metadata(mut event: RoomEvent, metadata: &Value) -> RoomEvent {
    event.metadata.clone_from(metadata.as_object().unwrap());
    event
}

fn router(rules: Vec<RoutingRule>, default_agent_id: Option<&str>) -> ConversationRouter {
    ConversationRouter::new(ConversationRouterOptions {
        rules,
        default_agent_id: default_agent_id.map(id),
        supervisor_id: None,
    })
}

fn rule(agent_id: &str, conditions: RoutingConditions) -> RoutingRule {
    RoutingRule::new(id(agent_id), conditions)
}

fn set<T: Ord>(items: impl IntoIterator<Item = T>) -> BTreeSet<T> {
    items.into_iter().collect()
}

fn phases(names: &[&str]) -> RoutingConditions {
    RoutingConditions {
        phases: Some(set(names.iter().map(|name| (*name).to_owned()))),
        ..RoutingConditions::new()
    }
}

fn in_phase(phase: &str) -> ConversationState {
    ConversationState {
        phase: phase.to_owned(),
        ..ConversationState::new()
    }
}

fn selected(
    router: &ConversationRouter,
    event: &RoomEvent,
    ctx: &RoomContext,
) -> Option<ChannelId> {
    selected_in(router, event, ctx, &ConversationState::new())
}

fn selected_in(
    router: &ConversationRouter,
    event: &RoomEvent,
    ctx: &RoomContext,
    state: &ConversationState,
) -> Option<ChannelId> {
    router.select_agent(event, ctx, state)
}

// Python: tests/orchestration/test_router.py::TestSelectAgentNoRules::test_no_rules_no_default_returns_none
#[test]
fn no_rules_and_no_default_select_no_agent() {
    let ctx = context(vec![sms("sms1")]);
    assert_eq!(selected(&router(vec![], None), &sms_event(), &ctx), None);
}

// Python: tests/orchestration/test_router.py::TestSelectAgentNoRules::test_no_rules_with_default
#[test]
fn no_rules_select_the_default() {
    let ctx = context(vec![sms("sms1"), ai("agent-triage")]);
    let router = router(vec![], Some("agent-triage"));
    assert_eq!(
        selected(&router, &sms_event(), &ctx),
        Some(id("agent-triage"))
    );
}

// Python: tests/orchestration/test_router.py::TestAgentAffinity::test_sticky_routing_when_active_agent_set
// Python: tests/orchestration/test_router.py::TestAgentAffinity::test_falls_through_when_active_agent_not_in_room
#[test]
fn the_active_agent_keeps_the_conversation_while_the_room_binds_it() {
    let ctx = context(vec![sms("sms1"), ai("agent-a"), ai("agent-default")]);
    let router = router(vec![], Some("agent-default"));
    let held = |agent: &str| ConversationState {
        active_agent_id: Some(id(agent)),
        ..ConversationState::new()
    };
    let event = sms_event();
    assert_eq!(
        selected_in(&router, &event, &ctx, &held("agent-a")),
        Some(id("agent-a"))
    );
    assert_eq!(
        selected_in(&router, &event, &ctx, &held("agent-gone")),
        Some(id("agent-default"))
    );
}

// Python: tests/orchestration/test_router.py::TestRuleMatching::test_phase_rule
#[test]
fn a_rule_applies_in_its_phases() {
    let ctx = context(vec![sms("sms1"), ai("agent-intake"), ai("agent-handling")]);
    let router = router(
        vec![
            rule("agent-intake", phases(&["intake"])),
            rule("agent-handling", phases(&["handling"])),
        ],
        None,
    );
    let event = sms_event();
    assert_eq!(
        selected_in(&router, &event, &ctx, &in_phase("intake")),
        Some(id("agent-intake"))
    );
    assert_eq!(
        selected_in(&router, &event, &ctx, &in_phase("handling")),
        Some(id("agent-handling"))
    );
}

// Python: tests/orchestration/test_router.py::TestRuleMatching::test_channel_type_rule
#[test]
fn a_rule_applies_to_its_source_channel_types() {
    let types = |types: Vec<ChannelType>| RoutingConditions {
        channel_types: Some(set(types)),
        ..RoutingConditions::new()
    };
    let router = router(
        vec![
            rule("agent-voice", types(vec![ChannelType::VOICE])),
            rule(
                "agent-text",
                types(vec![ChannelType::SMS, ChannelType::WEBSOCKET]),
            ),
        ],
        None,
    );
    let ctx = context(vec![sms("sms1"), ai("agent-voice"), ai("agent-text")]);
    assert_eq!(
        selected(&router, &sms_event(), &ctx),
        Some(id("agent-text"))
    );
    let voice = event_from("voice1", ChannelType::VOICE);
    assert_eq!(selected(&router, &voice, &ctx), Some(id("agent-voice")));
}

// Python: tests/orchestration/test_router.py::TestRuleMatching::test_source_channel_id_rule
#[test]
fn a_rule_applies_to_its_source_channels() {
    let vip = RoutingConditions {
        source_channel_ids: Some(set([id("vip-ws")])),
        ..RoutingConditions::new()
    };
    let router = router(vec![rule("agent-vip", vip)], Some("agent-default"));
    let ctx = context(vec![
        transport("vip-ws", ChannelType::WEBSOCKET),
        transport("normal-ws", ChannelType::WEBSOCKET),
        ai("agent-vip"),
        ai("agent-default"),
    ]);
    let vip_event = event_from("vip-ws", ChannelType::WEBSOCKET);
    let normal_event = event_from("normal-ws", ChannelType::WEBSOCKET);
    assert_eq!(selected(&router, &vip_event, &ctx), Some(id("agent-vip")));
    assert_eq!(
        selected(&router, &normal_event, &ctx),
        Some(id("agent-default"))
    );
}

// Python: tests/orchestration/test_router.py::TestRuleMatching::test_intent_rule
#[test]
fn a_rule_applies_to_its_intents() {
    let intents = RoutingConditions {
        intents: Some(set(["billing".to_owned(), "payment".to_owned()])),
        ..RoutingConditions::new()
    };
    let router = router(vec![rule("agent-billing", intents)], Some("agent-general"));
    let ctx = context(vec![sms("sms1"), ai("agent-billing"), ai("agent-general")]);
    let billing = with_metadata(sms_event(), &json!({"intent": "billing"}));
    let other = with_metadata(sms_event(), &json!({"intent": "support"}));
    assert_eq!(selected(&router, &billing, &ctx), Some(id("agent-billing")));
    assert_eq!(selected(&router, &other, &ctx), Some(id("agent-general")));
}

// Python: tests/orchestration/test_router.py::TestRuleMatching::test_custom_predicate
#[test]
fn a_rule_applies_where_the_host_s_predicate_holds() {
    let urgent = RoutingConditions {
        custom: Some(Arc::new(
            |event: &RoomEvent, _: &RoomContext, _: &ConversationState| {
                event.metadata.get("urgent") == Some(&json!(true))
            },
        )),
        ..RoutingConditions::new()
    };
    let router = router(vec![rule("agent-urgent", urgent)], Some("agent-normal"));
    let ctx = context(vec![sms("sms1"), ai("agent-urgent"), ai("agent-normal")]);
    let urgent = with_metadata(sms_event(), &json!({"urgent": true}));
    let normal = with_metadata(sms_event(), &json!({"urgent": false}));
    assert_eq!(selected(&router, &urgent, &ctx), Some(id("agent-urgent")));
    assert_eq!(selected(&router, &normal, &ctx), Some(id("agent-normal")));
}

// Python: tests/orchestration/test_router.py::TestRuleMatching::test_combined_conditions_and_logic
#[test]
fn every_condition_of_a_rule_must_hold() {
    let special = RoutingConditions {
        channel_types: Some(set([ChannelType::SMS])),
        ..phases(&["handling"])
    };
    let router = router(vec![rule("agent-special", special)], Some("agent-default"));
    let ctx = context(vec![sms("sms1"), ai("agent-special"), ai("agent-default")]);
    let handling = in_phase("handling");
    let ws = event_from("ws1", ChannelType::WEBSOCKET);
    assert_eq!(
        selected_in(&router, &sms_event(), &ctx, &handling),
        Some(id("agent-special"))
    );
    assert_eq!(
        selected_in(&router, &ws, &ctx, &handling),
        Some(id("agent-default"))
    );
}

// Python: tests/orchestration/test_router.py::TestRuleMatching::test_no_matching_rule_falls_to_default
#[test]
fn no_rule_applying_selects_the_default() {
    let router = router(
        vec![rule("agent-nope", phases(&["nonexistent"]))],
        Some("agent-default"),
    );
    let ctx = context(vec![sms("sms1"), ai("agent-default")]);
    assert_eq!(
        selected(&router, &sms_event(), &ctx),
        Some(id("agent-default"))
    );
}

// Python: tests/orchestration/test_router.py::TestRulePriority::test_lower_priority_wins
#[test]
fn the_lower_priority_wins() {
    let low = RoutingRule {
        priority: 10,
        ..rule("agent-low", phases(&["intake"]))
    };
    let router = router(vec![low, rule("agent-high", phases(&["intake"]))], None);
    let ctx = context(vec![sms("sms1"), ai("agent-low"), ai("agent-high")]);
    assert_eq!(
        selected(&router, &sms_event(), &ctx),
        Some(id("agent-high"))
    );
}

// Python: tests/orchestration/test_router.py::TestIntelligenceSourceSkipped::test_events_from_ai_channel_not_routed
#[test]
fn an_agent_s_output_is_routed_to_no_agent() {
    let router = router(vec![], Some("agent-a"));
    let ctx = context(vec![ai("agent-a"), ai("agent-b")]);
    let event = event_from("agent-a", ChannelType::AI);
    assert_eq!(selected(&router, &event, &ctx), None);
}

// Python: tests/orchestration/test_router.py::TestAsHook::test_hook_returns_allow_when_no_routing_from_transport
// Python: tests/orchestration/test_router.py::TestAddressedEvents::test_addressed_event_is_left_alone
// Python: tests/orchestration/test_router.py::TestAddressedEvents::test_address_beats_sticky_affinity
#[tokio::test]
async fn the_hook_allows_what_it_does_not_route_and_an_addressed_event() {
    let hook = router(vec![], Some("agent-default")).as_hook();
    let held = ConversationState {
        active_agent_id: Some(id("agent-a")),
        ..ConversationState::new()
    };
    let ctx = RoomContext {
        bindings: vec![sms("sms1"), ai("agent-a"), ai("agent-b")],
        ..RoomContext::new(set_conversation_state(&Room::new(room_id()), &held))
    };
    let mut addressed = sms_event();
    addressed.addressed_to = Some(vec![id("agent-b")]);
    let result = hook(addressed, ctx).await.unwrap();
    assert_eq!((result.action, result.event), (HookAction::Allow, None));

    let unrouted = router(vec![], None).as_hook();
    let result = unrouted(sms_event(), context(vec![sms("sms1")]))
        .await
        .unwrap();
    assert_eq!((result.action, result.event), (HookAction::Allow, None));
}

// Python: core/hooks.py `mark_router_hook`, `HookEngine.has_router_hook`.
#[tokio::test]
async fn a_router_s_hook_is_known_for_one_wherever_it_is_registered() {
    let room = RoomId::new("r1").unwrap();
    let other = RoomId::new("r2").unwrap();
    let router = ConversationRouter::new(ConversationRouterOptions::default());

    let kit = RoomKit::new(RoomKitOptions::default());
    assert!(!kit.hook_engine().has_router_hook(&room));
    let hook = HookRegistration::new(
        HookTrigger::BeforeBroadcast,
        HookExecution::Sync,
        router.as_hook(),
    );
    kit.hook_engine().add_room_hook(&room, hook).unwrap();
    assert!(kit.hook_engine().has_router_hook(&room));
    assert!(!kit.hook_engine().has_router_hook(&other));

    // The kit's own applies to every room.
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.hook(
        HookTrigger::BeforeBroadcast,
        router.as_hook(),
        HookOptions::default(),
    )
    .unwrap();
    assert!(kit.hook_engine().has_router_hook(&other));

    // A hook of another's is no router's, its closure wrapping one or not.
    let kit = RoomKit::new(RoomKitOptions::default());
    let own = router.as_hook();
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, context: RoomContext| own(event, context),
        HookOptions::default(),
    )
    .unwrap();
    assert!(!kit.hook_engine().has_router_hook(&room));
}
