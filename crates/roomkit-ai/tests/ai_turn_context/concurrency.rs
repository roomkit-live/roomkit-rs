use super::lifetime::pending;
use super::support::{binding, snapshot};
use roomkit::model::{Participant, ParticipantRole, RoomContext, RoomEvent};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiTool, RoleOverride, RoleOverrideOptions, ToolHandler,
    ToolPolicy, ToolPolicyOptions, ToolResult, current_tool_allowed_names,
};
use std::sync::{Arc, Mutex};

// Python: TestCurrentToolRoomId::test_interleaved_streams_keep_tool_and_hook_room
// Python: TestLoopContextRestoration::test_a_stream_drained_in_another_task_restores_that_task_context
// Shared: role-policy/interleaved-turns-keep-their-role-{streaming,non-streaming}
// Outside a kit Rust collects the stream into ChannelOutput; poll the real
// collecting future, then move it to a consumer with its own tool context.
#[tokio::test]
async fn interleaved_turns_restore_each_consumer() {
    use roomkit::Channel;
    use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
    let gates: Arc<std::collections::HashMap<String, Arc<tokio::sync::Notify>>> = Arc::new(
        ["r1", "r2"]
            .into_iter()
            .map(|room| (room.into(), Arc::default()))
            .collect(),
    );
    let handlers = Arc::clone(&gates);
    let agent = Arc::new(
        AiChannel::new(
            "ai1".parse().unwrap(),
            ScriptedProvider::new([
                ScriptedRound::calling("", [("a", "check", "{}")]),
                ScriptedRound::calling("", [("b", "check", "{}")]),
                ScriptedRound::answering("done"),
                ScriptedRound::answering("done"),
            ]),
            AiChannelOptions {
                tools: vec![AiTool::new("owner_tool", "Owner tool").unwrap().into()],
                tool_search: Some(false),
                tool_policy: Some(role_policy(&["observer"])),
                tool_handler: Some(ToolHandler::new(move |_, _| {
                    let gates = Arc::clone(&handlers);
                    async move {
                        let before = snapshot("before suspension");
                        let room = before.room_id.clone().unwrap();
                        assert_role_tools(&room);
                        gates.get(&room).unwrap().notified().await;
                        let after = snapshot("after suspension");
                        assert_role_tools(&room);
                        after.check(&room, Some(&room), 3);
                        assert!(Arc::ptr_eq(
                            before.room.as_ref().unwrap(),
                            after.room.as_ref().unwrap()
                        ));
                        Ok(ToolResult::Text("ok".into()))
                    }
                })),
                ..AiChannelOptions::default()
            },
        )
        .unwrap(),
    );
    let start = |room: &'static str| {
        let agent = Arc::clone(&agent);
        Box::pin(async move {
            let (event, context) = role_event(room);
            agent.on_event(event, binding(room), context).await.unwrap()
        })
    };
    let (mut first, mut second) = (start("r1"), start("r2"));
    pending(first.as_mut()).await;
    pending(second.as_mut()).await;
    assert!(roomkit_ai::current_tool_room_id().is_none());
    gates.get("r1").unwrap().notify_one();
    gates.get("r2").unwrap().notify_one();
    let first = tokio::spawn(async move {
        roomkit_ai::tool_turn_context(super::parent(), async {
            first.await;
            snapshot("consumer").check("outer", Some("parent"), 1);
            assert!(current_tool_allowed_names().is_none());
        })
        .await
        .unwrap();
    });
    second.await;
    first.await.unwrap();
    assert!(roomkit_ai::current_tool_room_id().is_none());
    assert!(current_tool_allowed_names().is_none());
    agent.close().await;
}

fn role_policy(roles: &[&str]) -> ToolPolicy {
    ToolPolicy::new(ToolPolicyOptions {
        role_overrides: roles
            .iter()
            .map(|role| {
                (
                    (*role).into(),
                    RoleOverride::new(RoleOverrideOptions {
                        deny: vec!["owner_tool".into()],
                        ..Default::default()
                    })
                    .unwrap(),
                )
            })
            .collect(),
        ..Default::default()
    })
    .unwrap()
}

fn role_event(room: &str) -> (RoomEvent, RoomContext) {
    let (event, mut context) = super::support::event(room, Some(room));
    let mut participant = Participant::new(
        room.parse().unwrap(),
        context.room.id.clone(),
        "sms1".parse().unwrap(),
    );
    participant.role = if room == "r1" {
        ParticipantRole::Observer
    } else {
        ParticipantRole::Owner
    };
    context.participants.push(participant);
    (event, context)
}

fn assert_role_tools(room: &str) {
    let names = current_tool_allowed_names().unwrap();
    assert!(names.contains("check"));
    assert_eq!(names.contains("owner_tool"), room == "r2");
}

// Python: test_ai_skill_gating.py::TestRoleBasedPolicy::test_no_participant_match_uses_base_policy
// The shared inbound path materializes unknown senders; this direct public
// channel call proves a context whose participant does not name its sender.
// Every explicit role denies the tool, distinguishing no role from a default member.
#[tokio::test]
async fn an_unmatched_sender_keeps_the_base_policy() {
    use roomkit::Channel;

    for streaming in [false, true] {
        let readings = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&readings);
        let agent = AiChannel::new(
            "ai1".parse().unwrap(),
            super::support::model(streaming, true),
            AiChannelOptions {
                tools: vec![AiTool::new("owner_tool", "Owner tool").unwrap().into()],
                tool_search: Some(false),
                tool_policy: Some(role_policy(&[
                    "owner", "agent", "member", "observer", "bot",
                ])),
                tool_handler: Some(ToolHandler::new(move |_, _| {
                    seen.lock()
                        .unwrap()
                        .push(current_tool_allowed_names().unwrap().contains("owner_tool"));
                    async { Ok(ToolResult::Text("done".into())) }
                })),
                ..Default::default()
            },
        )
        .unwrap();
        let (mut event, context) = role_event("r1");
        event.source.participant_id = Some("unknown".parse().unwrap());
        agent.on_event(event, binding("r1"), context).await.unwrap();
        assert_eq!(*readings.lock().unwrap(), vec![true, true]);
        agent.close().await;
    }
}
