//! Python: tests/test_tool_turn_context.py; the public installer only.
//! HITL 47e0cee0: a scoped future, u32 depth and an immutable Arc room.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

#[path = "tool_turn_context/lifetime.rs"]
mod lifetime;

use std::sync::Arc;

use roomkit::model::{Room, RoomId};
use roomkit_ai::{
    AiTool, ToolCallContext, ToolTurnContextOptions, current_response_metadata,
    current_tool_actor_id, current_tool_allowed_names, current_tool_call, current_tool_chain_depth,
    current_tool_requester, current_tool_room, current_tool_room_id, tool_turn_context,
};
use serde_json::json;

fn room_id(id: &str) -> RoomId {
    RoomId::new(id).unwrap()
}

fn outside() {
    assert!(current_tool_room_id().is_none());
    assert!(current_tool_room().is_none());
    assert!(current_tool_actor_id().is_none());
    assert!(current_tool_requester().is_none());
    assert!(current_tool_allowed_names().is_none());
    assert!(current_tool_call().is_none());
    assert!(current_response_metadata().is_none());
    assert_eq!(current_tool_chain_depth(), 0);
}

// Python: TestTheTurnItDescribes::test_the_accessors_answer_the_turn_inside_and_none_outside
#[tokio::test]
async fn the_accessors_answer_the_turn_inside_and_none_outside() {
    outside();
    let call = ToolCallContext {
        room_id: Some(room_id("r1")),
        tool_call_id: "tc1".into(),
        channel_id: Some("ai".parse().unwrap()),
        ..Default::default()
    };
    let options = ToolTurnContextOptions {
        room_id: call.room_id.clone(),
        actor_id: Some("alice".into()),
        tools: Some(vec![
            AiTool::new("lookup", "Look up").unwrap(),
            AiTool::new("pay", "").unwrap(),
        ]),
        call: Some(call.clone()),
        ..Default::default()
    };
    tool_turn_context(options, async {
        assert_eq!(current_tool_room_id(), Some(room_id("r1")));
        assert_eq!(current_tool_actor_id().as_deref(), Some("alice"));
        assert_eq!(
            current_tool_allowed_names(),
            Some(["lookup".into(), "pay".into()].into())
        );
        assert_eq!(current_tool_call(), Some(call));
        assert!(current_tool_room().is_none());
    })
    .await
    .unwrap();
    outside();
}

// Python: TestTheTurnItDescribes::test_a_room_names_the_turns_room_id
#[tokio::test]
async fn a_room_names_the_turns_room_id() {
    let mut value = Room::new(room_id("billing"));
    value.organization_id = Some("acme".into());
    let room = Arc::new(value);
    tool_turn_context(
        ToolTurnContextOptions {
            room: Some(Arc::clone(&room)),
            ..Default::default()
        },
        async {
            assert!(Arc::ptr_eq(&current_tool_room().unwrap(), &room));
            assert_eq!(current_tool_room_id(), Some(room_id("billing")));
            assert_eq!(
                current_tool_room().unwrap().organization_id.as_deref(),
                Some("acme")
            );
        },
    )
    .await
    .unwrap();
}

// Python: TestTheTurnItDescribes::test_a_room_and_the_same_room_id_agree
#[tokio::test]
async fn a_room_and_the_same_room_id_agree() {
    tool_turn_context(
        ToolTurnContextOptions {
            room: Some(Arc::new(Room::new(room_id("billing")))),
            room_id: Some(room_id("billing")),
            ..Default::default()
        },
        async {
            assert_eq!(current_tool_room_id(), Some(room_id("billing")));
        },
    )
    .await
    .unwrap();
}

// Python: TestTheTurnItDescribes::test_a_room_and_another_room_id_are_refused
#[tokio::test]
async fn a_room_and_another_room_id_are_refused() {
    let polled = std::cell::Cell::new(false);
    let result = tool_turn_context(
        ToolTurnContextOptions {
            room: Some(Arc::new(Room::new(room_id("billing")))),
            room_id: Some(room_id("support")),
            ..Default::default()
        },
        async {
            polled.set(true);
        },
    )
    .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("name different rooms")
    );
    assert!(!polled.get());
    outside();
}

// Python: TestTheTurnItDescribes::test_a_call_record_of_another_room_is_refused
#[tokio::test]
async fn a_call_record_of_another_room_is_refused() {
    for room in [Some(room_id("r1")), None] {
        let polled = std::cell::Cell::new(false);
        let result = tool_turn_context(
            ToolTurnContextOptions {
                room_id: room,
                call: Some(ToolCallContext {
                    room_id: Some(room_id("other")),
                    ..Default::default()
                }),
                ..Default::default()
            },
            async {
                polled.set(true);
            },
        )
        .await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("is not the turn's room")
        );
        assert!(!polled.get());
        outside();
    }
}

// Python: TestTheTurnItDescribes::test_a_call_record_of_the_turns_room_is_installed
#[tokio::test]
async fn a_call_record_of_the_turns_room_is_installed() {
    for room in [Some(Arc::new(Room::new(room_id("billing")))), None] {
        let call = ToolCallContext {
            room_id: room.as_ref().map(|room| room.id.clone()),
            tool_call_id: "tc1".into(),
            ..Default::default()
        };
        tool_turn_context(
            ToolTurnContextOptions {
                room,
                call: Some(call.clone()),
                ..Default::default()
            },
            async {
                assert_eq!(current_tool_call(), Some(call));
            },
        )
        .await
        .unwrap();
    }
}

// Python: TestTheTurnItDescribes::test_no_toolset_is_not_an_empty_one
#[tokio::test]
async fn no_toolset_is_not_an_empty_one() {
    for tools in [None, Some(Vec::new())] {
        let expected = tools.as_ref().map(|_| Default::default());
        tool_turn_context(
            ToolTurnContextOptions {
                tools,
                ..Default::default()
            },
            async {
                assert_eq!(current_tool_allowed_names(), expected);
            },
        )
        .await
        .unwrap();
    }
}

// Python: TestTheTurnItDescribes::test_a_turn_without_author_reads_none
#[tokio::test]
async fn a_turn_without_author_reads_none() {
    tool_turn_context(
        ToolTurnContextOptions {
            room_id: Some(room_id("r1")),
            ..Default::default()
        },
        async {
            assert_eq!(current_tool_room_id(), Some(room_id("r1")));
            assert!(current_tool_actor_id().is_none());
            assert!(current_tool_call().is_none());
        },
    )
    .await
    .unwrap();
}

// Python: TestTheTurnItDescribes::test_the_turn_carries_its_chain_depth
// HITL 47e0cee0 explicitly excludes Python's -1, 2**32 and 2**100 values.
#[tokio::test]
async fn the_turn_carries_its_chain_depth() {
    for chain_depth in [0, 1, 3, u32::MAX] {
        tool_turn_context(
            ToolTurnContextOptions {
                chain_depth,
                ..Default::default()
            },
            async {
                assert_eq!(current_tool_chain_depth(), chain_depth);
            },
        )
        .await
        .unwrap();
        assert_eq!(current_tool_chain_depth(), 0);
    }
}

// Python: TestTheTurnItDescribes::test_a_requester_is_explicit_and_an_inner_default_clears_it
#[tokio::test]
async fn a_requester_is_explicit_and_an_inner_default_clears_it() {
    tool_turn_context(
        ToolTurnContextOptions {
            requester: Some("Bob".into()),
            ..Default::default()
        },
        async {
            assert_eq!(current_tool_requester().as_deref(), Some("Bob"));
            tool_turn_context(Default::default(), async {
                assert!(current_tool_requester().is_none());
            })
            .await
            .unwrap();
            assert_eq!(current_tool_requester().as_deref(), Some("Bob"));
        },
    )
    .await
    .unwrap();
    outside();
}

// Python: TestWhatTheHandlerWritesBack::test_the_response_record_is_the_turns_one
#[tokio::test]
async fn the_response_record_is_the_turns_one() {
    let handler = roomkit_ai::ToolHandler::new(|_, _| async {
        current_response_metadata()
            .unwrap()
            .insert("sources", json!(["doc-1"]));
        Ok(roomkit_ai::ToolResult::Text("ok".into()))
    });
    tool_turn_context(Default::default(), async {
        let record = current_response_metadata().unwrap();
        handler
            .call("lookup".into(), Default::default())
            .await
            .unwrap();
        assert_eq!(record.get("sources"), Some(json!(["doc-1"])));
        assert_eq!(current_response_metadata(), Some(record));
    })
    .await
    .unwrap();
}

// Python: TestWhatTheHandlerWritesBack::test_the_structured_copy_lands_on_the_call_record_passed
#[tokio::test]
async fn the_structured_copy_lands_on_the_call_record_passed() {
    let call = ToolCallContext::default();
    tool_turn_context(
        ToolTurnContextOptions {
            call: Some(call.clone()),
            ..Default::default()
        },
        async {
            current_tool_call()
                .unwrap()
                .structured_content
                .replace(Some(Default::default()));
            current_tool_call()
                .unwrap()
                .structured_content
                .get()
                .unwrap()
                .insert("page", json!(3));
        },
    )
    .await
    .unwrap();
    assert_eq!(
        call.structured_content.get().unwrap().get("page"),
        Some(json!(3))
    );
}

// Python: TestWhatTheHandlerWritesBack::test_tasks_started_inside_inherit_the_turn
#[tokio::test]
async fn tasks_started_inside_inherit_the_turn() {
    tool_turn_context(
        ToolTurnContextOptions {
            actor_id: Some("alice".into()),
            ..Default::default()
        },
        async {
            let actor = || {
                roomkit::keep_context(async {
                    tokio::task::yield_now().await;
                    current_tool_actor_id()
                })
            };
            let (one, two) = (tokio::spawn(actor()), tokio::spawn(actor()));
            assert_eq!(one.await.unwrap().as_deref(), Some("alice"));
            assert_eq!(two.await.unwrap().as_deref(), Some("alice"));
            tokio::spawn(async {
                outside();
            })
            .await
            .unwrap();
        },
    )
    .await
    .unwrap();
}

// Python: TestRestoring::test_each_scope_has_a_fresh_response_record
#[tokio::test]
async fn each_scope_has_a_fresh_response_record() {
    tool_turn_context(Default::default(), async {
        let outer = current_response_metadata().unwrap();
        outer.insert("source", json!("outer"));
        tool_turn_context(Default::default(), async {
            let inner = current_response_metadata().unwrap();
            assert!(inner.is_empty());
            inner.insert("source", json!("inner"));
        })
        .await
        .unwrap();
        assert_eq!(
            current_response_metadata().unwrap().get("source"),
            Some(json!("outer"))
        );
        current_response_metadata()
            .unwrap()
            .insert("restored", json!(true));
        assert_eq!(outer.get("restored"), Some(json!(true)));
    })
    .await
    .unwrap();
    outside();
}

// Python: TestRestoring::test_a_nested_turn_gives_the_outer_one_back
#[tokio::test]
async fn a_nested_turn_gives_the_outer_one_back() {
    let outer_call = ToolCallContext {
        room_id: Some(room_id("outer")),
        ..Default::default()
    };
    tool_turn_context(
        ToolTurnContextOptions {
            room_id: outer_call.room_id.clone(),
            actor_id: Some("alice".into()),
            call: Some(outer_call.clone()),
            ..Default::default()
        },
        async {
            tool_turn_context(
                ToolTurnContextOptions {
                    room_id: Some(room_id("inner")),
                    actor_id: Some("bob".into()),
                    ..Default::default()
                },
                async {
                    assert_eq!(current_tool_room_id(), Some(room_id("inner")));
                    assert_eq!(current_tool_actor_id().as_deref(), Some("bob"));
                    assert!(current_tool_call().is_none());
                },
            )
            .await
            .unwrap();
            assert_eq!(current_tool_room_id(), Some(room_id("outer")));
            assert_eq!(current_tool_actor_id().as_deref(), Some("alice"));
            assert_eq!(current_tool_call(), Some(outer_call));
        },
    )
    .await
    .unwrap();
    outside();
}

// Python: TestRestoring::test_both_installers_restore_on_an_exception[public]
#[tokio::test]
async fn both_installers_restore_on_an_exception() {
    let output = tool_turn_context(
        ToolTurnContextOptions {
            actor_id: Some("alice".into()),
            ..Default::default()
        },
        async {
            assert_eq!(current_tool_actor_id().as_deref(), Some("alice"));
            Err::<(), _>("handler failed")
        },
    )
    .await
    .unwrap();
    assert_eq!(output, Err("handler failed"));
    outside();
}
