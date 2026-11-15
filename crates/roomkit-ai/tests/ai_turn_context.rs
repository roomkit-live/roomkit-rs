//! Actual local AI turns: Python `test_tool_context.py` and
//! `test_ai_turn_context_lifetime.py`. Native task-local and identity proofs.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

#[path = "ai_turn_context/catalogue.rs"]
mod catalogue;
#[path = "ai_turn_context/concurrency.rs"]
mod concurrency;
#[path = "ai_turn_context/human.rs"]
mod human;
#[path = "ai_turn_context/lifetime.rs"]
mod lifetime;
#[path = "ai_turn_context/support.rs"]
mod support;

use std::sync::{Arc, Mutex};

use roomkit::Channel;
use roomkit::model::{RoomContext, RoomId};
use roomkit_ai::{
    AiChannel, AiChannelOptions, ToolHandler, ToolResult, ToolTurnContextOptions,
    current_response_metadata, current_tool_actor_id, current_tool_chain_depth, current_tool_room,
    current_tool_room_id, tool_turn_context,
};
use serde_json::json;

use support::{Memory, Seen, binding, event, model, snapshot};

// Python: test_ai_turn_context_lifetime.py::test_preparation_installs_the_turn_after_ingestion
#[tokio::test]
async fn preparation_installs_the_turn_after_ingestion() {
    for streaming in [false, true] {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let config_seen = Arc::clone(&seen);
        let options = AiChannelOptions {
            memory: Some(Arc::new(Memory(Arc::clone(&seen)))),
            config_provider: Some(Arc::new(move |_, context| {
                config_seen.lock().unwrap().push(snapshot("config"));
                assert_eq!(*current_tool_room().unwrap(), context.room);
                current_response_metadata()
                    .unwrap()
                    .insert("configured", json!(true));
                Box::pin(async { Ok(None) })
            })),
            ..AiChannelOptions::default()
        };
        let agent =
            AiChannel::new("ai1".parse().unwrap(), model(streaming, false), options).unwrap();
        let (input, context) = event("r1", Some("alice"));
        let expected = context.room.clone();
        tool_turn_context(parent(), async {
            let output = agent.on_event(input, binding("r1"), context).await.unwrap();
            assert_eq!(
                output.response_metadata.get("configured"),
                Some(json!(true))
            );
            assert_eq!(current_tool_room_id().unwrap().as_str(), "outer");
            assert_eq!(current_tool_actor_id().as_deref(), Some("parent"));
        })
        .await
        .unwrap();
        let readings = seen.lock().unwrap();
        assert_eq!(
            readings.iter().map(|item| item.stage).collect::<Vec<_>>(),
            ["ingest", "config", "memory"]
        );
        assert_eq!(readings[0].room_id.as_deref(), Some("outer"));
        for value in &readings[1..] {
            value.check("r1", Some("alice"), 3);
            assert_eq!(**value.room.as_ref().unwrap(), expected);
        }
        assert!(Arc::ptr_eq(
            readings[1].room.as_ref().unwrap(),
            readings[2].room.as_ref().unwrap()
        ));
    }
}

fn parent() -> ToolTurnContextOptions {
    ToolTurnContextOptions {
        room_id: Some(RoomId::new("outer").unwrap()),
        actor_id: Some("parent".into()),
        chain_depth: 1,
        ..ToolTurnContextOptions::default()
    }
}

// Python: test_ai_turn_context_lifetime.py::test_a_failed_preparation_restores_its_caller
#[tokio::test]
async fn a_failed_preparation_restores_its_caller() {
    let agent = AiChannel::new(
        "ai1".parse().unwrap(),
        model(false, false),
        AiChannelOptions {
            config_provider: Some(Arc::new(|_, _| {
                snapshot("config").check("r1", Some("alice"), 3);
                Box::pin(async { Err("configuration failed".into()) })
            })),
            ..AiChannelOptions::default()
        },
    )
    .unwrap();
    tool_turn_context(parent(), async {
        let (input, context) = event("r1", Some("alice"));
        let error = agent
            .on_event(input, binding("r1"), context)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("configuration failed"));
        assert_eq!(current_tool_room_id().unwrap().as_str(), "outer");
        assert_eq!(current_tool_actor_id().as_deref(), Some("parent"));
        assert_eq!(current_tool_chain_depth(), 1);
    })
    .await
    .unwrap();
}

// Python: test_tool_context.py::TestCurrentToolActorId (both modes, absent author included)
// Python: TestCurrentToolRoomId::{test_streaming_handler_sees_turn_room,test_non_streaming_handler_sees_turn_room,test_shared_channel_tracks_each_turn}
// Python: TestCurrentToolRoom::{test_non_streaming_handler_sees_the_turn_room_object,test_streaming_handler_sees_the_turn_room_object,test_second_round_still_sees_the_room,test_shared_channel_tracks_each_turn_room}
// Python: TestLoopContextRestoration::test_the_enclosing_context_survives_a_nested_turn
#[tokio::test]
async fn handlers_keep_each_turn_room_author_depth_and_snapshot_across_rounds() {
    for streaming in [false, true] {
        let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();
        let kept = Arc::clone(&seen);
        let agent = AiChannel::new(
            "ai1".parse().unwrap(),
            model(streaming, true),
            AiChannelOptions {
                tool_handler: Some(ToolHandler::new(move |_, _| {
                    let kept = Arc::clone(&kept);
                    async move {
                        let value = snapshot("handler");
                        let call = roomkit_ai::current_tool_call().unwrap();
                        assert_eq!(
                            call.room_id.as_ref().map(RoomId::as_str),
                            value.room_id.as_deref()
                        );
                        kept.lock().unwrap().push(value);
                        Ok(ToolResult::Text("ok".into()))
                    }
                })),
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        for (room, actor) in [("r1", Some("alice")), ("r2", Some("bob")), ("r1", None)] {
            let (input, context) = event(room, actor);
            let expected = context.room.clone();
            tool_turn_context(parent(), async {
                agent.on_event(input, binding(room), context).await.unwrap();
                snapshot("restored").check("outer", Some("parent"), 1);
            })
            .await
            .unwrap();
            let values = std::mem::take(&mut *seen.lock().unwrap());
            assert_eq!(values.len(), 2);
            for value in &values {
                value.check(room, actor, 3);
                assert_eq!(**value.room.as_ref().unwrap(), expected);
            }
            assert!(Arc::ptr_eq(
                values[0].room.as_ref().unwrap(),
                values[1].room.as_ref().unwrap()
            ));
        }
        assert!(current_tool_room().is_none());
        assert!(current_tool_room_id().is_none());
        assert!(current_tool_actor_id().is_none());
        assert_eq!(current_tool_chain_depth(), 0);
    }
}

// Python: TestCurrentToolActorId::test_an_unidentified_speaker_reads_back_the_same
#[tokio::test]
async fn an_unidentified_speaker_reads_back_the_same() {
    use roomkit::model::{IdentificationStatus, Participant};
    for status in [IdentificationStatus::Unknown, IdentificationStatus::Pending] {
        let (input, mut context) = event("r1", Some("alice"));
        let mut participant = Participant::new(
            "alice".parse().unwrap(),
            "r1".parse().unwrap(),
            "sms1".parse().unwrap(),
        );
        participant.identification = status;
        context.participants.push(participant);
        let agent = AiChannel::new(
            "ai1".parse().unwrap(),
            model(false, false),
            AiChannelOptions {
                config_provider: Some(Arc::new(|_, _: RoomContext| {
                    assert_eq!(current_tool_actor_id().as_deref(), Some("alice"));
                    Box::pin(async { Ok(None) })
                })),
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        agent.on_event(input, binding("r1"), context).await.unwrap();
    }
}
