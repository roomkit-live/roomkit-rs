//! The Rust handle for Python's assignable `ToolCallContext` field (HITL 0536fcf0).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use pretty_assertions::assert_eq;
use roomkit::Channel;
use roomkit::model::{
    ChannelBinding, ChannelId, ChannelType, EventContent, EventSource, EventType, Metadata,
    ResponseMetadata, Room, RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiTool, ToolCallContext, ToolHandler, ToolResult,
    ToolStructuredContent, current_response_metadata, current_tool_call,
};
use serde_json::json;

#[test]
fn absence_and_an_empty_dictionary_are_distinct() {
    let absent = ToolStructuredContent::default();
    let empty = ToolStructuredContent::from(Metadata::new());
    assert!(absent.get().is_none());
    assert!(empty.get().unwrap().is_empty());
    assert_ne!(absent, empty);
    assert_eq!(absent, ToolStructuredContent::default());
    assert_eq!(empty, ToolStructuredContent::from(Metadata::new()));
}

#[test]
fn replacing_the_field_shares_assignments_but_preserves_old_map_aliases() {
    let call = ToolCallContext::default();
    let clone = call.clone();
    let old = ResponseMetadata::new();
    assert!(call.structured_content.replace(Some(old.clone())).is_none());
    clone
        .structured_content
        .get()
        .unwrap()
        .insert("page", json!(3));
    assert_eq!(old.get("page"), Some(json!(3)));
    let new = ResponseMetadata::new();
    let returned = clone.structured_content.replace(Some(new.clone())).unwrap();
    returned.insert("page", json!(9));
    assert_eq!(old.get("page"), Some(json!(9)));
    assert!(new.is_empty());
    new.insert("page", json!(12));
    assert_eq!(
        call.structured_content.get().unwrap().get("page"),
        Some(json!(12))
    );
    assert_eq!(call, clone);
    assert_eq!(call.structured_content.replace(None), Some(new.clone()));
    assert!(clone.structured_content.get().is_none());
    assert_eq!(new.get("page"), Some(json!(12)));
}

#[test]
fn a_map_callback_can_replace_the_field_without_holding_its_lock() {
    let field = ToolStructuredContent::from(json!({"page": 3}).as_object().unwrap().clone());
    let old = field.get().unwrap();
    old.with_value("page", |value| {
        assert_eq!(field.replace(None), Some(old.clone()));
        *value = json!(9);
    });
    assert!(field.get().is_none());
    assert_eq!(old.get("page"), Some(json!(9)));
}

// Python: tests/test_tool_context.py::TestCurrentToolCall::test_none_outside_a_tool_call,
// test_handler_sees_its_call_and_may_fill_the_structured_copy.
// Rust's approved context propagation also preserves the field's shared identity.
#[tokio::test]
async fn a_kept_task_writes_the_same_call_and_the_loop_snapshots_it() {
    assert!(current_tool_call().is_none());
    let retained = ToolStructuredContent::default();
    let remember = retained.clone();
    let handler = ToolHandler::new(move |_, _| {
        let remember = remember.clone();
        async move {
            let call = current_tool_call().unwrap();
            assert_eq!(call.tool_call_id, "c1");
            assert_eq!(call.room_id.as_ref().unwrap().as_str(), "r1");
            assert_eq!(call.channel_id.as_ref().unwrap().as_str(), "ai1");
            call.structured_content
                .replace(Some(ResponseMetadata::new()));
            let map = call.structured_content.get().unwrap();
            remember.replace(Some(map.clone()));
            current_response_metadata()
                .unwrap()
                .insert("turn", json!(true));
            tokio::spawn(roomkit::keep_context(async {
                current_tool_call()
                    .unwrap()
                    .structured_content
                    .get()
                    .unwrap()
                    .insert("page", json!(3));
            }))
            .await
            .unwrap();
            assert_eq!(map.get("page"), Some(json!(3)));
            assert!(
                tokio::spawn(async { current_tool_call().is_none() })
                    .await
                    .unwrap()
            );
            Ok(ToolResult::Text("model text".into()))
        }
    });
    let output = run(handler).await;
    assert!(current_tool_call().is_none());
    retained.get().unwrap().insert("late", json!(true));
    let row = output
        .response_events
        .iter()
        .find(|row| row.r#type == EventType::ToolCallEnd)
        .unwrap();
    let EventContent::ToolCall(content) = &row.content else {
        panic!("no call row")
    };
    assert_eq!(
        content
            .structured_content
            .clone()
            .map(serde_json::Value::Object),
        Some(json!({"page": 3}))
    );
    assert_eq!(output.response_metadata.get("turn"), Some(json!(true)));
    assert_eq!(output.response_metadata.get("page"), None);
}

async fn run(handler: ToolHandler) -> roomkit::model::ChannelOutput {
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "lookup", "{}")]),
        ScriptedRound::answering("Done."),
    ]);
    let channel = ChannelId::new("ai1").unwrap();
    let agent = AiChannel::new(
        channel.clone(),
        model,
        AiChannelOptions {
            tools: vec![AiTool::new("lookup", "Read").unwrap().into()],
            tool_handler: Some(handler),
            ..AiChannelOptions::default()
        },
    )
    .unwrap();
    let room = RoomId::new("r1").unwrap();
    let event = RoomEvent::new(
        room.clone(),
        EventSource::new(ChannelId::new("sms").unwrap(), ChannelType::SMS),
        EventContent::Text(TextContent::new("Read.")),
    );
    agent
        .on_event(
            event,
            ChannelBinding::new(channel, room.clone(), ChannelType::AI),
            RoomContext::new(Room::new(room)),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn an_empty_handler_copy_is_exactly_empty_in_the_end_row() {
    let output = run(ToolHandler::new(|_, _| async {
        current_tool_call()
            .unwrap()
            .structured_content
            .replace(Some(ResponseMetadata::new()));
        Ok(ToolResult::Text("model text".into()))
    }))
    .await;
    let row = output
        .response_events
        .iter()
        .find(|row| row.r#type == EventType::ToolCallEnd)
        .unwrap();
    let EventContent::ToolCall(content) = &row.content else {
        panic!("no call row")
    };
    assert_eq!(content.structured_content, Some(Metadata::new()));
}
