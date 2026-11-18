//! Readers obtained from a public executor retain their own room data source.
use super::{Env, link, worker};
use roomkit::model::{
    ChannelId, EventContent, EventDirection, EventId, EventType, RoomEvent, ToolCallContent,
    ToolCallOutcome,
};
use roomkit::{CommitEventOptions, CreateRoomOptions, RoomKit, RoomKitOptions};
use serde_json::{Value, json};
use std::sync::Arc;

fn tool_row(
    template: &RoomEvent,
    id: &str,
    tool: &str,
    kind: EventType,
    turn: &str,
    query: &str,
) -> RoomEvent {
    let mut row = template.clone();
    row.id = EventId::new(id).unwrap();
    row.r#type = kind;
    row.source.direction = EventDirection::Outbound;
    row.correlation_id = Some(turn.into());
    let mut tool = ToolCallContent::new(tool, "same-id");
    tool.arguments.insert("query".into(), json!(query));
    row.content = EventContent::ToolCall(tool);
    row
}

#[tokio::test]
async fn remembered_calls_keep_model_arguments_across_interleaved_turns_and_deleted_starts() {
    use EventType::{ToolCallEnd, ToolCallStart};
    let (link, _, _) = link(false);
    let req = &link.request;
    let kit = RoomKit::new(RoomKitOptions::default());
    let room = &req.room_id;
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    let mut template = req.event.clone();
    template.source.channel_id = req.channel_id.clone();
    let make = |id, name, kind, turn, query| tool_row(&template, id, name, kind, turn, query);
    let mut deleted = make(
        "deleted",
        "lookup",
        ToolCallStart,
        "deleted-turn",
        "removed",
    );
    deleted.metadata.insert("deleted".into(), json!(true));
    let mut refused = make(
        "refused",
        "lookup",
        ToolCallEnd,
        "refused-turn",
        "rewritten",
    );
    if let EventContent::ToolCall(call) = &mut refused.content {
        call.outcome = Some(ToolCallOutcome::Refused);
    }
    let mut other = make("other", "secret", ToolCallEnd, "other-turn", "private");
    other.source.channel_id = ChannelId::new("other").unwrap();
    let mut activated = make("b-end", "activate_skill", ToolCallEnd, "b", "activated");
    if let EventContent::ToolCall(call) = &mut activated.content {
        call.result = json!(["reference"]);
        call.outcome = Some(ToolCallOutcome::Served);
    }
    let mut message = template.clone();
    message.id = EventId::new("anchor").unwrap();
    let rows = [
        make("a-start", "lookup", ToolCallStart, "a", "user-input"),
        make("b-start", "activate_skill", ToolCallStart, "b", "asked"),
        activated,
        message,
        other,
        make("a-end", "lookup", ToolCallEnd, "a", "rewrite"),
        deleted,
        make(
            "no-start",
            "lookup",
            ToolCallEnd,
            "deleted-turn",
            "sensitive",
        ),
        refused,
    ];
    let mut first = 0;
    for row in rows {
        let row = kit
            .commit_event(room, row, CommitEventOptions::default())
            .await
            .unwrap();
        if row.id.as_str() == "a-end" {
            first = i64::try_from(row.index).unwrap();
        }
    }
    let wiring = kit.turn_executor().wiring();
    let usage = wiring.build_tool_usage_loader(&req.channel_id);
    let history = wiring.build_tool_history_loader(&req.channel_id);
    let calls = usage(room.clone()).await.unwrap();
    assert_eq!(
        Value::from(calls),
        json!([
            {"name":"activate_skill", "arguments":{"query":"asked"}, "ran_with":{"query":"activated"}, "outcome":"served", "result":["reference"]},
            {"name":"lookup","arguments":{"query":"user-input"},"outcome":null,"result":""},
            {"name":"lookup","arguments":{},"outcome":null,"result":""},
        ])
    );
    let all = history(room.clone(), -1).await.unwrap();
    assert_eq!(all.first().unwrap().id.as_str(), "a-start");
    let recent = history(room.clone(), first).await.unwrap();
    assert_eq!(
        recent.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
        ["a-end", "deleted", "no-start", "refused"]
    );
    assert!(!matches!(recent[1].content, EventContent::ToolCall(_)));
    kit.close().await.unwrap();
    drop(kit);
    assert!(matches!(
        usage(room.clone()).await,
        Err(roomkit::RoomKitError::KitClosed)
    ));
    assert!(matches!(
        history(room.clone(), first).await,
        Err(roomkit::RoomKitError::KitClosed)
    ));
}

#[tokio::test]
async fn task_reader_retains_the_environment_that_built_it() {
    let (link, _, _) = link(false);
    let req = &link.request;
    let worker = worker();
    let mut env = Env::new(req.context.clone());
    let task = json!({"task":"prepare", "status":"running"});
    env.tasks.push(task.as_object().unwrap().clone());
    worker.use_wiring(Arc::new(env));
    let original = worker.wiring().build_room_tasks_loader();
    worker.use_wiring(Arc::new(Env::new(req.context.clone())));
    let current = worker.wiring().build_room_tasks_loader();
    assert_eq!(
        Value::from(original(req.room_id.clone()).await.unwrap()),
        json!([task])
    );
    assert_eq!(
        Value::from(current(req.room_id.clone()).await.unwrap()),
        json!([])
    );
    worker.close().await;
}
