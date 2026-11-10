//! RFC §23.4: a task tool reads names from its turn's room snapshot.

use std::sync::Arc;

use roomkit::model::{ChannelId, Metadata, RoomId, ToolCall};
use roomkit::orchestration::{PostAsyncOptions, RecentOptions, StatusLevel};
use roomkit::{ConversationStore, CreateRoomOptions, RoomKit, RoomKitOptions};
use serde_json::{Value, json};

use super::{TaskStatusTool, TaskStatusToolOptions};
use crate::tools::{ToolContext, ToolEntry, ToolResult, served_by};

const WORKER: &str = "agent:worker-01";

fn names(handle: &str) -> Metadata {
    serde_json::from_value(json!({
        "_discussion": {"agents": [WORKER], "handles": {WORKER: handle}, "max_depth": 5}
    }))
    .unwrap()
}

async fn kit() -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    let mut room = kit
        .create_room(CreateRoomOptions {
            room_id: Some(RoomId::new("r").unwrap()),
            ..CreateRoomOptions::default()
        })
        .await
        .unwrap();
    room.metadata = names("StoredName");
    kit.store().update_room(room).await.unwrap();
    for (agent, task) in [(WORKER, "t1"), ("external-worker", "t2")] {
        kit.status_bus()
            .post_async(
                agent,
                "task",
                StatusLevel::Completed,
                PostAsyncOptions {
                    detail: "Found it.".to_owned(),
                    metadata: Some(
                        serde_json::from_value(json!({"room_id": "r", "task_id": task})).unwrap(),
                    ),
                },
            )
            .await;
    }
    kit
}

// Python: tests/test_task_status.py::test_task_status_names_a_discussions_agent_by_its_handle
#[tokio::test]
async fn task_status_names_a_discussions_agent_by_its_handle() {
    let kit = kit().await;
    let before = kit.status_bus().recent(RecentOptions::default()).await;
    let tool = Arc::new(TaskStatusTool::new(&kit, TaskStatusToolOptions::default()));
    let (served, _) = served_by(None, vec![ToolEntry::Tool(tool)])
        .unwrap()
        .unwrap();
    let context = || ToolContext::new(RoomId::new("r").unwrap(), ChannelId::new("reader").unwrap());
    for (context, expected) in [
        (context().reading_room(&names("TurnName")), "TurnName"),
        (context().reading_room(&Metadata::new()), WORKER),
        (context(), WORKER),
    ] {
        let call = ToolCall::new("c1", "task_status", Metadata::new());
        let ToolResult::Text(answer) = served.call(call, context).await.unwrap() else {
            panic!("task_status must answer with text")
        };
        let answer: Value = serde_json::from_str(&answer).unwrap();
        let agents: Vec<_> = answer["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|line| line["agent"].as_str().unwrap())
            .collect();
        assert_eq!(agents, [expected, "external-worker"]);
        assert_eq!(answer["tasks"][0]["result"], "Found it.");
    }
    assert_eq!(
        kit.status_bus().recent(RecentOptions::default()).await,
        before
    );
    assert_eq!(before[0].agent_id, WORKER);
    kit.close().await.unwrap();
}
