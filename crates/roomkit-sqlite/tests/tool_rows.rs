//! A turn's tool-call rows in SQLite (RFC §6.4 "The tools of earlier
//! turns", RMK-1023): the stamps a later turn pairs them by survive the
//! file, and a kit restarted on it replays each earlier round at its place.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod support;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelId, ChannelType, EventContent, EventDirection, EventId, EventSource, EventStatus,
    EventType, InboundMessage, Room, RoomContext, RoomEvent, RoomId, TextContent, Timestamp,
    ToolCallContent, ToolCallOutcome, ToolCallStatus,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, ProcessInboundOptions, RoomKit,
    RoomKitOptions, store::CommitEventOptions,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiAgent, AiContext, AiRole, AiTool, ToolOutput, Tools};
use roomkit_sqlite::{SqliteStore, SqliteStoreOptions};
use serde_json::json;

use crate::support::TempFile;
use roomkit_core::rules::transcript::{Message, Role, conversation};

fn id(text: &str) -> ChannelId {
    ChannelId::new(text).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

/// The room's event `evt_{index}`: `content` from `channel`, written in the
/// turn `turn` when given.
fn event(index: u64, channel: &str, content: EventContent, turn: Option<&str>) -> RoomEvent {
    let agent = turn.is_some();
    let (channel_type, direction) = if agent {
        (ChannelType::AI, EventDirection::Outbound)
    } else {
        (ChannelType::SMS, EventDirection::Inbound)
    };
    let mut event = RoomEvent {
        id: EventId::new(format!("evt_{index}")).unwrap(),
        created_at: Timestamp::UNIX_EPOCH,
        ..RoomEvent::new(
            room_id(),
            EventSource {
                direction,
                ..EventSource::new(id(channel), channel_type)
            },
            content,
        )
    };
    event.index = index;
    event.status = EventStatus::Delivered;
    if let Some(turn) = turn {
        event.correlation_id = Some(turn.to_owned());
        event.responds_to = Some(EventId::new("evt_0").unwrap());
    }
    event
}

/// A row of the call `c1` in the turn `turn`, at `index`: its start, or its
/// end reading `result`.
fn row(index: u64, turn: &str, result: Option<&str>) -> RoomEvent {
    let mut call = ToolCallContent::new("lookup_order", "c1");
    let mut row = event(index, "assistant", TextContent::new("").into(), Some(turn));
    row.r#type = EventType::ToolCallStart;
    if let Some(result) = result {
        row.r#type = EventType::ToolCallEnd;
        call.result = json!(result);
        call.status = ToolCallStatus::Completed;
        call.outcome = Some(ToolCallOutcome::Served);
    }
    row.content = EventContent::ToolCall(call);
    row
}

/// The review's overlap case (probe `py_turn_key.py`): two turns on one
/// question overlap, the model reusing its id. Stored in a file, read back
/// once it is opened again, each end pairs with the start of its own turn,
/// by the turn's correlation id, as the Python implementation pairs them.
#[tokio::test]
async fn overlapping_turns_read_back_from_the_file_pair_by_their_correlation_id() {
    let file = TempFile::new("tool-rows-overlap");
    let store = SqliteStore::new(SqliteStoreOptions {
        path: file.path().into(),
    });
    store.create_room(Room::new(room_id())).await.unwrap();
    let events = [
        event(0, "sms", TextContent::new("Order 42?").into(), None),
        row(1, "A", None),
        row(2, "B", None),
        row(3, "A", Some("A's")),
        row(4, "B", Some("B's")),
        event(5, "sms", TextContent::new("And then?").into(), None),
    ];
    for committed in events {
        let room_id = committed.room_id.clone();
        let options = CommitEventOptions {
            activity: false,
            ..CommitEventOptions::default()
        };
        store
            .commit_event(&room_id, committed, options)
            .await
            .unwrap();
    }
    drop(store);
    let store = SqliteStore::new(SqliteStoreOptions {
        path: file.path().into(),
    });
    let query = roomkit::store::ListEventsOptions {
        limit: u64::MAX,
        ..roomkit::store::ListEventsOptions::default()
    };
    let timeline = store.list_events(&room_id(), query).await.unwrap();
    let turns: Vec<Option<&str>> = timeline
        .iter()
        .map(|event| event.correlation_id.as_deref())
        .collect();
    assert_eq!(
        turns,
        [None, Some("A"), Some("B"), Some("A"), Some("B"), None]
    );
    let mut context = RoomContext::new(Room::new(room_id()));
    context.recent_events.clone_from(&timeline);
    let read = conversation(&id("assistant"), &timeline[5], &context, 50, |_| true);
    let call = |id: &str| {
        let call =
            roomkit::model::ToolCall::new(id, "lookup_order", roomkit::model::Metadata::new());
        Message::assistant_calls("", vec![call])
    };
    assert_eq!(
        read,
        [
            Message::new(Role::User, "Order 42?"),
            call("c1"),
            Message::tool_result("c1", "A's"),
            call("c1_2"),
            Message::tool_result("c1_2", "B's"),
            Message::new(Role::User, "And then?"),
        ]
    );
}

/// `lookup_order`, finding every order shipped.
fn lookup() -> Tools {
    let order = json!({ "type": "object", "properties": { "order": { "type": "string" } } });
    let definition = AiTool::new("lookup_order", "Find an order", order);
    Tools::new().tool(definition, |call, _context| async move {
        let order = call.arguments.get("order").cloned().unwrap_or_default();
        Ok(ToolOutput::Json(
            json!({ "order": order, "status": "shipped" }),
        ))
    })
}

/// A kit on `file`, with an SMS transport and an agent on `model`; the room
/// created and bound when `fresh`.
async fn kit_on(file: &TempFile, model: ScriptedProvider, fresh: bool) -> RoomKit<SqliteStore> {
    let store = SqliteStore::new(SqliteStoreOptions {
        path: file.path().into(),
    });
    let kit = RoomKit::new(RoomKitOptions::with_store(store));
    kit.register_channel(RecordingTransport::new(
        id("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    let agent = AiAgent::new(id("assistant"), model)
        .with_tools(lookup())
        .unwrap();
    kit.register_channel(agent).unwrap();
    if fresh {
        let options = CreateRoomOptions {
            room_id: Some(room_id()),
            ..Default::default()
        };
        kit.create_room(options).await.unwrap();
        for channel in ["sms", "assistant"] {
            let attach = AttachChannelOptions::default();
            kit.attach_channel(&room_id(), &id(channel), attach)
                .await
                .unwrap();
        }
    }
    kit
}

async fn says(kit: &RoomKit<SqliteStore>, text: &str) {
    let message = InboundMessage::new(id("sms"), "+15550001", TextContent::new(text));
    let options = ProcessInboundOptions {
        room_id: Some(room_id()),
        ..Default::default()
    };
    kit.process_inbound(message, options).await.unwrap();
}

/// The lines of `request`'s messages: each role and text, a round's calls,
/// a result and the call it answers.
fn lines(request: &AiContext) -> Vec<String> {
    request
        .messages
        .iter()
        .map(|message| {
            let calls: Vec<&str> = message
                .tool_calls()
                .into_iter()
                .map(|call| call.id.as_str())
                .collect();
            match (message.role, message.tool_result()) {
                (AiRole::Tool, Some(answers)) => {
                    format!("result {} {}", answers.tool_call_id, message.text())
                }
                _ if !calls.is_empty() => format!("calls {}", calls.join(" ")),
                (role, _) => format!("{role:?} {}", message.text()),
            }
        })
        .collect()
}

/// Two turns, each calling `lookup_order` under the id `c1`, on a kit whose
/// store is a file; the kit restarted on the file, a third turn reads both
/// rounds at their place, each turn's rows sharing their own correlation
/// id, the second call replayed under an id of its own (`c1_7`), as the
/// Python implementation's history replays the same rows (probe
/// `py_reads_py_rows.out`, room `kit`).
#[tokio::test]
async fn a_restarted_kit_replays_the_rounds_its_file_holds() {
    let file = TempFile::new("tool-rows-restart");
    let first = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "lookup_order", r#"{"order": "1"}"#)]),
        ScriptedRound::answering("One."),
        ScriptedRound::calling("", [("c1", "lookup_order", r#"{"order": "2"}"#)]),
        ScriptedRound::answering("Two."),
    ]);
    let kit = kit_on(&file, first, true).await;
    says(&kit, "first").await;
    says(&kit, "second").await;
    kit.close().await.unwrap();
    drop(kit);
    let after = ScriptedProvider::new([ScriptedRound::answering("Three.")]);
    let kit = kit_on(&file, after.clone(), false).await;
    says(&kit, "third").await;
    let query = roomkit::store::ListEventsOptions {
        limit: u64::MAX,
        ..roomkit::store::ListEventsOptions::default()
    };
    let timeline = kit.store().list_events(&room_id(), query).await.unwrap();
    kit.close().await.unwrap();
    let turn_of = |index: usize| timeline[index].correlation_id.clone().unwrap();
    assert_eq!(
        [3, 4, 5, 7, 8, 9].map(|index| timeline[index].correlation_id.clone()),
        [3, 3, 3, 7, 7, 7].map(|index| Some(turn_of(index)))
    );
    assert_ne!(turn_of(3), turn_of(7));
    assert_eq!(
        lines(&after.requests()[0]),
        [
            "User first",
            "calls c1",
            r#"result c1 {"order":"1","status":"shipped"}"#,
            "Assistant One.",
            "User second",
            "calls c1_7",
            r#"result c1_7 {"order":"2","status":"shipped"}"#,
            "Assistant Two.",
            "User third",
        ]
    );
}
