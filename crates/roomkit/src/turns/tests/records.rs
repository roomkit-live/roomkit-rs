//! RFC §6.4 "The turn's record", §10.6.3: the loop's end a channel's
//! stream ends on crosses whole. The wire's loop end carries Python's
//! `LoopEndMarker` fields, and the owner's reader ends the turn on the
//! marker.

use pretty_assertions::assert_eq;
use roomkit_core::model::{EventType, Metadata};
use serde_json::{Value, json};

use crate::model::{LoopEndMarker, LoopEndReason};

use super::support::{ReadingOn, Step, Streaming, Stuck, Timed, bind, kit_with, room_id, says};
use crate::ConversationStore;
use crate::turns::observe::Carried;

/// The turn's error, and the metadata of the agent's last message, for an
/// agent of its own whose stream ends on `end`.
async fn ending_on(end: LoopEndMarker) -> (Option<String>, Option<Value>) {
    let kit = kit_with(Streaming::ending(2, end)).await;
    let result = says(&kit, "Hi").await.unwrap();
    crate::testing::idle(&kit).await;
    let last = kit
        .store()
        .list_events(&room_id(), received())
        .await
        .unwrap()
        .into_iter()
        .rfind(|event| {
            event.r#type == EventType::Message && event.source.channel_id.as_str() == "ai"
        })
        .map(|event| Value::Object(event.metadata));
    (result.error.map(|error| error.message), last)
}

/// `value`, a JSON object, as metadata.
fn usage(value: Value) -> Metadata {
    let Value::Object(usage) = value else {
        panic!("a usage is an object");
    };
    usage
}

/// A marker that says nothing of its generations writes the two keys alone;
/// one that does writes them all, its fields crossing on its own item.
/// (The loop's end carried a record: an empty one wrote nothing, a key of
/// the channel's own crossed with it.)
#[tokio::test(start_paused = true)]
async fn a_loop_end_writes_its_record_whatever_crosses_the_backend() {
    let python = LoopEndMarker {
        rounds: 1,
        usage: usage(json!({ "input_tokens": 4 })),
        max_rounds: Some(3),
        ..LoopEndMarker::new(LoopEndReason::MaxRounds)
    };
    let rust = LoopEndMarker {
        last_usage: Some(usage(json!({ "input_tokens": 1 }))),
        usage_complete: Some(true),
        model: Some("m-1".to_owned()),
        provider: Some("p".to_owned()),
        ..python.clone()
    };
    assert_eq!(
        (ending_on(python).await, ending_on(rust).await),
        (
            (
                None,
                Some(json!({ "loop_end_reason": "max_rounds", "ai_usage": { "input_tokens": 4 } }))
            ),
            (
                None,
                Some(json!({
                    "loop_end_reason": "max_rounds",
                    "ai_usage": { "input_tokens": 4 },
                    "ai_last_usage": { "input_tokens": 1 },
                    "ai_usage_complete": true,
                    "ai_model": "m-1",
                    "ai_provider": "p"
                }))
            )
        )
    );
}

/// Each message the agent stored: its text, and `key` of its metadata.
async fn messages(kit: &crate::RoomKit, key: &str) -> Vec<(String, Option<Value>)> {
    kit.store()
        .list_events(&room_id(), received())
        .await
        .unwrap()
        .into_iter()
        .filter(|event| {
            event.r#type == EventType::Message && event.source.channel_id.as_str() == "ai"
        })
        .map(|event| {
            let roomkit_core::model::EventContent::Text(text) = &event.content else {
                panic!("the agent's messages are its text");
            };
            (text.body.clone(), event.metadata.get(key).cloned())
        })
        .collect()
}

/// Runs an agent's `steps`; what the caller's record says of `key`, and each
/// message the agent stored with it.
async fn read(steps: Vec<Step>, key: &str) -> (Option<Value>, Vec<(String, Option<Value>)>) {
    let kit = kit_with(Streaming::scripted(steps)).await;
    let result = says(&kit, "Hi").await.unwrap();
    crate::testing::idle(&kit).await;
    let stored = messages(&kit, key).await;
    kit.close().await.unwrap();
    (result.response_metadata.get(key), stored)
}

// Python: tests/test_turns_backend.py::test_a_record_written_mid_stream_reaches_the_owner_before_the_next_item
// Python: tests/test_turns_pull.py::test_the_record_a_reader_reads_is_the_one_sent_before_its_item
#[tokio::test(start_paused = true)]
async fn the_record_a_reader_reads_is_the_one_sent_before_its_item() {
    let (caller, stored) = read(
        vec![
            Step::Text("a"),
            Step::Write("x", json!(1)),
            Step::Text("b"),
            Step::Break,
            Step::Write("x", json!(2)),
            Step::Text("c"),
            Step::Write("x", json!(3)),
        ],
        "x",
    )
    .await;
    assert_eq!(
        stored,
        vec![
            ("ab".to_owned(), Some(json!(1))),
            ("c".to_owned(), Some(json!(3)))
        ]
    );
    assert_eq!(caller, Some(json!(3)));
}

// Python: tests/test_turns_record_changes.py::test_a_change_before_a_call_s_start_or_end_reaches_the_owner_before_it
#[tokio::test(start_paused = true)]
async fn a_change_before_a_call_s_start_or_end_reaches_the_owner_before_it() {
    let (caller, stored) = read(
        vec![
            Step::Text("Looking it up."),
            Step::Write("sources", json!(["a"])),
            Step::Append("sources", "b"),
            Step::Start("c1"),
            // Inside a value the record holds: none of its keys is written.
            Step::Append("sources", "c"),
            Step::End("c1"),
            Step::Text("Found it."),
        ],
        "sources",
    )
    .await;
    assert_eq!(
        stored,
        vec![
            ("Looking it up.".to_owned(), Some(json!(["a", "b"]))),
            ("Found it.".to_owned(), Some(json!(["a", "b", "c"]))),
        ]
    );
    assert_eq!(caller, Some(json!(["a", "b", "c"])));
}

// Python: tests/test_turns_record_changes.py::test_a_change_inside_a_value_between_deltas_reaches_the_owner_before_its_segment_ends
#[tokio::test(start_paused = true)]
async fn a_change_inside_a_value_between_deltas_reaches_the_owner_before_its_segment_ends() {
    let (_, stored) = read(
        vec![
            Step::Write("sources", json!(["a"])),
            Step::Text("One, "),
            Step::Wait,
            Step::Append("sources", "b"),
            Step::Text("two."),
            Step::Break,
        ],
        "sources",
    )
    .await;
    assert_eq!(
        stored,
        vec![("One, two.".to_owned(), Some(json!(["a", "b"])))]
    );
}

// Python: tests/test_turns_record_changes.py::test_a_change_inside_a_value_reaches_the_owner_however_the_stream_stops[error]
#[tokio::test(start_paused = true)]
async fn a_change_inside_a_value_reaches_the_caller_when_the_stream_fails() {
    let (caller, _) = read(
        vec![
            Step::Write("sources", json!(["a"])),
            Step::Text("One, "),
            Step::Append("sources", "b"),
            Step::Fail("down"),
        ],
        "sources",
    )
    .await;
    assert_eq!(caller, Some(json!(["a", "b"])));
}

// Python: tests/test_turns_pull.py::test_a_reader_that_stops_after_its_turn_ended_reads_the_turn_s_last_record
// Python: tests/test_turns_record_changes.py::test_a_change_inside_a_value_reaches_the_owner_however_the_stream_stops[close]
// A stream dropped as its reader closes it writes last, as Python's generator
// does in its `finally`: the owner reads it before the caller does.
#[tokio::test(start_paused = true)]
async fn a_reader_that_stops_reads_the_turn_s_last_record() {
    let kit = kit_with(Streaming::scripted(vec![
        Step::Text("One, "),
        Step::Wait,
        Step::Text("two, "),
        Step::Hold,
    ]))
    .await;
    bind(&kit, Stuck::returning(1)).await;
    let result = says(&kit, "Hi").await.unwrap();
    crate::testing::idle(&kit).await;
    kit.close().await.unwrap();
    assert_eq!(result.response_metadata.get("final"), Some(json!(true)));
}

// Python: tests/test_turns_record_changes.py::test_a_turn_copies_its_record_no_more_often_for_more_deltas
#[tokio::test(start_paused = true)]
async fn a_turn_sends_its_record_no_more_often_for_more_deltas() {
    let mut counts = Vec::new();
    for deltas in [4, 40] {
        let mut steps = vec![Step::Write("sources", json!(["a"]))];
        steps.extend(std::iter::repeat_n(Step::Text("word "), deltas));
        steps.push(Step::Write("usage", json!({ "deltas": deltas })));
        steps.push(Step::Text("end"));
        let kit = kit_with(Streaming::scripted(steps)).await;
        let timed = Timed::on(&kit, tokio::time::Instant::now());
        says(&kit, "Hi").await.unwrap();
        crate::testing::idle(&kit).await;
        counts.push(timed.count(Carried::Frame, "record"));
        kit.close().await.unwrap();
    }
    assert_eq!(counts[0], counts[1]);
    assert!(counts[0] > 0);
}

// Python: models/delivery.py DeliveryHandle.cancel reads the cascade's record,
// its stream read and its transport still sending (RFC 6.7, 10.1). The shared
// scenario comes with the next sync.
#[tokio::test(start_paused = true)]
async fn a_cut_call_reads_its_record_while_a_transport_still_sends() {
    let kit = kit_with(Streaming::scripted(vec![
        Step::Write("cited", json!(1)),
        Step::Text("Hi"),
    ]))
    .await;
    bind(&kit, Stuck::holding_answers()).await;
    let message = roomkit_core::model::InboundMessage::new(
        roomkit_core::model::ChannelId::new("sms").unwrap(),
        "+15550001",
        roomkit_core::model::TextContent::new("Hi"),
    );
    let result = kit
        .process_inbound(
            message,
            crate::ProcessInboundOptions {
                room_id: Some(room_id()),
                defer_delivery: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    // A sleep, for paused time to pass and the deadline to come.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while messages(&kit, "cited").await.is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the stream was never read"
        );
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    let handle = result.delivery.expect("a handle");
    let cut = handle
        .cancel(crate::CancelOptions::default())
        .await
        .unwrap();
    kit.close().await.unwrap();
    assert_eq!(cut.response_metadata.get("cited"), Some(json!(1)));
}

// Python: core/mixins/_streaming_segments.py _ended_record writes `ai_model`
// and `ai_provider` only from a loop end that names them: a host's own stay on
// its last message, never on its caller's record (RMK-1323).
#[tokio::test(start_paused = true)]
async fn a_host_s_own_model_and_provider_outlive_a_loop_end_that_names_none() {
    let steps = vec![
        Step::Write("ai_model", json!("its-model")),
        Step::Write("ai_provider", json!("its-provider")),
        Step::Text("Hi"),
        Step::Completed,
    ];
    let (caller, stored) = read(steps.clone(), "ai_model").await;
    let (_, provider) = read(steps, "ai_provider").await;
    assert_eq!(caller, None);
    assert_eq!(stored, [("Hi".to_owned(), Some(json!("its-model")))]);
    assert_eq!(provider, [("Hi".to_owned(), Some(json!("its-provider")))]);
}

// Python: tests/test_turns_pull.py::test_a_steering_cut_s_last_record_reaches_its_reader
// The stream dropped by the cut writes last, as Python's generator does in its
// `finally`: its caller reads it (RMK-1323).
#[tokio::test(start_paused = true)]
async fn a_steering_cut_s_last_record_reaches_its_reader() {
    let kit = kit_with(Streaming::scripted(vec![
        Step::Text("text"),
        Step::Break,
        Step::Hold,
    ]))
    .await;
    let target = ReadingOn::flushing(std::time::Duration::ZERO);
    bind(&kit, Stuck::reading_on(&target)).await;
    let message = roomkit_core::model::InboundMessage::new(
        roomkit_core::model::ChannelId::new("sms").unwrap(),
        "+15550001",
        roomkit_core::model::TextContent::new("Hi"),
    );
    let result = kit
        .process_inbound(
            message,
            crate::ProcessInboundOptions {
                room_id: Some(room_id()),
                defer_delivery: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    target.read.notified().await;
    // Paused time: the break is read before the cut, as Python's 20 ms.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let reached = kit
        .steer(
            crate::steering::Cancel::default(),
            &roomkit_core::model::ChannelId::new("ai").unwrap(),
            crate::SteerOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let handle = result.delivery.expect("a handle");
    let waited = handle.wait().await.unwrap();
    kit.close().await.unwrap();
    assert_eq!(reached, 1);
    assert_eq!(waited.response_metadata.get("final"), Some(json!(true)));
}

// Python: models/delivery.py report_cascade merges into the record the call
// returned at its commit: `result.response_metadata` is the one `wait()` fills
// (RMK-1321).
#[tokio::test(start_paused = true)]
async fn a_deferred_wait_fills_the_record_the_call_returned_at_its_commit() {
    let kit = kit_with(Streaming::scripted(vec![
        Step::Write("cited", json!(3)),
        Step::Text("Hi"),
        Step::Hold,
    ]))
    .await;
    let message = roomkit_core::model::InboundMessage::new(
        roomkit_core::model::ChannelId::new("sms").unwrap(),
        "+15550001",
        roomkit_core::model::TextContent::new("Hi"),
    );
    let result = kit
        .process_inbound(
            message,
            crate::ProcessInboundOptions {
                room_id: Some(room_id()),
                defer_delivery: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let at_the_commit = result.response_metadata.get("cited");
    let handle = result.delivery.clone().expect("a handle");
    let waited = handle.cancel(crate::CancelOptions::default()).await;
    waited.unwrap().response_metadata.insert("mine", json!(1));
    kit.close().await.unwrap();
    let turns = result.response_metadata.get("turns").unwrap_or_default();
    assert_eq!(at_the_commit, None);
    assert_eq!(result.response_metadata.get("cited"), Some(json!(3)));
    assert_eq!(result.response_metadata.get("mine"), Some(json!(1)));
    assert_eq!(turns["ai"]["loop_end_reason"], json!("cancelled"));
}

// Python: core/event_router.py keeps a muted stream's side effects, which RFC
// 7.5 always allows; the stream is closed unread (test_muted_target_streaming_response_suppressed).
#[tokio::test(start_paused = true)]
async fn a_muted_agent_s_stream_keeps_its_side_effects() {
    let mut agent = Streaming::scripted(vec![Step::Text("never")]);
    agent.updates.insert("topic".to_owned(), json!("sales"));
    let kit = kit_with(agent).await;
    kit.mute(
        &room_id(),
        &roomkit_core::model::ChannelId::new("ai").unwrap(),
        crate::MuteOptions::default(),
    )
    .await
    .unwrap();
    says(&kit, "Hi").await.unwrap();
    crate::testing::idle(&kit).await;
    let room = kit.store().get_room(&room_id()).await.unwrap().unwrap();
    let stored = messages(&kit, "topic").await;
    kit.close().await.unwrap();
    assert_eq!(room.metadata.get("topic"), Some(&json!("sales")));
    assert_eq!(stored, Vec::new());
}

/// Every event of a room's timeline the room received.
fn received() -> crate::store::ListEventsOptions {
    crate::store::ListEventsOptions {
        limit: u64::MAX,
        ..crate::store::ListEventsOptions::default()
    }
}
