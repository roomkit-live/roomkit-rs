//! An agent that takes a room's turns one at a time (`serialize_room_turns`,
//! RFC §6.4 "A room's turns one after the other"): its turns in a room run
//! in the order they were asked, each once the one before it is over, and
//! each reads the room as it stood when it was asked plus the agent's
//! answers since, never a message that came after it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelType, EventContent, InboundMessage, RoomContext, RoomEvent, TextContent, ToolCall,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    ConversationStore, DeleteEventOptions, HookOptions, HookTrigger, InboundResult,
    ProcessInboundOptions, RoomKit, RoomKitOptions, current_turn_host,
};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiProvider, AiResponse, AiTool, ProviderError,
    ToolContext, ToolResult, Tools,
};
use tokio::sync::watch;

use super::support::{Room, channel, room_id};

const WAIT: Duration = Duration::from_secs(2);

/// A model that answers each message once its gate opens, `answer to
/// <message>`, and keeps what each turn read.
#[derive(Clone, Default)]
struct Gated {
    read: Arc<Mutex<HashMap<String, Vec<String>>>>,
    gates: Arc<Mutex<HashMap<String, watch::Sender<bool>>>>,
    asked: Arc<Mutex<Vec<String>>>,
}

impl Gated {
    fn gate(&self, asked: &str) -> watch::Sender<bool> {
        self.gates
            .lock()
            .unwrap()
            .entry(asked.to_owned())
            .or_insert_with(|| watch::Sender::new(false))
            .clone()
    }

    fn answer(&self, asked: &str) {
        self.gate(asked).send_replace(true);
    }

    fn answer_all(&self) {
        for gate in self.gates.lock().unwrap().values() {
            gate.send_replace(true);
        }
    }

    fn read(&self, asked: &str) -> Option<Vec<String>> {
        self.read.lock().unwrap().get(asked).cloned()
    }

    /// The messages the model was asked, in the order the turns started.
    fn asked(&self) -> Vec<String> {
        self.asked.lock().unwrap().clone()
    }

    async fn until_started(&self, asked: &str) {
        let started = tokio::time::timeout(WAIT, async {
            while self.read(asked).is_none() {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await;
        assert!(
            started.is_ok(),
            "{asked:?} never started: {:?}",
            self.asked()
        );
    }
}

impl AiProvider for Gated {
    fn model_name(&self) -> &'static str {
        "gated"
    }

    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        let conversation: Vec<String> = context
            .messages
            .iter()
            .map(roomkit_ai::AiMessage::text)
            .collect();
        let asked = conversation.last().cloned().unwrap_or_default();
        self.read
            .lock()
            .unwrap()
            .insert(asked.clone(), conversation);
        self.asked.lock().unwrap().push(asked.clone());
        let mut gate = self.gate(&asked).subscribe();
        drop(gate.wait_for(|open| *open).await);
        Ok(AiResponse::new(format!("answer to {asked}")))
    }
}

/// A room whose agent, serialized, answers through `model`.
async fn serialized(
    model: Gated,
    adjust: impl FnOnce(AiChannelOptions) -> AiChannelOptions,
) -> Room {
    let options = adjust(AiChannelOptions {
        serialize_room_turns: true,
        ..AiChannelOptions::default()
    });
    Room::with_agent(AiChannel::new(channel("assistant"), model, options).unwrap()).await
}

/// Sends `body` to the room, its delivery deferred.
async fn send(kit: &RoomKit, body: &str) -> InboundResult {
    let message = InboundMessage::new(channel("sms"), "+15550001", TextContent::new(body));
    let options = ProcessInboundOptions {
        room_id: Some(room_id()),
        defer_delivery: true,
        ..ProcessInboundOptions::default()
    };
    kit.process_inbound(message, options).await.unwrap()
}

/// Waits for `result`'s delivery: it ends without an error.
async fn delivered(result: &InboundResult) {
    let delivery = result.delivery.as_ref().unwrap();
    let ended = tokio::time::timeout(WAIT, delivery.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(ended.error.is_none(), "{:?}", ended.error);
}

/// The texts of `channel_id`'s messages in the room, as a reader lists
/// them: no record of a blocked one.
async fn said(kit: &RoomKit, channel_id: &str) -> Vec<String> {
    let listed = roomkit::store::ListEventsOptions {
        limit: u64::MAX,
        ..roomkit::store::ListEventsOptions::default()
    };
    kit.store()
        .list_events(&room_id(), listed)
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.source.channel_id == channel(channel_id))
        .filter_map(|event| {
            if let EventContent::Text(text) = event.content {
                Some(text.body)
            } else {
                None
            }
        })
        .collect()
}

/// What `channel_id` said that was stored BLOCKED as a muted source's.
async fn withheld(kit: &RoomKit, channel_id: &str) -> Vec<String> {
    kit.store()
        .list_events(&room_id(), super::support::every_row())
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.source.channel_id == channel(channel_id))
        .filter(|event| event.blocked_by.as_deref() == Some("source_muted"))
        .filter_map(|event| {
            if let EventContent::Text(text) = event.content {
                Some(text.body)
            } else {
                None
            }
        })
        .collect()
}

fn strings(texts: &[&str]) -> Vec<String> {
    texts.iter().map(|text| (*text).to_owned()).collect()
}

// Python: tests/test_room_turns_in_order.py::test_a_room_s_turns_run_one_after_the_other_each_reading_the_answers_before_it
#[tokio::test]
async fn a_room_s_turns_run_one_after_the_other_each_reading_the_answers_before_it() {
    let model = Gated::default();
    let room = serialized(model.clone(), |options| options).await;
    let a = send(&room.kit, "task A").await;
    model.until_started("task A").await;
    let b = send(&room.kit, "task B").await;
    let c = send(&room.kit, "task C").await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(model.asked(), ["task A"]);

    model.answer("task A");
    model.until_started("task B").await;
    // B reads A answered, and not C, which came after it.
    assert_eq!(
        model.read("task B").unwrap(),
        strings(&["task A", "answer to task A", "task B"])
    );
    assert!(model.read("task C").is_none());

    model.answer("task B");
    model.until_started("task C").await;
    // In the room's order: the answers were written after C arrived.
    assert_eq!(
        model.read("task C").unwrap(),
        strings(&[
            "task A",
            "task B",
            "answer to task A",
            "answer to task B",
            "task C"
        ])
    );
    model.answer("task C");
    for result in [a, b, c] {
        delivered(&result).await;
    }
    room.kit.close().await.unwrap();
}

// Python: tests/test_room_turns_in_order.py::test_a_waiting_turn_whose_agent_leaves_the_room_never_starts
#[tokio::test]
async fn a_waiting_turn_whose_agent_leaves_the_room_never_starts() {
    let model = Gated::default();
    let room = serialized(model.clone(), |options| options).await;
    let a = send(&room.kit, "task A").await;
    model.until_started("task A").await;
    let b = send(&room.kit, "task B").await;

    room.kit
        .detach_channel(
            &room_id(),
            &channel("assistant"),
            roomkit::DetachChannelOptions::default(),
        )
        .await
        .unwrap();
    model.answer("task A");
    delivered(&a).await;
    delivered(&b).await;

    assert_eq!(model.asked(), ["task A"]);
    room.kit.close().await.unwrap();
}

// Python: tests/test_room_turns_in_order.py::test_a_message_removed_while_a_turn_waits_is_not_read
#[tokio::test]
async fn a_message_removed_while_a_turn_waits_is_not_read() {
    let model = Gated::default();
    let room = serialized(model.clone(), |options| options).await;
    let pin = send(&room.kit, "my pin is 4242").await;
    model.until_started("my pin is 4242").await;
    send(&room.kit, "task B").await;

    let pin_id = pin.event.unwrap().id;
    room.kit
        .delete_event(&room_id(), &pin_id, DeleteEventOptions::default())
        .await
        .unwrap();
    model.answer("my pin is 4242");
    model.until_started("task B").await;

    assert_eq!(
        model.read("task B").unwrap(),
        strings(&["answer to my pin is 4242", "task B"])
    );
    model.answer_all();
    room.kit.close().await.unwrap();
}

// Python: tests/test_room_turns_in_order.py::test_a_turn_muted_while_it_waits_is_not_delivered
#[tokio::test]
async fn a_turn_muted_while_it_waits_is_not_delivered() {
    let model = Gated::default();
    let room = serialized(model.clone(), |options| options).await;
    let a = send(&room.kit, "task A").await;
    model.until_started("task A").await;
    let b = send(&room.kit, "task B").await;

    room.kit
        .mute(
            &room_id(),
            &channel("assistant"),
            roomkit::MuteOptions::default(),
        )
        .await
        .unwrap();
    model.answer("task A");
    model.answer("task B");
    delivered(&a).await;
    delivered(&b).await;

    // B ran on the room it started in, its answer withheld; A's answer,
    // stored after the mute, is withheld too (RFC §7.5 rule 2, RMK-1170).
    assert!(model.read("task B").is_some());
    assert_eq!(said(&room.kit, "assistant").await, Vec::<String>::new());
    assert_eq!(
        withheld(&room.kit, "assistant").await,
        strings(&["answer to task A", "answer to task B"])
    );
    room.kit.close().await.unwrap();
}

// Python: tests/test_room_turns_in_order.py::test_a_waiting_turn_s_answer_is_judged_with_the_answers_before_it
#[tokio::test]
async fn a_waiting_turn_s_answer_is_judged_with_the_answers_before_it() {
    let model = Gated::default();
    let room = serialized(model.clone(), |options| options).await;
    let judged: Arc<Mutex<Vec<String>>> = Arc::default();
    let noted = Arc::clone(&judged);
    room.kit
        .hook(
            HookTrigger::BeforeBroadcast,
            move |event: RoomEvent, context: RoomContext| {
                let answer_b = event.source.channel_id == channel("assistant")
                    && matches!(&event.content, EventContent::Text(text) if text.body == "answer to task B");
                if answer_b {
                    noted.lock().unwrap().extend(
                        context
                            .recent_events
                            .iter()
                            .filter(|recent| recent.r#type == roomkit::model::EventType::Message)
                            .filter_map(|recent| {
                                if let EventContent::Text(text) = &recent.content {
                                    Some(text.body.clone())
                                } else {
                                    None
                                }
                            }),
                    );
                }
                async {}
            },
            HookOptions::default(),
        )
        .unwrap();
    send(&room.kit, "task A").await;
    model.until_started("task A").await;
    let b = send(&room.kit, "task B").await;
    model.answer("task A");
    model.answer("task B");
    delivered(&b).await;

    assert_eq!(
        *judged.lock().unwrap(),
        strings(&["task A", "task B", "answer to task A"])
    );
    room.kit.close().await.unwrap();
}

// Python: tests/test_room_turns_in_order.py::test_a_muted_agent_still_reads_each_message_at_once
#[tokio::test]
async fn a_muted_agent_still_reads_each_message_at_once() {
    let model = Gated::default();
    model.answer("hi");
    let room = serialized(model.clone(), |options| options).await;
    room.kit
        .mute(
            &room_id(),
            &channel("assistant"),
            roomkit::MuteOptions::default(),
        )
        .await
        .unwrap();

    tokio::time::timeout(WAIT, room.says("hi")).await.unwrap();

    assert_eq!(model.asked(), ["hi"]);
    assert_eq!(said(&room.kit, "assistant").await, Vec::<String>::new());
    room.kit.close().await.unwrap();
}

// Python: tests/test_room_turns_in_order.py::test_a_turn_that_waited_long_reads_the_history_it_was_asked_with
#[tokio::test]
async fn a_turn_that_waited_long_reads_the_history_it_was_asked_with() {
    let model = Gated::default();
    let room = serialized(model.clone(), |options| AiChannelOptions {
        max_context_events: 4,
        ..options
    })
    .await;
    send(&room.kit, "task A").await;
    model.until_started("task A").await;
    send(&room.kit, "task B").await;
    for later in 0..4 {
        send(&room.kit, &format!("later {later}")).await;
    }

    model.answer("task A");
    model.until_started("task B").await;
    assert_eq!(
        model.read("task B").unwrap(),
        strings(&["task A", "answer to task A", "task B"])
    );
    model.answer_all();
    room.kit.close().await.unwrap();
}

// Python: tests/test_room_turns_in_order.py::test_an_agent_s_turns_run_side_by_side_by_default
#[tokio::test]
async fn an_agent_s_turns_run_side_by_side_by_default() {
    let model = Gated::default();
    let options = AiChannelOptions::default();
    let agent = AiChannel::new(channel("assistant"), model.clone(), options).unwrap();
    let room = Room::with_agent(agent).await;
    send(&room.kit, "task A").await;
    model.until_started("task A").await;
    send(&room.kit, "task B").await;

    // B starts while A runs, A's order before it unanswered.
    model.until_started("task B").await;
    assert_eq!(
        model.read("task B").unwrap(),
        strings(&["task A", "task B"])
    );
    model.answer_all();
    room.kit.close().await.unwrap();
}

/// `ask_room`, whose call sends a nested question into the turn's own room
/// through `door`.
fn asking(kit: Arc<Mutex<Option<RoomKit>>>, door: &'static str) -> Tools {
    let tool = AiTool::new("ask_room", "Ask the room").unwrap();
    Tools::new().tool(tool, move |_call: ToolCall, _context: ToolContext| {
        let kit = kit.lock().unwrap().clone().unwrap();
        async move {
            let question = TextContent::new("nested question");
            if door == "turn host" {
                let host = current_turn_host().unwrap();
                host.send_event(
                    &room_id(),
                    &channel("sms"),
                    question,
                    roomkit::SendEventOptions::default(),
                )
                .await
                .unwrap();
                return Ok(ToolResult::Text("sent".to_owned()));
            }
            let message = InboundMessage::new(channel("sms"), "+15550001", question);
            let options = ProcessInboundOptions {
                room_id: Some(room_id()),
                defer_delivery: door == "deferred",
                ..ProcessInboundOptions::default()
            };
            let result = kit.process_inbound(message, options).await.unwrap();
            if let Some(delivery) = &result.delivery {
                delivery.wait().await.unwrap();
            }
            Ok(ToolResult::Text("sent".to_owned()))
        }
    })
}

// Python: tests/test_room_turns_in_order.py::test_a_message_a_turn_sends_into_its_own_room_is_answered_after_it
#[tokio::test]
async fn a_message_a_turn_sends_into_its_own_room_is_answered_after_it() {
    for door in ["process_inbound", "deferred", "turn host"] {
        let model = roomkit_ai::testing::ScriptedProvider::new([
            roomkit_ai::testing::ScriptedRound::calling("Let me ask.", [("tc1", "ask_room", "{}")]),
            roomkit_ai::testing::ScriptedRound::answering("outer answer"),
            roomkit_ai::testing::ScriptedRound::answering("nested answer"),
        ]);
        let kit_of_tool: Arc<Mutex<Option<RoomKit>>> = Arc::default();
        let options = AiChannelOptions {
            tools: asking(Arc::clone(&kit_of_tool), door).into(),
            serialize_room_turns: true,
            ..AiChannelOptions::default()
        };
        let agent = AiChannel::new(channel("assistant"), model, options).unwrap();
        let room = Room::with_agent(agent).await;
        *kit_of_tool.lock().unwrap() = Some(room.kit.clone());

        tokio::time::timeout(WAIT, room.says("go"))
            .await
            .unwrap_or_else(|_| panic!("{door}: the turn waited on itself"));
        tokio::time::timeout(WAIT, async {
            while said(&room.kit, "assistant").await.len() < 3 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{door}: {:?}", "the nested question was never answered"));

        assert_eq!(
            said(&room.kit, "assistant").await,
            ["Let me ask.", "outer answer", "nested answer"],
            "{door}"
        );
        room.kit.close().await.unwrap();
    }
}

/// RFC §10.6.2: what the owner knows of the agent says how it takes a
/// room's turns; steering them implies serializing them, as Python's
/// `AIChannel(steer_room_turns=True).serializes_room_turns`.
#[test]
fn an_agent_that_steers_its_room_turns_declares_them_serialized() {
    use roomkit::Channel;
    let built = |serialize_room_turns, steer_room_turns| {
        let options = AiChannelOptions {
            serialize_room_turns,
            steer_room_turns,
            ..AiChannelOptions::default()
        };
        let agent = AiChannel::new(channel("assistant"), Gated::default(), options).unwrap();
        let declared = agent.declaration();
        (declared.serializes_room_turns, declared.steers_room_turns)
    };
    assert_eq!(
        [built(false, false), built(true, false), built(false, true)],
        [(false, false), (true, false), (true, true)]
    );
    let declared = roomkit::ChannelDeclaration::new(channel("assistant"));
    assert_eq!(
        (declared.serializes_room_turns, declared.steers_room_turns),
        (false, false)
    );
}

/// RFC §6.4: a host holding the kit reads how a registered channel takes a
/// room's turns, as Python's `kit.get_channel(id).steers_room_turns`: an
/// agent that steers them serializes them, one that serializes them does not
/// steer them, an agent without either option does neither, nor does a
/// transport.
#[tokio::test]
async fn a_registered_channel_says_whether_it_steers_its_room_turns() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let agents = [
        ("steering", false, true),
        ("serializing", true, false),
        ("plain", false, false),
    ];
    for (id, serialize_room_turns, steer_room_turns) in agents {
        let options = AiChannelOptions {
            serialize_room_turns,
            steer_room_turns,
            ..AiChannelOptions::default()
        };
        let agent = AiChannel::new(channel(id), Gated::default(), options).unwrap();
        kit.register_channel(agent).unwrap();
    }
    let sms = RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    let read = |id: &str| {
        let registered = kit.get_channel(&channel(id)).unwrap();
        (
            registered.serializes_room_turns(),
            registered.steers_room_turns(),
        )
    };
    assert_eq!(
        [
            read("steering"),
            read("serializing"),
            read("plain"),
            read("sms")
        ],
        [(true, true), (true, false), (false, false), (false, false)]
    );
    kit.close().await.unwrap();
}

// Python: tests/test_room_turns_in_order.py::test_an_agent_asked_again_in_the_same_delivery_is_not_held_by_its_first_turn
#[tokio::test]
async fn an_agent_asked_again_in_the_same_delivery_is_not_held_by_its_first_turn() {
    use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
    // One message asks both agents; the planner's answer asks the writer
    // again, a turn read in the same delivery once its first turn is over.
    let writer = AiChannel::new(
        channel("assistant"),
        ScriptedProvider::new([
            ScriptedRound::answering("drafted"),
            ScriptedRound::answering("redrafted"),
        ]),
        AiChannelOptions {
            serialize_room_turns: true,
            ..AiChannelOptions::default()
        },
    )
    .unwrap();
    let planner = AiChannel::new(
        channel("planner"),
        ScriptedProvider::new([ScriptedRound::answering("plan")]),
        AiChannelOptions::default(),
    )
    .unwrap();
    let room = Room::with_agent(writer).await;
    room.kit.register_channel(planner).unwrap();
    room.kit
        .attach_channel(
            &room_id(),
            &channel("planner"),
            roomkit::AttachChannelOptions::default(),
        )
        .await
        .unwrap();

    let result = tokio::time::timeout(WAIT, room.says("go")).await;

    assert!(
        result.is_ok(),
        "the writer's second turn waited on its first"
    );
    let written = said(&room.kit, "assistant").await;
    assert_eq!(
        written.get(..2),
        Some(&strings(&["drafted", "redrafted"])[..])
    );
    room.kit.close().await.unwrap();
}

/// Sends `body` to the room with its delivery named `key`, deferred.
async fn send_keyed(kit: &RoomKit, body: &str, key: &str) -> InboundResult {
    let mut message = InboundMessage::new(channel("sms"), "+15550001", TextContent::new(body));
    message.delivery_key = Some(key.to_owned());
    let options = ProcessInboundOptions {
        room_id: Some(room_id()),
        defer_delivery: true,
        ..ProcessInboundOptions::default()
    };
    kit.process_inbound(message, options).await.unwrap()
}

/// The cancellation reason `result`'s delivery ends with.
async fn cancellation_of(result: &InboundResult) -> Option<String> {
    let delivery = result.delivery.as_ref().unwrap();
    tokio::time::timeout(WAIT, delivery.wait())
        .await
        .unwrap()
        .unwrap()
        .cancellation_reason
}

// Python: tests/test_delivery_cancel_turns.py::test_a_turn_queued_behind_the_room_s_turn_never_starts[serial]
#[tokio::test]
async fn a_turn_waiting_its_place_never_starts_once_its_delivery_is_cancelled() {
    let model = Gated::default();
    let room = serialized(model.clone(), |options| options).await;
    let a = send_keyed(&room.kit, "task A", "a").await;
    model.until_started("task A").await;
    let b = send_keyed(&room.kit, "task B", "b").await;

    let options = roomkit::CancelDeliveryOptions::default();
    assert_eq!(room.kit.cancel_delivery("b", options).await, 1);
    // B's delivery is over once its turn is, which waited its place behind
    // A's: A still runs.
    assert_eq!(
        cancellation_of(&b).await.as_deref(),
        Some("caller_cancelled")
    );
    model.answer("task A");
    assert_eq!(cancellation_of(&a).await, None);

    assert_eq!(model.asked(), ["task A"]);
    assert_eq!(said(&room.kit, "assistant").await, ["answer to task A"]);
    room.kit.close().await.unwrap();
}

// Python: tests/test_delivery_cancel_turns.py::test_a_running_turn_is_cut_and_the_room_s_next_turn_runs[serial]
#[tokio::test]
async fn a_running_turn_cut_lets_the_room_s_next_turn_run() {
    let model = Gated::default();
    let room = serialized(model.clone(), |options| options).await;
    let a = send_keyed(&room.kit, "task A", "a").await;
    model.until_started("task A").await;
    let b = send_keyed(&room.kit, "task B", "b").await;

    let options = roomkit::CancelDeliveryOptions::default();
    assert_eq!(room.kit.cancel_delivery("a", options).await, 1);
    assert_eq!(
        cancellation_of(&a).await.as_deref(),
        Some("caller_cancelled")
    );
    model.until_started("task B").await;
    model.answer("task B");
    assert_eq!(cancellation_of(&b).await, None);

    assert_eq!(said(&room.kit, "assistant").await, ["answer to task B"]);
    room.kit.close().await.unwrap();
}
