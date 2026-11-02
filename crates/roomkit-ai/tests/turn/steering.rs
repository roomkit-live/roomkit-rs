//! Steering a running turn (RFC §21.3, §10.6.8): a directive the host
//! steers through the kit reaches the turn's loop, which reads it at its
//! next stop, between two rounds of calls or once the model has answered.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use roomkit::model::{
    Cancel, DeleteContent, EditContent, EventContent, EventId, EventType, InjectMessage, RoomEvent,
    SteeringDirective,
};
use roomkit::{ConversationStore, RoomKit, RoomKitError, SteerOptions, current_turn_host};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiProvider, AiResponse, AiRole, ProviderError,
    ToolResult, Tools,
};
use tokio::sync::watch;

use super::support::{Room, channel, lookup, room_id};

const STEERED: &str = "Only the .py files.";

/// A scripted model whose generation `held` (0-based), or every one from
/// it with `from_held`, waits until it is released, counting the
/// generations running at once.
#[derive(Clone)]
struct Gated {
    inner: ScriptedProvider,
    held: usize,
    from_held: bool,
    entered: Arc<AtomicUsize>,
    inside: Arc<AtomicUsize>,
    most_at_once: Arc<AtomicUsize>,
    answering: watch::Sender<bool>,
    release: watch::Sender<bool>,
}

impl Gated {
    fn new(rounds: impl IntoIterator<Item = ScriptedRound>, held: usize) -> Self {
        Self {
            inner: ScriptedProvider::new(rounds),
            held,
            from_held: false,
            entered: Arc::default(),
            inside: Arc::default(),
            most_at_once: Arc::default(),
            answering: watch::Sender::new(false),
            release: watch::Sender::new(false),
        }
    }

    /// The same model, holding every generation from `held` on.
    fn holding_from(mut self) -> Self {
        self.from_held = true;
        self
    }

    /// Returns once `count` generations started.
    async fn entered(&self, count: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while self.entered.load(Ordering::SeqCst) < count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    /// Returns once the held generation started.
    async fn answering(&self) {
        let mut answering = self.answering.subscribe();
        answering.wait_for(|answering| *answering).await.unwrap();
    }

    fn release(&self) {
        self.release.send_replace(true);
    }

    fn requests(&self) -> Vec<AiContext> {
        self.inner.requests()
    }
}

impl AiProvider for Gated {
    fn model_name(&self) -> &'static str {
        "gated"
    }

    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        let entered = self.entered.fetch_add(1, Ordering::SeqCst);
        let inside = self.inside.fetch_add(1, Ordering::SeqCst) + 1;
        self.most_at_once.fetch_max(inside, Ordering::SeqCst);
        if entered == self.held || (self.from_held && entered > self.held) {
            self.answering.send_replace(true);
            let mut release = self.release.subscribe();
            release.wait_for(|release| *release).await.unwrap();
        }
        let answered = self.inner.generate(context).await;
        self.inside.fetch_sub(1, Ordering::SeqCst);
        answered
    }
}

/// What a `Paused` tool runs once, as its call starts.
type OnStart = Arc<Mutex<Option<Box<dyn FnOnce() + Send>>>>;

/// A tool that waits until released, once it started.
#[derive(Clone)]
struct Paused {
    started: watch::Sender<bool>,
    release: watch::Sender<bool>,
    /// What runs as the call starts, before it waits.
    on_start: OnStart,
}

impl Paused {
    fn new() -> Self {
        Self {
            started: watch::Sender::new(false),
            release: watch::Sender::new(true),
            on_start: Arc::default(),
        }
    }

    /// The same tool, holding each call until released.
    fn holding(self) -> Self {
        self.release.send_replace(false);
        self
    }

    async fn started(&self) {
        let mut started = self.started.subscribe();
        started.wait_for(|started| *started).await.unwrap();
    }

    fn release(&self) {
        self.release.send_replace(true);
    }

    /// `lookup_order`, as this tool serves it.
    fn tools(&self) -> Tools {
        let paused = self.clone();
        Tools::new().tool(lookup(), move |_call, _context| {
            let paused = paused.clone();
            async move {
                paused.started.send_replace(true);
                if let Some(run) = paused.on_start.lock().unwrap().take() {
                    run();
                }
                let mut release = paused.release.subscribe();
                release.wait_for(|release| *release).await.unwrap();
                Ok(ToolResult::Text("a.py b.txt".to_owned()))
            }
        })
    }
}

/// The room, its agent answering through `model` with `tools`.
async fn with_tools(model: Gated, tools: Tools) -> Room {
    let options = AiChannelOptions {
        tools: tools.into(),
        ..AiChannelOptions::default()
    };
    Room::with_agent(AiChannel::new(channel("assistant"), model, options).unwrap()).await
}

fn call() -> ScriptedRound {
    ScriptedRound::calling(
        "Let me check.",
        [("c1", "lookup_order", r#"{"order": "42"}"#)],
    )
}

fn in_room() -> SteerOptions {
    SteerOptions {
        room_id: Some(room_id()),
        ..SteerOptions::default()
    }
}

/// Whether `request` carries a message holding `text`.
fn read(request: &AiContext, text: &str) -> bool {
    request
        .messages
        .iter()
        .any(|message| message.text().contains(text))
}

/// The texts of the agent's MESSAGE rows, and the end reason the last one
/// records.
fn answers(timeline: &[RoomEvent]) -> (Vec<String>, Option<String>) {
    let messages: Vec<&RoomEvent> = timeline
        .iter()
        .filter(|event| event.source.channel_id == channel("assistant"))
        .filter(|event| event.r#type == EventType::Message)
        .collect();
    let texts = messages
        .iter()
        .filter_map(|event| {
            if let EventContent::Text(text) = &event.content {
                Some(text.body.clone())
            } else {
                None
            }
        })
        .collect();
    let reason = messages.last().and_then(|event| {
        event
            .metadata
            .get("loop_end_reason")
            .and_then(|reason| reason.as_str().map(str::to_owned))
    });
    (texts, reason)
}

/// Runs `room`'s first turn on "List the files." in the background.
fn first_turn(room: &Arc<Room>) -> tokio::task::JoinHandle<()> {
    let room = Arc::clone(room);
    tokio::spawn(async move {
        room.says("List the files.").await;
    })
}

async fn steer(kit: &RoomKit, directive: impl Into<SteeringDirective>) -> usize {
    kit.steer(directive, &channel("assistant"), in_room())
        .await
        .unwrap()
}

// Python: tests/test_steering_during_turn.py::test_a_message_steered_while_the_model_answers_asks_it_again
#[tokio::test]
async fn a_message_steered_while_the_model_answers_asks_it_again() {
    let model = Gated::new(
        [
            call(),
            ScriptedRound::answering("a.py, b.txt"),
            ScriptedRound::answering("a.py"),
        ],
        1,
    );
    let room = Arc::new(with_tools(model.clone(), Paused::new().tools()).await);
    let turn = first_turn(&room);
    model.answering().await;

    let reached = steer(&room.kit, InjectMessage::new(STEERED)).await;
    model.release();
    turn.await.unwrap();

    let requests = model.requests();
    let asked_again = &requests[2];
    let last_two: Vec<AiRole> = asked_again.messages[asked_again.messages.len() - 2..]
        .iter()
        .map(|message| message.role)
        .collect();
    assert_eq!((reached, requests.len()), (1, 3));
    assert_eq!(last_two, [AiRole::Assistant, AiRole::User]);
    assert!(read(asked_again, "a.py, b.txt") && read(asked_again, STEERED));
    assert_eq!(
        answers(&room.timeline().await),
        (
            vec![
                "Let me check.".to_owned(),
                "a.py, b.txt".to_owned(),
                "a.py".to_owned()
            ],
            Some("completed".to_owned())
        )
    );
    room.kit.close().await.unwrap();
}

/// RFC §21.3: once the model has answered with nothing steered in, the
/// loop reads its queue no more, and the host is told so.
#[tokio::test]
async fn a_message_steered_after_the_last_stop_reaches_nothing() {
    let model = Gated::new([ScriptedRound::answering("a.py")], usize::MAX);
    let room = with_tools(model.clone(), Paused::new().tools()).await;
    room.says("List the files.").await;

    assert_eq!(steer(&room.kit, InjectMessage::new(STEERED)).await, 0);
    assert_eq!(model.requests().len(), 1);
    room.kit.close().await.unwrap();
}

// Python: tests/test_turns_control.py::test_a_directive_for_a_room_reaches_its_turns_loop
// Python: tests/test_channels/test_ai_steering.py::TestToolLoopCancellation::test_inject_message_during_tool_loop
#[tokio::test]
async fn a_directive_for_a_room_reaches_its_turns_loop() {
    let model = Gated::new([call(), ScriptedRound::answering("done")], usize::MAX);
    let tool = Paused::new().holding();
    let room = Arc::new(with_tools(model.clone(), tool.tools()).await);
    let turn = first_turn(&room);
    tool.started().await;

    let reached = steer(&room.kit, InjectMessage::new("Mind the budget.")).await;
    tool.release();
    turn.await.unwrap();

    let requests = model.requests();
    assert_eq!(reached, 1);
    assert!(read(&requests[1], "Mind the budget."));
    let injected = requests[1]
        .messages
        .iter()
        .filter(|message| message.text() == "Mind the budget.")
        .count();
    assert_eq!(injected, 1);
    room.kit.close().await.unwrap();
}

// Python: tests/test_turns_control.py::test_a_cancel_for_a_room_ends_its_turn_before_another_round
// Python: tests/test_channels/test_ai_steering.py::TestToolLoopCancellation::test_cancel_mid_loop
// Python: tests/test_channels/test_ai_steering.py::TestStreamingToolLoopCancellation::test_cancel_stops_streaming_loop
// Python: tests/test_channels/test_ai_steering.py::TestStreamingToolLoopCancellation::test_cancel_event_prequeued_streaming
#[tokio::test]
async fn a_cancel_for_a_room_ends_its_turn_before_another_round() {
    let model = Gated::new(
        [call(), ScriptedRound::answering("never asked")],
        usize::MAX,
    );
    let tool = Paused::new().holding();
    let room = Arc::new(with_tools(model.clone(), tool.tools()).await);
    let turn = first_turn(&room);
    tool.started().await;

    let reached = steer(
        &room.kit,
        Cancel {
            reason: "stop".to_owned(),
        },
    )
    .await;
    tool.release();
    turn.await.unwrap();

    assert_eq!((reached, model.requests().len()), (1, 1));
    assert_eq!(
        answers(&room.timeline().await),
        (
            vec!["Let me check.".to_owned()],
            Some("cancelled".to_owned())
        )
    );
    room.kit.close().await.unwrap();
}

// Python: tests/test_channels/test_ai_steering.py::TestToolLoopCancellation::test_cancel_before_first_round
#[tokio::test]
async fn a_cancel_during_a_generation_runs_none_of_its_calls() {
    let model = Gated::new([call(), ScriptedRound::answering("never asked")], 0);
    let tool = Paused::new();
    let room = Arc::new(with_tools(model.clone(), tool.tools()).await);
    let turn = first_turn(&room);
    model.answering().await;

    let reached = steer(&room.kit, Cancel::default()).await;
    model.release();
    turn.await.unwrap();

    // The generation stops at its first event since the cancel, which is
    // dropped with its stream: neither its text nor its call is written.
    let agent_rows = room
        .timeline()
        .await
        .into_iter()
        .filter(|event| event.source.channel_id == channel("assistant"))
        .count();
    assert_eq!((reached, model.requests().len(), agent_rows), (1, 1, 0));
    assert!(!*tool.started.borrow(), "the call never ran");
    room.kit.close().await.unwrap();
}

/// RFC §21.3: a `Cancel` that comes while a round's calls are announced,
/// before they run, stops them: none runs, each ends failed and
/// `cancelled`, so no start row stays pending.
#[tokio::test]
async fn a_cancel_while_the_calls_are_announced_stops_them() {
    let model = Gated::new(
        [
            ScriptedRound::calling(
                "",
                [
                    ("c1", "lookup_order", r#"{"order": "1"}"#),
                    ("c2", "lookup_order", r#"{"order": "2"}"#),
                ],
            ),
            ScriptedRound::answering("never asked"),
        ],
        usize::MAX,
    );
    let tool = Paused::new();
    let room = with_tools(model.clone(), tool.tools()).await;
    let kit = room.kit.downgrade();
    // Each start row is stored, its hooks run, before the loop goes on: a
    // cancel there comes while the calls are announced.
    room.kit
        .hook(
            roomkit::HookTrigger::BeforeBroadcast,
            move |event: RoomEvent, _context| {
                let kit = kit.upgrade();
                async move {
                    if let (Some(kit), EventType::ToolCallStart) = (kit, &event.r#type) {
                        kit.steer(Cancel::default(), &channel("assistant"), in_room())
                            .await
                            .unwrap();
                    }
                }
            },
            roomkit::HookOptions::default(),
        )
        .unwrap();
    room.says("Look both up.").await;

    let ends: Vec<_> = room
        .timeline()
        .await
        .into_iter()
        .filter(|event| event.r#type == EventType::ToolCallEnd)
        .filter_map(|event| {
            if let EventContent::ToolCall(row) = event.content {
                Some((row.tool_id, row.outcome, row.status))
            } else {
                None
            }
        })
        .collect();
    let cancelled = (
        Some(roomkit::model::ToolCallOutcome::Cancelled),
        roomkit::model::ToolCallStatus::Failed,
    );
    assert_eq!(model.requests().len(), 1);
    assert!(!*tool.started.borrow(), "no call ran");
    assert_eq!(
        ends,
        [
            ("c1".to_owned(), cancelled.0, cancelled.1),
            ("c2".to_owned(), cancelled.0, cancelled.1)
        ]
    );
    room.kit.close().await.unwrap();
}

// Python: tests/test_turns_control.py::test_a_directive_for_a_room_with_no_turn_reaches_nothing
#[tokio::test]
async fn a_directive_for_a_room_with_no_turn_reaches_nothing() {
    let model = Gated::new([], usize::MAX);
    let room = with_tools(model, Paused::new().tools()).await;

    assert_eq!(steer(&room.kit, Cancel::default()).await, 0);
    let both = SteerOptions {
        room_id: Some(room_id()),
        turn_id: Some("t1".to_owned()),
        ..SteerOptions::default()
    };
    let refused = room
        .kit
        .steer(Cancel::default(), &channel("assistant"), both)
        .await;
    assert!(
        matches!(refused, Err(RoomKitError::Value(_))),
        "{refused:?}"
    );
    room.kit.close().await.unwrap();
}

// Python: tests/test_turns_control.py::test_a_turn_addressed_by_the_id_its_tool_reads
#[tokio::test]
async fn a_turn_addressed_by_the_id_its_tool_reads() {
    let model = Gated::new([call(), ScriptedRound::answering("done")], usize::MAX);
    let tool = Paused::new().holding();
    let turn_ids: Arc<Mutex<Vec<String>>> = Arc::default();
    let noted = Arc::clone(&turn_ids);
    *tool.on_start.lock().unwrap() = Some(Box::new(move || {
        let turn = current_turn_host().unwrap();
        noted.lock().unwrap().push(turn.turn_id().to_owned());
    }));
    let room = Arc::new(with_tools(model.clone(), tool.tools()).await);
    let turn = first_turn(&room);
    tool.started().await;

    let turn_id = turn_ids.lock().unwrap()[0].clone();
    let options = SteerOptions {
        turn_id: Some(turn_id),
        ..SteerOptions::default()
    };
    let reached = room
        .kit
        .steer(
            InjectMessage::new("Mind the budget."),
            &channel("assistant"),
            options,
        )
        .await
        .unwrap();
    tool.release();
    turn.await.unwrap();

    assert_eq!(reached, 1);
    assert!(read(&model.requests()[1], "Mind the budget."));
    room.kit.close().await.unwrap();
}

/// RFC §17.2: with an organization, only the turns of its rooms are
/// reached, and a room of another organization is not found.
#[tokio::test]
async fn a_directive_for_another_organization_s_room_is_refused() {
    let model = Gated::new([], usize::MAX);
    let room = with_tools(model, Paused::new().tools()).await;
    let options = SteerOptions {
        room_id: Some(room_id()),
        organization_id: Some("org_b".to_owned()),
        ..SteerOptions::default()
    };
    let refused = room
        .kit
        .steer(Cancel::default(), &channel("assistant"), options)
        .await;
    assert!(
        matches!(refused, Err(RoomKitError::RoomNotFound { .. })),
        "{refused:?}"
    );
    room.kit.close().await.unwrap();
}

// Python: tests/test_channels/test_ai_steering_rooms.py::test_a_stop_in_one_room_leaves_the_other_rooms_turn_running
#[tokio::test]
async fn a_stop_in_one_room_leaves_the_other_rooms_turn_running() {
    let model = Gated::new(
        [
            ScriptedRound::answering("done"),
            ScriptedRound::answering("done"),
        ],
        0,
    )
    .holding_from();
    let room = Arc::new(with_tools(model.clone(), Paused::new().tools()).await);
    let other = roomkit::model::RoomId::new("room_2").unwrap();
    room.kit
        .create_room(roomkit::CreateRoomOptions {
            room_id: Some(other.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
    for id in ["sms", "assistant"] {
        room.kit
            .attach_channel(
                &other,
                &channel(id),
                roomkit::AttachChannelOptions::default(),
            )
            .await
            .unwrap();
    }
    let turn_a = first_turn(&room);
    let kit = room.kit.clone();
    let room_b = other.clone();
    let turn_b = tokio::spawn(async move {
        let message = roomkit::model::InboundMessage::new(
            channel("sms"),
            "+15550002",
            roomkit::model::TextContent::new("go"),
        );
        let options = roomkit::ProcessInboundOptions {
            room_id: Some(room_b),
            ..Default::default()
        };
        kit.process_inbound(message, options).await.unwrap();
    });
    model.entered(2).await;

    let stop = Cancel {
        reason: "stop pressed".to_owned(),
    };
    let reached = steer(&room.kit, stop).await;
    model.release();
    turn_a.await.unwrap();
    turn_b.await.unwrap();

    let in_b = room
        .kit
        .store()
        .list_events(&other, super::support::every_row())
        .await
        .unwrap();
    assert_eq!(reached, 1);
    assert_eq!(answers(&room.timeline().await), (vec![], None));
    assert_eq!(
        answers(&in_b),
        (vec!["done".to_owned()], Some("completed".to_owned()))
    );
    room.kit.close().await.unwrap();
}

/// The room, its agent steering its room turns, answering through `model`
/// with `tools`.
async fn steering_turns(model: Gated, tools: Tools) -> Room {
    let options = AiChannelOptions {
        tools: tools.into(),
        steer_room_turns: true,
        ..AiChannelOptions::default()
    };
    Room::with_agent(AiChannel::new(channel("assistant"), model, options).unwrap()).await
}

/// The texts of `channel_id`'s messages in the room.
async fn bodies(room: &Room, channel_id: &str) -> Vec<String> {
    room.timeline()
        .await
        .into_iter()
        .filter(|event| event.source.channel_id == channel(channel_id))
        .filter(|event| event.r#type == EventType::Message)
        .filter_map(|event| {
            if let EventContent::Text(text) = event.content {
                Some(text.body)
            } else {
                None
            }
        })
        .collect()
}

// Python: tests/test_steering_during_turn.py::test_a_message_during_a_tool_round_is_stored_and_read_by_the_running_turn
#[tokio::test]
async fn a_message_during_a_tool_round_is_stored_and_read_by_the_running_turn() {
    let model = Gated::new([call(), ScriptedRound::answering("a.py")], usize::MAX);
    let tool = Paused::new().holding();
    let room = Arc::new(steering_turns(model.clone(), tool.tools()).await);
    let turn = first_turn(&room);
    tool.started().await;

    let sent = room.says(STEERED).await;
    tool.release();
    turn.await.unwrap();

    let requests = model.requests();
    let read_once = requests[1]
        .messages
        .iter()
        .filter(|message| message.text().contains(STEERED))
        .count();
    assert!(!sent.blocked);
    assert_eq!((requests.len(), read_once), (2, 1));
    assert_eq!(bodies(&room, "sms").await, ["List the files.", STEERED]);
    assert_eq!(bodies(&room, "assistant").await, ["Let me check.", "a.py"]);
    room.kit.close().await.unwrap();
}

// Python: tests/test_steering_during_turn.py::test_a_message_while_the_model_answers_is_answered_by_the_same_turn
#[tokio::test]
async fn a_message_while_the_model_answers_is_answered_by_the_same_turn() {
    let model = Gated::new(
        [
            ScriptedRound::answering("a.py, b.txt"),
            ScriptedRound::answering("a.py"),
        ],
        0,
    );
    let room = Arc::new(steering_turns(model.clone(), Paused::new().tools()).await);
    let turn = first_turn(&room);
    model.answering().await;

    room.says(STEERED).await;
    model.release();
    turn.await.unwrap();

    let requests = model.requests();
    assert_eq!(model.most_at_once.load(Ordering::SeqCst), 1);
    assert_eq!(requests.len(), 2);
    assert!(read(&requests[1], STEERED));
    assert_eq!(bodies(&room, "assistant").await, ["a.py, b.txt", "a.py"]);
    room.kit.close().await.unwrap();
}

/// A change of the turn's message, sent while the model answers: steered,
/// it returns at once; waiting its place, once the turn ends. The model's
/// requests once both are over.
async fn a_change_while_the_model_answers(
    change: impl FnOnce(EventId) -> roomkit::model::InboundMessage,
) -> Vec<AiContext> {
    let model = Gated::new(
        [
            ScriptedRound::answering("a.py, b.txt"),
            ScriptedRound::answering("a.py"),
        ],
        0,
    );
    let room = Arc::new(steering_turns(model.clone(), Paused::new().tools()).await);
    let turn = first_turn(&room);
    model.answering().await;
    let target = room
        .timeline()
        .await
        .into_iter()
        .find(|event| event.r#type == EventType::Message)
        .unwrap()
        .id;
    let mut sent = {
        let room = Arc::clone(&room);
        let message = change(target);
        tokio::spawn(async move {
            let options = roomkit::ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            };
            room.kit.process_inbound(message, options).await.unwrap()
        })
    };
    let early = tokio::time::timeout(std::time::Duration::from_millis(50), &mut sent).await;
    model.release();
    turn.await.unwrap();
    let result = match early {
        Ok(joined) => joined.unwrap(),
        Err(_) => sent.await.unwrap(),
    };
    assert!(!result.blocked, "{:?}", result.reason);
    room.kit.close().await.unwrap();
    model.requests()
}

// Python: tests/test_steering_during_turn.py::test_an_edit_or_a_delete_while_the_model_answers_is_not_steered
#[tokio::test]
async fn an_edit_while_the_model_answers_is_not_steered() {
    let requests = a_change_while_the_model_answers(|target| {
        let edit = EditContent::new(
            target,
            roomkit::model::TextContent::new("List the .py files."),
        );
        let mut message = roomkit::model::InboundMessage::new(channel("sms"), "+15550001", edit);
        message.event_type = EventType::Edit;
        message
    })
    .await;
    assert_eq!(requests.len(), 1);
}

// Python: tests/test_steering_during_turn.py::test_an_edit_or_a_delete_while_the_model_answers_is_not_steered
#[tokio::test]
async fn a_delete_while_the_model_answers_is_not_steered() {
    let requests = a_change_while_the_model_answers(|target| {
        let delete = DeleteContent::new(target);
        let mut message = roomkit::model::InboundMessage::new(channel("sms"), "+15550001", delete);
        message.event_type = EventType::Delete;
        message
    })
    .await;
    assert_eq!(requests.len(), 1);
}

// Python: tests/test_steering_during_turn.py::test_off_a_message_during_a_turn_starts_its_own
#[tokio::test]
async fn off_a_message_during_a_turn_starts_its_own() {
    let model = Gated::new(
        [
            ScriptedRound::answering("first"),
            ScriptedRound::answering("second"),
        ],
        0,
    );
    let room = Arc::new(with_tools(model.clone(), Paused::new().tools()).await);
    let turn = first_turn(&room);
    model.answering().await;
    let second = {
        let room = Arc::clone(&room);
        tokio::spawn(async move {
            room.says("two").await;
        })
    };
    model.entered(2).await;
    model.release();
    turn.await.unwrap();
    second.await.unwrap();

    assert_eq!(model.most_at_once.load(Ordering::SeqCst), 2);
    room.kit.close().await.unwrap();
}

/// The model's requests once `second` (a sender and its name, if any)
/// wrote [`STEERED`] by SMS while the turn on `first`'s "go" ran a tool,
/// in a group room where both write through one channel.
async fn steered_in_a_group(
    first: (&str, Option<&str>),
    second: (&str, Option<&str>),
) -> Vec<AiContext> {
    let model = Gated::new([call(), ScriptedRound::answering("ok")], usize::MAX);
    let tool = Paused::new().holding();
    let options = AiChannelOptions {
        tools: tool.tools().into(),
        steer_room_turns: true,
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(channel("assistant"), model.clone(), options).unwrap();
    let kit = RoomKit::new(roomkit::RoomKitOptions::default());
    // A transport that keeps a message's metadata, its sender's name among
    // it, as Python's SMS channel does.
    let sms = roomkit::TransportChannel::new(
        channel("sms"),
        roomkit::model::ChannelType::SMS,
        roomkit::TransportChannelOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    kit.register_channel(agent).unwrap();
    kit.create_room(roomkit::CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    let group = roomkit::AttachChannelOptions {
        group: true,
        ..Default::default()
    };
    kit.attach_channel(&room_id(), &channel("sms"), group)
        .await
        .unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("assistant"),
        roomkit::AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let said = |(sender, name): (&str, Option<&str>), body: &str| {
        let mut message = roomkit::model::InboundMessage::new(
            channel("sms"),
            sender,
            roomkit::model::TextContent::new(body),
        );
        if let Some(name) = name {
            message
                .metadata
                .insert("sender_name".to_owned(), name.into());
        }
        let options = roomkit::ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        };
        (message, options)
    };
    let (first, options) = said(first, "go");
    let turn = {
        let kit = kit.clone();
        tokio::spawn(async move {
            kit.process_inbound(first, options).await.unwrap();
        })
    };
    tool.started().await;

    let (steered, options) = said(second, STEERED);
    kit.process_inbound(steered, options).await.unwrap();
    tool.release();
    turn.await.unwrap();

    kit.close().await.unwrap();
    model.requests()
}

/// The texts of `request`'s messages, for an assertion's message.
fn texts(request: &AiContext) -> Vec<String> {
    request
        .messages
        .iter()
        .map(roomkit_ai::AiMessage::text)
        .collect()
}

// Python: tests/test_steering_during_turn.py::test_a_steered_message_carries_its_speaker_when_the_room_has_several
#[tokio::test]
async fn a_steered_message_carries_its_speaker_when_the_room_has_several() {
    let requests =
        steered_in_a_group(("+15550001", Some("Alice")), ("+15550002", Some("Bob"))).await;
    let line = format!("Bob: \"{STEERED}\"");
    assert!(read(&requests[1], &line), "{:?}", texts(&requests[1]));
}

// Python: tests/test_people_taking_part.py::TestEveryReaderCountsSources::test_two_senders_with_no_name_on_one_channel_are_labelled[steered message]
#[tokio::test]
async fn a_steered_message_from_a_second_nameless_sender_carries_its_rank() {
    let requests = steered_in_a_group(("+15550001", None), ("+15550002", None)).await;
    let line = format!("@sms (2): \"{STEERED}\"");
    assert!(read(&requests[1], &line), "{:?}", texts(&requests[1]));
}
