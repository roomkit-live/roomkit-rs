//! What a turn costs, end to end through the kit: a message in, the agent's
//! answer streamed and stored (step 4 Risks, "The backend under every
//! turn"). Not a check: it prints its numbers, and runs only when asked.
//!
//! ```text
//! cargo test --release -p roomkit-ai --test turn_cost -- --ignored --nocapture --test-threads 1
//! ```
//!
//! - a small turn: a room of its own, whose history is the message alone,
//!   the room's setup measured apart;
//! - a buffered turn (an echo, no stream) on a history of 50 events: the
//!   turn's fixed cost;
//! - a history-sized turn: a room holding 50 events, the most a turn's
//!   context carries;
//! - a turn with a tool call on that history, its handler writing to the
//!   room through its turn or not;
//! - an answer in 100 deltas on that history: the cost of each item.
//!
//! Each runs on one thread, then on four, as a server runs.

#![expect(clippy::unwrap_used, reason = "a measurement fails on a broken setup")]
#![expect(clippy::print_stdout, reason = "a measurement prints its numbers")]

use std::time::Duration;

use tokio::time::Instant;

use roomkit::model::{ChannelId, ChannelType, InboundMessage, RoomId, TextContent};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, ProcessInboundOptions, RoomKit, RoomKitOptions,
    current_turn_host,
};
use roomkit_ai::{
    AiAgent, AiProvider, AiRequest, Done, ProviderError, ProviderEvent, ProviderResponse,
    ProviderStream, ProviderToolCall, ToolDefinition, ToolOutput, Tools,
};
use roomkit_core::rules::transcript::Role;
use serde_json::json;

/// A model that answers at once: with a call to `lookup` when `calls` and
/// the request ends on the person's message, else with a sentence, in
/// `deltas` pieces when more than one.
struct Immediate {
    calls: bool,
    deltas: usize,
}

impl AiProvider for Immediate {
    fn generate<'a>(&'a self, request: &'a AiRequest) -> ProviderStream<'a> {
        let after_tool = request
            .messages
            .last()
            .is_some_and(|message| message.role == Role::Tool);
        let response = if self.calls && !after_tool {
            ProviderResponse::new(
                "Let me check.",
                vec![ProviderToolCall::new("call-1", "lookup", r#"{"q":"x"}"#)],
                Done::default(),
            )
        } else if self.deltas > 1 {
            let pieces: Vec<Result<ProviderEvent, ProviderError>> = (0..self.deltas)
                .map(|_| Ok(ProviderEvent::Text("word ".to_owned())))
                .chain([Ok(ProviderEvent::Done(Done::default()))])
                .collect();
            return Box::pin(Pieces(pieces.into_iter()));
        } else {
            ProviderResponse::new("It is 42.", Vec::new(), Done::default())
        };
        ProviderResponse::into_stream(async move { Ok(response) })
    }
}

/// A generation's events, each ready at once.
struct Pieces(std::vec::IntoIter<Result<ProviderEvent, ProviderError>>);

impl futures_core::Stream for Pieces {
    type Item = Result<ProviderEvent, ProviderError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Ready(self.get_mut().0.next())
    }
}

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

/// A kit with an SMS transport and an agent, its tool writing to the room
/// through its turn.
fn kit(calls: bool, deltas: usize) -> RoomKit {
    kit_writing(calls, deltas, true)
}

/// The kit, its tool writing to the room through its turn when `writes`.
fn kit_writing(calls: bool, deltas: usize, writes: bool) -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    let lookup = ToolDefinition::new(
        "lookup",
        "Look it up.",
        json!({ "type": "object", "properties": { "q": { "type": "string" } } }),
    );
    let tools = Tools::new().tool(lookup, move |_call, context| async move {
        if writes {
            let turn = current_turn_host().unwrap();
            let note = TextContent::new("(looking it up)");
            turn.send_event(
                &context.room_id,
                &channel("ai"),
                note,
                roomkit::SendEventOptions::default(),
            )
            .await
            .unwrap();
        }
        Ok(ToolOutput::Text("42".to_owned()))
    });
    let agent = AiAgent::new(channel("ai"), Immediate { calls, deltas })
        .with_tools(tools)
        .unwrap();
    kit.register_channel(agent).unwrap();
    kit
}

async fn room(kit: &RoomKit, id: &str, history: usize) -> RoomId {
    let room = RoomId::new(id).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room, &channel("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    for index in 0..history {
        let message = InboundMessage::new(
            channel("sms"),
            "+15550000001",
            TextContent::new(format!("Earlier message {index}")),
        );
        kit.process_inbound(
            message,
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }
    kit.attach_channel(&room, &channel("ai"), AttachChannelOptions::default())
        .await
        .unwrap();
    room
}

async fn turn(kit: &RoomKit, room: &RoomId) {
    let message = InboundMessage::new(channel("sms"), "+15550000001", TextContent::new("What?"));
    let result = kit
        .process_inbound(
            message,
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(!result.blocked);
}

fn per_turn(label: &str, turns: u32, took: Duration) {
    let micros = took.as_secs_f64() * 1e6 / f64::from(turns);
    println!("{label}: {micros:.1} µs per turn ({turns} turns)");
}

const TURNS: u32 = 2_000;

#[tokio::test(flavor = "current_thread")]
#[ignore = "a measurement, run on demand"]
async fn what_a_turn_costs() {
    measure().await;
}

/// The same, on four worker threads, as a server runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement, run on demand"]
async fn what_a_turn_costs_on_four_threads() {
    measure().await;
}

async fn measure() {
    // A small turn, in a room of its own; the room's setup measured apart.
    let small = kit(false, 1);
    let started = Instant::now();
    for index in 0..TURNS {
        room(&small, &format!("setup_{index}"), 0).await;
    }
    let setup = started.elapsed();
    per_turn("room setup alone", TURNS, setup);
    let started = Instant::now();
    for index in 0..TURNS {
        let room = room(&small, &format!("small_{index}"), 0).await;
        turn(&small, &room).await;
    }
    per_turn("small turn, setup included", TURNS, started.elapsed());
    per_turn(
        "small turn, setup taken off",
        TURNS,
        started.elapsed().saturating_sub(setup),
    );

    // A buffered answer, no stream: the turn's fixed cost alone.
    let echo = RoomKit::new(RoomKitOptions::default());
    echo.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    echo.register_channel(roomkit::testing::EchoAgent::new(
        channel("ai"),
        "Hello.",
        roomkit::testing::EchoAgentOptions::default(),
    ))
    .unwrap();
    let room_id = room(&echo, "echo", 50).await;
    let started = Instant::now();
    for _ in 0..TURNS {
        turn(&echo, &room_id).await;
    }
    per_turn(
        "history-sized buffered turn (echo)",
        TURNS,
        started.elapsed(),
    );

    for (label, calls, deltas, writes) in [
        ("history-sized turn", false, 1, false),
        ("history-sized turn with a tool", true, 1, true),
        (
            "history-sized turn with a tool that does not write",
            true,
            1,
            false,
        ),
        ("history-sized turn of 100 deltas", false, 100, false),
    ] {
        let kit = kit_writing(calls, deltas, writes);
        let room = room(&kit, "history", 50).await;
        for _ in 0..50 {
            turn(&kit, &room).await;
        }
        let started = Instant::now();
        for _ in 0..TURNS {
            turn(&kit, &room).await;
        }
        per_turn(label, TURNS, started.elapsed());
    }
}
