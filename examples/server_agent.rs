//! An AI agent served over WebSocket, its rooms kept in SQLite: an axum
//! server where each WebSocket connection is a person taking part in a room
//! through the `websocket` channel, an agent answering each message through
//! an OpenAI-compatible endpoint (RFC §6.4), and the rooms in a SQLite file
//! (RFC §14), so that a restarted server goes on with each conversation.
//! The server is the examples' own, in `shared/server.rs`, which `web_chat`
//! serves a page in front of.
//!
//! - A connection to `/conversations/{conversation}?user=alice&name=Alice`
//!   is the participant `alice`, named Alice, of the room whose id is the
//!   conversation's (RFC §5.5). A new conversation's room is created with
//!   the agent and the `websocket` channel attached, whole even when the
//!   client leaves meanwhile.
//! - Each text frame it sends is a message from `alice` into that room,
//!   through the inbound pipeline (RFC §10.1), its delivery deferred
//!   (`defer_delivery`, RFC §10.1 step 18): the call returns at the
//!   commit, and the connection reads on while the agent answers. A
//!   message the room refuses, or whose turn fails or is ended by a stop,
//!   gets `{"error": "..."}` back, a reason of the server's own. A message
//!   or a frame over 64 KiB closes the connection with 1009, a frame at its
//!   header, and a stop closes them all with 1001.
//! - The agent serves a tool, `lookup_order`, which finds an order by its
//!   number (RFC §6.4, §21): a turn that calls it stores the round's text,
//!   the call's start and end rows, then the answer, which carries the
//!   turn's record (`loop_end_reason`, `ai_usage`); a later turn reads what
//!   each round said, never the call.
//! - The room's deliveries to `websocket` go out on each of the room's
//!   connections, one JSON frame per event: the agent's rows, a call's
//!   `tool` and the turn's `ended` included, in index order, each with its
//!   `id` and `index`. A room delivers an event to every channel but its
//!   source (RFC §10.2 step 2), so the people on `websocket` see the
//!   agent's answers, not each other's messages. A client that stops
//!   reading is dropped, reader included, once one send waits 5 seconds; a
//!   stop gives the connections 10 seconds, then closes the kit, which
//!   drains what still runs for up to 5 seconds (`RoomKit::close`).
//! - The agent's answer streams (RFC §6.1, §12.2 steps 13s and 14s): a
//!   connection opened with `&stream=1` receives its text as the model
//!   writes it, `{"type": "delta", "stream_id": "...", "text": "..."}`
//!   frames, then each row as the store holds it, the deltas joining to the
//!   row's text, which carries the same `stream_id`: two answers streaming
//!   at once into the room are told apart by it. Any other connection
//!   receives the rows only. Once every client of the room has
//!   left, the answer stops: the text sent so far is stored marked
//!   `cancelled`, and nothing more is generated. The deltas are shown
//!   before `BEFORE_BROADCAST` judges the row they end in (RFC §6.4): a
//!   service whose hooks rewrite answers keeps its clients from streaming.
//! - The conversation and the `user` are 1 to 64 letters, digits, `_` or
//!   `-`, and the name 64 characters at most, none a control character;
//!   anything else is refused with 400.
//!
//! What it leaves out, which a service built from it adds:
//! - TLS: it serves plain `ws://`, to be put behind a TLS proxy.
//! - Authentication: `user` is whoever the client says it is; a service
//!   takes the participant from its session.
//! - The `Origin` check: none is made, so with a session held in a cookie
//!   any web page could open a socket as its visitor (cross-site WebSocket
//!   hijacking). `web_chat`, which serves its page itself, makes it in a
//!   layer; a service checks it in `connect`, given the request's
//!   `headers: HeaderMap`:
//!
//!   ```text
//!   if headers.get(ORIGIN).is_some_and(|origin| origin != "https://chat.example.com") {
//!       return StatusCode::FORBIDDEN.into_response();
//!   }
//!   ```
//!
//! - Who may create and join a room: any client creates one by naming a
//!   new conversation, and joins any it names.
//! - A rate limit: nothing bounds how often a client writes, and each
//!   message costs a model call.
//! - A cap on connections: nothing bounds how many are open, and each can
//!   hold 64 queued frames of up to the answers' size, beside its kernel
//!   buffers, several MB.
//!
//! Its checks run offline, against a fake of the OpenAI wire, which streams,
//! and a temporary file: Alice and Bob talk with the agent in one room, the
//! agent looking up Bob's order, the server restarts on the same file, and
//! Alice goes on; Erin reads an answer as it is written beside Finn, who
//! reads its rows; Gina leaves mid-answer.
//!
//! With `--serve`, it stays up on 127.0.0.1:8080, or the address given
//! after the flag, its rooms in `server_agent.db`, until Ctrl+C. The agent
//! calls the endpoint `OPENAI_BASE_URL`, `OPENAI_API_KEY` and
//! `OPENAI_MODEL` name, or, without them, the fake, which answers from the
//! checks' script.
//!
//! ```sh
//! cargo run --example server_agent
//! OPENAI_BASE_URL=https://api.openai.com/v1 OPENAI_API_KEY=sk-... \
//!     OPENAI_MODEL=gpt-6-luna cargo run --example server_agent -- --serve
//! websocat 'ws://127.0.0.1:8080/conversations/bakery?user=alice&name=Alice&stream=1'
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::env;
use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::process::ExitCode;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use roomkit::model::{EventContent, EventType, RoomEvent, RoomId};
use roomkit::{ConversationStore, HookTrigger, PostOptions, text_of};
use roomkit_sqlite::SqliteStore;
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::{WebSocketStream, tungstenite};

use shared::bakery::{ORDER_42, SYSTEM_PROMPT};
use shared::server::{
    AgentServer, CHECKING, MESSAGE_LIMIT, Patience, SERVING, STOPPED, TURN_FAILED,
};
use shared::{Checks, FakeOpenAi, OpenAiEndpoint, Reply, TempDatabase, every_row};

/// Where `--serve` listens unless told another address, and the file it
/// keeps its rooms in.
const SERVE_ADDRESS: &str = "127.0.0.1:8080";
const SERVE_DATABASE: &str = "server_agent.db";

/// The conversation of the checks, and what is said in it.
const CONVERSATION: &str = "bakery";
const ALICE_ASKS: &str = "Hi! Do you have sourdough today?";
const BOB_ASKS: &str = "Is my order 42 ready?";
const ALICE_AGAIN: &str = "Are the loaves still warm?";

/// What the fake answers, turn after turn: each turn's answer.
const ANSWERS: &[&str] = &[
    "Yes, we have sourdough until noon.",
    "Yes Bob, order 42 is ready: two loaves, to pick up at 5 pm.",
    "Yes Carol, flour comes in 1 kg bags.",
    "They came out of the oven an hour ago, Alice.",
];

/// What the fake says before it looks up Bob's order, and the call.
const LOOKING_UP: &str = "Let me look up your order.";
const LOOKUP_CALL: &str = "call_order_42";

/// The conversation's script: each answer, Bob's turn calling the lookup
/// first.
fn script() -> Vec<Reply> {
    let mut script: Vec<Reply> = ANSWERS.iter().map(|answer| Reply::from(*answer)).collect();
    let lookup = Reply::calling(
        LOOKING_UP,
        [(LOOKUP_CALL, "lookup_order", r#"{"order": "42"}"#)],
    );
    script.insert(1, lookup);
    script
}

/// Another conversation on the first server, and what Carol asks in it.
const PANTRY: &str = "pantry";
const CAROL_ASKS: &str = "Do you sell flour?";

/// Where the checks' servers listen: a free port of the loopback.
const ANY_PORT: &str = "127.0.0.1:0";

/// The kitchen's conversation, where Dave stops reading, what he sends
/// once he fell behind, and the posts that make him fall behind: large
/// ones, and as many as it takes, up to a bound.
const KITCHEN: &str = "kitchen";
const DAVE_LATE: &str = "Sorry, I was away. Is there any rye left?";
const LARGE_POST: usize = 60 * 1024;
const POSTS_AT_MOST: usize = 400;

/// The conversation where a stop comes during a turn.
const GARDEN: &str = "garden";

/// The conversation a client closes during a turn.
const ORCHARD: &str = "orchard";

/// The conversation a client leaves while its join makes the room.
const PORCH: &str = "porch";

/// The conversation where Erin reads the answer as it is written and Finn
/// its rows, what Erin asks, and what the model answers.
const STUDIO: &str = "studio";
const ERIN_ASKS: &str = "When does the rye come out?";
const RYE: &str = "Fresh rye comes out of the oven at noon.";

/// The conversation Gina leaves mid-answer, what she asks, the answer the
/// model writes slowly, and how many of its pieces she reads.
const TERRACE: &str = "terrace";
const GINA_ASKS: &str = "When does the terrace open?";
const TERRACE_OPENS: &str = "The terrace opens at nine in the morning, every day of the week.";
const GINA_READS: usize = 2;
const WORD_PAUSE: Duration = Duration::from_millis(50);

/// The conversation where the model is unreachable, and how long a client
/// there waits to be told.
const CELLAR: &str = "cellar";
const TOLD_PATIENCE: Duration = Duration::from_secs(2);

/// The length of the frame a client announces and never sends: far over
/// the limit, far under what the WebSocket library takes by default
/// (16 MiB).
const ANNOUNCED: u64 = 1 << 20;

/// The receive buffer of a client that falls behind, in bytes.
const SMALL_BUFFER: u32 = 4096;

/// How long a check waits for a connection to end or a server to stop: a
/// stop's grace and its kit's close drain included.
const STOP_PATIENCE: Duration = Duration::from_secs(15);

/// How long a client waits for an answer: a real model's included.
const ANSWER_PATIENCE: Duration = Duration::from_secs(60);

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let mut args = env::args().skip(1);
    if args.next().as_deref() == Some("--serve") {
        serve(args.next().as_deref().unwrap_or(SERVE_ADDRESS)).await?;
        return Ok(ExitCode::SUCCESS);
    }
    check().await
}

/// `--serve`: the server on `address`, until Ctrl+C.
async fn serve(address: &str) -> Result<(), Box<dyn Error>> {
    let endpoint = OpenAiEndpoint::from_env(&script()).await?;
    let server = AgentServer::start(
        Path::new(SERVE_DATABASE),
        &endpoint,
        address.parse()?,
        SERVING,
    )
    .await?;
    println!("The agent calls {}.", endpoint.shown_base_url());
    println!(
        "Serving ws://{}/conversations/{{conversation}}?user=...&name=..., \
         the rooms in {SERVE_DATABASE}. Ctrl+C stops.",
        server.address()
    );
    tokio::signal::ctrl_c().await?;
    server.stop().await
}

/// The checks: two people in one room, then a restart on the same file,
/// and a client that stops reading.
async fn check() -> Result<ExitCode, Box<dyn Error>> {
    let mut checks = Checks::default();
    the_conversation_survives_a_restart(&mut checks).await?;
    a_client_reads_the_answer_as_it_is_written(&mut checks).await?;
    a_client_leaves_mid_answer(&mut checks).await?;
    a_client_stops_reading(&mut checks).await?;
    what_a_client_is_told(&mut checks).await?;
    what_a_connection_must_name(&mut checks).await?;
    a_client_leaves_while_it_joins(&mut checks).await?;
    a_stop_during_a_turn(&mut checks).await?;
    a_close_during_a_turn(&mut checks).await?;
    Ok(checks.finish())
}

/// Alice and Bob talk with the agent, the server restarts on the same file,
/// and Alice goes on.
async fn the_conversation_survives_a_restart(checks: &mut Checks) -> Result<(), Box<dyn Error>> {
    let endpoint = OpenAiEndpoint::from_env(&script()).await?;
    println!("The agent calls {}.", endpoint.shown_base_url());
    let database = TempDatabase::new("server_agent");
    let (talked, pantry) = two_people_talk(database.path(), &endpoint).await?;
    let Talked {
        answers: [first, second],
        bob_turn,
    } = talked;
    println!("The server restarts on the same file.");
    let third = alice_goes_on(database.path(), &endpoint).await?;
    let timeline = timeline_in(database.path(), CONVERSATION).await?;
    shared::print_timeline(&timeline);
    let said = messages_of(&timeline);
    let carol = pantry
        .as_ref()
        .and_then(|frame| frame.get("text"))
        .and_then(Value::as_str);
    let carol = carol.map(str::to_owned);
    let answers = [&first[0], &second[0], &carol, &third];

    checks.expect(
        "the agent answers each message, and its answer reaches both people's sockets",
        answers.iter().all(|answer| answer.is_some())
            && first[0] == first[1]
            && second[0] == second[1],
    );
    checks.expect(
        "after the restart, the conversation goes on: Alice's next message is answered",
        third.is_some(),
    );
    checks.expect(
        "a room's deliveries reach its own connections only: Carol's first frame, in \
         another room, is her own answer",
        pantry.as_ref().and_then(|frame| frame.get("room_id")) == Some(&Value::from(PANTRY))
            && carol.is_some(),
    );
    // Each question, then what the agent said to it: Bob's turn, every
    // message it delivered.
    let bob_said = texts_of(&messages_in(&bob_turn));
    let mut conversation: Vec<Option<&str>> = vec![Some(ALICE_ASKS), first[0].as_deref()];
    conversation.push(Some(BOB_ASKS));
    conversation.extend(bob_said.iter().map(|text| Some(text.as_str())));
    conversation.extend([Some(ALICE_AGAIN), third.as_deref()]);
    checks.expect(
        "the file holds the whole conversation, both servers' turns, in order",
        said.iter().map(|text| Some(text.as_str())).eq(conversation),
    );
    checks.expect(
        "Bob's turn reaches his socket in index order, and ends on its answer",
        indexes_rise(&bob_turn)
            && bob_turn.last().and_then(|frame| frame.get("ended")) == Some(&json!("completed")),
    );
    if let Some(fake) = endpoint.fake() {
        check_the_lookup(checks, &bob_turn, &timeline);
        check_the_model_s_turns(checks, &fake.requests(), &answers);
    }
    Ok(())
}

/// What the first server's checks read: the answers to Alice's and to Bob's
/// questions, each as Alice's and Bob's sockets received it, and every
/// frame of Bob's turn on his socket.
struct Talked {
    answers: [[Option<String>; 2]; 2],
    bob_turn: Vec<Value>,
}

/// The checks of the lookup in Bob's turn, against the fake's script: the
/// round's text, the call's two rows and the answer reach his socket, the
/// file holds them with the turn's record on the answer.
fn check_the_lookup(checks: &mut Checks, bob_turn: &[Value], timeline: &[RoomEvent]) {
    let kinds: Vec<&str> = bob_turn
        .iter()
        .filter_map(|frame| frame["type"].as_str())
        .collect();
    checks.expect(
        "Bob's socket receives the round's text, the call's start and end, then the answer",
        kinds == ["message", "tool_call_start", "tool_call_end", "message"]
            && bob_turn.first().map(|frame| &frame["text"]) == Some(&json!(LOOKING_UP))
            && bob_turn
                .get(2)
                .and_then(|frame| frame.pointer("/tool/result"))
                == Some(&json!(ORDER_42)),
    );
    let rows: Vec<&RoomEvent> = timeline
        .iter()
        .filter(|event| {
            matches!(&event.content, EventContent::ToolCall(call) if call.tool_id == LOOKUP_CALL)
        })
        .collect();
    let recorded = timeline.iter().find(|event| {
        event.event_type == EventType::Message
            && ANSWERS
                .get(1)
                .is_some_and(|answer| text_of(&event.content) == *answer)
    });
    checks.expect(
        "the file holds the call's start and end rows, and the turn's record on its answer",
        rows.iter()
            .map(|event| event.event_type)
            .eq([EventType::ToolCallStart, EventType::ToolCallEnd])
            && recorded.is_some_and(|answer| {
                answer.metadata.get("loop_end_reason") == Some(&json!("completed"))
                    && answer
                        .metadata
                        .get("ai_usage")
                        .is_some_and(Value::is_object)
            }),
    );
}

/// The message frames of `frames`.
fn messages_in(frames: &[Value]) -> Vec<&Value> {
    frames
        .iter()
        .filter(|frame| frame["type"] == "message")
        .collect()
}

/// The texts of `frames`.
fn texts_of(frames: &[&Value]) -> Vec<String> {
    frames
        .iter()
        .filter_map(|frame| frame["text"].as_str())
        .map(str::to_owned)
        .collect()
}

/// Whether the frames' indexes rise, each event after the one before.
fn indexes_rise(frames: &[Value]) -> bool {
    let indexes: Vec<u64> = frames
        .iter()
        .filter_map(|frame| frame["index"].as_u64())
        .collect();
    indexes.len() == frames.len()
        && indexes
            .windows(2)
            .all(|pair| matches!(pair, [earlier, later] if earlier < later))
}

/// The first server: Alice and Bob join the conversation, and Carol
/// another one; Alice asks, then Bob, then Carol. For each of Alice's and
/// Bob's questions, the answer each socket received, Alice's then Bob's;
/// and the first frame Carol's socket received, after both.
async fn two_people_talk(
    database: &Path,
    endpoint: &OpenAiEndpoint,
) -> Result<(Talked, Option<Value>), Box<dyn Error>> {
    let server = AgentServer::start(database, endpoint, ANY_PORT.parse()?, CHECKING).await?;
    let mut alice = Client::connect(server.address(), CONVERSATION, "alice", "Alice").await?;
    let mut bob = Client::connect(server.address(), CONVERSATION, "bob", "Bob").await?;
    let mut carol = Client::connect(server.address(), PANTRY, "carol", "Carol").await?;
    alice.say(ALICE_ASKS).await?;
    let first = [alice.answer().await, bob.answer().await];
    bob.say(BOB_ASKS).await?;
    let alice_heard = alice.answer().await;
    let bob_turn = bob.turn().await;
    let bob_heard = messages_in(&bob_turn)
        .last()
        .and_then(|frame| frame["text"].as_str())
        .map(str::to_owned);
    let second = [alice_heard, bob_heard];
    // Had the bakery's answers reached her, they would come first.
    carol.say(CAROL_ASKS).await?;
    let pantry = carol.next_frame(ANSWER_PATIENCE).await;
    println!("  Carol receives {pantry:?}");
    for client in [alice, bob, carol] {
        client.close().await;
    }
    server.stop().await?;
    let talked = Talked {
        answers: [first, second],
        bob_turn,
    };
    Ok((talked, pantry))
}

/// The second server, on the same file: Alice comes back and asks again.
/// The answer she received.
async fn alice_goes_on(
    database: &Path,
    endpoint: &OpenAiEndpoint,
) -> Result<Option<String>, Box<dyn Error>> {
    let server = AgentServer::start(database, endpoint, ANY_PORT.parse()?, CHECKING).await?;
    let mut alice = Client::connect(server.address(), CONVERSATION, "alice", "Alice").await?;
    alice.say(ALICE_AGAIN).await?;
    let answer = alice.answer().await;
    alice.close().await;
    server.stop().await?;
    Ok(answer)
}

/// Erin reads the answer as the model writes it, Finn, in the same room,
/// reads its rows: Erin's deltas join to the row's text, which comes with
/// its id and index, and every frame Finn receives is a row the file holds
/// (RFC §6.1, §12.2 step 14s).
async fn a_client_reads_the_answer_as_it_is_written(
    checks: &mut Checks,
) -> Result<(), Box<dyn Error>> {
    println!("Erin reads the answer as it is written; Finn reads its rows.");
    let endpoint = OpenAiEndpoint::Fake(FakeOpenAi::start(&[RYE]).await?);
    let database = TempDatabase::new("server_agent_studio");
    let server =
        AgentServer::start(database.path(), &endpoint, ANY_PORT.parse()?, CHECKING).await?;
    let mut erin = Client::connect_streaming(server.address(), STUDIO, "erin", "Erin").await?;
    let mut finn = Client::connect(server.address(), STUDIO, "finn", "Finn").await?;
    erin.say(ERIN_ASKS).await?;
    let read = erin.turn().await;
    let finn_read = finn.turn().await;
    for client in [erin, finn] {
        client.close().await;
    }
    server.stop().await?;
    let timeline = timeline_in(database.path(), STUDIO).await?;
    let deltas: String = read
        .iter()
        .filter(|frame| frame["type"] == "delta")
        .filter_map(|frame| frame["text"].as_str())
        .collect();
    let row = read.last();
    let stored = row.and_then(|row| {
        timeline
            .iter()
            .find(|event| Some(&json!(event.id)) == row.get("id"))
    });
    // The answer's stream is named by the agent and the message it answers.
    let asked = timeline
        .iter()
        .find(|event| text_of(&event.content) == ERIN_ASKS)
        .map(|event| json!(format!("assistant:{}", event.id)));
    checks.expect(
        "a client that streams receives the answer's pieces, then its row, all naming one \
         stream, the agent's answer to Erin's message",
        asked.is_some()
            && read
                .iter()
                .all(|frame| frame.get("stream_id") == asked.as_ref()),
    );
    checks.expect(
        "a client that streams receives the answer's pieces, then its row with its id and \
         index, the pieces joining to the row's text",
        read.len() > 2
            && read
                .iter()
                .take(read.len() - 1)
                .all(|frame| frame["type"] == "delta")
            && deltas == RYE
            && row.and_then(|row| row["text"].as_str()) == Some(RYE)
            && stored.is_some_and(|event| {
                row.and_then(|row| row["index"].as_u64()) == Some(event.index)
            }),
    );
    let held = |frame: &Value| {
        timeline
            .iter()
            .any(|event| Some(&json!(event.id)) == frame.get("id"))
    };
    checks.expect(
        "a client that does not stream receives the rows only, each an event the file holds",
        !finn_read.is_empty()
            && finn_read
                .iter()
                .all(|frame| frame["type"] != "delta" && held(frame))
            && messages_in(&finn_read)
                .last()
                .and_then(|frame| frame["text"].as_str())
                == Some(RYE),
    );
    Ok(())
}

/// Gina reads the first pieces of an answer the model writes slowly, then
/// leaves: the answer stops, the text she was sent stored marked
/// `cancelled` (RFC §12.2 step 13s).
async fn a_client_leaves_mid_answer(checks: &mut Checks) -> Result<(), Box<dyn Error>> {
    println!("Gina leaves while the answer is written.");
    let slow = Reply::from(TERRACE_OPENS).pausing(WORD_PAUSE);
    let endpoint = OpenAiEndpoint::Fake(FakeOpenAi::start(&[slow]).await?);
    let database = TempDatabase::new("server_agent_terrace");
    let server =
        AgentServer::start(database.path(), &endpoint, ANY_PORT.parse()?, CHECKING).await?;
    let mut gina = Client::connect_streaming(server.address(), TERRACE, "gina", "Gina").await?;
    gina.say(GINA_ASKS).await?;
    let mut read = String::new();
    for _ in 0..GINA_READS {
        if let Some(frame) = gina.next_frame(ANSWER_PATIENCE).await {
            read.push_str(
                frame
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            );
        }
    }
    println!("  Gina read {read:?}, and leaves");
    gina.leave();
    // The stop closes the kit, which lets the answer's turn end first.
    server.stop().await?;
    let timeline = timeline_in(database.path(), TERRACE).await?;
    let answer = timeline
        .iter()
        .find(|event| event.source.channel_id.as_str() == "assistant");
    let stored = answer.map(|answer| text_of(&answer.content));
    println!("  the file holds {stored:?}");
    checks.expect(
        "a client that leaves mid-answer leaves the text it was sent stored, marked \
         cancelled, and nothing more of the answer",
        answer.is_some_and(|answer| {
            let text = text_of(&answer.content);
            text.starts_with(&read)
                && text.len() < TERRACE_OPENS.len()
                && TERRACE_OPENS.starts_with(&text)
                && answer.metadata.get("cancelled") == Some(&json!(true))
                && answer.metadata.get("loop_end_reason") == Some(&json!("cancelled"))
        }),
    );
    Ok(())
}

/// Dave joins the kitchen's conversation and stops reading, while the host
/// posts one large menu after another to the room. Once his connection
/// cannot keep up, the server drops it, reader included, so that what he
/// sends then never enters the room, and a stop does not wait for him.
async fn a_client_stops_reading(checks: &mut Checks) -> Result<(), Box<dyn Error>> {
    println!("Dave stops reading while the host posts large menus to the kitchen.");
    let endpoint = OpenAiEndpoint::Fake(FakeOpenAi::start(ANSWERS).await?);
    let database = TempDatabase::new("server_agent_kitchen");
    let server =
        AgentServer::start(database.path(), &endpoint, ANY_PORT.parse()?, CHECKING).await?;
    let mut dave =
        Client::connect_reading_little(server.address(), KITCHEN, "dave", "Dave").await?;
    let room = RoomId::new(KITCHEN)?;
    let menu = format!("Today's menu{}", ".".repeat(LARGE_POST));
    let mut posts = 0;
    while server.websocket().connections() > 0 && posts < POSTS_AT_MOST {
        server
            .kit()
            .post(
                &room,
                EventContent::text(menu.as_str()),
                PostOptions::default(),
            )
            .await?;
        posts += 1;
    }
    let dropped = tokio::time::timeout(STOP_PATIENCE, server.connections_down_to(0))
        .await
        .is_ok();
    println!("  after {posts} menus, Dave's connection ended: {dropped}");
    // Sent to a connection the server closed, it fails or goes nowhere.
    let _sent = dave.say(DAVE_LATE).await;
    let stopped = tokio::time::timeout(STOP_PATIENCE, server.stop()).await;
    let late = messages_of(&timeline_in(database.path(), KITCHEN).await?)
        .iter()
        .any(|text| text == DAVE_LATE);
    checks.expect(
        "a client that stops reading is dropped, reader included: what it sends then \
         never enters the room",
        dropped && !late,
    );
    checks.expect(
        "with a client that reads nothing, the server stops within its grace",
        matches!(stopped, Ok(Ok(()))),
    );
    Ok(())
}

/// What a client is told. The model unreachable, Frank's message gets an
/// error frame; Grace's message over the limit closes her connection with
/// 1009 (too large), and so does Henry's frame announced over it, before
/// its body; the server's stop closes Frank's with 1001 (going away).
async fn what_a_client_is_told(checks: &mut Checks) -> Result<(), Box<dyn Error>> {
    println!("The model is unreachable.");
    let endpoint = unreachable_model().await?;
    let database = TempDatabase::new("server_agent_cellar");
    let server =
        AgentServer::start(database.path(), &endpoint, ANY_PORT.parse()?, CHECKING).await?;
    let mut frank = Client::connect(server.address(), CELLAR, "frank", "Frank").await?;
    frank.say("Is anyone there?").await?;
    let told = frank.next_frame(TOLD_PATIENCE).await;
    println!("  Frank receives {told:?}");
    let mut grace = Client::connect(server.address(), CELLAR, "grace", "Grace").await?;
    println!("Grace sends a message one byte over the limit.");
    grace.send("a".repeat(MESSAGE_LIMIT + 1)).await?;
    let too_large = grace.close_code(TOLD_PATIENCE).await;
    let mut henry = Client::connect(server.address(), CELLAR, "henry", "Henry").await?;
    println!("Henry announces a frame of {ANNOUNCED} bytes and sends its first KiB.");
    henry.announce(ANNOUNCED).await?;
    let refused_at_header = henry.close_code(TOLD_PATIENCE).await;
    tokio::time::timeout(STOP_PATIENCE, server.stop()).await??;
    let going_away = frank.close_code(TOLD_PATIENCE).await;

    checks.expect(
        "a turn that fails tells its client, with a reason that holds no secret",
        told.as_ref()
            .and_then(|frame| frame.get("error"))
            .and_then(Value::as_str)
            == Some(TURN_FAILED),
    );
    checks.expect(
        "a message over the limit closes its connection with 1009, too large",
        too_large == Some(1009),
    );
    checks.expect(
        "a frame announced over the limit is refused at its header, before its body",
        refused_at_header == Some(1009),
    );
    checks.expect(
        "a stop closes the connections with 1001, going away",
        going_away == Some(1001),
    );
    Ok(())
}

/// What a connection must name: its conversation and user plainly, its
/// name without control characters, each within 64 characters. Each
/// connection here names one thing wrong, and is refused with 400.
async fn what_a_connection_must_name(checks: &mut Checks) -> Result<(), Box<dyn Error>> {
    println!("Connections that name something wrong.");
    let endpoint = OpenAiEndpoint::Fake(FakeOpenAi::start(ANSWERS).await?);
    let database = TempDatabase::new("server_agent_names");
    let server =
        AgentServer::start(database.path(), &endpoint, ANY_PORT.parse()?, CHECKING).await?;
    let long = "a".repeat(65);
    let mut refused = Vec::new();
    for (what, path) in [
        (
            "a user with a space",
            "/conversations/bakery?user=al%20ice".to_owned(),
        ),
        (
            "a user with a slash",
            "/conversations/bakery?user=al%2Fice".to_owned(),
        ),
        ("no user", "/conversations/bakery".to_owned()),
        (
            "a conversation of 65 characters",
            format!("/conversations/{long}?user=alice"),
        ),
        (
            "a conversation with a dot",
            "/conversations/bak.ery?user=alice".to_owned(),
        ),
        (
            "a name with a bell",
            "/conversations/bakery?user=alice&name=Al%07ice".to_owned(),
        ),
        (
            "a name with a line break",
            "/conversations/bakery?user=alice&name=Al%0Aice".to_owned(),
        ),
        (
            "a name of 65 characters",
            format!("/conversations/bakery?user=alice&name={long}"),
        ),
    ] {
        let status = Client::refused(server.address(), &path).await?;
        println!("  {what}: {status:?}");
        refused.push(status);
    }
    tokio::time::timeout(STOP_PATIENCE, server.stop()).await??;
    checks.expect(
        "a conversation or user other than 1 to 64 letters, digits, _ or -, or a name over \
         64 characters or with a control character, is refused with 400",
        refused.iter().all(|status| *status == Some(400)),
    );
    Ok(())
}

/// Ivan opens the first connection to the porch's conversation and leaves
/// while his join makes its room, which a hook of the check's holds until
/// the server dropped his request. Judy then joins the same conversation,
/// and the agent answers her: the room was made whole.
async fn a_client_leaves_while_it_joins(checks: &mut Checks) -> Result<(), Box<dyn Error>> {
    println!("Ivan leaves while his join makes the porch's room.");
    let endpoint = OpenAiEndpoint::Fake(FakeOpenAi::start(ANSWERS).await?);
    let database = TempDatabase::new("server_agent_porch");
    let server =
        AgentServer::start(database.path(), &endpoint, ANY_PORT.parse()?, CHECKING).await?;
    let (created, mut creating) = mpsc::channel(1);
    let (release, released) = watch::channel(false);
    server
        .kit()
        .observe("hold_the_porch", HookTrigger::OnRoomCreated, move |_, _| {
            let (created, mut released) = (created.clone(), released.clone());
            async move {
                // The check waits for one, and the release outlives the hook.
                let _told = created.send(()).await;
                let _released = released.wait_for(|go| *go).await;
            }
        })?;
    let mut ivan = TcpStream::connect(server.address()).await?;
    let request = Client::upgrade_request(
        server.address(),
        &format!("/conversations/{PORCH}?user=ivan"),
    );
    ivan.write_all(request.as_bytes()).await?;
    let held = tokio::time::timeout(STOP_PATIENCE, creating.recv())
        .await
        .is_ok();
    drop(ivan);
    let dropped = tokio::time::timeout(STOP_PATIENCE, server.connections_down_to(0))
        .await
        .is_ok();
    println!("  the room's making held: {held}; Ivan's request dropped: {dropped}");
    release.send_replace(true);
    let mut judy = Client::connect(server.address(), PORCH, "judy", "Judy").await?;
    judy.say("Is the porch open?").await?;
    let answer = judy.next_frame(TOLD_PATIENCE).await;
    println!("  Judy receives {answer:?}");
    judy.close().await;
    tokio::time::timeout(STOP_PATIENCE, server.stop()).await??;
    checks.expect(
        "a client that leaves while its join makes a new room leaves it whole: the next \
         one is answered",
        held && dropped
            && answer
                .as_ref()
                .and_then(|frame| frame.get("type"))
                .and_then(Value::as_str)
                == Some("message"),
    );
    Ok(())
}

/// Kim asks while the model takes longer to answer than the server's
/// grace: the stop ends the turn, and Kim is told the server stopped
/// before the answer, then closed with 1001.
async fn a_stop_during_a_turn(checks: &mut Checks) -> Result<(), Box<dyn Error>> {
    println!("The model takes longer to answer than the server's grace.");
    let (endpoint, silent) = silent_model().await?;
    let database = TempDatabase::new("server_agent_garden");
    let brief = Patience {
        grace: Duration::from_millis(200),
        ..CHECKING
    };
    let server = AgentServer::start(database.path(), &endpoint, ANY_PORT.parse()?, brief).await?;
    let mut kim = Client::connect(server.address(), GARDEN, "kim", "Kim").await?;
    kim.say("Are the tables outside free?").await?;
    // Once the turn's request reached the model, which never answers.
    let _asked = tokio::time::timeout(STOP_PATIENCE, silent.accept()).await??;
    // The turn outlives the grace: the stop says so.
    let _outlived = tokio::time::timeout(STOP_PATIENCE, server.stop()).await;
    let told = kim.next_frame(TOLD_PATIENCE).await;
    println!("  Kim receives {told:?}");
    let going_away = kim.close_code(TOLD_PATIENCE).await;
    checks.expect(
        "a stop during a turn tells its client the server stopped before the answer, then \
         closes with 1001",
        told.as_ref()
            .and_then(|frame| frame.get("error"))
            .and_then(Value::as_str)
            == Some(STOPPED)
            && going_away == Some(1001),
    );
    Ok(())
}

/// Lee asks while the model never answers, then closes the connection:
/// the server reads on while the agent answers, its message's delivery
/// deferred, so the close is answered at once, not once the turn ends.
async fn a_close_during_a_turn(checks: &mut Checks) -> Result<(), Box<dyn Error>> {
    println!("Lee closes the connection while the model has not answered.");
    let (endpoint, silent) = silent_model().await?;
    let database = TempDatabase::new("server_agent_orchard");
    let brief = Patience {
        grace: Duration::from_millis(200),
        ..CHECKING
    };
    let server = AgentServer::start(database.path(), &endpoint, ANY_PORT.parse()?, brief).await?;
    let mut lee = Client::connect(server.address(), ORCHARD, "lee", "Lee").await?;
    lee.say("Are the apples ripe?").await?;
    let asked = tokio::time::timeout(STOP_PATIENCE, silent.accept()).await??;
    let answered = lee.close_answered(TOLD_PATIENCE).await;
    // The model goes away: the turn fails, and the stop waits for nothing.
    drop((asked, silent));
    tokio::time::timeout(STOP_PATIENCE, server.stop()).await??;
    checks.expect(
        "a client that closes during a turn has its close answered at once: the server \
         reads on while the agent answers",
        answered,
    );
    Ok(())
}

/// An endpoint that takes connections and never answers, open while the
/// listener it returns is held.
async fn silent_model() -> Result<(OpenAiEndpoint, TcpListener), Box<dyn Error>> {
    let listener = TcpListener::bind(ANY_PORT).await?;
    let endpoint = OpenAiEndpoint::Real {
        base_url: format!("http://{}/v1", listener.local_addr()?),
        api_key: String::new(),
        model: "silent".to_owned(),
    };
    Ok((endpoint, listener))
}

/// An endpoint where nothing listens: a port the system gave and took
/// back.
async fn unreachable_model() -> Result<OpenAiEndpoint, Box<dyn Error>> {
    let address = TcpListener::bind(ANY_PORT).await?.local_addr()?;
    Ok(OpenAiEndpoint::Real {
        base_url: format!("http://{address}/v1"),
        api_key: String::new(),
        model: "unreachable".to_owned(),
    })
}

/// The timeline of the room `conversation`, as the file holds it once the
/// servers are gone.
async fn timeline_in(
    database: &Path,
    conversation: &str,
) -> Result<Vec<RoomEvent>, Box<dyn Error>> {
    let store = SqliteStore::open(database).await?;
    Ok(store
        .list_events(&RoomId::new(conversation)?, &every_row())
        .await?)
}

/// The texts of a timeline's messages.
fn messages_of(timeline: &[RoomEvent]) -> Vec<String> {
    timeline
        .iter()
        .filter(|event| event.event_type == EventType::Message)
        .map(|event| text_of(&event.content))
        .collect()
}

/// The checks of what the agent sent the fake, turn after turn, and of the
/// answers the sockets received from it.
fn check_the_model_s_turns(checks: &mut Checks, requests: &[Value], answers: &[&Option<String>]) {
    // Alice's turn, Bob's two rounds, Carol's, then the turn after the
    // restart.
    let after_the_restart = requests.get(4);
    println!("The agent's turn after the restart sent:");
    for message in after_the_restart.map(messages).unwrap_or_default() {
        println!("  {message}");
    }
    checks.expect(
        "the answers are the model's",
        answers.len() == ANSWERS.len()
            && answers
                .iter()
                .zip(ANSWERS)
                .all(|(answer, reply)| answer.as_deref() == Some(*reply)),
    );
    checks.expect(
        "the lookup's round declares the tool, and its result reaches the model's next round",
        requests.get(1).is_some_and(|request| {
            request.pointer("/tools/0/function/name") == Some(&json!("lookup_order"))
        }) && requests.get(2).is_some_and(|request| {
            messages(request).iter().any(|message| {
                message["role"] == "tool"
                    && message["tool_call_id"] == LOOKUP_CALL
                    && message["content"] == ORDER_42
            })
        }),
    );
    checks.expect(
        "the first turn labels Alice's message: two people take part, Bob silent",
        requests.first().is_some_and(|request| {
            matches!(sent(request).as_slice(), [system, alice]
                if is_system(system) && is_labelled(alice, "Alice", ALICE_ASKS))
        }),
    );
    checks.expect(
        "the turn after the restart reads the conversation from the file, each person \
         labelled as before, and replays no call",
        after_the_restart.is_some_and(goes_on),
    );
}

/// A person's WebSocket client in one of the checks' conversations.
struct Client {
    name: &'static str,
    socket: WebSocketStream<TcpStream>,
}

impl Client {
    /// `name`'s connection to `conversation` on the server at `address`, as
    /// the participant `user`.
    async fn connect(
        address: SocketAddr,
        conversation: &str,
        user: &str,
        name: &'static str,
    ) -> Result<Self, Box<dyn Error>> {
        Self::open(address, conversation, user, name, TcpSocket::new_v4()?).await
    }

    /// The same connection, asking for the agent's answers as they are
    /// written (`&stream=1`).
    async fn connect_streaming(
        address: SocketAddr,
        conversation: &str,
        user: &str,
        name: &'static str,
    ) -> Result<Self, Box<dyn Error>> {
        let conversation = format!("{conversation}?user={user}&name={name}&stream=1");
        Self::open_at(address, &conversation, name, TcpSocket::new_v4()?).await
    }

    /// The same connection, whose socket takes in a few KiB at most before
    /// the server must wait for it to read: a client that falls behind
    /// quickly.
    async fn connect_reading_little(
        address: SocketAddr,
        conversation: &str,
        user: &str,
        name: &'static str,
    ) -> Result<Self, Box<dyn Error>> {
        let socket = TcpSocket::new_v4()?;
        socket.set_recv_buffer_size(SMALL_BUFFER)?;
        Self::open(address, conversation, user, name, socket).await
    }

    async fn open(
        address: SocketAddr,
        conversation: &str,
        user: &str,
        name: &'static str,
        socket: TcpSocket,
    ) -> Result<Self, Box<dyn Error>> {
        let path = format!("{conversation}?user={user}&name={name}");
        Self::open_at(address, &path, name, socket).await
    }

    /// `name`'s connection to the conversation `path` names, its query
    /// included.
    async fn open_at(
        address: SocketAddr,
        path: &str,
        name: &'static str,
        socket: TcpSocket,
    ) -> Result<Self, Box<dyn Error>> {
        let stream = socket.connect(address).await?;
        let url = format!("ws://{address}/conversations/{path}");
        let (socket, _response) = tokio_tungstenite::client_async(url, stream).await?;
        let conversation = path.split('?').next().unwrap_or_default();
        println!("{name} connects to {conversation}.");
        Ok(Self { name, socket })
    }

    /// Sends `text`.
    async fn say(&mut self, text: &str) -> Result<(), Box<dyn Error>> {
        println!("{} says {text:?}", self.name);
        self.socket.send(tungstenite::Message::text(text)).await?;
        Ok(())
    }

    /// The HTTP status the server at `address` refuses the upgrade to
    /// `path` with, `None` when it accepts it.
    async fn refused(address: SocketAddr, path: &str) -> Result<Option<u16>, Box<dyn Error>> {
        let stream = TcpStream::connect(address).await?;
        match tokio_tungstenite::client_async(format!("ws://{address}{path}"), stream).await {
            Ok(_accepted) => Ok(None),
            Err(tungstenite::Error::Http(response)) => Ok(Some(response.status().as_u16())),
            Err(error) => Err(error.into()),
        }
    }

    /// The HTTP request that asks the server at `address` to upgrade
    /// `path` to a WebSocket, as a client writes it.
    fn upgrade_request(address: SocketAddr, path: &str) -> String {
        format!(
            "GET {path} HTTP/1.1\r\nHost: {address}\r\nUpgrade: websocket\r\n\
             Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
             Sec-WebSocket-Version: 13\r\n\r\n"
        )
    }

    /// Sends `text` without printing it: a large one.
    async fn send(&mut self, text: String) -> Result<(), Box<dyn Error>> {
        self.socket.send(tungstenite::Message::text(text)).await?;
        Ok(())
    }

    /// Writes, under the WebSocket, the header of a text frame of `length`
    /// bytes, then its first KiB, and nothing more.
    async fn announce(&mut self, length: u64) -> Result<(), Box<dyn Error>> {
        // FIN and text; masked, a 64-bit length, the mask 0 leaving the
        // payload as it is.
        let mut frame = vec![0x81, 0x80 | 127];
        frame.extend_from_slice(&length.to_be_bytes());
        frame.extend_from_slice(&[0; 4]);
        frame.extend_from_slice(&[b'a'; 1024]);
        self.socket.get_mut().write_all(&frame).await?;
        Ok(())
    }

    /// The next JSON frame the server sends, if one comes within
    /// `patience`.
    async fn next_frame(&mut self, patience: Duration) -> Option<Value> {
        let next = async {
            while let Some(Ok(received)) = self.socket.next().await {
                if let tungstenite::Message::Text(frame) = received {
                    return serde_json::from_str(frame.as_str()).ok();
                }
            }
            None
        };
        tokio::time::timeout(patience, next).await.ok().flatten()
    }

    /// The code of the close frame the server sends, past any other frame;
    /// `None` when the connection ends without one, or none comes within
    /// `patience`.
    async fn close_code(&mut self, patience: Duration) -> Option<u16> {
        let closed = async {
            while let Some(Ok(received)) = self.socket.next().await {
                if let tungstenite::Message::Close(frame) = received {
                    return frame.map(|frame| u16::from(frame.code));
                }
            }
            None
        };
        let code = tokio::time::timeout(patience, closed).await.ok().flatten();
        println!("  {} is closed with the code {code:?}", self.name);
        code
    }

    /// The frames of the next turn the room delivers, up to the message
    /// that ends it (`ended`), as many as come in time.
    async fn turn(&mut self) -> Vec<Value> {
        let mut frames = Vec::new();
        let reading = async {
            while let Some(Ok(received)) = self.socket.next().await {
                let tungstenite::Message::Text(frame) = received else {
                    continue;
                };
                let frame: Value = serde_json::from_str(frame.as_str()).unwrap_or_default();
                let ends = frame.get("ended").is_some();
                frames.push(frame);
                if ends {
                    break;
                }
            }
        };
        let _in_time = tokio::time::timeout(ANSWER_PATIENCE, reading).await;
        for frame in &frames {
            let shown = frame
                .pointer("/tool/name")
                .and_then(Value::as_str)
                .map_or_else(
                    || format!("{:?}", frame["text"].as_str().unwrap_or_default()),
                    |tool| {
                        format!(
                            "{tool} {}",
                            frame.pointer("/tool/status").unwrap_or(&Value::Null)
                        )
                    },
                );
            println!(
                "  {} receives #{} {} {shown}",
                self.name, frame["index"], frame["type"]
            );
        }
        frames
    }

    /// The text of the message that ends the next turn the room delivers,
    /// if it comes in time.
    async fn answer(&mut self) -> Option<String> {
        let frames = self.turn().await;
        frames
            .last()
            .filter(|frame| frame.get("ended").is_some())
            .and_then(|frame| frame["text"].as_str())
            .map(str::to_owned)
    }

    /// Leaves at once, as a closed browser tab does: the connection is
    /// dropped, with no close handshake.
    fn leave(self) {
        drop(self.socket);
    }

    /// Closes the connection, and tells whether the server answered the
    /// close within `patience`.
    async fn close_answered(mut self, patience: Duration) -> bool {
        let _closing = self.socket.close(None).await;
        let answered = async {
            while let Some(Ok(received)) = self.socket.next().await {
                if received.is_close() {
                    break;
                }
            }
        };
        let answered = tokio::time::timeout(patience, answered).await.is_ok();
        println!("  {}'s close is answered: {answered}", self.name);
        answered
    }

    /// Closes the connection, and waits for the server to close its side.
    async fn close(mut self) {
        // A connection the server closed first is closed already.
        let _closed = self.socket.close(None).await;
        let drained = async { while self.socket.next().await.is_some() {} };
        let _drained = tokio::time::timeout(ANSWER_PATIENCE, drained).await;
    }
}

/// The role and text of each message a Chat Completions request sent.
fn sent(request: &Value) -> Vec<(String, String)> {
    messages(request)
        .iter()
        .map(|message| {
            let field = |key: &str| message[key].as_str().unwrap_or_default().to_owned();
            (field("role"), field("content"))
        })
        .collect()
}

/// The messages of a Chat Completions request.
fn messages(request: &Value) -> Vec<Value> {
    request["messages"].as_array().cloned().unwrap_or_default()
}

fn is_system(message: &(String, String)) -> bool {
    message == &("system".to_owned(), SYSTEM_PROMPT.to_owned())
}

/// Whether `message` is the user's `text`, labelled `label` (RFC §6.4).
fn is_labelled(message: &(String, String), label: &str, text: &str) -> bool {
    message.0 == "user" && message.1.starts_with(&format!("{label}: {text:?}"))
}

/// Whether the turn after the restart sent the system prompt, then the
/// conversation before it: Alice's question, the first answer, Bob's, what
/// the agent said before its lookup and its answer, each a message of its
/// own, and Alice's new question, labelled `Alice` as before rather than as
/// a second Alice; and no call nor result of the lookup.
fn goes_on(request: &Value) -> bool {
    let sent = sent(request);
    let [system, alice, first, bob, looking, second, again] = sent.as_slice() else {
        return false;
    };
    let answered = |message: &(String, String), reply: &str| {
        message == &("assistant".to_owned(), reply.to_owned())
    };
    let replays_no_call = messages(request)
        .iter()
        .all(|message| message["role"] != "tool" && message.get("tool_calls").is_none());
    is_system(system)
        && is_labelled(alice, "Alice", ALICE_ASKS)
        && ANSWERS.first().is_some_and(|reply| answered(first, reply))
        && is_labelled(bob, "Bob", BOB_ASKS)
        && answered(looking, LOOKING_UP)
        && ANSWERS.get(1).is_some_and(|reply| answered(second, reply))
        && is_labelled(again, "Alice", ALICE_AGAIN)
        && replays_no_call
}
