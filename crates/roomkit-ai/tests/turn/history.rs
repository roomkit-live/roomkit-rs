//! An agent's earlier rounds of tool calls in its later turns' history
//! (RFC §6.4 "The tools of earlier turns"), through the kit: each round
//! reads at its place, as the assistant's calls then their results, the
//! way the turn itself sent it.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use roomkit::model::{EventContent, EventType, HookResult, Metadata};
use roomkit::transcript::{Message, Role};
use roomkit::{HookExecution, HookSpec, HookTrigger, ToolVerdict, UpdateEventOptions};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiAgent, AiProvider, AiRequest, Done, ProviderResponse, ProviderStream, ProviderToolCall,
    ToolDefinition, ToolError, Tools,
};
use serde_json::json;

use super::support::{Room, channel, room_id, served};

/// A model that does not stream: each generation hands its whole
/// response at once (RFC §6.4 "One loop, streamed or not").
#[derive(Clone, Default)]
struct Whole {
    script: Arc<Mutex<VecDeque<ProviderResponse>>>,
    requests: Arc<Mutex<Vec<AiRequest>>>,
}

impl Whole {
    fn new(responses: impl IntoIterator<Item = ProviderResponse>) -> Self {
        Self {
            script: Arc::new(Mutex::new(responses.into_iter().collect())),
            requests: Arc::default(),
        }
    }
}

impl AiProvider for Whole {
    fn generate<'a>(&'a self, request: &'a AiRequest) -> ProviderStream<'a> {
        self.requests.lock().unwrap().push(request.clone());
        let next = self.script.lock().unwrap().pop_front();
        ProviderResponse::into_stream(async move { next.ok_or_else(|| "no response left".into()) })
    }
}

/// `lookup_order`, served, and `fail`, which raises.
fn tools() -> Tools {
    let nothing = json!({ "type": "object", "properties": {} });
    served().tool(
        ToolDefinition::new("fail", "Fails", nothing),
        |_call, _context| async { Err(ToolError::raised_as("RuntimeError", "broken")) },
    )
}

/// The first turn's rounds: `lookup_order` and `fail` at once, then
/// `lookup_order` again, then the answer; then the second turn's answer.
const ROUNDS: [&[(&str, &str, &str)]; 2] = [
    &[
        ("c1", "lookup_order", r#"{"order": "42"}"#),
        ("c2", "fail", "{}"),
    ],
    &[("c3", "lookup_order", r#"{"order": "43"}"#)],
];

/// What the model was asked, generation by generation, when Alice asks
/// twice in a room whose agent answers through `agent`.
async fn two_turns<P: AiProvider>(agent: AiAgent<P>) {
    let room = Room::with_agent(agent.with_tools(tools()).unwrap()).await;
    room.says("first question").await;
    room.says("second question").await;
    room.kit.close().await;
}

/// The checks on what the model read: the second turn reads the first
/// turn's rounds between its question and its answer, word for word as the
/// first turn sent them to its last round.
fn check(requests: &[AiRequest]) {
    assert_eq!(requests.len(), 4);
    let first_turn_last_round = &requests.get(2).unwrap().messages;
    let second_turn = &requests.get(3).unwrap().messages;
    let roles: Vec<(Role, usize)> = second_turn
        .iter()
        .map(|message| (message.role, message.tool_calls.len()))
        .collect();
    assert_eq!(
        roles,
        [
            (Role::User, 0),
            (Role::Assistant, 2),
            (Role::Tool, 0),
            (Role::Tool, 0),
            (Role::Assistant, 1),
            (Role::Tool, 0),
            (Role::Assistant, 0),
            (Role::User, 0),
        ]
    );
    assert_eq!(second_turn.get(..6), first_turn_last_round.get(..6));
    assert_eq!(
        second_turn.get(6..),
        Some(
            &[
                Message::new(Role::Assistant, "Answer one."),
                Message::new(Role::User, "second question")
            ][..]
        )
    );
    assert_eq!(
        second_turn.get(3),
        Some(&Message::tool_result(
            "c2",
            r#"{"error": "Tool 'fail' failed (RuntimeError)"}"#
        ))
    );
}

/// RFC §6.4: a second turn's history carries the first turn's calls and
/// results as tool messages, from a model that streams its text.
#[tokio::test]
async fn an_earlier_round_reads_as_its_calls_then_their_results_streamed() {
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", ROUNDS[0].iter().copied()),
        ScriptedRound::calling("", ROUNDS[1].iter().copied()),
        ScriptedRound::answering("Answer one.").in_pieces(["Answer ", "one."]),
        ScriptedRound::answering("Answer two.").in_pieces(["Answer ", "two."]),
    ]);
    two_turns(AiAgent::new(channel("assistant"), model.clone())).await;
    check(&model.requests());
}

/// The same, from a model that hands each response whole.
#[tokio::test]
async fn an_earlier_round_reads_as_its_calls_then_their_results_whole() {
    let calling = |calls: &[(&str, &str, &str)]| {
        let calls = calls
            .iter()
            .map(|(id, name, arguments)| ProviderToolCall::new(*id, *name, *arguments))
            .collect();
        let done = Done::new(Some("tool_calls".to_owned()), Metadata::new());
        ProviderResponse::new("", calls, done)
    };
    let answering = |text: &str| {
        let done = Done::new(Some("stop".to_owned()), Metadata::new());
        ProviderResponse::new(text, Vec::new(), done)
    };
    let model = Whole::new([
        calling(ROUNDS[0]),
        calling(ROUNDS[1]),
        answering("Answer one."),
        answering("Answer two."),
    ]);
    two_turns(AiAgent::new(channel("assistant"), model.clone())).await;
    check(&model.requests.lock().unwrap());
}

/// RFC §6.4: a round replays the result the model read, an `ON_TOOL_CALL`
/// hook's rewrite, never what the tool returned.
#[tokio::test]
async fn a_hook_s_rewrite_is_what_is_replayed() {
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "lookup_order", r#"{"order": "1"}"#)]),
        ScriptedRound::answering("One."),
        ScriptedRound::answering("Two."),
    ]);
    let room = Room::new(model.clone(), served()).await;
    room.kit
        .on_tool_call("rewrite", |_event, _context| async {
            Ok(ToolVerdict::Replace(json!("as the hook rewrote it")))
        })
        .unwrap();
    room.says("first").await;
    room.says("second").await;
    room.kit.close().await;
    let read: Vec<Vec<String>> = model
        .requests()
        .iter()
        .skip(1)
        .map(|request| {
            request
                .messages
                .iter()
                .filter(|message| message.role == Role::Tool)
                .map(|message| message.text.clone())
                .collect()
        })
        .collect();
    assert_eq!(
        read,
        [
            vec!["as the hook rewrote it".to_owned()],
            vec!["as the hook rewrote it".to_owned()]
        ]
    );
}

/// Each message of `request`, on one line: its role and text, the ids it
/// calls, or the call its result answers.
fn shape(request: &AiRequest) -> Vec<String> {
    request
        .messages
        .iter()
        .map(|message| match message.role {
            Role::Assistant if !message.tool_calls.is_empty() => {
                let ids: Vec<&str> = message
                    .tool_calls
                    .iter()
                    .map(|call| call.id.as_str())
                    .collect();
                format!("calls {}", ids.join(" "))
            }
            Role::Tool => format!(
                "result {} {}",
                message.tool_call_id.as_deref().unwrap_or_default(),
                message.text
            ),
            Role::User => format!("user {}", message.text),
            Role::Assistant | _ => format!("assistant {}", message.text),
        })
        .collect()
}

/// The lines `shape` gives a round calling `ids`, each served.
fn round_of(ids: &[String]) -> Vec<String> {
    let mut lines = vec![format!("calls {}", ids.join(" "))];
    lines.extend(ids.iter().map(|id| {
        let order = id.trim_start_matches(['c', 't']);
        format!(r#"result {id} {{"order":"{order}","status":"shipped"}}"#)
    }));
    lines
}

/// A round calling `ids`, each `lookup_order` of the order its id names.
fn calling_each(ids: &[String]) -> ScriptedRound {
    let arguments: Vec<String> = ids
        .iter()
        .map(|id| json!({ "order": id.trim_start_matches(['c', 't']) }).to_string())
        .collect();
    let calls: Vec<(&str, &str, &str)> = ids
        .iter()
        .zip(&arguments)
        .map(|(id, arguments)| (id.as_str(), "lookup_order", arguments.as_str()))
        .collect();
    ScriptedRound::calling("", calls)
}

/// RFC §6.4: a turn of 30 calls in one round leaves the next turn its
/// question, its last 8 calls and its answer, as the reference
/// implementation's history reads them: the room's latest 50 messages and
/// the agent's latest 60 tool rows, read apart (review probe `py_window.out`,
/// `heavy`). The room's latest 50 events of every kind held the answer
/// alone.
#[tokio::test]
async fn a_turn_of_thirty_calls_leaves_its_question_and_last_calls_to_the_next() {
    let ids: Vec<String> = (0..30).map(|n| format!("c{n}")).collect();
    let model = ScriptedProvider::new([
        calling_each(&ids),
        ScriptedRound::answering("Answer one."),
        ScriptedRound::answering("Answer two."),
        ScriptedRound::answering("Answer three."),
    ]);
    let room = Room::new(model.clone(), served()).await;
    for text in ["first question", "second question", "third question"] {
        room.says(text).await;
    }
    room.kit.close().await;
    let requests = model.requests();
    let mut second = vec!["user first question".to_owned()];
    second.extend(round_of(&ids[22..]));
    second.extend(["assistant Answer one.", "user second question"].map(str::to_owned));
    assert_eq!(shape(&requests[2]), second);
    let mut third = second;
    third.extend(["assistant Answer two.", "user third question"].map(str::to_owned));
    assert_eq!(shape(&requests[3]), third);
}

/// RFC §6.4: twelve turns of two calls each leave the thirteenth every
/// message, the room's 25, and the last 8 calls, at their place (review
/// probe `py_window.out`, `many`). The room's latest 50 events of every kind
/// opened on the fourth answer.
#[tokio::test]
async fn twelve_turns_of_two_calls_leave_every_message_and_the_last_calls() {
    let pair = |n: usize| vec![format!("t{n}a"), format!("t{n}b")];
    let mut script = Vec::new();
    for n in 0..12 {
        script.push(calling_each(&pair(n)));
        script.push(ScriptedRound::answering(format!("Answer {n}.")));
    }
    script.push(ScriptedRound::answering("Answer last."));
    let model = ScriptedProvider::new(script);
    let room = Room::new(model.clone(), served()).await;
    for n in 0..13 {
        room.says(&format!("question {n}")).await;
    }
    room.kit.close().await;
    let mut expected = Vec::new();
    for n in 0..12 {
        expected.push(format!("user question {n}"));
        if n >= 8 {
            expected.extend(round_of(&pair(n)));
        }
        expected.push(format!("assistant Answer {n}."));
    }
    expected.push("user question 12".to_owned());
    assert_eq!(shape(model.requests().last().unwrap()), expected);
}

/// The Python implementation's stamps (probe `py_turn_correlation.out`): every
/// row of a turn carries the turn's own `correlation_id`, 32 lowercase
/// hexadecimal digits, a fresh one per turn, never the one its trigger
/// carries, which the integrator set.
#[tokio::test]
async fn every_row_of_a_turn_carries_the_turn_s_own_correlation_id() {
    let model = ScriptedProvider::new([
        ScriptedRound::calling(
            "Let me check.",
            [("c1", "lookup_order", r#"{"order": "1"}"#)],
        ),
        ScriptedRound::answering("One."),
        ScriptedRound::calling("", [("c1", "lookup_order", r#"{"order": "2"}"#)]),
        ScriptedRound::answering("Two."),
    ]);
    let room = Room::new(model, served()).await;
    let spec = HookSpec::new(
        "reference",
        HookTrigger::BeforeBroadcast,
        HookExecution::Sync,
    );
    room.kit
        .hook(spec, |mut event, _context| async move {
            if event.source.channel_id != channel("sms") {
                return Ok(HookResult::allow());
            }
            event.correlation_id = Some("integrator-ref".to_owned());
            Ok(HookResult::modify(event))
        })
        .unwrap();
    room.says("first").await;
    room.says("second").await;
    room.kit.close().await;
    let timeline = room.timeline().await;
    let triggers: Vec<_> = timeline
        .iter()
        .filter(|event| event.source.channel_id == channel("sms"))
        .collect();
    let turns: Vec<Vec<Option<String>>> = triggers
        .iter()
        .map(|trigger| {
            timeline
                .iter()
                .filter(|row| row.responds_to.as_ref() == Some(&trigger.id))
                .map(|row| row.correlation_id.clone())
                .collect()
        })
        .collect();
    let kinds = |trigger: usize| {
        timeline
            .iter()
            .filter(|row| row.responds_to.as_ref() == Some(&triggers[trigger].id))
            .map(|row| row.event_type)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        (kinds(0), kinds(1)),
        (
            vec![
                EventType::Message,
                EventType::ToolCallStart,
                EventType::ToolCallEnd,
                EventType::Message
            ],
            vec![
                EventType::ToolCallStart,
                EventType::ToolCallEnd,
                EventType::Message
            ]
        )
    );
    let own: Vec<&str> = turns
        .iter()
        .map(|rows| {
            let first = rows[0].as_deref().unwrap();
            assert!(
                rows.iter().all(|row| row.as_deref() == Some(first)),
                "{rows:?}"
            );
            first
        })
        .collect();
    assert!(
        own.iter().all(|id| id.len() == 32
            && id
                .chars()
                .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))),
        "{own:?}"
    );
    assert_ne!(own[0], own[1]);
    assert_eq!(
        triggers
            .iter()
            .map(|trigger| trigger.correlation_id.as_deref())
            .collect::<Vec<_>>(),
        [Some("integrator-ref"); 2]
    );
}

/// A round of three calls whose middle start row the host then marks
/// deleted: the next turn, whose history hands that row with its content
/// withheld, replays the round as two, `c1` then `c3`, as the reference
/// implementation does (probe `py_deleted_start.out`; light review,
/// `probe_deleted_start_e2e.out`).
#[tokio::test]
async fn a_deleted_start_row_ends_its_round_in_the_next_turn() {
    let ids: Vec<String> = (1..=3).map(|n| format!("c{n}")).collect();
    let model = ScriptedProvider::new([
        calling_each(&ids),
        ScriptedRound::answering("Answer one."),
        ScriptedRound::answering("Answer two."),
    ]);
    let room = Room::new(model.clone(), served()).await;
    room.says("first question").await;
    let timeline = room.timeline().await;
    let start = timeline
        .iter()
        .find(|row| {
            row.event_type == EventType::ToolCallStart
                && matches!(&row.content, EventContent::ToolCall(call) if call.tool_id == "c2")
        })
        .unwrap();
    let mut metadata = start.metadata.clone();
    metadata.insert("deleted".to_owned(), json!(true));
    let options = UpdateEventOptions {
        metadata: Some(metadata),
        ..UpdateEventOptions::default()
    };
    room.kit
        .update_event(&room_id(), &start.id, options)
        .await
        .unwrap()
        .unwrap();
    room.says("second question").await;
    room.kit.close().await;
    let mut expected = vec!["user first question".to_owned()];
    expected.extend(round_of(&ids[..1]));
    expected.extend(round_of(&ids[2..]));
    expected.extend(["assistant Answer one.", "user second question"].map(str::to_owned));
    assert_eq!(shape(model.requests().last().unwrap()), expected);
}
