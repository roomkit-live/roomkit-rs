//! An agent's earlier rounds of tool calls in its later turns' history
//! (RFC §6.4 "The tools of earlier turns"), through the kit: each round
//! reads at its place, as the assistant's calls then their results, the
//! way the turn itself sent it.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use roomkit::model::{EventType, HookResult, Metadata};
use roomkit::transcript::{Message, Role};
use roomkit::{HookExecution, HookSpec, HookTrigger, ToolVerdict};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiAgent, AiProvider, AiRequest, Done, ProviderResponse, ProviderStream, ProviderToolCall,
    ToolDefinition, ToolError, Tools,
};
use serde_json::json;

use super::support::{Room, channel, served};

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
