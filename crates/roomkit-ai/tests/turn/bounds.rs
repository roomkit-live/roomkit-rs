//! The loop's bounds of Appendix A.9 (RFC §6.4): its deadline, read between
//! rounds, and the tries a round with no call to run gets, as the reference
//! implementation applies them (`tasks/probes/step5-lot5/py_loop_bounds.py`
//! on roomkit 38ef0625).

use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{EventType, RoomEvent};
use roomkit::testing::TurnTap;
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiMessage, AiProvider, AiResponse, ProviderError,
};
use serde_json::{Value, json};

use super::support::{Room, channel, served};

/// What a model whose round of calls was followed by an empty answer reads.
const EMPTY_RETRY: &str = "You called tools and already have their results above. Now write \
your final answer to the user in plain text. Do not call any more tools.";

/// What a model whose call its provider could not parse reads.
const MALFORMED: &str = "Your last tool call could not be parsed, so it did not run. Call the \
tool again with arguments that are valid JSON matching its parameters, or answer in plain text.";

/// What a model that called a tool its request did not enable reads.
const UNEXPECTED: &str = "The tool you called is not available in this request, so it did not \
run. Use one of the declared tools, or answer in plain text.";

/// A provider whose every generation takes `delay`.
struct Slow {
    model: ScriptedProvider,
    delay: Duration,
}

impl AiProvider for Slow {
    fn model_name(&self) -> &'static str {
        "slow"
    }

    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        tokio::time::sleep(self.delay).await;
        self.model.generate(context).await
    }
}

/// The loop's end as the turn's wire carries it: why, after how many rounds
/// of calls, under which deadline.
fn loop_end(tap: &TurnTap) -> (Value, Value, Value) {
    tap.messages()
        .into_iter()
        .filter_map(|message| serde_json::from_str::<Value>(&message.json).ok())
        .find_map(|message| {
            let item = message.pointer("/message/item")?;
            (item["kind"] == "loop_end").then(|| {
                (
                    item["reason"].clone(),
                    item["rounds"].clone(),
                    item["timeout_seconds"].clone(),
                )
            })
        })
        .unwrap()
}

/// The texts of the last two messages each request of `model` carried.
fn tails(model: &ScriptedProvider) -> Vec<Vec<String>> {
    model
        .requests()
        .iter()
        .map(|request| {
            let mut texts: Vec<String> = request.messages.iter().map(AiMessage::text).collect();
            let start = texts.len().saturating_sub(2);
            texts.split_off(start)
        })
        .collect()
}

/// Whether any call of the turn started.
fn a_call_started(timeline: &[RoomEvent]) -> bool {
    timeline
        .iter()
        .any(|event| event.r#type == EventType::ToolCallStart)
}

fn call() -> ScriptedRound {
    ScriptedRound::calling("", [("c1", "lookup_order", r#"{"order": "1"}"#)])
}

/// RFC §6.4, Appendix A.9: an empty answer after a round of calls is asked
/// again, the model told it has its results, within `max_empty_retries`
/// tries; a response that ran out of room is not, nor an empty answer with
/// no round before it; a try takes a round's place, the last one included.
#[tokio::test]
async fn an_empty_answer_after_a_round_is_asked_again_within_its_tries() {
    // The tries, the round bound, the script; why the loop ended, after how
    // many rounds of calls, how many requests went, how many read the word.
    let cases = [
        (
            1,
            50,
            vec![
                call(),
                ScriptedRound::answering(""),
                ScriptedRound::answering("Recovered"),
            ],
            ("completed", 1, 3, 1),
        ),
        (
            0,
            50,
            vec![call(), ScriptedRound::answering("")],
            ("empty_response", 1, 2, 0),
        ),
        (
            1,
            50,
            vec![
                call(),
                ScriptedRound::answering(""),
                ScriptedRound::answering(""),
            ],
            ("empty_response", 1, 3, 1),
        ),
        (
            1,
            50,
            vec![
                call(),
                ScriptedRound::answering("").with_finish_reason("length"),
            ],
            ("truncated", 1, 2, 0),
        ),
        (
            2,
            50,
            vec![ScriptedRound::answering("")],
            ("completed", 0, 1, 0),
        ),
        (
            1,
            1,
            vec![
                call(),
                ScriptedRound::answering(""),
                ScriptedRound::answering("late"),
            ],
            ("max_rounds", 1, 2, 0),
        ),
    ];
    for (tries, max_rounds, rounds, (reason, ran, requests, nudges)) in cases {
        let model = ScriptedProvider::new(rounds);
        let room = Room::equipped(model.clone(), served(), |options| AiChannelOptions {
            max_empty_retries: tries,
            max_tool_rounds: max_rounds,
            ..options
        })
        .await;
        let tap = TurnTap::on(&room.kit);
        room.says("Order 1?").await;
        let nudged = tails(&model)
            .iter()
            .filter(|tail| tail.last().map(String::as_str) == Some(EMPTY_RETRY))
            .count();
        let (ended, after, _) = loop_end(&tap);
        assert_eq!(
            (ended, after, model.requests().len(), nudged),
            (json!(reason), json!(ran), requests, nudges),
            "{reason}"
        );
        room.kit.close().await.unwrap();
    }
}

/// RFC §6.4: a call its provider did not hand over is no answer, on any
/// round: the model is told it did not run, what it said kept before the
/// word to it, and asked again.
#[tokio::test]
async fn a_call_the_provider_did_not_hand_over_is_told_and_asked_again() {
    for (finish_reason, told) in [
        ("MALFORMED_FUNCTION_CALL", MALFORMED),
        ("unexpected_tool_call", UNEXPECTED),
    ] {
        let model = ScriptedProvider::new([
            ScriptedRound::answering("Let me check.").with_finish_reason(finish_reason),
            ScriptedRound::answering("Done."),
        ]);
        let room = Room::new(model.clone(), served()).await;
        let tap = TurnTap::on(&room.kit);
        room.says("Order 1?").await;
        assert_eq!(
            (loop_end(&tap).0, tails(&model).last().cloned()),
            (
                json!("completed"),
                Some(vec!["Let me check.".to_owned(), told.to_owned()])
            ),
            "{finish_reason}"
        );
        room.kit.close().await.unwrap();
    }
}

/// Appendix A.9: the loop's deadline is read between rounds: past it, the
/// loop ends `timeout` before the round's calls run, and an empty answer is
/// not asked again; a zero deadline is none.
#[tokio::test(start_paused = true)]
async fn the_deadline_ends_the_loop_between_rounds() {
    let cases = [
        (
            10,
            vec![call(), ScriptedRound::answering("never")],
            (json!("timeout"), json!(0), json!(0.01)),
            false,
            1,
        ),
        (
            0,
            vec![call(), ScriptedRound::answering("Shipped.")],
            (json!("completed"), json!(1), Value::Null),
            true,
            2,
        ),
        (
            70,
            vec![
                call(),
                ScriptedRound::answering(""),
                ScriptedRound::answering("x"),
            ],
            (json!("timeout"), json!(1), json!(0.07)),
            true,
            2,
        ),
    ];
    for (millis, rounds, end, started, requests) in cases {
        let model = ScriptedProvider::new(rounds);
        let slow = Slow {
            model: model.clone(),
            delay: Duration::from_millis(50),
        };
        let options = AiChannelOptions {
            tools: served().into(),
            tool_loop_timeout: Some(Duration::from_millis(millis)),
            ..AiChannelOptions::default()
        };
        let agent = AiChannel::new(channel("assistant"), slow, options).unwrap();
        let room = Room::with_agent(agent).await;
        let tap = TurnTap::on(&room.kit);
        room.says("Order 1?").await;
        assert_eq!(
            (
                loop_end(&tap),
                a_call_started(&room.timeline().await),
                model.requests().len()
            ),
            (end, started, requests),
            "{millis} ms"
        );
        room.kit.close().await.unwrap();
    }
}
