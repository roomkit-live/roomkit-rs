//! The loop's bounds of Appendix A.9 (RFC §6.4): its deadline, read between
//! rounds, and the tries a round with no call to run gets, as the reference
//! implementation applies them (`tasks/probes/step5-lot5/py_loop_bounds.py`
//! on roomkit 38ef0625).

use std::sync::Arc;
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{EventType, RetryPolicy, RoomEvent};
use roomkit::testing::TurnTap;
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiError, AiMessage, AiProvider, AiResponse, AiTool,
    Date, DynAiProvider, HumanInputToolHandler, HumanInputToolHandlerOptions, ModelInfo,
    ModelPricing, ProviderError, SkillRegistry, ToolPolicy, ToolResult, Tools,
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

/// RFC §21.6: a tool's call bound by zero is refused when the channel is
/// built, as Python's constructor raises `ValueError` for
/// `tool_timeout_seconds=0` (`tasks/probes/step5-review/ai/py_ctor_bounds.out`);
/// none bounds nothing. It was taken, every call then timing out at once.
#[test]
fn a_zero_tool_timeout_is_refused() {
    let built = |tool_timeout| {
        AiChannel::new(
            channel("assistant"),
            ScriptedProvider::new([]),
            AiChannelOptions {
                tool_timeout,
                ..AiChannelOptions::default()
            },
        )
        .err()
    };
    assert_eq!(
        [built(Some(Duration::ZERO)), built(None)],
        [Some(AiError::ToolTimeout), None]
    );
}

// Python: tests/test_tool_timeout.py::TestToolTimeouts::test_a_bound_that_is_not_positive_is_refused
#[test]
fn a_tool_s_own_bound_of_zero_is_refused() {
    let built = |bound| {
        AiChannel::new(
            channel("assistant"),
            ScriptedProvider::new([]),
            AiChannelOptions {
                tool_timeouts: [("slow".to_owned(), bound)].into(),
                ..AiChannelOptions::default()
            },
        )
        .err()
    };
    assert_eq!(
        [
            built(Some(Duration::ZERO)),
            built(None),
            built(Some(Duration::from_secs(1)))
        ],
        [
            Some(AiError::ToolTimeoutOf {
                name: "slow".to_owned()
            }),
            None,
            None
        ]
    );
}

// Python: tests/test_channels/test_turn_budget.py::test_a_budget_is_a_positive_number
#[test]
fn a_token_budget_of_zero_is_refused() {
    let built = |turn_budget_tokens| {
        AiChannel::new(
            channel("assistant"),
            ScriptedProvider::new([]),
            AiChannelOptions {
                turn_budget_tokens,
                ..AiChannelOptions::default()
            },
        )
        .err()
    };
    assert_eq!(
        [built(Some(0)), built(Some(1200)), built(None)],
        [Some(AiError::TurnBudgetTokens), None, None]
    );
}

// Python: tests/test_channels/test_turn_budget.py::test_a_cost_budget_needs_a_priced_model
// and test_a_budget_is_a_positive_number (turn_budget_usd=0).
#[test]
fn a_cost_budget_needs_a_priced_model_and_a_positive_number() {
    // The scripted model has no catalogue price.
    let built = |turn_budget_usd| {
        AiChannel::new(
            channel("assistant"),
            ScriptedProvider::new([]),
            AiChannelOptions {
                turn_budget_usd,
                ..AiChannelOptions::default()
            },
        )
        .err()
    };
    let shown = |shown: &str| AiError::TurnBudgetUsd {
        shown: shown.to_owned(),
    };
    assert_eq!(
        [
            built(Some(0.05)),
            built(Some(0.0)),
            built(Some(-1.0)),
            built(Some(f64::NAN)),
            built(None)
        ],
        [
            Some(AiError::UnpricedModel {
                model: "scripted".to_owned()
            }),
            Some(shown("0.0")),
            Some(shown("-1.0")),
            Some(shown("nan")),
            None
        ]
    );
    assert!(
        AiError::UnpricedModel {
            model: "scripted".to_owned()
        }
        .to_string()
        .starts_with("turn_budget_usd needs a model with a known price")
    );
}

/// A scripted model with a catalogue price.
pub(super) struct Priced(pub(super) ScriptedProvider);

impl AiProvider for Priced {
    fn model_name(&self) -> &'static str {
        "priced"
    }

    fn catalog_entry(&self) -> Option<ModelInfo> {
        let verified = Date::new(2026, 9, 30).unwrap();
        Some(ModelInfo {
            pricing: Some(ModelPricing::new(10.0, 50.0, verified).unwrap()),
            ..ModelInfo::new("priced")
        })
    }

    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        self.0.generate(context).await
    }
}

/// RFC §6.4: a generation whose usage the cost budget's price cannot read
/// fails the turn, as Python's `ModelPricing.cost_for` raises in its loop;
/// Rust raises it as the failure of the provider that served it, the
/// fallback's when it did (HITL 5a287312).
#[tokio::test]
async fn a_usage_the_price_cannot_read_fails_the_turn() {
    let unreadable = || {
        let usage = json!({"input_tokens": -1}).as_object().cloned().unwrap();
        ScriptedRound::answering("Done.").with_usage(usage)
    };
    let busy = ScriptedRound::answering("")
        .refusing(503, "busy")
        .retrying();
    let cases = [
        (ScriptedProvider::new([unreadable()]), None, "Priced"),
        (
            ScriptedProvider::new([busy]),
            Some(ScriptedProvider::new([unreadable()])),
            "scripted",
        ),
    ];
    for (primary, fallback, named) in cases {
        let fallback: Option<Arc<dyn DynAiProvider>> = match fallback {
            Some(fallback) => Some(Arc::new(fallback)),
            None => None,
        };
        let options = AiChannelOptions {
            turn_budget_usd: Some(1.0),
            fallback_provider: fallback,
            tools: served().into(),
            ..AiChannelOptions::default()
        };
        let agent = AiChannel::new(channel("assistant"), Priced(primary), options).unwrap();
        let room = Room::with_agent(agent).await;
        let result = room.says("Order 1?").await;
        assert_eq!(
            result.error.map(|error| error.message),
            Some(format!(
                "{named}: usage counter 'input_tokens' must be a non-negative integer"
            ))
        );
        room.kit.close().await.unwrap();
    }
}

/// The budgets the loop's end states, as the turn's wire carries them.
pub(super) fn loop_end_budgets(tap: &TurnTap) -> (Value, Value) {
    tap.messages()
        .into_iter()
        .filter_map(|message| serde_json::from_str::<Value>(&message.json).ok())
        .find_map(|message| {
            let item = message.pointer("/message/item")?;
            (item["kind"] == "loop_end")
                .then(|| (item["budget_tokens"].clone(), item["budget_usd"].clone()))
        })
        .unwrap()
}

// Python: tests/test_channels/test_loop_end_marker_limits.py::test_every_exit_states_the_limits_the_turn_ran_under
// (its budget_tokens and budget_usd).
#[tokio::test]
async fn the_loop_s_end_states_the_budgets_the_turn_ran_under() {
    for (usd, stated) in [(None, json!(null)), (Some(0.5), json!(0.5))] {
        let model = ScriptedProvider::new([ScriptedRound::answering("Done.")]);
        let options = AiChannelOptions {
            turn_budget_tokens: Some(500),
            turn_budget_usd: usd,
            tools: served().into(),
            ..AiChannelOptions::default()
        };
        let agent = AiChannel::new(channel("assistant"), Priced(model), options).unwrap();
        let room = Room::with_agent(agent).await;
        let tap = TurnTap::on(&room.kit);
        room.says("Order 1?").await;
        assert_eq!(loop_end_budgets(&tap), (json!(500), stated));
        room.kit.close().await.unwrap();
    }
}

// Python: tests/test_channels/test_turn_budget.py::test_a_budget_is_a_positive_number
// (the budgets checked before the tool bounds, as Python's constructor
// stores them first).
#[test]
fn the_budgets_are_checked_before_the_tool_bounds() {
    let built = |turn_budget_tokens, turn_budget_usd| {
        AiChannel::new(
            channel("assistant"),
            ScriptedProvider::new([]),
            AiChannelOptions {
                tool_timeout: Some(Duration::ZERO),
                turn_budget_tokens,
                turn_budget_usd,
                ..AiChannelOptions::default()
            },
        )
        .err()
    };
    assert_eq!(
        [
            built(Some(0), None),
            built(None, Some(0.0)),
            built(None, None)
        ],
        [
            Some(AiError::TurnBudgetTokens),
            Some(AiError::TurnBudgetUsd {
                shown: "0.0".to_owned()
            }),
            Some(AiError::ToolTimeout)
        ]
    );
}

// Python: channels/ai.py AIChannel.__init__'s order (RMK-1290): ToolPolicy,
// RetryPolicy and AITool refuse as they are built, before it runs; then
// _store_turn_budget, _store_prompt_sources, _store_tool_bounds and
// _init_tool_surface.
#[test]
fn a_channel_refuses_in_python_s_order() {
    let refused = |options: AiChannelOptions| {
        AiChannel::new(channel("assistant"), ScriptedProvider::new([]), options)
            .err()
            .unwrap()
    };
    let zero_budget = || AiChannelOptions {
        turn_budget_tokens: Some(0),
        ..AiChannelOptions::default()
    };
    let bad_label = || vec![("no identifier!".to_owned(), None)];
    let policy = refused(AiChannelOptions {
        tool_policy: Some(ToolPolicy {
            allow: vec!["[a]".to_owned()],
            deny: Vec::new(),
        }),
        ..zero_budget()
    });
    assert!(matches!(policy, AiError::ToolPattern { .. }), "{policy}");
    let retry = refused(AiChannelOptions {
        retry_policy: Some(RetryPolicy {
            base_delay_seconds: 0.0,
            ..RetryPolicy::default()
        }),
        ..zero_budget()
    });
    assert!(matches!(retry, AiError::RetryPolicy { .. }), "{retry}");
    let named = AiTool {
        name: "no name!".to_owned(),
        description: String::new(),
        parameters: json!({}),
        tags: Vec::new(),
    };
    let name = refused(AiChannelOptions {
        tools: vec![named.clone().into()],
        ..zero_budget()
    });
    assert!(matches!(name, AiError::ToolName { .. }), "{name}");
    let budget = refused(AiChannelOptions {
        server_instructions: bad_label(),
        ..zero_budget()
    });
    assert!(matches!(budget, AiError::TurnBudgetTokens), "{budget}");
    let label = refused(AiChannelOptions {
        server_instructions: bad_label(),
        tool_timeout: Some(Duration::ZERO),
        ..AiChannelOptions::default()
    });
    assert!(matches!(label, AiError::ServerLabel { .. }), "{label}");
    // A `Tool`'s definition is read in the constructor's tool surface, after
    // the budgets; a person's is an `AITool`, refused as it is built.
    let serving = Tools::new().tool(named.clone(), |_call, _context| async {
        Ok(ToolResult::Text("served".to_owned()))
    });
    let served = refused(AiChannelOptions {
        tools: serving.into(),
        ..zero_budget()
    });
    assert!(matches!(served, AiError::TurnBudgetTokens), "{served}");
    let person = HumanInputToolHandler::new(
        ["no name!"],
        HumanInputToolHandlerOptions {
            tool_definitions: vec![named.clone()],
            ..HumanInputToolHandlerOptions::default()
        },
    );
    assert!(matches!(person.err(), Some(AiError::ToolName { .. })));
    let twice = || AiTool::new("twice", "Twice").unwrap().into();
    let timeout = refused(AiChannelOptions {
        tool_timeout: Some(Duration::ZERO),
        tools: vec![twice(), twice()],
        ..AiChannelOptions::default()
    });
    assert!(matches!(timeout, AiError::ToolTimeout), "{timeout}");
}

// Python: channels/ai.py _init_tool_surface, _served_tools.py
// refuse_host_tools: a registry reserves the skill tools' names against a
// person's tools, and names the host's tool first as they are declared.
#[test]
fn a_registry_reserves_its_names_against_every_tool() {
    let refused = |options: AiChannelOptions| {
        AiChannel::new(channel("assistant"), ScriptedProvider::new([]), options)
            .err()
            .unwrap()
    };
    let reserved = AiTool::new("activate_skill", "Mine").unwrap();
    let person = HumanInputToolHandler::new(
        ["activate_skill"],
        HumanInputToolHandlerOptions {
            tool_definitions: vec![reserved],
            ..HumanInputToolHandlerOptions::default()
        },
    )
    .unwrap();
    let skilled = refused(AiChannelOptions {
        human_input_handler: Some(person),
        skills: Some(SkillRegistry::new()),
        ..AiChannelOptions::default()
    });
    assert_eq!(
        skilled,
        AiError::DuplicateTool {
            name: "activate_skill".to_owned()
        }
    );
    let host = |name: &str| AiTool::new(name, "Mine").unwrap().into();
    let first = refused(AiChannelOptions {
        tools: vec![host("read_skill_reference"), host("activate_skill")],
        skills: Some(SkillRegistry::new()),
        ..AiChannelOptions::default()
    });
    assert_eq!(
        first,
        AiError::DuplicateTool {
            name: "read_skill_reference".to_owned()
        }
    );
    // The host's tools are refused before a person's are read.
    let person = HumanInputToolHandler::new(
        ["x"],
        HumanInputToolHandlerOptions {
            tool_definitions: vec![AiTool::new("x", "Mine").unwrap()],
            ..HumanInputToolHandlerOptions::default()
        },
    )
    .unwrap();
    let before = refused(AiChannelOptions {
        tools: vec![host("activate_skill"), host("x")],
        human_input_handler: Some(person),
        skills: Some(SkillRegistry::new()),
        ..AiChannelOptions::default()
    });
    assert_eq!(
        before,
        AiError::DuplicateTool {
            name: "activate_skill".to_owned()
        }
    );
}
