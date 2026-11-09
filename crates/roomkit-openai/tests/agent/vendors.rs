//! The vendor providers of the OpenAI wire on a fake of it: what a host
//! writes to select a vendor's dialect, on the vendor's API or on a server
//! of its own, and what each request then carries.
//! Python: `tests/test_providers/test_minimax_kimi.py` and `test_deepseek.py`.

use std::pin::Pin;

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit_ai::{
    AiChannelOptions, AiContext, AiMessage, AiProvider, AiRole, AiTool, StreamEvent, ToolResult,
    Tools,
};
use roomkit_openai::deepseek::{DeepSeekAiProvider, DeepSeekConfig};
use roomkit_openai::kimi::{KimiAiProvider, KimiConfig};
use roomkit_openai::minimax::{MiniMaxAiProvider, MiniMaxConfig};
use roomkit_openai::{OpenAiAiProvider, OpenAiConfig};
use serde_json::{Value, json};

use super::fake::{FakeOpenAi, Reply};
use super::{API_KEY, Room, chunk, ending};

/// A round that reasons, says it looks and calls; a round that only calls;
/// the answer; then the next turn's answer.
fn rounds() -> Vec<Reply> {
    let call = |id: &str| {
        chunk(
            &json!({ "tool_calls": [{ "index": 0, "id": id, "type": "function",
            "function": { "name": "get_weather", "arguments": "{}" } }] }),
        )
    };
    let mut first = vec![
        chunk(&json!({ "reasoning_content": "WHY." })),
        chunk(&json!({ "content": "Let me look." })),
        call("c1"),
    ];
    first.extend(ending("tool_calls", 10, 5));
    let mut second = vec![call("c2")];
    second.extend(ending("tool_calls", 10, 5));
    let mut answer = vec![
        chunk(&json!({ "reasoning_content": "DONE." })),
        chunk(&json!({ "content": "Sunny." })),
    ];
    answer.extend(ending("stop", 10, 5));
    let mut later = vec![chunk(&json!({ "content": "You are welcome." }))];
    later.extend(ending("stop", 10, 5));
    [first, second, answer, later]
        .iter()
        .map(|chunks| Reply::events(chunks))
        .collect()
}

fn weather() -> Tools {
    let tool = AiTool {
        parameters: json!({ "type": "object" }),
        ..AiTool::new("get_weather", "The weather.").unwrap()
    };
    Tools::new().tool(tool, |_call, _context| async move {
        Ok(ToolResult::Text("sunny".to_owned()))
    })
}

/// Each assistant message of a request: its text, and its reasoning field.
fn reasoning(request: &Value) -> Vec<(Value, Value)> {
    request["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| message["role"] == "assistant")
        .map(|message| {
            let field = message.get("reasoning_content").cloned();
            (
                message["content"].clone(),
                field.unwrap_or_else(|| "absent".into()),
            )
        })
        .collect()
}

/// Two turns through a room: the first with its two tool rounds.
async fn two_turns<P: AiProvider>(provider: impl FnOnce(&str) -> P) -> Vec<Value> {
    let equip = |options| AiChannelOptions {
        tools: weather().into(),
        ..options
    };
    let room = Room::with_provider(rounds(), provider, equip).await;
    room.says(&room.sms, "+15550001", "Alice", "go").await;
    room.says(&room.sms, "+15550001", "Alice", "thanks").await;
    room.fake
        .requests()
        .into_iter()
        .map(|request| request.body)
        .collect()
}

// Python: tests/test_providers/test_minimax_kimi.py::test_a_host_selects_the_dialect_by_the_class_on_a_server_of_its_own
/// The end-to-end proof: a room, an AI channel, the class chosen, a
/// generic server's URL. A round's reasoning goes back in the field for as
/// long as the turn lasts, then no more; no cap is sent unless one is set.
#[tokio::test]
async fn a_host_selects_the_dialect_by_the_class_on_a_server_of_its_own() {
    let kimi = two_turns(|url| {
        KimiAiProvider::new(KimiConfig {
            base_url: url.to_owned(),
            ..KimiConfig::new(API_KEY, "moonshotai/Kimi-K3")
        })
        .unwrap()
    })
    .await;
    let minimax = two_turns(|url| {
        MiniMaxAiProvider::new(MiniMaxConfig {
            base_url: url.to_owned(),
            ..MiniMaxConfig::new(API_KEY, "MiniMaxAI/MiniMax-M3")
        })
        .unwrap()
    })
    .await;
    for (requests, empty) in [(&kimi, json!(" ")), (&minimax, json!("absent"))] {
        assert_eq!(requests.len(), 4);
        assert_eq!(requests[0].get("max_tokens"), None);
        assert_eq!(requests[0].get("max_completion_tokens"), None);
        assert_eq!(
            reasoning(&requests[2]),
            [
                (json!("Let me look."), json!("WHY.")),
                (Value::Null, empty.clone())
            ]
        );
        // The round's text and its calls, one message as the turn sent it
        // (RMK-1370).
        assert_eq!(
            reasoning(&requests[3]),
            [
                (json!("Let me look."), empty.clone()),
                (Value::Null, empty.clone()),
                (json!("Sunny."), json!("absent")),
            ]
        );
    }
}

/// One generation's request through `provider`, on the fake.
async fn sent<P: AiProvider>(
    provider: impl FnOnce(&str) -> P,
    context: AiContext,
) -> (Value, Result<(), String>) {
    let fake = FakeOpenAi::start(vec![Reply::completion("Hello.")]).await;
    let provider = provider(fake.base_url());
    let answered = provider
        .generate(&context)
        .await
        .map(|_| ())
        .map_err(|error| format!("{} {}", error.provider, error.message));
    let body = fake
        .requests()
        .first()
        .map_or(Value::Null, |request| request.body.clone());
    (body, answered)
}

fn hello() -> AiContext {
    AiContext {
        temperature: 0.2,
        messages: vec![AiMessage::new(AiRole::User, "Hi")],
        ..AiContext::default()
    }
}

/// The members of `body` among `keys`.
fn members(body: &Value, keys: &[&str]) -> Value {
    let picked: serde_json::Map<String, Value> = keys
        .iter()
        .filter_map(|key| {
            body.get(*key)
                .map(|value| ((*key).to_owned(), value.clone()))
        })
        .collect();
    Value::Object(picked)
}

/// Kimi's provider for `model` on a server's URL.
fn kimi(model: &'static str) -> impl FnOnce(&str) -> KimiAiProvider {
    move |url: &str| {
        KimiAiProvider::new(KimiConfig {
            base_url: url.to_owned(),
            ..KimiConfig::new(API_KEY, model)
        })
        .unwrap()
    }
}

/// MiniMax's provider for `model` on a server's URL.
fn minimax(model: &'static str) -> impl FnOnce(&str) -> MiniMaxAiProvider {
    move |url: &str| {
        MiniMaxAiProvider::new(MiniMaxConfig {
            base_url: url.to_owned(),
            ..MiniMaxConfig::new(API_KEY, model)
        })
        .unwrap()
    }
}

/// DeepSeek's provider on a server's URL, sending no temperature.
fn deepseek(url: &str) -> DeepSeekAiProvider {
    DeepSeekAiProvider::new(DeepSeekConfig {
        base_url: url.to_owned(),
        supports_custom_temperature: false,
        ..DeepSeekConfig::new(API_KEY, "deepseek-v4-pro")
    })
    .unwrap()
}

/// `context`, declaring a tool.
fn with_tool(context: AiContext) -> AiContext {
    AiContext {
        tools: vec![AiTool {
            parameters: json!({ "type": "object" }),
            ..AiTool::new("get_weather", "The weather.").unwrap()
        }],
        ..context
    }
}

/// A turn stating `effort` and `enable`.
fn turn(effort: Option<&str>, enable: Option<bool>) -> AiContext {
    AiContext {
        reasoning_effort: effort.map(str::to_owned),
        enable_thinking: enable,
        ..hello()
    }
}

// Python: tests/test_providers/test_minimax_kimi.py::test_what_a_turn_states_of_its_reasoning
#[tokio::test]
async fn what_a_turn_states_of_its_reasoning() {
    let keys = ["reasoning_effort", "thinking", "temperature"];
    let cases = [
        // Each cannot stop reasoning: a turn that states off gets the least it takes.
        (
            sent(kimi("kimi-k3.1-pro"), turn(None, Some(false))).await,
            json!({ "reasoning_effort": "low" }),
        ),
        (
            sent(kimi("kimi-k3"), turn(Some("none"), None)).await,
            json!({ "reasoning_effort": "low" }),
        ),
        (
            sent(minimax("MiniMax-M3.1-Flash"), turn(None, Some(false))).await,
            json!({ "reasoning_effort": "low" }),
        ),
        // M3 can, through MiniMax's own switch.
        (
            sent(minimax("MiniMax-M3"), turn(None, Some(false))).await,
            json!({ "thinking": { "type": "disabled" } }),
        ),
        // A level the model does not take goes as the nearest it takes.
        (
            sent(kimi("kimi-k3.1-pro"), turn(Some("medium"), None)).await,
            json!({ "reasoning_effort": "low" }),
        ),
        (
            sent(kimi("kimi-k3"), turn(Some("xhigh"), None)).await,
            json!({ "reasoning_effort": "high" }),
        ),
        (
            sent(minimax("MiniMax-M3.1-Flash"), turn(Some("minimal"), None)).await,
            json!({ "reasoning_effort": "low" }),
        ),
        // A model the catalogue does not name: what the turn states goes as it is.
        (
            sent(kimi("moonshotai/Kimi-K3"), turn(Some("medium"), None)).await,
            json!({ "reasoning_effort": "medium" }),
        ),
        (
            sent(kimi("moonshotai/Kimi-K3"), turn(None, Some(false))).await,
            json!({}),
        ),
        (
            sent(minimax("MiniMaxAI/MiniMax-M3"), turn(Some("none"), None)).await,
            json!({ "reasoning_effort": "none" }),
        ),
        // A turn with tools sends its effort too, where the generic provider sends none.
        (
            sent(kimi("kimi-k3"), with_tool(turn(Some("high"), None))).await,
            json!({ "reasoning_effort": "high" }),
        ),
        (
            sent(
                minimax("MiniMax-M3.1-Flash"),
                with_tool(turn(Some("high"), None)),
            )
            .await,
            json!({ "reasoning_effort": "high" }),
        ),
        (
            sent(deepseek, with_tool(turn(Some("high"), None))).await,
            json!({ "thinking": { "type": "enabled", "reasoning_effort": "high" } }),
        ),
    ];
    for ((body, answered), expected) in cases {
        assert_eq!(answered, Ok(()));
        assert_eq!(members(&body, &keys), expected);
    }
}

// Python: tests/test_providers/test_minimax_kimi.py::test_the_vendor_s_own_endpoint_is_the_default
// and ::test_the_temperature_goes_where_the_server_takes_it
#[test]
fn each_vendor_s_endpoint_cap_and_temperature_are_python_s() {
    let kimi = KimiConfig::new(API_KEY, "kimi-k3");
    let minimax = MiniMaxConfig::new(API_KEY, "MiniMax-M3");
    let deepseek = DeepSeekConfig::new(API_KEY, "deepseek-v4-pro");
    assert_eq!(
        [
            (
                kimi.base_url.as_str(),
                kimi.max_tokens,
                kimi.use_max_completion_tokens,
                kimi.supports_custom_temperature
            ),
            (
                minimax.base_url.as_str(),
                minimax.max_tokens,
                minimax.use_max_completion_tokens,
                minimax.supports_custom_temperature
            ),
            (
                deepseek.base_url.as_str(),
                deepseek.max_tokens,
                deepseek.use_max_completion_tokens,
                deepseek.supports_custom_temperature
            ),
        ],
        [
            ("https://api.moonshot.ai/v1", None, true, false),
            ("https://api.minimax.io/v1", None, true, false),
            ("https://api.deepseek.com/v1", Some(1024), false, true),
        ]
    );
    let names = [
        KimiAiProvider::new(kimi).unwrap().name().to_owned(),
        MiniMaxAiProvider::new(minimax).unwrap().name().to_owned(),
        DeepSeekAiProvider::new(deepseek).unwrap().name().to_owned(),
    ];
    assert_eq!(names, ["kimi", "minimax", "DeepSeekAIProvider"]);
}

#[tokio::test]
async fn a_cap_goes_under_the_field_the_vendor_reads() {
    let (kimi, _) = sent(
        |url: &str| {
            KimiAiProvider::new(KimiConfig {
                base_url: url.to_owned(),
                max_tokens: Some(2000),
                ..KimiConfig::new(API_KEY, "kimi-k3")
            })
            .unwrap()
        },
        hello(),
    )
    .await;
    let (deepseek, _) = sent(
        |url: &str| {
            DeepSeekAiProvider::new(DeepSeekConfig {
                base_url: url.to_owned(),
                ..DeepSeekConfig::new(API_KEY, "deepseek-v4-pro")
            })
            .unwrap()
        },
        hello(),
    )
    .await;
    let keys = ["max_tokens", "max_completion_tokens", "temperature"];
    assert_eq!(
        (members(&kimi, &keys), members(&deepseek, &keys)),
        (
            json!({ "max_completion_tokens": 2000 }),
            json!({ "max_tokens": 1024, "temperature": 0.2 })
        )
    );
}

// Python: tests/test_providers/test_reasoning_settings.py, the configs that
// apply their vendor's own rule refuse a statement of what the server takes.
#[test]
fn a_vendor_s_config_refuses_a_statement_of_its_tool_turns() {
    let refused = [
        KimiAiProvider::new(KimiConfig {
            supports_reasoning_effort_with_tools: Some(true),
            ..KimiConfig::new(API_KEY, "kimi-k3")
        })
        .is_err(),
        MiniMaxAiProvider::new(MiniMaxConfig {
            supports_reasoning_effort_with_tools: Some(false),
            ..MiniMaxConfig::new(API_KEY, "MiniMax-M3")
        })
        .is_err(),
        DeepSeekAiProvider::new(DeepSeekConfig {
            supports_reasoning_effort_with_tools: Some(true),
            ..DeepSeekConfig::new(API_KEY, "deepseek-v4-pro")
        })
        .is_err(),
    ];
    assert_eq!(refused, [true, true, true]);
}

// Python: tests/test_providers/test_chat_request.py::test_a_config_whose_vendor_names_the_field_refuses_another
#[test]
fn a_vendor_s_config_refuses_a_thinking_field() {
    let field = || Some("reasoning_content".to_owned());
    let refused = [
        KimiAiProvider::new(KimiConfig {
            thinking_field: field(),
            ..KimiConfig::new(API_KEY, "kimi-k3")
        })
        .is_err(),
        MiniMaxAiProvider::new(MiniMaxConfig {
            thinking_field: field(),
            ..MiniMaxConfig::new(API_KEY, "MiniMax-M3")
        })
        .is_err(),
        DeepSeekAiProvider::new(DeepSeekConfig {
            thinking_field: field(),
            ..DeepSeekConfig::new(API_KEY, "deepseek-v4-pro")
        })
        .is_err(),
    ];
    assert_eq!(refused, [true, true, true]);
}

// Python: tests/test_providers/test_chat_request.py::test_a_configured_thinking_field_carries_a_round_s_reasoning_to_the_server
/// A generic provider told the field a server reads: a round's reasoning goes
/// back under that very name.
#[tokio::test]
async fn a_generic_provider_told_the_field_sends_reasoning_under_that_name() {
    let bodies = two_turns(|url: &str| {
        OpenAiAiProvider::new(OpenAiConfig {
            base_url: Some(url.to_owned()),
            thinking_field: Some("reasoning".to_owned()),
            ..OpenAiConfig::new(API_KEY, "glm-5.3-flash")
        })
        .unwrap()
    })
    .await;
    let messages = bodies[1]["messages"].as_array().unwrap();
    let round = messages
        .iter()
        .find(|message| message["role"] == "assistant")
        .unwrap();
    assert_eq!(round["reasoning"], "WHY.");
    assert!(round.get("reasoning_content").is_none());
}

// Python: tests/test_providers/test_minimax_kimi.py::test_a_server_decides_what_a_model_the_catalogue_does_not_name_reads
#[test]
fn a_server_decides_what_a_model_the_catalogue_does_not_name_reads() {
    let kimi = |model: &str, base_url: Option<&str>| {
        let config = KimiConfig::new(API_KEY, model);
        let base_url = base_url.map_or(config.base_url.clone(), str::to_owned);
        KimiAiProvider::new(KimiConfig { base_url, ..config })
            .unwrap()
            .supports_vision()
    };
    let minimax = |model: &str, base_url: Option<&str>| {
        let config = MiniMaxConfig::new(API_KEY, model);
        let base_url = base_url.map_or(config.base_url.clone(), str::to_owned);
        MiniMaxAiProvider::new(MiniMaxConfig { base_url, ..config })
            .unwrap()
            .supports_vision()
    };
    let deepseek =
        DeepSeekAiProvider::new(DeepSeekConfig::new(API_KEY, "deepseek-v4-pro")).unwrap();
    let server = Some("http://gpu:8000/v1");
    assert_eq!(
        [
            kimi("kimi-k3", None),
            kimi("unknown", None),
            kimi("unknown", server),
            minimax("MiniMax-M3", None),
            minimax("unknown", None),
            minimax("unknown", server),
            deepseek.supports_vision(),
        ],
        [true, false, true, true, false, true, false]
    );
}

// Python: tests/test_providers/test_minimax_kimi.py::test_kimi_s_tool_names_are_checked_before_the_request
// and test_vendor_endpoint.py's deepseek and kimi sites
#[tokio::test]
async fn a_vendor_s_tool_names_are_checked_on_its_own_endpoint_alone() {
    let declaring = |name: &str| AiContext {
        tools: vec![AiTool {
            parameters: json!({ "type": "object" }),
            ..AiTool::new(name, "A tool.").unwrap()
        }],
        ..hello()
    };
    let kimi = |name: &str| {
        let provider = KimiAiProvider::new(KimiConfig::new(API_KEY, "kimi-k3")).unwrap();
        let context = declaring(name);
        async move {
            provider
                .generate(&context)
                .await
                .err()
                .map(|error| (error.provider, error.before_request))
        }
    };
    let long = "a".repeat(129);
    // Refused before the request (Python: tests/text_conformance/test_requests.py
    // test_a_name_the_vendor_refuses_fails_before_the_request).
    let refused = || Some(("kimi".to_owned(), true));
    assert_eq!(
        [kimi("a.b").await, kimi("1abc").await, kimi(&long).await,],
        [refused(), refused(), refused()]
    );
    // Behind another URL the server decides: a dotted name goes on.
    let (body, answered) = sent(
        |url: &str| {
            KimiAiProvider::new(KimiConfig {
                base_url: url.to_owned(),
                ..KimiConfig::new(API_KEY, "kimi-k3")
            })
            .unwrap()
        },
        declaring("a.b"),
    )
    .await;
    assert_eq!(
        (body["tools"][0]["function"]["name"].clone(), answered),
        (json!("a.b"), Ok(()))
    );
    let deepseek =
        DeepSeekAiProvider::new(DeepSeekConfig::new(API_KEY, "deepseek-v4-pro")).unwrap();
    let refused = deepseek
        .generate(&declaring("files.read"))
        .await
        .unwrap_err();
    assert_eq!(refused.provider, "deepseek");
}

/// A whole completion carrying `usage`.
fn used(usage: &Value) -> Reply {
    let body = json!({
        "id": "chatcmpl-1",
        "object": "chat.completion",
        "choices": [{ "index": 0, "message": { "role": "assistant", "content": "Hi" },
            "finish_reason": "stop" }],
        "usage": usage,
    });
    Reply::status(200, &body.to_string())
}

// Python: tests/test_providers/test_deepseek.py::TestUsage::test_cache_hits_are_not_billed_as_input
/// DeepSeek's cache hits are read through its provider, answered whole and
/// streamed.
#[tokio::test]
async fn deepseek_s_cache_hits_are_not_billed_as_input() {
    let usage = json!({ "prompt_tokens": 1000, "completion_tokens": 50,
        "prompt_cache_hit_tokens": 960, "prompt_cache_miss_tokens": 40 });
    let deepseek = |url: &str| {
        DeepSeekAiProvider::new(DeepSeekConfig {
            base_url: url.to_owned(),
            ..DeepSeekConfig::new(API_KEY, "deepseek-v4-pro")
        })
        .unwrap()
    };
    let fake = FakeOpenAi::start(vec![used(&usage)]).await;
    let whole = deepseek(fake.base_url())
        .generate(&hello())
        .await
        .unwrap()
        .usage;
    let chunks = [
        chunk(&json!({ "content": "Hi" })),
        json!({ "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop" }] }),
        json!({ "choices": [], "usage": usage }),
    ];
    let streamed_fake = FakeOpenAi::start(vec![Reply::events(&chunks)]).await;
    let streaming = deepseek(streamed_fake.base_url());
    let context = hello();
    let mut events = streaming.generate_structured_stream(&context);
    let mut streamed = None;
    while let Some(event) = std::future::poll_fn(|cx| Pin::new(&mut events).poll_next(cx)).await {
        if let StreamEvent::Done(done) = event.unwrap() {
            streamed = Some(done.usage);
        }
    }
    let expected =
        json!({ "input_tokens": 40, "output_tokens": 50, "cache_read_input_tokens": 960 });
    assert_eq!(serde_json::to_value(whole).unwrap(), expected);
    assert_eq!(serde_json::to_value(streamed.unwrap()).unwrap(), expected);
}

// Python: tests/test_providers/test_deepseek.py::TestDeepSeekThinking::test_config_extra_body_survives_thinking
#[tokio::test]
async fn a_configured_extra_body_survives_and_the_turn_s_thinking_wins() {
    let mut extra = roomkit::model::Metadata::new();
    extra.insert("top_k".to_owned(), json!(5));
    extra.insert("thinking".to_owned(), json!({ "type": "enabled" }));
    let (body, answered) = sent(
        |url: &str| {
            DeepSeekAiProvider::new(DeepSeekConfig {
                base_url: url.to_owned(),
                extra_body: Some(extra),
                ..DeepSeekConfig::new(API_KEY, "deepseek-v4-pro")
            })
            .unwrap()
        },
        AiContext {
            enable_thinking: Some(false),
            ..hello()
        },
    )
    .await;
    assert_eq!(answered, Ok(()));
    assert_eq!(
        members(&body, &["top_k", "thinking"]),
        json!({ "top_k": 5, "thinking": { "type": "disabled" } })
    );
}

// Python: tests/test_providers/test_minimax_kimi.py::test_the_temperature_goes_where_the_server_takes_it
#[tokio::test]
async fn a_temperature_goes_where_the_config_says_the_server_takes_it() {
    let (kimi, _) = sent(
        |url: &str| {
            KimiAiProvider::new(KimiConfig {
                base_url: url.to_owned(),
                supports_custom_temperature: true,
                ..KimiConfig::new(API_KEY, "kimi-k3")
            })
            .unwrap()
        },
        hello(),
    )
    .await;
    let (minimax, _) = sent(
        |url: &str| {
            MiniMaxAiProvider::new(MiniMaxConfig {
                base_url: url.to_owned(),
                supports_custom_temperature: true,
                ..MiniMaxConfig::new(API_KEY, "MiniMax-M3")
            })
            .unwrap()
        },
        hello(),
    )
    .await;
    assert_eq!(
        (kimi["temperature"].clone(), minimax["temperature"].clone()),
        (json!(0.2), json!(0.2))
    );
}

// Python: tests/test_providers/test_deepseek.py::TestDeepSeekProvider::test_sdk_error_wrapped_with_deepseek_provider
#[tokio::test]
async fn a_vendor_s_failure_names_the_vendor() {
    let answer = Reply::status(429, r#"{"error": {"message": "slow down"}}"#);
    let fake = FakeOpenAi::start(vec![answer]).await;
    let deepseek = DeepSeekAiProvider::new(DeepSeekConfig {
        base_url: fake.base_url().to_owned(),
        ..DeepSeekConfig::new(API_KEY, "deepseek-v4-pro")
    })
    .unwrap();
    let failed = deepseek.generate(&hello()).await.unwrap_err();
    assert_eq!(
        (
            failed.provider.as_str(),
            failed.retryable,
            failed.status_code
        ),
        ("deepseek", true, Some(429))
    );
}

// Python: OpenAIAIProvider._listing, a vendor's catalogue backfilling its listing
#[tokio::test]
async fn a_vendor_s_listing_reads_its_own_catalogue() {
    let fake = FakeOpenAi::start(vec![Reply::models(&["kimi-k3", "other"])]).await;
    let kimi = KimiAiProvider::new(KimiConfig {
        base_url: fake.base_url().to_owned(),
        ..KimiConfig::new(API_KEY, "kimi-k3")
    })
    .unwrap();
    let windows: Vec<(String, Option<u64>)> = kimi
        .list_models()
        .await
        .unwrap()
        .into_iter()
        .map(|model| (model.id, model.context_window))
        .collect();
    assert_eq!(
        windows,
        [
            ("kimi-k3".to_owned(), Some(1_048_576)),
            ("other".to_owned(), None)
        ]
    );
}

// Python: DeepSeekAIProvider inherits OpenAIAIProvider.supports_vision
#[test]
fn deepseek_reads_a_model_it_does_not_name_as_openai_does() {
    let deepseek = |model: &str| {
        DeepSeekAiProvider::new(DeepSeekConfig {
            base_url: "http://gateway:8000/v1".to_owned(),
            ..DeepSeekConfig::new(API_KEY, model)
        })
        .unwrap()
        .supports_vision()
    };
    assert_eq!(
        [
            deepseek("deepseek/deepseek-v4-pro"),
            deepseek("gpt-4o-mini"),
            deepseek("deepseek-v4-pro"),
        ],
        [false, true, false]
    );
}
