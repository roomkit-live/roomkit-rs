//! A turn with tools carries the reasoning the config says the server
//! behind a `base_url` takes there (RFC §6.7), translated from Python's
//! `tests/test_providers/test_reasoning_with_tools.py`, its
//! `openai-base-url` path: `supports_reasoning_effort_with_tools` decides,
//! `true` sending the effort as on any turn, `false` leaving it out, unset
//! leaving it out.

use pretty_assertions::assert_eq;
use roomkit_ai::{AiContext, AiMessage, AiProvider, AiRole, AiTool};
use roomkit_openai::{OpenAiAiProvider, OpenAiConfig};
use serde_json::{Value, json};

use super::fake::{FakeOpenAi, Reply};

/// The `reasoning_effort` a request of a turn, with tools when `tools`,
/// carries behind a `base_url`, the config's switch `stated`.
async fn sent(stated: Option<bool>, effort: Option<&str>, tools: bool) -> Value {
    let fake = FakeOpenAi::start(vec![Reply::completion("ok")]).await;
    let config = OpenAiConfig {
        base_url: Some(fake.base_url().to_owned()),
        reasoning_effort: effort.map(str::to_owned),
        supports_reasoning_effort_with_tools: stated,
        ..OpenAiConfig::new("sk-test", "mercury-2.5")
    };
    let provider = OpenAiAiProvider::new(config).unwrap();
    let mut context = AiContext {
        messages: vec![AiMessage::new(AiRole::User, "Hi")],
        ..AiContext::default()
    };
    if tools {
        context.tools = vec![AiTool::new("lookup", "x").unwrap()];
    }
    provider.generate(&context).await.unwrap();
    fake.requests()[0].body["reasoning_effort"].clone()
}

// Python: tests/test_providers/test_reasoning_with_tools.py::TestTheConfigDecides::test_a_server_said_to_take_it_gets_the_effort_on_a_tool_turn
#[tokio::test]
async fn a_server_said_to_take_it_gets_the_effort_on_a_tool_turn() {
    assert_eq!(sent(Some(true), Some("low"), true).await, json!("low"));
}

// Python: tests/test_providers/test_reasoning_with_tools.py::TestTheConfigDecides::test_a_server_said_not_to_take_it_gets_nothing_and_no_warning
#[tokio::test]
async fn a_server_said_not_to_take_it_gets_nothing() {
    assert_eq!(sent(Some(false), Some("low"), true).await, Value::Null);
}

// Python: tests/test_providers/test_reasoning_with_tools.py::TestTheConfigDecides::test_a_turn_without_tools_is_left_as_it_was
#[tokio::test]
async fn a_turn_without_tools_is_left_as_it_was() {
    assert_eq!(sent(Some(false), Some("low"), false).await, json!("low"));
}

// Python: tests/test_providers/test_reasoning_with_tools.py::TestUnsaid::test_it_is_left_out_with_one_warning_naming_the_field
#[tokio::test]
async fn unsaid_it_is_left_out() {
    assert_eq!(sent(None, Some("low"), true).await, Value::Null);
}

/// Python's default, `supports_reasoning_effort_with_tools=None`.
#[test]
fn the_switch_is_unset_by_default() {
    let config = OpenAiConfig::new("sk-test", "mercury-2.5");
    assert_eq!(config.supports_reasoning_effort_with_tools, None);
}
