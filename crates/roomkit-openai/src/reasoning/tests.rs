//! Translated from Python's `tests/test_providers/test_reasoning_with_tools.py`
//! (`TestOnOpenAIsOwnEndpoint`, `TestUnsaid`, the openai-base-url path),
//! `test_reasoning_settings.py` (OpenAI's floors) and
//! `test_request_refusals.py` (`TestOpenAIModelsChatRefuses`): what a Chat
//! Completions request carries of the effort, read as Python's
//! `_apply_sampling_kwargs` reads it.

use pretty_assertions::assert_eq;
use roomkit_ai::{AiContext, AiMessage, AiProvider, AiRole, AiTool};

use super::*;
use crate::{OpenAiAiProvider, OpenAiConfig};

#[path = "../../../roomkit/tests/support/warnings.rs"]
mod warnings;

use warnings::Warnings;

/// The config field the one warning names.
const FLAG: &str = "supports_reasoning_effort_with_tools";

/// A provider for `model` on OpenAI's own endpoint.
fn official(model: &str, effort: Option<&str>, stated: Option<bool>) -> OpenAiAiProvider {
    let config = OpenAiConfig {
        reasoning_effort: effort.map(str::to_owned),
        supports_reasoning_effort_with_tools: stated,
        ..OpenAiConfig::new("sk-test", model)
    };
    OpenAiAiProvider::new(config).unwrap()
}

/// A provider behind a `base_url`, whose model it cannot know.
fn behind_base_url(effort: Option<&str>, stated: Option<bool>) -> OpenAiAiProvider {
    let config = OpenAiConfig {
        base_url: Some("https://api.compatible.test/v1".to_owned()),
        reasoning_effort: effort.map(str::to_owned),
        supports_reasoning_effort_with_tools: stated,
        ..OpenAiConfig::new("sk-test", "mercury-2.5")
    };
    OpenAiAiProvider::new(config).unwrap()
}

// Python: tests/test_providers/test_reasoning_with_tools.py::TestOnOpenAIsOwnEndpoint::test_a_tagged_model_follows_the_catalogue_whatever_the_config_states
#[test]
fn a_tagged_model_follows_the_catalogue_whatever_the_config_states() {
    let (warnings, _capturing) = Warnings::capture();
    for stated in [None, Some(true), Some(false)] {
        let before_5_4 = official("gpt-5-mini", Some("low"), stated).reasoning_effort(None, true);
        let unset = official("gpt-6-luna", None, stated).reasoning_effort(None, true);
        let from_5_4 = official("gpt-6-luna", Some("low"), stated).reasoning_effort(None, true);
        assert_eq!(before_5_4, Some("low".to_owned()), "{stated:?}");
        assert_eq!(unset, Some("none".to_owned()), "{stated:?}");
        assert_eq!(from_5_4, Some("none".to_owned()), "{stated:?}");
    }
    assert_eq!(warnings.containing(FLAG), Vec::<String>::new());
}

// Python: tests/test_providers/test_reasoning_with_tools.py::TestOnOpenAIsOwnEndpoint::test_the_config_decides_for_a_model_the_catalogue_does_not_tag
#[test]
fn the_config_decides_for_a_model_the_catalogue_does_not_tag() {
    for (stated, expected) in [(true, Some("low".to_owned())), (false, None)] {
        let provider = official("gpt-7-preview", Some("low"), Some(stated));
        assert_eq!(provider.reasoning_effort(None, true), expected, "{stated}");
    }
}

// Python: tests/test_providers/test_reasoning_with_tools.py::TestOnOpenAIsOwnEndpoint::test_a_model_the_catalogue_does_not_tag_is_left_out_with_a_warning
#[test]
fn a_model_the_catalogue_does_not_tag_is_left_out_with_a_warning() {
    let (warnings, _capturing) = Warnings::capture();
    let provider = official("gpt-7-preview", Some("low"), None);
    assert_eq!(provider.reasoning_effort(None, true), None);
    assert_eq!(warnings.containing(FLAG).len(), 1);
}

// Python: tests/test_providers/test_reasoning_with_tools.py::TestUnsaid::test_it_is_left_out_with_one_warning_naming_the_field
#[test]
fn it_is_left_out_with_one_warning_naming_the_field() {
    let (warnings, _capturing) = Warnings::capture();
    let provider = behind_base_url(Some("low"), None);
    assert_eq!(provider.reasoning_effort(None, true), None);
    assert_eq!(provider.reasoning_effort(None, true), None);
    let warned = warnings.containing(FLAG);
    assert_eq!(warned.len(), 1);
    assert!(warned[0].contains("'low'") && warned[0].contains("'mercury-2.5'"));
}

// Python: tests/test_providers/test_reasoning_with_tools.py::TestUnsaid::test_nothing_stated_warns_of_nothing
#[test]
fn nothing_stated_warns_of_nothing() {
    let (warnings, _capturing) = Warnings::capture();
    assert_eq!(
        behind_base_url(None, None).reasoning_effort(None, true),
        None
    );
    assert_eq!(warnings.containing(FLAG), Vec::<String>::new());
}

// Python: tests/test_providers/test_reasoning_with_tools.py::TestTheConfigDecides::test_a_server_said_not_to_take_it_gets_nothing_and_no_warning
#[test]
fn a_server_said_not_to_take_it_gets_no_warning() {
    let (warnings, _capturing) = Warnings::capture();
    assert_eq!(
        behind_base_url(Some("low"), Some(false)).reasoning_effort(None, true),
        None
    );
    assert_eq!(warnings.containing(FLAG), Vec::<String>::new());
}

// Python: tests/test_providers/test_reasoning_settings.py::test_off_sends_the_model_s_floor
#[test]
fn off_sends_the_model_s_floor() {
    let floors = [
        ("gpt-5.1", "none"),
        ("gpt-5.2", "none"),
        ("gpt-5", "minimal"),
        ("gpt-5-nano", "minimal"),
        ("o3", "low"),
        ("o4-mini", "low"),
    ];
    for (model, floor) in floors {
        for tools in [false, true] {
            let provider = official(model, Some("none"), None);
            assert_eq!(
                provider.reasoning_effort(None, tools).as_deref(),
                Some(floor),
                "{model} tools={tools}"
            );
        }
    }
}

/// Behind a base URL no catalogue speaks for the model: an effort of
/// `none` goes as stated, as Python's `floored_effort` without a floor.
#[test]
fn behind_a_base_url_an_effort_of_none_has_no_floor() {
    let provider = OpenAiAiProvider::new(OpenAiConfig {
        base_url: Some("http://local.test/v1".to_owned()),
        reasoning_effort: Some("none".to_owned()),
        ..OpenAiConfig::new("", "gpt-5")
    })
    .unwrap();
    assert_eq!(
        provider.reasoning_effort(None, false),
        Some("none".to_owned())
    );
}

fn context(tools: bool) -> AiContext {
    AiContext {
        messages: vec![AiMessage::new(AiRole::User, "hi")],
        tools: if tools {
            vec![AiTool::new("now", "d").unwrap()]
        } else {
            Vec::new()
        },
        ..AiContext::default()
    }
}

// Python: tests/test_providers/test_request_refusals.py::TestOpenAIModelsChatRefuses::test_tools_for_a_model_that_takes_none_fail_before_the_request
#[tokio::test]
async fn tools_for_a_model_that_takes_none_fail_before_the_request() {
    for model in ["gpt-6-astra", "gpt-6.1-sol"] {
        let error = official(model, None, None)
            .generate(&context(true))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("takes no function tools"),
            "{error}"
        );
        assert!(!error.retryable);
    }
}

// Python: tests/test_providers/test_request_refusals.py::TestOpenAIModelsChatRefuses::test_the_same_model_without_tools_is_asked
#[test]
fn the_same_model_without_tools_is_asked() {
    let entry = crate::models::entry("gpt-6-astra");
    assert!(check_model_serves("gpt-6-astra", entry, false).is_ok());
    assert!(check_model_serves("gpt-6-astra", entry, true).is_err());
}

// Python: tests/test_providers/test_request_refusals.py::TestOpenAIModelsChatRefuses::test_a_responses_only_model_fails_before_the_request
#[tokio::test]
async fn a_responses_only_model_fails_before_the_request() {
    for model in ["gpt-5.5-pro", "gpt-5.4-pro", "o3-pro"] {
        let provider = official(model, None, None);
        let context = context(false);
        let mut stream = provider.generate_structured_stream(&context);
        let first = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        let error = first.unwrap().unwrap_err();
        assert!(error.to_string().contains("Responses API only"), "{error}");
    }
}

// Python: tests/test_providers/test_reasoning_settings.py::test_a_turn_value_outranks_the_configured_one_even_when_falsy
#[test]
fn a_turn_value_outranks_the_configured_one_even_when_falsy() {
    assert_eq!(
        floored_effort(Some(""), Some("high"), None).as_deref(),
        Some("")
    );
    assert_eq!(
        floored_effort(None, Some("high"), None).as_deref(),
        Some("high")
    );
    assert_eq!(
        floored_effort(Some("low"), Some("high"), None).as_deref(),
        Some("low")
    );
    assert_eq!(floored_effort(None, None, None), None);
}

// Python: tests/test_providers/test_reasoning_settings.py::test_the_turn_effort_outranks_the_configured_one
#[test]
fn the_turn_effort_outranks_the_configured_one() {
    let provider = official("gpt-5-mini", Some("high"), None);
    assert_eq!(
        provider.reasoning_effort(Some("low"), false).as_deref(),
        Some("low")
    );
    assert_eq!(
        provider.reasoning_effort(None, false).as_deref(),
        Some("high")
    );
}

// Python: tests/test_providers/test_reasoning_settings.py::test_a_tool_turn_carries_the_effort_as_a_turn_without_does
#[test]
fn a_tool_turn_carries_the_effort_as_a_turn_without_does() {
    let provider = official("gpt-5-mini", Some("high"), None);
    assert_eq!(
        provider.reasoning_effort(Some("low"), true).as_deref(),
        Some("low")
    );
}

// Python: tests/test_providers/test_reasoning_with_tools.py::TestTheConfigDecides::test_the_turn_effort_outranks_the_config_there
#[test]
fn the_turn_effort_outranks_the_config_there() {
    let provider = behind_base_url(Some("high"), Some(true));
    assert_eq!(
        provider.reasoning_effort(Some("minimal"), true).as_deref(),
        Some("minimal")
    );
}

// Python: tests/test_providers/test_reasoning_with_tools.py::TestPathRules::test_a_turn_that_states_off_sends_none_where_the_server_takes_it
#[test]
fn a_turn_that_states_off_sends_none_where_the_server_takes_it() {
    let provider = behind_base_url(Some("low"), Some(true));
    assert_eq!(
        provider.reasoning_effort(Some("none"), true).as_deref(),
        Some("none")
    );
}

// Python: tests/test_providers/test_reasoning_settings.py::test_off_sends_the_model_s_floor (its effort_none form, stated by the turn)
#[test]
fn a_turn_s_off_sends_the_model_s_floor() {
    let provider = official("gpt-5", Some("high"), None);
    for tools in [false, true] {
        assert_eq!(
            provider.reasoning_effort(Some("none"), tools).as_deref(),
            Some("minimal"),
            "tools={tools}"
        );
    }
}
