//! The vendors' rules, as Python's providers state them.

use pretty_assertions::assert_eq;
use roomkit_ai::AiTool;
use serde_json::json;

use super::*;
use crate::reasoning::nearest_level;

fn tool(name: &str) -> AiTool {
    AiTool {
        parameters: json!({ "type": "object" }),
        ..AiTool::new(name, "A tool.").unwrap()
    }
}

// Python: tests/test_providers/test_minimax_kimi.py::test_kimi_s_tool_names_are_checked_before_the_request
#[test]
fn kimi_s_tool_names_follow_its_documented_rule() {
    let accepted = |name: &str| KIMI_NAMES.check(&[tool(name)]).is_ok();
    assert_eq!(
        [
            accepted(&"a".repeat(128)),
            accepted("_lookup-2"),
            accepted(&"a".repeat(129)),
            accepted("1abc"),
            accepted("a.b"),
        ],
        [true, true, false, false, false]
    );
}

// Python: tests/test_providers/test_vendor_endpoint.py::test_the_vendor_s_url_written_out_gets_the_vendor_s_rules
#[test]
fn a_vendor_s_rule_holds_on_its_own_endpoint_written_out_alone() {
    let kimi = |url: &str| Vendor::Kimi.name_rule(url).is_some();
    let deepseek = |url: &str| {
        Vendor::DeepSeek {
            enable_thinking: None,
        }
        .name_rule(url)
        .is_some()
    };
    assert_eq!(
        [
            kimi("https://api.moonshot.ai/v1"),
            kimi("https://api.moonshot.ai/v1/"),
            kimi("http://gpu:8000/v1"),
            deepseek("https://api.deepseek.com/v1"),
            deepseek("https://api.deepseek.com"),
            deepseek("https://user:key@api.deepseek.com/v1"),
            Vendor::MiniMax
                .name_rule("https://api.minimax.io/v1")
                .is_some(),
        ],
        [true, true, false, true, true, false, false]
    );
}

// Python: tests/test_providers/test_reasoning_settings.py, nearest_level
#[test]
fn an_effort_goes_as_the_nearest_level_the_model_takes() {
    let kimi = |effort| nearest_level(Some(effort), &KIMI_LEVELS, &SCALE);
    assert_eq!(
        [
            kimi("medium"),
            kimi("xhigh"),
            kimi("minimal"),
            kimi("max"),
            kimi("none")
        ],
        [Some("low"), Some("high"), Some("low"), Some("max"), None]
    );
}

fn request(effort: Option<&str>, switch: Option<bool>) -> Request {
    let context = roomkit_ai::AiContext {
        reasoning_effort: effort.map(str::to_owned),
        enable_thinking: switch,
        messages: vec![roomkit_ai::AiMessage::new(roomkit_ai::AiRole::User, "Hi")],
        ..roomkit_ai::AiContext::default()
    };
    Request::of(&context).unwrap()
}

fn thinking(sampling: &Sampling) -> Option<Value> {
    sampling.extra.get("thinking").cloned()
}

// Python: tests/test_providers/test_deepseek.py::TestThinking
#[test]
fn deepseek_s_thinking_object_is_its_one_reasoning_setting() {
    let deepseek = |enable_thinking, configured, effort, switch| {
        let vendor = Vendor::DeepSeek { enable_thinking };
        let sampling = vendor.sampling(None, &request(effort, switch), configured);
        assert_eq!(
            sampling.effort, None,
            "a top-level reasoning_effort is never sent"
        );
        thinking(&sampling)
    };
    assert_eq!(
        [
            // Nothing stated: the model's default.
            deepseek(None, None, None, None),
            // The effort nests inside the object.
            deepseek(None, Some("high"), None, None),
            // The configured switch, either way.
            deepseek(Some(false), None, None, None),
            deepseek(Some(true), None, None, None),
            // The turn outranks the configuration.
            deepseek(Some(true), None, None, Some(false)),
            deepseek(None, Some("high"), Some("low"), None),
            // An effort of none states off.
            deepseek(None, None, Some("none"), None),
        ],
        [
            None,
            Some(json!({ "type": "enabled", "reasoning_effort": "high" })),
            Some(json!({ "type": "disabled" })),
            Some(json!({ "type": "enabled" })),
            Some(json!({ "type": "disabled" })),
            Some(json!({ "type": "enabled", "reasoning_effort": "low" })),
            Some(json!({ "type": "disabled" })),
        ]
    );
}

// Python: tests/test_providers/test_deepseek.py::TestThinking::test_zero_budget_disables
// and ::test_positive_budget_enables_without_a_size
#[test]
fn deepseek_reads_a_budget_as_a_switch_without_its_size() {
    let budgeted = |budget| {
        let context = roomkit_ai::AiContext {
            thinking_budget: Some(budget),
            messages: vec![roomkit_ai::AiMessage::new(roomkit_ai::AiRole::User, "Hi")],
            ..roomkit_ai::AiContext::default()
        };
        let request = Request::of(&context).unwrap();
        thinking(
            &Vendor::DeepSeek {
                enable_thinking: None,
            }
            .sampling(None, &request, None),
        )
    };
    assert_eq!(
        [budgeted(0), budgeted(2048)],
        [
            Some(json!({ "type": "disabled" })),
            Some(json!({ "type": "enabled" }))
        ]
    );
}
