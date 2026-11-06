//! DeepSeek's provider: its catalogue and its usage.
//! Python: tests/test_providers/test_deepseek.py.

use pretty_assertions::assert_eq;
use roomkit_ai::AiProvider;
use serde_json::{Value, json};

use super::*;

// Python: tests/test_providers/test_deepseek.py::TestCatalog
#[test]
fn the_catalogue_is_python_s_and_reads_no_image() {
    let models = DeepSeekAiProvider::available_models();
    let ids: Vec<&str> = models.iter().map(|model| model.id.as_str()).collect();
    assert_eq!(ids, ["deepseek-v4-pro", "deepseek-v4-flash"]);
    assert!(
        models
            .iter()
            .all(|model| model.pricing.is_some() && model.context_window.is_some())
    );
    assert!(
        models
            .iter()
            .all(|model| model.supports_vision == Some(false))
    );
}

fn usage(usage: Value) -> Value {
    let body = json!({
        "id": "c",
        "choices": [{ "index": 0, "message": { "role": "assistant", "content": "Hi" },
            "finish_reason": "stop" }],
        "usage": usage,
    });
    let read = crate::wire::response(body.to_string().as_bytes(), "", true).unwrap();
    serde_json::to_value(read.done.usage).unwrap()
}

// Python: tests/test_providers/test_deepseek.py::TestUsage
#[test]
fn cache_hits_are_not_billed_as_input() {
    assert_eq!(
        [
            usage(json!({ "prompt_tokens": 1000, "completion_tokens": 50,
                "prompt_cache_hit_tokens": 960, "prompt_cache_miss_tokens": 40 })),
            // The miss counter derived when it is left out.
            usage(json!({ "prompt_tokens": 1000, "completion_tokens": 50,
                "prompt_cache_hit_tokens": 960 })),
            // A zero hit reports no cache counter.
            usage(json!({ "prompt_tokens": 100, "completion_tokens": 5,
                "prompt_cache_hit_tokens": 0, "prompt_cache_miss_tokens": 100 })),
            // OpenAI's shape, from a server that reports no hits.
            usage(json!({ "prompt_tokens": 100, "completion_tokens": 5,
                "prompt_tokens_details": { "cached_tokens": 80 } })),
        ],
        [
            json!({ "input_tokens": 40, "output_tokens": 50, "cache_read_input_tokens": 960 }),
            json!({ "input_tokens": 40, "output_tokens": 50, "cache_read_input_tokens": 960 }),
            json!({ "input_tokens": 100, "output_tokens": 5 }),
            json!({ "input_tokens": 20, "output_tokens": 5, "cache_read_input_tokens": 80 }),
        ]
    );
}

// Python: OpenAIAIProvider reads only OpenAI's shape: hits a generic server
// reports are input there.
#[test]
fn a_generic_provider_reads_no_cache_hits() {
    let body = json!({
        "id": "c",
        "choices": [{ "index": 0, "message": { "role": "assistant", "content": "Hi" },
            "finish_reason": "stop" }],
        "usage": { "prompt_tokens": 1000, "completion_tokens": 50, "prompt_cache_hit_tokens": 960 },
    });
    let read = crate::wire::response(body.to_string().as_bytes(), "", false).unwrap();
    assert_eq!(
        serde_json::to_value(read.done.usage).unwrap(),
        json!({ "input_tokens": 1000, "output_tokens": 50 })
    );
}

// Python: tests/test_providers/test_deepseek.py::TestDeepSeekProvider::test_provider_name_is_deepseek
#[test]
fn an_error_names_deepseek() {
    let provider = DeepSeekAiProvider::new(DeepSeekConfig::new("k", "deepseek-v4-pro")).unwrap();
    let failed = provider
        .inner
        .failed(crate::error::OpenAiError::Malformed { reason: "x".into() });
    assert_eq!(
        (provider.name(), failed.provider.as_str()),
        ("DeepSeekAIProvider", "deepseek")
    );
}
