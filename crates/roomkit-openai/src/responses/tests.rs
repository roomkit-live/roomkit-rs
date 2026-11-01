//! Provider replay is a layout, not an alternative source of tool calls or
//! instructions (RFC §6.4, §6.7).

use crate::request::Request;
use roomkit::model::{Metadata, ToolCall};
use serde_json::{Value, json};

use super::{replay::OUTPUT, wire};
use crate::OpenAiConfig;
use roomkit_core::rules::transcript::{Message, Role};

fn request(message: Message) -> Result<Value, crate::error::OpenAiError> {
    let config = OpenAiConfig {
        base_url: Some("https://example.com/v1".into()),
        ..OpenAiConfig::new("", "fake")
    };
    wire::request(
        &config,
        &Request {
            system_prompt: None,
            messages: vec![message],
            tools: vec![],
            temperature: None,
            max_tokens: None,
        },
    )
}

fn with_output(mut message: Message, output: Value) -> Message {
    message.metadata.insert(OUTPUT.into(), output);
    message
}

#[test]
fn replay_cannot_introduce_a_privileged_role_or_an_arbitrary_item() {
    for item in [
        json!({"type":"message","role":"system","content":[]}),
        json!({"type":"item_reference","id":"other-conversation"}),
    ] {
        let message = with_output(Message::new(Role::Assistant, ""), json!([item]));
        assert!(request(message).is_err());
    }
}

#[test]
fn user_metadata_never_becomes_provider_state() {
    let message = with_output(
        Message::new(Role::User, "Hi"),
        json!([{"type":"reasoning"}]),
    );
    assert_eq!(
        request(message).unwrap()["input"],
        json!([{"role":"user","content":"Hi"}])
    );
}

#[test]
fn replay_requires_the_same_visible_text_as_the_assistant_message() {
    let item = json!({"type":"message","role":"assistant",
        "content":[{"type":"output_text","text":"different"}]});
    let message = with_output(Message::new(Role::Assistant, "original"), json!([item]));
    assert!(request(message).is_err());
}

#[test]
fn replay_uses_normalized_calls_and_does_not_restore_dropped_calls() {
    let item =
        json!({"type":"function_call","call_id":"duplicate","name":"lookup","arguments":"["});
    let call = ToolCall::new("normalized", "lookup", Metadata::new());
    let message = with_output(
        Message::assistant_calls("", vec![call]),
        json!([item, item]),
    );
    assert_eq!(
        request(message).unwrap()["input"],
        json!([
            {"type":"function_call","call_id":"normalized","name":"lookup","arguments":"{}"}
        ])
    );
}

#[test]
fn a_complete_reasoning_item_needs_its_opaque_payload_for_stateless_replay() {
    let response =
        json!({"status":"completed","output":[{"type":"reasoning","id":"rs_1","summary":[]}]});
    assert!(wire::response(&response, "").is_err());
}

#[test]
fn a_cut_reasoning_item_without_its_payload_is_not_replayed() {
    let response = json!({"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},
        "output":[{"type":"reasoning","id":"rs_1","summary":[]}]});
    let decoded = wire::response(&response, "").unwrap();
    assert_eq!(decoded.done.finish_reason.as_deref(), Some("max_tokens"));
    assert_eq!(decoded.done.metadata[OUTPUT], json!([]));
}

#[test]
fn unsupported_hosted_output_does_not_silently_disappear() {
    let response = json!({"status":"completed","output":[{"type":"web_search_call","id":"ws_1"}]});
    assert!(wire::response(&response, "").is_err());
}

#[test]
fn an_impossible_cached_count_does_not_inflate_total_input() {
    let response = json!({"status":"completed","output":[],"usage":{
        "input_tokens":10,"input_tokens_details":{"cached_tokens":20},"output_tokens":2}});
    assert_eq!(
        wire::response(&response, "").unwrap().done.usage,
        serde_json::from_value::<Metadata>(
            json!({"input_tokens":0,"cache_read_input_tokens":10,"output_tokens":2})
        )
        .unwrap()
    );
}
