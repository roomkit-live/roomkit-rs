//! Provider-owned outcomes survive the carrier boundary.

use roomkit_ai::{AiToolCall, ServedCall, StreamToolCall};
use serde_json::json;

// Python: tests/test_served_call_marker.py::test_a_call_survives_streaming_and_back
#[test]
fn a_call_survives_streaming_and_back() {
    let mut call = AiToolCall::new("b1", "Bash");
    call.arguments.insert("cmd".to_owned(), json!("ls"));
    call.metadata
        .insert("signature".to_owned(), json!("provider-owned"));
    call.partial = true;
    call.garbled = true;
    call.intent = Some("I list the files.".to_owned());
    call.served = Some(ServedCall {
        result: "a.txt".to_owned(),
        is_error: true,
    });
    assert_eq!(AiToolCall::from(StreamToolCall::from(call.clone())), call);
}

#[test]
fn absence_and_an_empty_provider_result_remain_distinct() {
    let mut call = AiToolCall::new("c1", "lookup");
    assert!(StreamToolCall::from(call.clone()).served.is_none());
    call.served = Some(ServedCall::default());
    let stream = StreamToolCall::from(call);
    assert_eq!(
        stream.served,
        Some(ServedCall {
            result: String::new(),
            is_error: false
        })
    );
}
