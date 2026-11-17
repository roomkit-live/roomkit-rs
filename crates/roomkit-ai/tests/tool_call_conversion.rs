//! Conversion of provider calls between complete and streamed responses.

use roomkit_ai::{AiToolCall, ServedCall, stream_call_of, tool_call_of};
use serde_json::json;

#[test]
fn both_conversions_keep_all_fields_and_own_their_nested_values() {
    let mut call = AiToolCall::new("call", "lookup");
    call.arguments.insert("query".into(), json!({"term":"one"}));
    call.metadata.insert("receipt".into(), json!({"id":1}));
    call.served = Some(ServedCall {
        result: "first".into(),
        is_error: true,
    });
    call.intent = Some("Look it up.".into());
    call.partial = true;
    call.garbled = true;
    let mut streamed = stream_call_of(&call);
    let mut returned = tool_call_of(&streamed);
    assert_eq!(returned, call);
    streamed.arguments["query"]["term"] = json!("two");
    returned.arguments["query"]["term"] = json!("three");
    streamed.metadata["receipt"]["id"] = json!(2);
    returned.metadata["receipt"]["id"] = json!(3);
    streamed.served.as_mut().unwrap().result = "second".into();
    returned.served.as_mut().unwrap().result = "third".into();
    assert_eq!(call.arguments["query"]["term"], "one");
    assert_eq!(streamed.arguments["query"]["term"], "two");
    assert_eq!(call.metadata["receipt"]["id"], 1);
    assert_eq!(streamed.metadata["receipt"]["id"], 2);
    assert_eq!(call.served.as_ref().unwrap().result, "first");
    assert_eq!(streamed.served.as_ref().unwrap().result, "second");
}
