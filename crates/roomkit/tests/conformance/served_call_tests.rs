//! A provider marker must reach the production loop through each fixture door.

use std::future::poll_fn;
use std::sync::Arc;

use roomkit_ai::{AiContext, AiProvider, ServedCall, StreamEvent};
use serde_json::{Value, json};

#[test]
fn malformed_provider_markers_are_rejected_at_both_call_shapes() {
    for served in [
        json!(false),
        json!("forged"),
        json!([]),
        json!({"extra": 1}),
        json!({"result": 1}),
        json!({"is_error": 1}),
    ] {
        for stream in [false, true] {
            let call = json!({"id": "c1", "name": "lookup", "served": served});
            let round = if stream {
                json!({"stream": [{"call": call}]})
            } else {
                json!({"calls": [call]})
            };
            let scenario = json!({"setup": {"channels": [{"id": "agent", "type": "ai", "model": {"rounds": [round]}}]}});
            let problems = crate::shape::unknown_members(&scenario);
            assert!(
                problems.iter().any(|problem| problem.contains("served")),
                "{scenario}: {problems:?}"
            );
        }
    }
}

#[tokio::test]
async fn provider_markers_reach_whole_regular_and_explicit_stream_fixtures() {
    let mut store = None;
    let (kit, _kept) = crate::new_kit(|options| {
        store = Some(Arc::clone(&options.store));
        options
    })
    .await;
    let store = store.unwrap();
    let run = Arc::new(crate::run::Run::new(
        kit,
        Arc::clone(&store),
        store,
        &json!({}),
    ));
    for streaming in [false, true] {
        for explicit in [false, true] {
            let first = json!({"id": "same", "name": "lookup", "served": {}});
            let second = json!({"id": "same", "name": "lookup", "served": {"result": "second", "is_error": true}});
            let third = json!({"id": "last", "name": "lookup", "served": null});
            let round = match (streaming, explicit) {
                (true, true) => {
                    json!({"stream": [{"call": first}, {"call": second}, {"call": third}]})
                }
                (false, true) => json!({"whole_response": {}, "calls": [first, second, third]}),
                (_, false) => json!({"calls": [first, second, third]}),
            };
            check_fixture(&run, json!({"streaming": streaming, "rounds": [round]})).await;
        }
    }
    run.kit().close().await.unwrap();
}

async fn check_fixture(run: &Arc<crate::run::Run>, spec: Value) {
    let (_, model) = crate::agents::scripted_model(&spec, run, "agent", None).unwrap();
    let context = AiContext::default();
    let mut events = model.generate_structured_stream(&context);
    let mut markers = Vec::new();
    while let Some(event) = poll_fn(|cx| events.as_mut().poll_next(cx)).await {
        if let StreamEvent::ToolCall(call) = event.unwrap() {
            markers.push(call.served);
        }
    }
    assert_eq!(
        markers,
        [
            Some(ServedCall::default()),
            Some(ServedCall {
                result: "second".into(),
                is_error: true
            }),
            None
        ]
    );
}
