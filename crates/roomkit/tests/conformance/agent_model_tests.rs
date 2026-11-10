//! Generation mode, concurrent reservation and cancellation in the fixture.

use crate::agents::{Paced, scripted_model};
use crate::run::{Run, lock};
use roomkit_ai::{AiContext, AiProvider, ProviderError, StreamEvent};
use serde_json::{Value, json};
use std::future::poll_fn;
use std::sync::Arc;

fn methods(run: &Run) -> Vec<&'static str> {
    let methods = Arc::clone(lock(&run.model_methods).get("agent").unwrap());
    lock(&methods).clone()
}

async fn text(model: &Paced) -> Result<String, ProviderError> {
    let context = AiContext::default();
    let mut stream = model.generate_structured_stream(&context);
    let mut text = String::new();
    while let Some(event) = poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
        if let StreamEvent::TextDelta(delta) = event? {
            text.push_str(&delta.text);
        }
    }
    Ok(text)
}

async fn reserved(run: &Arc<Run>, streaming: bool, cancel: bool) {
    let spec = json!({"streaming": streaming, "rounds": [
        {"gate": "first", "text": "first"}, {"text": "second"}, {"text": "third"}
    ]});
    let (_, model) = scripted_model(&spec, run, "agent", None).unwrap();
    let mut held = Box::pin(text(&model));
    tokio::select! {
        reached = run.gates.wait_for("first") => assert!(reached),
        result = &mut held => panic!("the first round escaped its gate: {result:?}"),
    }
    assert_eq!(text(&model).await.unwrap(), "second");
    if cancel {
        drop(held);
    } else {
        run.gates.open("first");
        assert_eq!(held.await.unwrap(), "first");
    }
    assert_eq!(text(&model).await.unwrap(), "third");
    let method = if streaming {
        "generate_structured_stream"
    } else {
        "generate"
    };
    assert_eq!(methods(run), vec![method; 3]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_generation_reserves_its_round_even_when_cancelled() {
    for streaming in [false, true] {
        for cancel in [false, true] {
            let mut store = None;
            let (kit, _kept) = crate::new_kit(|options| {
                store = Some(Arc::clone(&options.store));
                options
            })
            .await;
            let store = store.unwrap();
            let run = Arc::new(Run::new(kit, Arc::clone(&store), store, &json!({})));
            reserved(&run, streaming, cancel).await;
            run.kit().close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn a_whole_generation_failure_is_not_a_successful_empty_response() {
    let mut store = None;
    let (kit, _kept) = crate::new_kit(|options| {
        store = Some(Arc::clone(&options.store));
        options
    })
    .await;
    let store = store.unwrap();
    let run = Arc::new(Run::new(kit, Arc::clone(&store), store, &json!({})));
    let spec = json!({"streaming": false, "rounds": [{"fails": "broken"}, {"text": "next"}]});
    let (_, model) = scripted_model(&spec, &run, "agent", None).unwrap();
    assert!(
        text(&model)
            .await
            .unwrap_err()
            .to_string()
            .contains("broken")
    );
    assert_eq!(text(&model).await.unwrap(), "next");
    assert_eq!(methods(&run), vec!["generate"; 2]);
    run.kit().close().await.unwrap();
}

#[tokio::test]
async fn a_replacement_starts_its_own_generation_log() {
    let mut store = None;
    let (kit, _kept) = crate::new_kit(|options| {
        store = Some(Arc::clone(&options.store));
        options
    })
    .await;
    let store = store.unwrap();
    let run = Arc::new(Run::new(kit, Arc::clone(&store), store, &json!({})));
    let spec = json!({"streaming": false, "rounds": [{"text": "old"}]});
    let (_, old) = scripted_model(&spec, &run, "agent", None).unwrap();
    assert_eq!(text(&old).await.unwrap(), "old");
    let spec = json!({"rounds": [{"text": "replacement"}]});
    let (_, replacement) = scripted_model(&spec, &run, "agent", None).unwrap();
    assert_eq!(text(&replacement).await.unwrap(), "replacement");
    assert_eq!(methods(&run), vec!["generate_structured_stream"]);
    run.kit().close().await.unwrap();
}

fn refused_model(model: &Value, expected: &str) {
    for location in ["model", "fallback_model", "summarizer"] {
        let mut channel = json!({"id": "agent", "type": "ai", "model": {"rounds": []}});
        if location == "summarizer" {
            channel["memory"] = json!({"summarizer": model});
        } else {
            channel[location] = model.clone();
        }
        let scenario = json!({"setup": {"channels": [channel]}});
        assert!(
            crate::shape::unknown_members(&scenario)
                .iter()
                .any(|problem| problem.contains(expected)),
            "{location}: {model}"
        );
    }
}

#[test]
fn stream_only_fields_and_non_boolean_modes_are_refused() {
    for failure in [
        json!({"fails": "broken"}),
        json!({"refuses": {"status": 500, "message": "broken"}}),
    ] {
        let mut round = failure;
        round["whole_response"] = json!({});
        refused_model(
            &json!({"streaming": false, "rounds": [round]}),
            "whole_response",
        );
    }
    for field in [
        "deltas",
        "thinking",
        "stream",
        "done",
        "failure_after_calls",
        "failure_after_done",
    ] {
        refused_model(&json!({"streaming": false, "rounds": [{field: []}]}), field);
    }
    for flag in [Value::Null, json!(0), json!("false"), json!([])] {
        refused_model(&json!({"streaming": flag, "rounds": []}), "streaming");
    }
}
