//! Responses against a local HTTP peer: the real provider and tool loop,
//! without credentials or a remote account (RFC §6.4, §6.7).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use pretty_assertions::assert_eq;
use roomkit::model::{EventContent, InboundMessage, RoomId};
use roomkit::{AttachChannelOptions, CreateRoomOptions, ProcessInboundOptions};
use roomkit_ai::{ToolDefinition, ToolOutput, Tools};
use roomkit_openai::OpenAiApi;
use serde_json::{Value, json};

use super::{API_KEY, Reply, Room, SYSTEM_PROMPT, channel, next, on_error};

fn message(text: &str) -> Value {
    json!({"type":"message", "role":"assistant", "status":"completed",
        "phase":"commentary", "content":[{"type":"output_text", "text":text, "annotations":[]}]})
}

fn complete(output: Vec<Value>) -> Value {
    let mut response = json!({"id":"resp_1", "status":"completed", "model":"fake-reasoning-model",
        "usage":{"input_tokens":30,"output_tokens":6,
        "input_tokens_details":{"cached_tokens":20},"output_tokens_details":{"reasoning_tokens":4}}});
    response["output"] = Value::Array(output);
    response
}

fn terminal(response: Value) -> Value {
    let mut event = json!({"type":format!("response.{}", response["status"].as_str().unwrap())});
    event["response"] = response;
    event
}

fn answer(text: &str) -> Reply {
    Reply::events(&[terminal(complete(vec![message(text)]))])
}

fn call(id: &str, arguments: &str) -> Value {
    json!({"type":"function_call", "id":format!("fc_{id}"), "call_id":id,
        "name":"lookup", "arguments":arguments, "status":"completed"})
}

fn reasoning(id: &str) -> Value {
    json!({"type":"reasoning", "id":id, "summary":[], "encrypted_content":format!("opaque-{id}")})
}

async fn configured(replies: Vec<Reply>, tools: Tools) -> Room {
    Room::built(
        replies,
        API_KEY,
        |config| {
            config
                .with_api(OpenAiApi::Responses)
                .with_reasoning_effort("medium")
        },
        |agent| agent.with_tools(tools).unwrap(),
    )
    .await
}

fn lookup(count: &Arc<AtomicUsize>) -> Tools {
    let count = Arc::clone(count);
    let tool = ToolDefinition::new(
        "lookup",
        "Find an order",
        json!({"type":"object",
        "properties":{"order":{"type":"string"}}, "required":["order"]}),
    );
    Tools::new().tool(tool, move |call, _| {
        count.fetch_add(1, Ordering::SeqCst);
        async move { Ok(ToolOutput::Json(json!({"order":call.arguments["order"]}))) }
    })
}

#[tokio::test]
async fn a_responses_request_is_stateless_with_instructions_and_flat_tools() {
    let room = configured(vec![answer("Yes.")], lookup(&Arc::new(AtomicUsize::new(0)))).await;
    let result = room.says(&room.sms, "alice", "Alice", "Is it ready?").await;
    assert_eq!(result.error, None);
    let requests = room.fake.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, "/v1/responses");
    assert_eq!(requests[0].authorization.as_deref(), Some("Bearer sk-test"));
    assert_eq!(
        requests[0].body,
        json!({"model":"fake-model", "instructions":SYSTEM_PROMPT,
        "input":[{"role":"user", "content":"Is it ready?"}], "stream":true, "store":false,
        "include":["reasoning.encrypted_content"], "reasoning":{"effort":"medium"},
        "tools":[{"type":"function", "name":"lookup", "description":"Find an order",
            "parameters":{"type":"object", "properties":{"order":{"type":"string"}},
                "required":["order"]}, "strict":false}]})
    );
}

#[tokio::test]
async fn streamed_text_is_not_duplicated_and_usage_counts_cached_tokens_separately() {
    let reply = Reply::events(&[
        json!({"type":"response.created", "response":{"status":"in_progress"}}),
        json!({"type":"response.output_text.delta", "delta":"Ready "}),
        json!({"type":"response.output_text.delta", "delta":"at "}),
        terminal(complete(vec![message("Ready at nine.")])),
    ]);
    let room = configured(vec![reply], Tools::new()).await;
    assert_eq!(
        room.says(&room.sms, "alice", "Alice", "When?").await.error,
        None
    );
    assert_eq!(room.sms.texts(), ["Ready at nine."]);
    let event = room.timeline().await.pop().unwrap();
    assert_eq!(
        event.metadata["ai_usage"],
        json!({"input_tokens":10,"output_tokens":6,
        "cache_read_input_tokens":20,"reasoning_tokens":4})
    );
}

#[tokio::test]
async fn whole_json_responses_use_the_same_decoder_without_requiring_a_key() {
    let body = complete(vec![message("Yes.")]).to_string();
    let room = Room::configured(vec![Reply::status(200, &body)], "", |config| {
        config.with_api(OpenAiApi::Responses)
    })
    .await;
    assert_eq!(
        room.says(&room.sms, "alice", "Alice", "Ready?").await.error,
        None
    );
    assert_eq!(room.sms.texts(), ["Yes."]);
    assert_eq!(room.fake.requests()[0].authorization, None);
}

#[tokio::test]
async fn multiple_tool_rounds_replay_order_phases_and_reasoning_without_persisting_them() {
    let count = Arc::new(AtomicUsize::new(0));
    let first = vec![
        message("Checking."),
        reasoning("rs_1"),
        call("a", r#"{"order":"42"}"#),
    ];
    let second = vec![
        reasoning("rs_2"),
        message("One more."),
        call("b", r#"{"order":"43"}"#),
    ];
    let room = configured(
        vec![
            Reply::events(&[terminal(complete(first.clone()))]),
            Reply::events(&[terminal(complete(second.clone()))]),
            answer("Both shipped."),
        ],
        lookup(&count),
    )
    .await;
    assert_eq!(
        room.says(&room.sms, "alice", "Alice", "Check orders.")
            .await
            .error,
        None
    );
    assert_eq!(count.load(Ordering::SeqCst), 2);
    let requests = room.fake.requests();
    let input = requests[2].body["input"].as_array().unwrap();
    assert_eq!(input.len(), 9);
    assert_eq!(input[1], first[0]);
    assert_eq!(input[2], first[1]);
    assert_eq!(input[3]["call_id"], "a");
    assert_eq!(input[3]["id"], "fc_a");
    assert_eq!(
        input[4],
        json!({"type":"function_call_output","call_id":"a","output":r#"{"order":"42"}"#})
    );
    assert_eq!(input[5], second[0]);
    assert_eq!(input[6], second[1]);
    assert_eq!(input[7]["call_id"], "b");
    assert_eq!(input[8]["call_id"], "b");
    let stored = serde_json::to_string(&room.timeline().await).unwrap();
    assert!(!stored.contains("encrypted_content"));
    assert!(!stored.contains("opaque-rs_"));

    // A later user turn rebuilds the visible room transcript; there is no
    // response-id or provider cache silently tying it to the previous turn.
    assert_eq!(
        room.says(&room.sms, "alice", "Alice", "Thanks.")
            .await
            .error,
        None
    );
    let requests = room.fake.requests();
    assert!(!requests[3].body.to_string().contains("opaque-rs_"));
    assert!(requests[3].body.get("previous_response_id").is_none());
}

#[tokio::test]
async fn one_provider_has_no_reasoning_state_to_leak_to_another_room() {
    let count = Arc::new(AtomicUsize::new(0));
    let room = configured(
        vec![
            Reply::events(&[terminal(complete(vec![
                reasoning("private"),
                call("a", r#"{"order":"42"}"#),
            ]))]),
            answer("Shipped."),
            answer("Hello."),
        ],
        lookup(&count),
    )
    .await;
    assert_eq!(
        room.says(&room.sms, "alice", "Alice", "Check.").await.error,
        None
    );
    let other = RoomId::new("other").unwrap();
    room.kit
        .create_room(CreateRoomOptions {
            room_id: Some(other.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
    for id in ["web", "assistant"] {
        room.kit
            .attach_channel(&other, &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let incoming = InboundMessage::new(channel("web"), "bob", EventContent::text("Hi"));
    assert_eq!(
        room.kit
            .process_inbound(
                incoming,
                ProcessInboundOptions {
                    room_id: Some(other),
                    ..Default::default()
                }
            )
            .await
            .unwrap()
            .error,
        None
    );
    let requests = room.fake.requests();
    assert_eq!(
        requests[2].body["input"],
        json!([{"role":"user","content":"Hi"}])
    );
}

#[tokio::test]
async fn a_dropped_stream_never_executes_an_announced_call() {
    let event = json!({"type":"response.output_item.done","output_index":0,"item":call("a", r#"{"order":"42"}"#)});
    for reply in [
        Reply::sse(&format!("data: {event}\n\n")),
        Reply::events(&[event]),
    ] {
        let count = Arc::new(AtomicUsize::new(0));
        let room = configured(vec![reply], lookup(&count)).await;
        let error = room
            .says(&room.sms, "alice", "Alice", "Check.")
            .await
            .error
            .unwrap();
        assert!(error.message.contains("terminal"), "{error:?}");
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert_eq!(room.fake.requests().len(), 1);
    }
}

#[tokio::test]
async fn a_failed_stream_reports_its_error_without_echoing_the_credential() {
    let reply = Reply::events(&[
        json!({"type":"response.output_item.done","output_index":0,"item":call("a", "{}")}),
        json!({"type":"response.failed", "response":{"status":"failed",
            "error":{"code":"quota_exceeded", "message":"Rejected bearer sk-test"}}}),
    ]);
    let count = Arc::new(AtomicUsize::new(0));
    let room = configured(vec![reply], lookup(&count)).await;
    let mut reported = on_error(&room);
    let error = room
        .says(&room.sms, "alice", "Alice", "Check.")
        .await
        .error
        .unwrap();
    assert!(error.message.contains("quota_exceeded"));
    assert!(!error.message.contains(API_KEY));
    assert!(
        !serde_json::to_string(&next(&mut reported).await)
            .unwrap()
            .contains(API_KEY)
    );
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn an_incomplete_call_is_reported_as_partial_and_never_runs() {
    let mut cut = complete(vec![call("a", r#"{"order":"#)]);
    cut["status"] = "incomplete".into();
    cut["incomplete_details"] = json!({"reason":"max_output_tokens"});
    let count = Arc::new(AtomicUsize::new(0));
    let room = configured(
        vec![Reply::events(&[terminal(cut)]), answer("The call was cut.")],
        lookup(&count),
    )
    .await;
    assert_eq!(
        room.says(&room.sms, "alice", "Alice", "Check.").await.error,
        None
    );
    assert_eq!(count.load(Ordering::SeqCst), 0);
    let requests = room.fake.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1].body["input"][2]["type"], "function_call_output");
    assert!(
        requests[1].body["input"][2]["output"]
            .as_str()
            .unwrap()
            .contains("Tool call cut off")
    );
}

#[tokio::test]
async fn terminal_status_mismatches_and_changed_text_fail() {
    let mut mismatch = terminal(complete(vec![message("Yes.")]));
    mismatch["response"]["status"] = "incomplete".into();
    for reply in [
        Reply::events(&[mismatch]),
        Reply::events(&[
            json!({"type":"response.output_text.delta","delta":"Other"}),
            terminal(complete(vec![message("Yes.")])),
        ]),
    ] {
        let room = configured(vec![reply], Tools::new()).await;
        assert!(
            room.says(&room.sms, "alice", "Alice", "Hi.")
                .await
                .error
                .is_some()
        );
    }
}

#[tokio::test]
async fn a_refusal_is_visible_text_without_becoming_a_tool_call() {
    let output = json!({"type":"message","role":"assistant","content":[{"type":"refusal","refusal":"Cannot help."}]});
    let room = configured(
        vec![Reply::events(&[
            json!({"type":"response.refusal.delta","delta":"Cannot "}),
            terminal(complete(vec![output])),
        ])],
        Tools::new(),
    )
    .await;
    assert_eq!(
        room.says(&room.sms, "alice", "Alice", "Hi.").await.error,
        None
    );
    assert_eq!(room.sms.texts(), ["Cannot help."]);
}

#[tokio::test]
async fn an_eof_after_a_terminal_event_without_its_separator_still_completes() {
    let event = terminal(complete(vec![message("Yes.")]));
    let room = configured(vec![Reply::sse(&format!("data: {event}"))], Tools::new()).await;
    assert_eq!(
        room.says(&room.sms, "alice", "Alice", "Hi.").await.error,
        None
    );
    assert_eq!(room.sms.texts(), ["Yes."]);
}

#[tokio::test]
async fn namespaced_tools_round_trip_with_fresh_credentials_and_history() {
    use roomkit_openai::{TokenFuture, TokenSource};
    struct Credentials(AtomicUsize);
    impl TokenSource for Credentials {
        fn token(&self) -> TokenFuture<'_> {
            let round = self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok(format!("round-{round}")) })
        }
    }
    let mut item = call("one", r#"{"order":"bread"}"#);
    item["namespace"] = "functions".into();
    let count = Arc::new(AtomicUsize::new(0));
    let tools = lookup(&count);
    let room = Room::built(
        vec![
            Reply::events(&[terminal(complete(vec![item]))]),
            answer("Ready"),
            answer("Again"),
        ],
        "unused-static",
        |config| {
            config
                .with_api(OpenAiApi::Responses)
                .with_tool_namespace("functions", "Available tools")
                .with_token_source(Arc::new(Credentials(AtomicUsize::new(0))))
        },
        |agent| agent.with_tools(tools).unwrap(),
    )
    .await;
    assert_eq!(
        room.says(&room.sms, "alice", "Alice", "First").await.error,
        None
    );
    assert_eq!(
        room.says(&room.sms, "alice", "Alice", "Second").await.error,
        None
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let requests = room.fake.requests();
    for (round, request) in requests.iter().enumerate() {
        assert_eq!(
            request.authorization.as_deref(),
            Some(format!("Bearer round-{round}").as_str())
        );
        assert_eq!(request.body["tools"][0]["type"], "namespace");
        assert_eq!(request.body["tools"][0]["name"], "functions");
        assert_eq!(request.body["tools"][0]["tools"][0]["name"], "lookup");
        for item in request.body["input"].as_array().unwrap() {
            if item["type"] == "function_call" {
                assert_eq!(item["namespace"], "functions");
            }
        }
    }
}

#[tokio::test]
async fn a_call_in_an_unknown_namespace_never_executes() {
    let mut item = call("one", r#"{"order":"bread"}"#);
    item["namespace"] = "unadvertised".into();
    let count = Arc::new(AtomicUsize::new(0));
    let tools = lookup(&count);
    let room = Room::built(
        vec![Reply::events(&[terminal(complete(vec![item]))])],
        "",
        |config| {
            config
                .with_api(OpenAiApi::Responses)
                .with_tool_namespace("functions", "Available tools")
        },
        |agent| agent.with_tools(tools).unwrap(),
    )
    .await;
    assert!(
        room.says(&room.sms, "alice", "Alice", "First")
            .await
            .error
            .is_some()
    );
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn item_events_supply_text_tools_reasoning_and_usage_when_terminal_output_is_empty() {
    let count = Arc::new(AtomicUsize::new(0));
    let items = [
        reasoning("rs_thin"),
        message("Checking."),
        call("thin", r#"{"order":"42"}"#),
    ];
    let mut events = vec![json!({"type":"response.output_text.delta","delta":"Checking."})];
    // Out-of-order completion and an identical repeated event remain deterministic.
    for index in [2, 0, 1, 2] {
        events.push(
            json!({"type":"response.output_item.done","output_index":index,"item":items[index]}),
        );
    }
    events.push(terminal(complete(vec![])));
    let final_events = [
        json!({"type":"response.output_item.added","output_index":0,"item":{"type":"message"}}),
        json!({"type":"response.output_text.delta","delta":"Ready."}),
        json!({"type":"response.output_item.done","output_index":0,"item":message("Ready.")}),
        terminal(complete(vec![])),
    ];
    let room = configured(
        vec![Reply::events(&events), Reply::events(&final_events)],
        lookup(&count),
    )
    .await;
    assert_eq!(
        room.says(&room.sms, "alice", "Alice", "Check.").await.error,
        None
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(room.sms.texts().join(""), "Checking.Ready.");
    let requests = room.fake.requests();
    let input = requests[1].body["input"].as_array().unwrap();
    assert_eq!(input[1], items[0]);
    assert_eq!(input[2], items[1]);
    assert_eq!(input[3]["call_id"], "thin");
    assert_eq!(input[4]["type"], "function_call_output");
    assert_eq!(
        room.timeline().await.last().unwrap().metadata["ai_usage"]["output_tokens"],
        12
    );
}

#[tokio::test]
async fn inconsistent_item_streams_never_execute_tools() {
    for prefix in [
        vec![json!({"type":"response.output_item.done","output_index":1,"item":call("a", "{}")})],
        vec![json!({"type":"response.output_item.added","output_index":0,"item":call("a", "{}")})],
        vec![
            json!({"type":"response.output_item.done","output_index":0,"item":call("a", "{}")}),
            json!({"type":"response.output_item.done","output_index":0,"item":call("b", "{}")}),
        ],
        vec![
            json!({"type":"response.output_item.done","output_index":4096,"item":call("a", "{}")}),
        ],
    ] {
        let count = Arc::new(AtomicUsize::new(0));
        let mut events = prefix;
        events.push(terminal(complete(vec![])));
        let room = configured(vec![Reply::events(&events)], lookup(&count)).await;
        assert!(
            room.says(&room.sms, "alice", "Alice", "Check.")
                .await
                .error
                .is_some()
        );
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }
}
