//! Public Chat Completions rendering (RFC §6.7).
use roomkit_ai::{
    AiContentPart, AiMessage, AiRole, AiTextPart, AiThinkingPart, OPENAI_CHAT, chat_messages,
};
use serde_json::json;

#[test]
fn plain_text_and_parts_keep_distinct_wire_shapes() {
    let messages = [
        AiMessage::new(AiRole::User, "hi"),
        AiMessage::new(
            AiRole::Assistant,
            vec![AiContentPart::Text(AiTextPart::new("done"))],
        ),
        AiMessage::new(
            AiRole::Assistant,
            vec![AiContentPart::Thinking(AiThinkingPart::new("hidden"))],
        ),
        AiMessage::new(AiRole::User, Vec::new()),
        AiMessage::new(AiRole::User, ""),
    ];
    let rows = chat_messages(&messages, Some("brief"), &OPENAI_CHAT, "my-app").unwrap();
    assert_eq!(
        json!(rows),
        json!([
            {"role":"system","content":"brief"},
            {"role":"user","content":"hi"},
            {"role":"assistant","content":[{"type":"text","text":"done"}]},
            {"role":"assistant","content":""},
            {"role":"user","content":[]},
            {"role":"user","content":""}
        ])
    );
}

#[test]
fn a_call_wins_over_results_and_keeps_python_argument_text() {
    use roomkit_ai::{AiToolCallPart, AiToolResultPart};
    let mut call = AiToolCallPart::new("c1", "lookup");
    call.arguments.insert("q".into(), json!("café"));
    call.arguments.insert("n".into(), json!(42));
    for role in [AiRole::Assistant, AiRole::Tool] {
        let msg = AiMessage::new(
            role,
            vec![
                AiContentPart::Thinking(AiThinkingPart::new("hidden")),
                AiContentPart::Text(AiTextPart::new("looking")),
                AiContentPart::ToolCall(call.clone()),
                AiContentPart::ToolResult(AiToolResultPart::new("c1", "lookup", "done")),
            ],
        );
        assert_eq!(
            json!(chat_messages(&[msg], None, &OPENAI_CHAT, "app").unwrap()),
            json!([
            {"role":"assistant","content":"looking","tool_calls":[
                {"id":"c1","type":"function","function":{"name":"lookup","arguments":"{\"q\": \"caf\\u00e9\", \"n\": 42}"}}
            ]}])
        );
    }
}

#[test]
fn messages_keep_image_positions_and_errors_name_the_caller() {
    use roomkit_ai::AiImagePart;
    let image = AiContentPart::Image(AiImagePart {
        url: "data:;base64,YQ".into(),
        mime_type: Some("image/png".into()),
    });
    let msg = AiMessage::new(
        AiRole::User,
        vec![
            AiContentPart::Text(AiTextPart::new("a")),
            image,
            AiContentPart::Text(AiTextPart::new("b")),
        ],
    );
    assert_eq!(
        json!(chat_messages(&[msg], None, &OPENAI_CHAT, "app").unwrap()),
        json!([{"role":"user","content":[
            {"type":"text","text":"a"}, {"type":"image_url","image_url":{"url":"data:image/png;base64,YQ=="}}, {"type":"text","text":"b"}
        ]}])
    );
    let bad = AiMessage::new(
        AiRole::User,
        vec![AiContentPart::Image(AiImagePart::new("data:broken"))],
    );
    let err = chat_messages(&[bad], None, &OPENAI_CHAT, "app").unwrap_err();
    assert_eq!(err.provider, "app");
    assert!(!err.retryable);
    assert_eq!(
        err.message,
        "invalid image part: data URI carries no payload"
    );
}

#[test]
fn images_from_tool_results_follow_all_the_tool_rows() {
    use roomkit_ai::{AiImagePart, AiToolResultPart, ToolResult, ToolResultPart};
    let content = ToolResult::Parts(vec![
        ToolResultPart::Text(AiTextPart::new("found")),
        ToolResultPart::Image(AiImagePart::new("https://example.org/image.png")),
    ]);
    let msg = AiMessage::new(
        AiRole::Tool,
        vec![
            AiContentPart::ToolResult(AiToolResultPart::new("c2", "second", content)),
            AiContentPart::ToolResult(AiToolResultPart::new("c1", "first", "done")),
        ],
    );
    assert_eq!(
        json!(chat_messages(&[msg], Some(""), &OPENAI_CHAT, "app").unwrap()),
        json!([
            {"role":"tool","content":"found","tool_call_id":"c2"},
            {"role":"tool","content":"done","tool_call_id":"c1"},
            {"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.org/image.png"}}]}
        ])
    );
}
#[test]
fn dialect_validates_dependencies_at_construction() {
    use roomkit_ai::{ChatDialect, ChatDialectOptions};
    assert_eq!(ChatDialect::default(), OPENAI_CHAT);
    assert_eq!(
        ChatDialect::new(ChatDialectOptions::default()).unwrap(),
        OPENAI_CHAT
    );
    for (options, error) in [
        (
            ChatDialectOptions {
                round_thinking_required: true,
                ..Default::default()
            },
            "round_thinking_required needs a thinking_field to send",
        ),
        (
            ChatDialectOptions {
                round_thinking_placeholder: "x".into(),
                ..Default::default()
            },
            "round_thinking_placeholder needs round_thinking_required",
        ),
    ] {
        assert_eq!(ChatDialect::new(options).unwrap_err().to_string(), error);
    }
}

#[test]
fn an_application_can_combine_all_five_dialect_options() {
    use roomkit_ai::{AiToolCallPart, AiToolResultPart, ChatDialect, ChatDialectOptions};
    let dialect = ChatDialect::new(ChatDialectOptions {
        thinking_field: Some("my_reason".into()),
        names_tool_results: true,
        flattens_text: true,
        round_thinking_required: true,
        round_thinking_placeholder: ".".into(),
    })
    .unwrap();
    let text = |s| AiContentPart::Text(AiTextPart::new(s));
    let call = || AiContentPart::ToolCall(AiToolCallPart::new("c", "x"));
    let messages = [
        AiMessage::new(AiRole::Assistant, vec![text("A"), text("B")]),
        AiMessage::new(
            AiRole::Assistant,
            vec![AiContentPart::Thinking(AiThinkingPart::new("only"))],
        ),
        AiMessage::new(AiRole::User, ""),
        AiMessage::new(AiRole::Assistant, vec![call()]),
        AiMessage::new(AiRole::Tool, vec![call()]),
        AiMessage::new(
            AiRole::Tool,
            vec![AiContentPart::ToolResult(AiToolResultPart::new(
                "c", "x", "ok",
            ))],
        ),
    ];
    let row = json!({"id":"c","type":"function","function":{"name":"x","arguments":"{}"}});
    assert_eq!(
        json!(chat_messages(&messages, None, &dialect, "my-app").unwrap()),
        json!([
            {"role":"assistant","content":"AB"},
            {"role":"assistant","content":null,"tool_calls":[row],"my_reason":"."},
            {"role":"assistant","content":null,"tool_calls":[row]},
            {"role":"tool","tool_call_id":"c","content":"ok","name":"x"}
        ])
    );
}
