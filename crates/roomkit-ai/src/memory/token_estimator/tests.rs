//! Python: `tests/test_memory_new.py` (`TestEstimateTokens`,
//! `TestEstimateMessageTokens`, `TestEstimateContextTokens`,
//! `TestHistoryBudget`), and `tests/test_compacting_memory_cost.py`.

use pretty_assertions::assert_eq;
use roomkit::model::{ChannelId, ChannelType, EventSource, MediaContent, RoomId, TextContent};
use serde_json::json;

use super::*;
use crate::provider::{AiImagePart, AiRole, AiTextPart, AiToolCallPart, AiToolResultPart};

fn event(content: EventContent) -> RoomEvent {
    RoomEvent::new(
        RoomId::new("r1").unwrap(),
        EventSource::new(ChannelId::new("sms").unwrap(), ChannelType::SMS),
        content,
    )
}

fn parts(role: AiRole, parts: Vec<AiContentPart>) -> AiMessage {
    AiMessage::new(role, parts)
}

// Python: TestEstimateTokens::test_empty_string, test_short_string,
// test_long_string and test_unicode_text
#[test]
fn a_text_costs_a_token_for_four_code_points() {
    assert_eq!(
        [
            estimate_tokens(""),
            estimate_tokens("Hello, world!"),
            estimate_tokens(&"x".repeat(400)),
            estimate_tokens("Bonjour le monde! 🌍"),
        ],
        [1, 4, 101, 5]
    );
}

// Python: TestEstimateMessageTokens::test_string_content and test_text_part_content
#[test]
fn a_message_costs_its_text_and_its_overhead() {
    let text = AiMessage::new(AiRole::User, "Hello world");
    let part = parts(
        AiRole::Assistant,
        vec![AiContentPart::Text(AiTextPart::new("Some response text"))],
    );
    assert_eq!(
        (
            estimate_message_tokens(&text),
            estimate_message_tokens(&part)
        ),
        (
            4 + estimate_tokens("Hello world"),
            4 + estimate_tokens("Some response text")
        )
    );
}

// Python: TestEstimateMessageTokens::test_image_part_content
#[test]
fn an_image_costs_a_flat_thousand() {
    let image = AiContentPart::Image(AiImagePart::new("https://example.com/img.jpg"));
    assert_eq!(
        estimate_message_tokens(&parts(AiRole::User, vec![image])),
        1004
    );
}

// Python: TestEstimateMessageTokens::test_tool_call_part, test_tool_result_part
// and test_mixed_content
#[test]
fn a_call_costs_its_name_and_its_arguments_as_json_and_a_result_its_text() {
    let mut call = AiToolCallPart::new("tc1", "search");
    call.arguments.insert("q".to_owned(), json!("test"));
    let calling = parts(
        AiRole::Assistant,
        vec![AiContentPart::ToolCall(call.clone())],
    );
    let result = AiToolResultPart::new("tc1", "search", "found it");
    let answered = parts(AiRole::Tool, vec![AiContentPart::ToolResult(result)]);
    let mixed = parts(
        AiRole::Assistant,
        vec![
            AiContentPart::Text(AiTextPart::new("Let me search")),
            AiContentPart::ToolCall(call),
        ],
    );
    // `{"q": "test"}`, as Python's json.dumps writes it: 13 code points.
    let call_tokens = estimate_tokens("search") + estimate_tokens(r#"{"q": "test"}"#);
    assert_eq!(
        [
            estimate_message_tokens(&calling),
            estimate_message_tokens(&answered),
            estimate_message_tokens(&mixed),
        ],
        [
            4 + call_tokens,
            4 + estimate_tokens("found it"),
            4 + estimate_tokens("Let me search") + call_tokens,
        ]
    );
}

// Python: TestEstimateContextTokens::test_empty_context, test_system_prompt_counted,
// test_messages_counted and test_tools_counted
#[test]
fn a_context_costs_its_prompt_its_messages_and_its_tools() {
    let empty = AiContext::default();
    let prompted = AiContext {
        system_prompt: Some("Be helpful".to_owned()),
        ..AiContext::default()
    };
    let messages = vec![
        AiMessage::new(AiRole::User, "hello"),
        AiMessage::new(AiRole::Assistant, "hi"),
    ];
    let spoken = AiContext {
        messages: messages.clone(),
        ..AiContext::default()
    };
    let tool = AiTool {
        parameters: json!({ "type": "object" }),
        ..AiTool::new("search", "Search the web").unwrap()
    };
    let tooled = AiContext {
        tools: vec![tool],
        ..AiContext::default()
    };
    assert_eq!(
        [
            estimate_context_tokens(&empty),
            estimate_context_tokens(&prompted),
            estimate_context_tokens(&spoken),
            estimate_context_tokens(&tooled),
        ],
        [
            0,
            estimate_tokens("Be helpful"),
            messages.iter().map(estimate_message_tokens).sum(),
            estimate_tokens("search")
                + estimate_tokens("Search the web")
                + estimate_tokens(r#"{"type": "object"}"#),
        ]
    );
}

// Python: TestHistoryBudget::test_margin_only_when_nothing_is_declared,
// test_margin_defaults_to_the_published_fifteen_percent and test_never_negative
#[test]
fn the_history_takes_the_window_less_its_margin_never_below_zero() {
    let reserved = HistoryBudgetOptions {
        reserved_tokens: 5000,
        ..HistoryBudgetOptions::default()
    };
    assert_eq!(
        (
            history_budget(1000, HistoryBudgetOptions::default()),
            history_budget(1000, reserved)
        ),
        (850, 0)
    );
}

// Python: TestHistoryBudget::test_reserved_and_messages_are_both_subtracted
#[test]
fn the_reserved_tokens_and_the_messages_are_both_subtracted() {
    let block = [AiMessage::new(AiRole::User, "x".repeat(400))];
    let options = HistoryBudgetOptions {
        reserved_tokens: 100,
        messages: Some(&block),
        ..HistoryBudgetOptions::default()
    };
    assert_eq!(
        history_budget(1000, options),
        850 - 100 - estimate_message_tokens(&block[0])
    );
}

/// The reply budget is reserved once: the larger of the margin and it,
/// and the current turn is paid for too.
#[test]
fn the_reply_and_the_current_turn_are_reserved() {
    let current = event(TextContent::new("x".repeat(40)).into());
    let options = |reply_tokens| HistoryBudgetOptions {
        reply_tokens,
        current_event: Some(&current),
        ..HistoryBudgetOptions::default()
    };
    assert_eq!(
        [
            history_budget(1000, options(100)),
            history_budget(1000, options(300))
        ],
        [850 - 11, 700 - 11]
    );
}

// Python: tests/test_compacting_memory_cost.py::test_images_by_url_fill_the_window_as_the_provider_bills_them
// and test_an_inline_image_does_not_count_its_base64
#[test]
fn an_image_costs_its_caption_and_a_flat_thousand_whatever_its_payload() {
    let by_url = MediaContent::new("https://cdn.example.com/a.png", "image/png").unwrap();
    let inline = MediaContent::new(
        format!("data:image/png;base64,{}", "A".repeat(40_000)),
        "image/png",
    )
    .unwrap();
    let captioned = MediaContent {
        caption: Some("A cat".to_owned()),
        ..by_url.clone()
    };
    let costs: Vec<usize> = [by_url, inline, captioned]
        .into_iter()
        .map(|media| {
            estimate_event_tokens(
                &event(EventContent::Media(media)),
                EstimateEventTokensOptions::default(),
            )
        })
        .collect();
    assert_eq!(costs, [1004, 1004, 1004 + estimate_tokens("A cat")]);
}

/// A media no vision model takes reads as its label, and a text-only
/// estimate leaves the image's flat cost out.
#[test]
fn a_document_reads_as_its_label_and_text_only_leaves_the_image_out() {
    let document = MediaContent {
        filename: Some("report.pdf".to_owned()),
        ..MediaContent::new("https://cdn.example.com/r.pdf", "application/pdf").unwrap()
    };
    let image = MediaContent::new("https://cdn.example.com/a.png", "image/png").unwrap();
    let text_only = EstimateEventTokensOptions { text_only: true };
    assert_eq!(
        (
            estimate_event_tokens(
                &event(EventContent::Media(document)),
                EstimateEventTokensOptions::default()
            ),
            estimate_event_tokens(&event(EventContent::Media(image)), text_only),
        ),
        (estimate_tokens("[Media: report.pdf]"), 0)
    );
}

/// What carries no text reads as Python's `str()` of it.
#[test]
fn a_location_reads_as_python_s_str_of_it() {
    let location = roomkit::model::LocationContent::new(
        roomkit::model::Latitude::new(45.5).unwrap(),
        roomkit::model::Longitude::new(-73.0).unwrap(),
    );
    assert_eq!(
        extract_event_text(&event(EventContent::Location(location))),
        "type='location' latitude=45.5 longitude=-73.0 label=None address=None"
    );
}
