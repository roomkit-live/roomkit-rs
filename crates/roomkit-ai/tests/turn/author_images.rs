//! The images a person sent, as a turn's provider refusal marks them (RFC
//! §6.4): what no shared scenario can say, the two implementations failing
//! differently on a schema outside the portable subset.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use pretty_assertions::assert_eq;
use roomkit::model::hook::ModifyOptions;
use roomkit::model::{HookResult, InboundMessage, MediaContent, RoomContext};
use roomkit::{HookOptions, HookTrigger, ProcessInboundOptions};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContentPart, AiGenerationEvent, AiMessageContent,
    MockAiProvider, MockAiProviderOptions,
};
use serde_json::json;

use super::support::{Room, channel, room_id};

/// A `BEFORE_AI_GENERATION` hook whose schema is outside the portable subset
/// fails the turn before any request (HITL 17bfa822), and that refusal is no
/// provider refusing the images the turn would have sent: Python's
/// assignment raises in the hook and nothing is refused, so the next turn
/// sends them as the person did.
#[tokio::test]
async fn a_hook_s_schema_outside_the_subset_refuses_no_image() {
    let model = MockAiProvider::new(MockAiProviderOptions {
        responses: vec!["Seen.".to_owned()],
        vision: true,
        response_schema: true,
        ..MockAiProviderOptions::default()
    });
    let agent = AiChannel::new(
        channel("assistant"),
        model.clone(),
        AiChannelOptions::default(),
    )
    .unwrap();
    let room = Room::with_agent(agent).await;
    let generations = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&generations);
    room.kit
        .hook(
            HookTrigger::BeforeAiGeneration,
            move |mut event: AiGenerationEvent, _context: RoomContext| {
                let first = counted.fetch_add(1, Ordering::SeqCst) == 0;
                async move {
                    if first && let serde_json::Value::Object(schema) = json!({ "type": "string" })
                    {
                        event.ai_context.response_schema = Some(schema);
                    }
                    HookResult::modify(event, ModifyOptions::default())
                }
            },
            HookOptions {
                name: "schema".to_owned(),
                ..Default::default()
            },
        )
        .unwrap();
    let image = MediaContent::new("data:image/png;base64,AQIDBA==", "image/png").unwrap();
    room.kit
        .process_inbound(
            InboundMessage::new(channel("sms"), "+15550001", image),
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    room.says("and now?").await;

    let asked = model.calls();
    assert_eq!(asked.len(), 1, "the first turn sent no request");
    let images: Vec<String> = asked[0]
        .messages
        .iter()
        .filter_map(|message| match &message.content {
            AiMessageContent::Parts(parts) => Some(parts),
            AiMessageContent::Text(_) => None,
        })
        .flatten()
        .filter_map(|part| match part {
            AiContentPart::Image(image) => Some(image.url.clone()),
            AiContentPart::Text(_)
            | AiContentPart::ToolCall(_)
            | AiContentPart::ToolResult(_)
            | AiContentPart::Thinking(_) => None,
        })
        .collect();
    assert_eq!(images, ["data:image/png;base64,AQIDBA=="]);
    room.kit.close().await.unwrap();
}
