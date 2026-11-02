//! `MockAiProvider`, Python's `MockAIProvider`: an AI channel over it
//! answers with its responses, round-robin, and the provider keeps what each
//! turn asked.

use pretty_assertions::assert_eq;
use roomkit::model::{EventType, RoomEvent};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiProvider, AiResponse, MockAiProvider, MockAiProviderOptions,
};

use super::support::{Room, channel};

/// The texts the agent answered, in order.
fn answers(timeline: &[RoomEvent]) -> Vec<String> {
    timeline
        .iter()
        .filter(|event| {
            event.r#type == EventType::Message && event.source.channel_id == channel("assistant")
        })
        .map(|event| roomkit_core::rules::transcoding::text_of(&event.content))
        .collect()
}

/// Python's mock: its texts round-robin, `Hello from AI` without any, whole
/// responses in their place when given; it keeps each context, and names
/// itself `MockAIProvider`, its model `mock`.
#[tokio::test]
async fn a_channel_over_the_mock_answers_its_responses_round_robin() {
    let cases = [
        (
            vec!["One.".to_owned(), "Two.".to_owned()],
            None,
            ["One.", "Two.", "One."],
        ),
        (Vec::new(), None, ["Hello from AI"; 3]),
        (
            vec!["unused".to_owned()],
            Some(vec![AiResponse::new("Whole.")]),
            ["Whole."; 3],
        ),
    ];
    for (responses, ai_responses, expected) in cases {
        for streaming in [false, true] {
            let model = MockAiProvider::new(MockAiProviderOptions {
                responses: responses.clone(),
                ai_responses: ai_responses.clone(),
                streaming,
                ..MockAiProviderOptions::default()
            });
            let options = AiChannelOptions::default();
            let agent = AiChannel::new(channel("assistant"), model.clone(), options).unwrap();
            let room = Room::with_agent(agent).await;
            for question in ["Hi?", "Again?", "Once more?"] {
                room.says(question).await;
            }
            assert_eq!(
                (
                    answers(&room.timeline().await),
                    model.calls().len(),
                    model.name(),
                    model.model_name(),
                    model.supports_streaming(),
                ),
                (
                    expected.map(str::to_owned).to_vec(),
                    3,
                    "MockAIProvider",
                    "mock",
                    streaming
                ),
                "{responses:?} {streaming}"
            );
            room.kit.close().await.unwrap();
        }
    }
}

/// The capabilities of an agent over a mock that sees, or does not.
fn capabilities_over(vision: bool) -> roomkit::model::ChannelCapabilities {
    let options = MockAiProviderOptions {
        vision,
        ..MockAiProviderOptions::default()
    };
    let provider = MockAiProvider::new(options);
    let agent = AiChannel::new(channel("ai1"), provider, AiChannelOptions::default()).unwrap();
    roomkit::Channel::capabilities(&agent)
}

// Python: tests/test_channels/test_ai.py::TestAIChannel::test_capabilities_with_vision_provider
#[test]
fn capabilities_with_vision_provider() {
    let capabilities = capabilities_over(true);
    assert!(
        capabilities
            .media_types
            .contains(&roomkit::model::ChannelMediaType::Media)
    );
    assert!(capabilities.supports_media);
}

// Python: tests/test_channels/test_ai.py::TestAIChannel::test_capabilities_without_vision
/// A model that does not see still takes media, rich content too: the
/// channel reads each media itself, the event a turn answers as the same
/// event in its history (RFC §6.4).
#[test]
fn capabilities_without_vision() {
    use roomkit::model::ChannelMediaType;
    let capabilities = capabilities_over(false);
    for kind in [
        ChannelMediaType::Text,
        ChannelMediaType::Rich,
        ChannelMediaType::Media,
    ] {
        assert!(capabilities.media_types.contains(&kind), "{kind:?}");
    }
    assert!(capabilities.supports_media);
    assert!(capabilities.supports_rich_text);
}
