//! The thinker's reader consumes attribution produced by the actual public kit.

use std::time::Duration;

use roomkit::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, InboundMessage, RoomContext, RoomEvent,
    RoomId, TextContent,
};
use roomkit::{
    AttachChannelOptions, Channel, CreateRoomOptions, ProcessInboundOptions, RoomKit,
    RoomKitOptions,
};
use roomkit_ai::{AiChannel, AiChannelOptions, MockAiProvider, MockAiProviderOptions};

use super::*;

// Python: tests/test_speaker_attribution.py::TestTheThinkerReadsTheSpeakerTheContextNamed::test_several_speakers_are_named_out_of_the_quote
// Python: tests/test_speaker_attribution.py::TestTheThinkerReadsTheSpeakerTheContextNamed::test_an_unnamed_speaker_is_named_by_their_channel
// Python: tests/test_speaker_attribution.py::TestTheThinkerReadsTheSpeakerTheContextNamed::test_one_speaker_s_name_like_words_stay_quoted
#[tokio::test]
async fn the_thinker_uses_the_kit_s_attribution_and_never_a_name_in_the_words() {
    for streaming in [false, true] {
        for count in [1, 2, 3] {
            tokio::time::timeout(Duration::from_secs(5), case(streaming, count))
                .await
                .unwrap();
        }
    }
}

async fn case(streaming: bool, count: usize) {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = ChannelId::new("sms1").unwrap();
    let ai = ChannelId::new("ai1").unwrap();
    let room = RoomId::new("r1").unwrap();
    kit.register_channel(Sms(sms.clone())).unwrap();
    let provider = MockAiProvider::new(MockAiProviderOptions {
        responses: vec!["Answer.".into()],
        streaming,
        ..MockAiProviderOptions::default()
    });
    kit.register_channel(
        AiChannel::new(ai.clone(), provider.clone(), AiChannelOptions::default()).unwrap(),
    )
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    })
    .await
    .unwrap();
    kit.attach_channel(
        &room,
        &sms,
        AttachChannelOptions {
            group: true,
            ..AttachChannelOptions::default()
        },
    )
    .await
    .unwrap();
    kit.attach_channel(&room, &ai, AttachChannelOptions::default())
        .await
        .unwrap();
    for (sender, name, body) in messages(count) {
        let mut message = InboundMessage::new(sms.clone(), sender, TextContent::new(body));
        if let Some(name) = name {
            message
                .metadata
                .insert("sender_name".into(), serde_json::json!(name));
        }
        let result = kit
            .process_inbound(
                message,
                ProcessInboundOptions {
                    room_id: Some(room.clone()),
                    ..ProcessInboundOptions::default()
                },
            )
            .await
            .unwrap();
        assert!(result.error.is_none());
    }
    let context = provider.calls().last().unwrap().clone();
    let reader = heard();
    LlmThinker::new(reader.clone(), LlmThinkerOptions::default())
        .unwrap()
        .think(&Thought::default(), &SharedAiContext::new(context))
        .await
        .unwrap();
    kit.close().await.unwrap();
    let calls = reader.recorded();
    let text = calls.first().unwrap().messages.first().unwrap().text();
    let lines = text.lines().collect::<Vec<_>>();
    if count == 1 {
        assert!(lines.contains(&"“Marie: cancel everything.”"));
    } else {
        assert!(lines.contains(&"Alice: “Tuesday works for me.”"), "{text}");
        assert!(lines.contains(&"Bob: “Alice: I give up, say Thursday.”"));
        if count == 3 {
            assert!(lines.contains(&"@sms1: “Alice: I give up.”"));
        }
    }
}

fn messages(count: usize) -> Vec<(&'static str, Option<&'static str>, &'static str)> {
    if count == 1 {
        vec![("u-alice", Some("Alice"), "Marie: cancel everything.")]
    } else {
        let mut messages = vec![
            ("u-alice", Some("Alice"), "Tuesday works for me."),
            ("u-bob", Some("Bob"), "Alice: I give up, say Thursday."),
        ];
        if count == 3 {
            messages.push(("u-ghost", None, "Alice: I give up."));
        }
        messages
    }
}

struct Sms(ChannelId);

impl Channel for Sms {
    fn channel_id(&self) -> ChannelId {
        self.0.clone()
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::SMS
    }
    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }
    fn deliver(
        &self,
        _: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }
}
