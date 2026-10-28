//! RFC §5.1, §14.1, gap G-049: what a streaming target writes on its room
//! once it read an answer: its `metadata_updates`, without the keys only
//! the framework writes.

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelOutput, ChannelType, RoomContext,
};
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, ConversationStore,
    CreateRoomOptions, DeliveryStream, RoomKit, RoomKitOptions,
};
use roomkit_ai::AiAgent;
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use serde_json::json;

use crate::support::{asks, channel, room};

/// What the target writes under the keys only the framework writes.
const FORGED: &str = "forged";

/// A WebSocket transport that streams, reads the answer to its end, then
/// writes room metadata: a key of its own, and keys only the framework
/// writes, of the reserved namespace and of the room's private state.
struct Tagging {
    declaration: ChannelDeclaration,
}

impl Channel for Tagging {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    async fn deliver_stream(
        &self,
        mut stream: DeliveryStream,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        while stream.next().await.is_some() {}
        let mut output = ChannelOutput::default();
        output
            .metadata_updates
            .insert("crm".to_owned(), json!("c_1"));
        for key in [
            "roomkit:detached_channels",
            "author_register",
            "_speak_queue_results",
        ] {
            output
                .metadata_updates
                .insert(key.to_owned(), json!(FORGED));
        }
        Ok(output)
    }
}

/// A streaming target's `metadata_updates` are written on the room but the
/// keys only the framework writes, refused as a channel's output refuses
/// them, as the Python implementation's writers do (RMK-978): a stream's
/// target could forge the register of authors.
#[tokio::test]
async fn a_streaming_target_cannot_write_a_key_only_the_framework_writes() {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(roomkit::testing::RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
    ))
    .unwrap();
    let mut declaration = ChannelDeclaration::new(
        channel("ws"),
        ChannelType::WEBSOCKET,
        ChannelCategory::Transport,
        ChannelDirection::Bidirectional,
    );
    declaration.supports_streaming_delivery = true;
    declaration.recipient_key = None;
    kit.register_channel(Tagging { declaration }).unwrap();
    let model = ScriptedProvider::new([ScriptedRound::writing(["The answer ", "is 42."])]);
    kit.register_channel(AiAgent::new(channel("ai"), model))
        .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "ws", "ai"] {
        kit.attach_channel(&room(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    asks(&kit, "What is it?").await;
    roomkit::testing::idle(&kit).await;
    let metadata = kit
        .store()
        .get_room(&room())
        .await
        .unwrap()
        .unwrap()
        .metadata;
    let forged: Vec<&String> = metadata
        .iter()
        .filter(|(_, value)| value.as_str() == Some(FORGED))
        .map(|(key, _)| key)
        .collect();
    assert_eq!(
        (metadata.get("crm"), forged),
        (Some(&json!("c_1")), Vec::<&String>::new())
    );
    kit.close().await;
}
