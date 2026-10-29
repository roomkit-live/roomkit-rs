//! RFC §5.1, §14.1, gap G-049: what a streaming target writes on its room
//! once it read an answer: its `metadata_updates`, without the keys only
//! the framework writes. RFC §10.6.4: how a stream's deltas reach it.

use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelOutput, ChannelType, RoomContext,
};
use roomkit::testing::TurnTap;
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, ConversationStore,
    CreateRoomOptions, DeliveryStream, RoomKit, RoomKitOptions, StreamItem, text_of,
};
use roomkit_ai::AiAgent;
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use serde_json::{Value, json};

use crate::support::{asks, channel, received, room};

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
    kit.close().await.unwrap();
}

/// A WebSocket transport that streams and keeps the length of each text
/// it is handed.
#[derive(Clone)]
struct Counting {
    sizes: Arc<Mutex<Vec<usize>>>,
}

impl Channel for Counting {
    fn declaration(&self) -> ChannelDeclaration {
        let mut declaration = ChannelDeclaration::new(
            channel("ws"),
            ChannelType::WEBSOCKET,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        );
        declaration.supports_streaming_delivery = true;
        declaration.recipient_key = None;
        declaration
    }

    async fn deliver_stream(
        &self,
        mut stream: DeliveryStream,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        while let Some(item) = stream.next().await {
            if let Ok(StreamItem::Text(text)) = item {
                self.sizes.lock().unwrap().push(text.len());
            }
        }
        Ok(ChannelOutput::default())
    }
}

/// RFC §10.6.4 (RMK-1017, D1): the deltas a stream yields without a wait
/// between them cross as one item, their text joined and their number
/// told, up to what a pull grants, 64, as the Python implementation's
/// `FrameSender` sends them: 200 deltas at once reach the target as 4
/// items of 246, 284, 320 and 40 characters, carrying 64, 64, 64 and 8
/// deltas, as Python's (`rmk1017-lightreview5/lr5_probe_py.out` D1). On
/// 8a2ca47 each delta crossed as an item of its own: 200 items.
#[tokio::test]
async fn deltas_yielded_together_cross_as_one_item() {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(roomkit::testing::RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
    ))
    .unwrap();
    let target = Counting {
        sizes: Arc::default(),
    };
    kit.register_channel(target.clone()).unwrap();
    let words: Vec<String> = (0..200).map(|n| format!("w{n} ")).collect();
    let model = ScriptedProvider::new([ScriptedRound::writing(words.iter().map(String::as_str))]);
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
    let tap = TurnTap::on(&kit);
    asks(&kit, "Count.").await;
    roomkit::testing::idle(&kit).await;
    let bursts: Vec<(usize, Value)> = tap
        .messages()
        .into_iter()
        .filter(|message| message.family == "frame")
        .filter_map(|message| {
            let frame: Value = serde_json::from_str(&message.json).unwrap();
            let item = frame["message"]["item"].clone();
            (item["kind"] == "text").then(|| {
                let text = item["text"].as_str().unwrap_or_default().len();
                (text, item["deltas"].clone())
            })
        })
        .collect();
    let events = kit.store().list_events(&room(), &received()).await.unwrap();
    let answer = events.last().map(|event| text_of(&event.content).len());
    assert_eq!(
        (target.sizes.lock().unwrap().clone(), bursts, answer),
        (
            vec![246, 284, 320, 40],
            vec![
                (246, json!(64)),
                (284, json!(64)),
                (320, json!(64)),
                (40, json!(8))
            ],
            Some(890)
        )
    );
    kit.close().await.unwrap();
}
