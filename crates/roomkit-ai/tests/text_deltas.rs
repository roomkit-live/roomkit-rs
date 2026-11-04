//! An answer's text published by its turn and after it: Python's
//! `tests/test_turn_ephemerals_scope.py`.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use roomkit::model::{ChannelId, ChannelType, Metadata, RoomId};
use roomkit::realtime::{
    EphemeralCallback, EphemeralEvent, EphemeralEventType, InMemoryRealtime, RealtimeBackend,
    RealtimeFuture,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, DelegateOptions, RoomKit, RoomKitOptions, TurnHost,
    current_turn_host,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions, AiTool, ToolResult, Tools};
use serde_json::{Value, json};

/// The texts a kit's realtime is handed, a task's room included.
#[derive(Default)]
struct Texts {
    realtime: InMemoryRealtime,
    published: Mutex<Vec<String>>,
}

impl RealtimeBackend for Texts {
    fn publish<'a>(&'a self, channel: &'a str, event: EphemeralEvent) -> RealtimeFuture<'a, ()> {
        if event.r#type == EphemeralEventType::TextDelta {
            let text = event.data["text"].as_str().unwrap().to_owned();
            self.published.lock().unwrap().push(text);
        }
        self.realtime.publish(channel, event)
    }

    fn subscribe<'a>(
        &'a self,
        channel: &'a str,
        callback: EphemeralCallback,
    ) -> RealtimeFuture<'a, String> {
        self.realtime.subscribe(channel, callback)
    }

    fn unsubscribe<'a>(&'a self, subscription_id: &'a str) -> RealtimeFuture<'a, bool> {
        self.realtime.unsubscribe(subscription_id)
    }
}

/// A `TEXT_DELTA` of the channel `ai` in `room_id`.
fn text_delta(room_id: &RoomId, text: &str) -> EphemeralEvent {
    let mut data = Metadata::new();
    data.insert("text".to_owned(), Value::from(text));
    EphemeralEvent {
        data,
        ..EphemeralEvent::new(
            room_id.clone(),
            EphemeralEventType::TextDelta,
            "ai".to_owned(),
        )
    }
}

// Python: tests/test_turn_ephemerals_scope.py::test_work_a_turn_began_publishes_after_it_as_that_turn_and_the_room_allow,
// its case "an answer's text": a turn read as a trace publishes while it runs;
// a host kept past its end publishes no more of the answer's text.
#[tokio::test]
async fn an_answer_s_text_published_after_its_trace_ended_is_dropped() {
    let texts = Arc::new(Texts::default());
    let kit = RoomKit::new(RoomKitOptions {
        realtime: Some(Arc::clone(&texts) as Arc<dyn RealtimeBackend>),
        ..RoomKitOptions::default()
    });
    let sms = RecordingTransport::new(
        ChannelId::new("sms").unwrap(),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    let kept: Arc<Mutex<Option<(TurnHost, RoomId)>>> = Arc::default();
    let keeping = Arc::clone(&kept);
    let tool = AiTool {
        parameters: json!({ "type": "object", "properties": {} }),
        ..AiTool::new("work", "Works.").unwrap()
    };
    let tools = Tools::new().tool(tool, move |_call, context| {
        let host = current_turn_host().unwrap();
        let room_id = context.room_id.clone();
        host.publish_ephemeral(text_delta(&room_id, "while it runs"));
        *keeping.lock().unwrap() = Some((host, room_id));
        async { Ok(ToolResult::Text("done".to_owned())) }
    });
    let provider = ScriptedProvider::new(vec![
        ScriptedRound::calling("", [("c1", "work", "{}")]),
        ScriptedRound::answering("Done."),
    ]);
    let options = AiChannelOptions {
        tools: tools.into(),
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(ChannelId::new("ai").unwrap(), provider, options).unwrap();
    kit.register_channel(agent).unwrap();
    let room = RoomId::new("parent").unwrap();
    let create = CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    let sms = ChannelId::new("sms").unwrap();
    kit.attach_channel(&room, &sms, AttachChannelOptions::default())
        .await
        .unwrap();
    let wait = DelegateOptions {
        wait: true,
        ..DelegateOptions::default()
    };
    let agent = ChannelId::new("ai").unwrap();
    kit.delegate(&room, &agent, "go", wait).await.unwrap();
    roomkit::testing::idle(&kit).await;
    let (host, room_id) = kept.lock().unwrap().take().unwrap();
    host.publish_ephemeral(text_delta(&room_id, "after it"));
    roomkit::testing::idle(&kit).await;
    kit.close().await.unwrap();
    assert_eq!(*texts.published.lock().unwrap(), ["while it runs"]);
}
