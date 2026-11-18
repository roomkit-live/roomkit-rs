//! Committed messages reach all discussion memories concurrently (§19.7.5).

use super::*;
use roomkit::AgentIdentity;
use roomkit::ChannelDeclaration;
use roomkit::testing::TurnTap;
use tokio::sync::Barrier;

struct MeetingAgent {
    id: ChannelId,
    together: Arc<Barrier>,
    seen: Arc<Mutex<Vec<(ChannelId, String)>>>,
}

impl Channel for MeetingAgent {
    fn channel_id(&self) -> ChannelId {
        self.id.clone()
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }
    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }
    fn declaration(&self) -> ChannelDeclaration {
        ChannelDeclaration {
            identity: Some(AgentIdentity::default()),
            ..ChannelDeclaration::new(self.id.clone())
        }
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
    async fn on_room_event(&self, _: RoomId, event: RoomEvent) -> Result<(), ChannelError> {
        let EventContent::Text(text) = event.content else {
            return Ok(());
        };
        self.together.wait().await;
        self.seen.lock().unwrap().push((self.id.clone(), text.body));
        Ok(())
    }
}

#[tokio::test]
async fn a_committed_message_reaches_every_discussion_memory_at_once() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let together = Arc::new(Barrier::new(2));
    let seen = Arc::new(Mutex::new(Vec::new()));
    for name in ["ana", "bob"] {
        kit.register_channel(MeetingAgent {
            id: id(name),
            together: Arc::clone(&together),
            seen: Arc::clone(&seen),
        })
        .unwrap();
    }
    for name in ["offline-a", "offline-b"] {
        kit.register_channel(ChannelDeclaration {
            identity: Some(AgentIdentity::default()),
            ..ChannelDeclaration::new(id(name))
        })
        .unwrap();
    }
    kit.register_channel(RecordingTransport::new(
        id("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room()),
        orchestration: Some(Some(Cluster::discussion_of(&[
            "ana",
            "bob",
            "offline-a",
            "offline-b",
        ]))),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room(), &id("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    let tap = TurnTap::on(&kit);
    let text = "@nobody hello.";
    let message = InboundMessage::new(id("sms"), "user", TextContent::new(text));
    tokio::time::timeout(
        Duration::from_secs(2),
        kit.process_inbound(
            message,
            ProcessInboundOptions {
                room_id: Some(room()),
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let mut handed = seen.lock().unwrap().clone();
    handed.sort();
    assert_eq!(
        handed,
        [(id("ana"), text.to_owned()), (id("bob"), text.to_owned())]
    );
    let mut notified = Vec::new();
    for raw in tap
        .messages()
        .into_iter()
        .filter(|message| message.family == "channel_control")
    {
        let envelope: roomkit::turns::wire::ChannelControlEnvelope =
            serde_json::from_str(&raw.json).unwrap();
        if let roomkit::turns::ChannelControl::CommittedEvent { channel_id, .. } = envelope.message
        {
            notified.push(channel_id);
        }
    }
    notified.sort();
    assert_eq!(notified, [id("ana"), id("bob")]);
    kit.close().await.unwrap();
}
