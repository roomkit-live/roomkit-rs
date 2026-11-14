//! Python: `tests/test_channels/test_acp_publication_scope.py`.

use super::*;
use roomkit::model::{
    ChannelCategory, ChannelId, InboundMessage, RoomContext, RoomEvent, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{ChannelError, CreateRoomOptions, ProcessInboundOptions};

struct Caller {
    acp: AcpChannel<fixture::Direct>,
    target: RoomId,
}

impl Channel for Caller {
    fn channel_id(&self) -> ChannelId {
        ChannelId::new("caller").unwrap()
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }
    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }
    fn handle_inbound(
        &self,
        _: InboundMessage,
        _: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> {
        std::future::ready(Ok(test_input("unused").trigger))
    }
    async fn on_event(
        &self,
        _: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let values = self
            .acp
            .set_config_option(&self.target, "model", json!("sonnet"))
            .await?;
        assert_eq!(values["model"], "sonnet");
        Ok(ChannelOutput::empty())
    }
    fn deliver(
        &self,
        _: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> {
        std::future::ready(Ok(ChannelOutput::empty()))
    }
}

#[tokio::test]
async fn test_a_setter_cannot_publish_as_another_channels_turn() {
    let mut leaked = Vec::new();
    for target in ["caller-room", "other-room"] {
        let (acp, _) = fixture::channel(Stage::AnsweredPrompt);
        let realtime = Arc::new(AwaitedRealtime::default());
        let kit = RoomKit::new(RoomKitOptions {
            realtime: Some(Arc::<AwaitedRealtime>::clone(&realtime)),
            ..Default::default()
        });
        let events = Arc::new(Mutex::new(Vec::<EphemeralEvent>::new()));
        let recorded = Arc::clone(&events);
        realtime
            .subscribe(
                "all",
                Arc::new(move |event| {
                    let recorded = Arc::clone(&recorded);
                    Box::pin(async move {
                        recorded.lock().unwrap().push(event);
                    })
                }),
            )
            .await;
        kit.register_channel(acp.clone()).unwrap();
        kit.register_channel(Caller {
            acp,
            target: RoomId::new(target).unwrap(),
        })
        .unwrap();
        kit.register_channel(RecordingTransport::new(
            ChannelId::new("sms").unwrap(),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        ))
        .unwrap();
        for room in ["caller-room", "other-room"] {
            kit.create_room(CreateRoomOptions {
                room_id: Some(RoomId::new(room).unwrap()),
                ..Default::default()
            })
            .await
            .unwrap();
        }
        let room = RoomId::new("caller-room").unwrap();
        for channel in ["sms", "caller"] {
            kit.attach_channel(
                &room,
                &ChannelId::new(channel).unwrap(),
                roomkit::AttachChannelOptions::default(),
            )
            .await
            .unwrap();
        }
        let result = kit
            .process_inbound(
                InboundMessage::new(
                    ChannelId::new("sms").unwrap(),
                    "alice",
                    TextContent::new("go"),
                ),
                ProcessInboundOptions {
                    room_id: Some(room),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(result.error.is_none(), "{:?}", result.error);
        kit.close().await.unwrap();
        let custom: Vec<_> = events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.data.get("type") == Some(&json!("acp_config_options")))
            .map(|e| e.room_id.clone())
            .collect();
        if !custom.is_empty() {
            leaked.push((target, realtime.topics.lock().unwrap().clone(), custom));
        }
    }
    assert!(leaked.is_empty(), "{leaked:?}");
}
