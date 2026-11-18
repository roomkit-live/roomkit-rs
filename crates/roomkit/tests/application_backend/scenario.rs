//! One application-supplied transport driving the owner's public API (RFC §10.6).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use roomkit::model::{
    ChannelId, ChannelType, EventContent, EventDirection, EventSource, EventType, InboundMessage,
    RoomEvent, RoomId, TextContent,
};
use roomkit::store::ListEventsOptions;
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions,
};
use roomkit::turns::wire::{Control, Frame, WireOutput};
use roomkit::turns::{
    ChannelControl, ChannelResult, LocalPresenceClaims, RoomOwner, ServedExecutor, TurnBackend,
    TurnFuture, TurnLink, TurnRequest,
};
use roomkit::{
    AttachChannelOptions, ChannelCallOptions, ConversationStore, CreateRoomOptions,
    DetachChannelOptions, ProcessInboundOptions, RoomKit, RoomKitError, RoomKitOptions,
};
use serde_json::json;

#[derive(Default)]
struct Backend {
    calls: Mutex<Vec<&'static str>>,
    owner: Mutex<Option<Arc<dyn RoomOwner>>>,
    attended_executor: Mutex<Option<Weak<dyn ServedExecutor>>>,
    claims: LocalPresenceClaims,
    releases: Arc<AtomicUsize>,
}

impl TurnBackend for Backend {
    fn attend(&self, owner: Arc<dyn RoomOwner>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push("attend");
            *self.attended_executor.lock().unwrap() =
                Some(Arc::downgrade(&owner.executor().unwrap()));
            *self.owner.lock().unwrap() = Some(owner);
            Ok(())
        })
    }

    fn serve(&self, executor: Arc<dyn ServedExecutor>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async move {
            let attended = self.attended_executor.lock().unwrap();
            let same = attended
                .as_ref()
                .unwrap()
                .ptr_eq(&Arc::downgrade(&executor));
            self.calls.lock().unwrap().push(if same {
                "serve"
            } else {
                "serve_different_executor"
            });
            assert!(executor.hosts_channel(&ChannelId::new("agent").unwrap()));
            Ok(())
        })
    }

    fn control(
        &self,
        message: ChannelControl,
    ) -> TurnFuture<'_, Result<Option<ChannelResult>, RoomKitError>> {
        Box::pin(async move {
            match message {
                ChannelControl::RoomAttached {
                    channel_id,
                    room_id,
                    ..
                } => {
                    assert_eq!(channel_id.as_str(), "agent");
                    assert_eq!(room_id.as_str(), "room");
                    self.calls.lock().unwrap().push("attach_agent");
                }
                ChannelControl::ChannelCall(call) => {
                    self.calls.lock().unwrap().push("channel_call");
                    assert_eq!(call.channel_id.as_str(), "agent");
                    assert_eq!(call.room_id.as_str(), "room");
                    assert!(!call.handed());
                    call.hand_over();
                    return Ok(Some(ChannelResult {
                        result: json!({"application_verb": call.verb}),
                        error: None,
                        room_state: None,
                        executor_id: Some("application".into()),
                    }));
                }
                ChannelControl::RoomDetached { .. }
                | ChannelControl::CommittedEvent { .. }
                | ChannelControl::ChannelReleased { .. } => {}
            }
            Ok(None)
        })
    }

    fn submit(
        &self,
        request: TurnRequest,
    ) -> TurnFuture<'_, Result<Arc<dyn TurnLink>, RoomKitError>> {
        Box::pin(async move {
            assert_eq!(request.channel_id.as_str(), "agent");
            assert_eq!(request.room_id.as_str(), "room");
            assert!(!request.handed());
            request.hand_over();
            self.calls.lock().unwrap().push("submit");
            let link: Arc<dyn TurnLink> =
                Arc::new(ReplyLink::new(request, Arc::clone(&self.releases)));
            Ok(link)
        })
    }

    fn claim_presence<'a>(
        &'a self,
        channel_id: &'a str,
        room_id: &'a str,
        online: Option<bool>,
    ) -> TurnFuture<'a, Result<bool, RoomKitError>> {
        Box::pin(async move {
            assert_eq!((channel_id, room_id), ("agent", "room"));
            let changed = self.claims.claim(channel_id, room_id, online);
            if changed || online.is_none() {
                let operation = match online {
                    Some(true) => "online",
                    Some(false) => "offline",
                    None => "forget",
                };
                self.calls.lock().unwrap().push(operation);
            }
            Ok(changed)
        })
    }

    fn close(&self) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push("close");
            Ok(())
        })
    }
}

struct ReplyLink {
    request: TurnRequest,
    frames: Mutex<std::vec::IntoIter<(u64, Frame)>>,
    releases: Arc<AtomicUsize>,
}

fn text(event: &RoomEvent) -> Option<&str> {
    if let EventContent::Text(text) = &event.content {
        Some(&text.body)
    } else {
        None
    }
}

impl ReplyLink {
    fn new(request: TurnRequest, releases: Arc<AtomicUsize>) -> Self {
        let answer = RoomEvent::new(
            request.room_id.clone(),
            EventSource {
                direction: EventDirection::Outbound,
                ..EventSource::new(request.channel_id.clone(), ChannelType::AI)
            },
            TextContent::new(format!("application: {}", text(&request.event).unwrap())),
        );
        let output = WireOutput {
            responded: true,
            response_events: vec![answer],
            ..WireOutput::default()
        };
        let frames = vec![
            (
                1,
                Frame::Accepted {
                    executor_id: "application".into(),
                },
            ),
            (
                2,
                Frame::Output {
                    output,
                    streams: false,
                },
            ),
            (3, Frame::End),
        ];
        Self {
            request,
            frames: Mutex::new(frames.into_iter()),
            releases,
        }
    }
}

impl TurnLink for ReplyLink {
    fn request(&self) -> &TurnRequest {
        &self.request
    }
    fn taken(&self) -> bool {
        self.request.handed()
    }
    fn receive(&self) -> TurnFuture<'_, Result<(u64, Frame), RoomKitError>> {
        Box::pin(async {
            self.frames
                .lock()
                .unwrap()
                .next()
                .ok_or(RoomKitError::KitClosed)
        })
    }
    fn send(&self, _message: Control) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async { Ok(()) })
    }
    fn release(&self) {
        self.releases.fetch_add(1, Ordering::Relaxed);
    }
}

async fn bind<S: ConversationStore>(
    kit: &RoomKit<S>,
    room: &RoomId,
    tx: &ChannelId,
    agent: &ChannelId,
) {
    kit.register_channel(RecordingTransport::new(
        tx.clone(),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(EchoAgent::new(
        agent.clone(),
        "local fallback",
        EchoAgentOptions::default(),
    ))
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    })
    .await
    .unwrap();
    for channel in [tx, agent] {
        kit.attach_channel(room, channel, AttachChannelOptions::default())
            .await
            .unwrap();
    }
}

async fn assert_history<S: ConversationStore>(kit: &RoomKit<S>, room: &RoomId) {
    let rows = kit
        .store()
        .list_events(room, ListEventsOptions::default())
        .await
        .unwrap();
    let messages: Vec<_> = rows
        .iter()
        .filter(|row| row.r#type == EventType::Message)
        .map(|row| (row.source.channel_id.as_str(), text(row).unwrap()))
        .collect();
    assert_eq!(
        messages,
        [
            ("tx", "first"),
            ("agent", "application: first"),
            ("tx", "second"),
            ("agent", "application: second")
        ]
    );
}

fn new_kit<S: ConversationStore>(store: S) -> (RoomKit<S>, Arc<Backend>) {
    let backend = Arc::new(Backend::default());
    let application: Arc<dyn TurnBackend> = Arc::<Backend>::clone(&backend);
    let kit = RoomKit::new(RoomKitOptions {
        turn_backend: Some(application),
        ..RoomKitOptions::with_store(store)
    });
    (kit, backend)
}

pub(crate) async fn run<S: ConversationStore>(store: S) {
    let (kit, backend) = new_kit(store);
    let room = RoomId::new("room").unwrap();
    let tx = ChannelId::new("tx").unwrap();
    let agent = ChannelId::new("agent").unwrap();
    assert!(backend.calls.lock().unwrap().is_empty());
    bind(&kit, &room, &tx, &agent).await;
    assert_eq!(
        *backend.calls.lock().unwrap(),
        ["attend", "serve", "attach_agent", "online"]
    );

    let reply = kit
        .channel_call(&agent, &room, "who", ChannelCallOptions::default())
        .await
        .unwrap();
    assert_eq!(reply, json!({"application_verb":"who"}));
    for body in ["first", "second"] {
        let input = InboundMessage::new(tx.clone(), "u", TextContent::new(body));
        let options = ProcessInboundOptions {
            room_id: Some(room.clone()),
            ..ProcessInboundOptions::default()
        };
        let result = kit.process_inbound(input, options).await.unwrap();
        assert!(result.error.is_none(), "{:?}", result.error);
    }
    assert_history(&kit, &room).await;
    // Registrations may keep only weak references to avoid retaining a kit.
    assert!(
        backend
            .attended_executor
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap()
            .hosts_channel(&agent)
    );

    assert!(
        kit.detach_channel(&room, &agent, DetachChannelOptions::default())
            .await
            .unwrap()
    );
    kit.close().await.unwrap();
    assert_eq!(
        *backend.calls.lock().unwrap(),
        [
            "attend",
            "serve",
            "attach_agent",
            "online",
            "channel_call",
            "submit",
            "submit",
            "forget",
            "close"
        ]
    );
    assert_eq!(backend.releases.load(Ordering::Relaxed), 2);
}

pub(crate) async fn abandon<S: ConversationStore>(store: S) {
    let (kit, backend) = new_kit(store);
    let weak = kit.downgrade();
    let room = RoomId::new("room").unwrap();
    let tx = ChannelId::new("tx").unwrap();
    let agent = ChannelId::new("agent").unwrap();
    bind(&kit, &room, &tx, &agent).await;
    let owner = Arc::clone(backend.owner.lock().unwrap().as_ref().unwrap());
    drop(kit);
    drop(backend);
    // Allow pending observations of the attach to release their transient handles.
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while weak.upgrade().is_some() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(owner.executor().is_none());
}
