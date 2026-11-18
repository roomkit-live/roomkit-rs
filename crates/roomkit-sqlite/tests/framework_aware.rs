//! An application channel gets its registering kit with the store's real type.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod support;

use std::future::{Future, ready};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, InboundMessage, RoomContext, RoomEvent,
    RoomId,
};
use roomkit::testing::EventTap;
use roomkit::{
    Channel, ChannelError, ConversationStore, CreateRoomOptions, FrameworkAwareChannel,
    GetRoomOptions, InMemoryStore, RoomKit, RoomKitError, RoomKitOptions, WeakRoomKit,
    default_inbound_event, framework_aware,
};
use roomkit_sqlite::{SqliteStore, SqliteStoreOptions};

struct Injection<S> {
    framework: WeakRoomKit<S>,
    events: EventTap,
}

struct Application<S> {
    seen: Arc<Mutex<Vec<Injection<S>>>>,
    refuse: bool,
}

impl<S: ConversationStore> Channel for Application<S> {
    fn channel_id(&self) -> ChannelId {
        ChannelId::new("app").unwrap()
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::WEBSOCKET
    }
    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(default_inbound_event(self, &message, &context)))
    }
    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }
}

impl<S: ConversationStore> FrameworkAwareChannel<S> for Application<S> {
    fn set_framework(&self, kit: WeakRoomKit<S>) -> Result<(), RoomKitError> {
        let framework = kit.upgrade().unwrap();
        assert!(framework.get_channel(&self.channel_id()).is_some());
        let events = EventTap::on(&framework, &["channel_registered", "channel_unregistered"]);
        self.seen.lock().unwrap().push(Injection {
            framework: kit,
            events,
        });
        if self.refuse {
            return Err(RoomKitError::Runtime("setup failed".into()));
        }
        Ok(())
    }
}

#[tokio::test]
async fn the_opted_in_channel_can_use_the_same_sqlite_store() {
    let file = support::TempFile::new("framework-aware");
    let store = SqliteStore::new(SqliteStoreOptions {
        path: file.path().into(),
    });
    let kit = RoomKit::new(RoomKitOptions::with_store(store));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let make = || Application {
        seen: Arc::clone(&seen),
        refuse: false,
    };
    kit.register_channel(framework_aware(make())).unwrap();
    assert!(matches!(
        kit.register_channel(framework_aware(make())),
        Err(RoomKitError::ChannelAlreadyRegistered(..))
    ));
    let (weak, events) = {
        let calls = seen.lock().unwrap();
        assert_eq!(calls.len(), 1);
        (calls[0].framework.clone(), calls[0].events.clone())
    };
    let registered = tokio::time::timeout(Duration::from_secs(1), events.wait_for(|_| true))
        .await
        .unwrap();
    assert_eq!(registered.r#type, "channel_registered");
    let injected = weak.upgrade().unwrap();
    assert!(std::ptr::eq(injected.store(), kit.store()));
    let id = RoomId::new("example").unwrap();
    injected
        .create_room(CreateRoomOptions {
            room_id: Some(id.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        kit.get_room(&id, GetRoomOptions::default())
            .await
            .unwrap()
            .id,
        id
    );
    drop(injected);
    kit.close().await.unwrap();
    drop(kit);
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn an_unmarked_channel_and_a_rejected_registration_behave_as_on_the_host() {
    let kit = RoomKit::<InMemoryStore>::new(RoomKitOptions::default());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let make = || Application {
        seen: Arc::clone(&seen),
        refuse: true,
    };
    kit.register_channel(make()).unwrap();
    assert!(seen.lock().unwrap().is_empty());
    let id = ChannelId::new("app").unwrap();
    kit.unregister_channel(&id).unwrap();
    let failure = kit.register_channel(framework_aware(make())).unwrap_err();
    assert_eq!(failure, RoomKitError::Runtime("setup failed".into()));
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert!(kit.get_channel(&id).is_some());
    let events = seen.lock().unwrap()[0].events.clone();
    kit.unregister_channel(&id).unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        events.wait_for(|event| event.r#type == "channel_unregistered"),
    )
    .await
    .unwrap();
    assert_eq!(events.types(), ["channel_unregistered"]);
    kit.close().await.unwrap();
}
