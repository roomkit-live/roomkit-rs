//! A delivery set's record and the kit's close (RFC §5.13, §12.10.4, §14.3):
//! a turn the close cuts is no failed delivery, and the record's wait for the
//! room's lock cuts no call that is still before its commit.

use std::future::{Future, pending, ready};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelDirection, ChannelId,
    ChannelOutput, ChannelType, DeliveryResultStatus, HookResult, InboundMessage, RoomContext,
    RoomEvent, RoomId, TextContent,
};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, ConversationStore, CreateRoomOptions, HookOptions,
    HookTrigger, InMemoryStore, ProcessInboundOptions, RoomKit, SendEventOptions,
};
use roomkit_core::ops::ChannelConfig;
use roomkit_core::rules::transcoding::text_of;
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::held::{Hold, Holding, closed, deferred, kit_of, room, said, spawn_close};
use crate::support::{channel, transport};

/// An intelligence channel whose turn never ends.
#[derive(Clone)]
struct Stuck {
    began: Arc<Notify>,
}

impl Stuck {
    fn config() -> ChannelConfig {
        ChannelConfig::new(
            channel("agent"),
            ChannelType::AI,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
        )
    }
}

impl Channel for Stuck {
    fn channel_id(&self) -> ChannelId {
        Self::config().channel_id
    }

    fn channel_type(&self) -> ChannelType {
        Self::config().channel_type
    }

    fn category(&self) -> ChannelCategory {
        Self::config().category
    }

    fn direction(&self) -> ChannelDirection {
        Self::config().direction
    }

    fn capabilities(&self) -> ChannelCapabilities {
        Self::config().capabilities
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::default()))
    }

    fn on_event(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let began = Arc::clone(&self.began);
        async move {
            began.notify_one();
            pending::<()>().await;
            Ok(ChannelOutput::default())
        }
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }
}

/// A transport that fails the texts it is told to, the one it is told to
/// hold once released.
#[derive(Clone)]
struct Failing {
    id: &'static str,
    refused: &'static [&'static str],
    held: &'static [&'static str],
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl Failing {
    fn new(
        id: &'static str,
        refused: &'static [&'static str],
        held: &'static [&'static str],
    ) -> Self {
        Self {
            id,
            refused,
            held,
            entered: Arc::default(),
            release: Arc::default(),
        }
    }
}

impl Channel for Failing {
    fn channel_id(&self) -> ChannelId {
        transport(self.id).channel_id
    }

    fn channel_type(&self) -> ChannelType {
        transport(self.id).channel_type
    }

    fn category(&self) -> ChannelCategory {
        transport(self.id).category
    }

    fn direction(&self) -> ChannelDirection {
        transport(self.id).direction
    }

    fn capabilities(&self) -> ChannelCapabilities {
        transport(self.id).capabilities
    }

    fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let this = self.clone();
        async move {
            let text = text_of(&event.content);
            if this.held.contains(&text.as_str()) {
                this.entered.notify_one();
                this.release.notified().await;
            }
            if this.refused.contains(&text.as_str()) {
                return Err("socket closed".into());
            }
            Ok(ChannelOutput::default())
        }
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }
}

/// A transport whose attach holds the room's lock until released.
#[derive(Clone)]
struct Late {
    inside: Arc<Notify>,
    go: Arc<Notify>,
}

impl Channel for Late {
    fn channel_id(&self) -> ChannelId {
        transport("late").channel_id
    }

    fn channel_type(&self) -> ChannelType {
        transport("late").channel_type
    }

    fn category(&self) -> ChannelCategory {
        transport("late").category
    }

    fn direction(&self) -> ChannelDirection {
        transport("late").direction
    }

    fn capabilities(&self) -> ChannelCapabilities {
        transport("late").capabilities
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::default()))
    }

    fn on_room_attached(
        &self,
        _room_id: RoomId,
        _binding: ChannelBinding,
    ) -> impl Future<Output = Result<(), ChannelError>> + Send {
        let (inside, go) = (Arc::clone(&self.inside), Arc::clone(&self.go));
        async move {
            inside.notify_one();
            go.notified().await;
            Ok(())
        }
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }
}

/// The room `r`, binding `ids`.
async fn open<S: ConversationStore>(kit: &RoomKit<S>, ids: &[&str]) {
    let options = CreateRoomOptions {
        room_id: Some(room("r")),
        ..Default::default()
    };
    kit.create_room(options).await.unwrap();
    for id in ids {
        kit.attach_channel(&room("r"), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
}

/// What a message in the room `r` reports and stores of its failures, the
/// agent's turn stuck as the close starts, a send in another room keeping
/// the close in its grace: by channel, in the call's result and on the event.
async fn cut_within_the_grace(failing: bool) -> (Vec<(String, DeliveryResultStatus)>, Vec<String>) {
    let held = Holding::new("ws", [("held", Hold::UntilReleased)]);
    let kit = kit_of(InMemoryStore::default(), &[&held], &["busy"]).await;
    let stuck = Stuck {
        began: Arc::default(),
    };
    let began = Arc::clone(&stuck.began);
    kit.register_channel(stuck).unwrap();
    let bad: &[&str] = if failing { &["Go."] } else { &[] };
    kit.register_channel(Failing::new("bad", bad, &[])).unwrap();
    open(&kit, &["sms", "agent", "bad"]).await;
    let holder = kit.clone();
    let call = tokio::spawn(async move {
        let options = ProcessInboundOptions {
            room_id: Some(room("r")),
            ..Default::default()
        };
        holder.process_inbound(said("Go."), options).await
    });
    began.notified().await;
    let _busy = deferred(&kit, "held", "busy").await;
    held.entered("held").await;

    let close = spawn_close(&kit);
    let result = tokio::time::timeout(Duration::from_secs(1), call)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let event = result.event.as_ref().unwrap();
    let stored = kit.store().get_event(&event.id).await.unwrap().unwrap();
    held.release.notify_one();
    closed(close).await;
    let reported = result
        .delivery_results
        .iter()
        .filter(|(_, delivered)| delivered.status == DeliveryResultStatus::Failed)
        .map(|(id, delivered)| (id.as_str().to_owned(), delivered.status))
        .collect();
    (reported, stored.delivery_results.keys().cloned().collect())
}

/// A turn the close cuts leaves no result: nothing failed, nothing recorded,
/// as Python's `_leaves_no_result`.
#[tokio::test(start_paused = true)]
async fn a_turn_the_close_cuts_is_no_failed_delivery() {
    assert_eq!(cut_within_the_grace(false).await, (Vec::new(), Vec::new()));
}

/// The record of what did fail beside it is written all the same, the close
/// still in its grace, with no word of the turn.
#[tokio::test(start_paused = true)]
async fn a_failure_beside_a_turn_the_close_cuts_is_recorded_alone() {
    let (reported, stored) = cut_within_the_grace(true).await;
    assert_eq!(
        (reported, stored.contains(&"agent".to_owned())),
        (
            vec![("bad".to_owned(), DeliveryResultStatus::Failed)],
            false
        )
    );
    assert!(stored.contains(&"bad".to_owned()), "{stored:?}");
}

/// The record of an entry an off-lock hook made, waiting for the room's lock
/// behind an attach as the close starts, is no call waiting before its
/// commit: the hook of the undecided trigger keeps its grace.
#[tokio::test(start_paused = true)]
async fn a_record_waiting_for_the_lock_leaves_the_call_s_hook_its_grace() {
    let gate = Failing::new("g", &["notice"], &["notice"]);
    let kit = kit_of(InMemoryStore::default(), &[], &[]).await;
    kit.register_channel(gate.clone()).unwrap();
    open(&kit, &["sms", "g"]).await;
    let late = Late {
        inside: Arc::default(),
        go: Arc::default(),
    };
    let (late_in, late_go) = (Arc::clone(&late.inside), Arc::clone(&late.go));
    kit.register_channel(late).unwrap();
    let finished: Arc<Mutex<Option<Duration>>> = Arc::default();
    let started = Instant::now();
    let (weak, done) = (kit.downgrade(), Arc::clone(&finished));
    let hook = move |event: RoomEvent, _context| {
        let (weak, done) = (weak.clone(), Arc::clone(&done));
        let ours = text_of(&event.content) == "trigger";
        async move {
            if ours {
                let kit = weak.upgrade().unwrap();
                let notice = TextContent::new("notice");
                let options = SendEventOptions::default();
                kit.send_event(&room("r"), &channel("sms"), notice, options)
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_secs(3)).await;
                *done.lock().unwrap() = Some(started.elapsed());
            }
            Ok(HookResult::allow(AllowOptions::default()))
        }
    };
    let options = HookOptions {
        name: "offlock".to_owned(),
        needs_lock: false,
        ..Default::default()
    };
    kit.hook(HookTrigger::BeforeBroadcast, hook, options)
        .unwrap();
    let holder = kit.clone();
    let call = tokio::spawn(async move {
        let options = ProcessInboundOptions {
            room_id: Some(room("r")),
            ..Default::default()
        };
        holder.process_inbound(said("trigger"), options).await
    });
    gate.entered.notified().await;
    let attacher = kit.clone();
    let attach = tokio::spawn(async move {
        attacher
            .attach_channel(
                &room("r"),
                &channel("late"),
                AttachChannelOptions::default(),
            )
            .await
    });
    late_in.notified().await;
    gate.release.notify_one();
    tokio::time::sleep(Duration::from_millis(10)).await;

    let close = spawn_close(&kit);
    call.await.unwrap().unwrap();

    assert_eq!(*finished.lock().unwrap(), Some(Duration::from_secs(3)));
    late_go.notify_one();
    drop(attach.await);
    closed(close).await;
}
