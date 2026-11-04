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
use roomkit::telemetry::{MockTelemetryProvider, TelemetryConfig, TelemetryProvider};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, ConversationStore, CreateRoomOptions, HookOptions,
    HookTrigger, InMemoryStore, InboundResult, ProcessInboundOptions, RoomKit, RoomKitOptions,
    SendEventOptions,
};
use roomkit_core::ops::ChannelConfig;
use roomkit_core::rules::transcoding::text_of;
use serde_json::json;
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
    cut_in(&kit, &held, failing).await
}

/// [`cut_within_the_grace`] in `kit`, whose room `busy` binds `held`.
async fn cut_in(
    kit: &RoomKit<InMemoryStore>,
    held: &Holding,
    failing: bool,
) -> (Vec<(String, DeliveryResultStatus)>, Vec<String>) {
    let stuck = Stuck {
        began: Arc::default(),
    };
    let began = Arc::clone(&stuck.began);
    kit.register_channel(stuck).unwrap();
    let bad: &[&str] = if failing { &["Go."] } else { &[] };
    kit.register_channel(Failing::new("bad", bad, &[])).unwrap();
    open(kit, &["sms", "agent", "bad"]).await;
    let holder = kit.clone();
    let call = tokio::spawn(async move {
        let options = ProcessInboundOptions {
            room_id: Some(room("r")),
            ..Default::default()
        };
        holder.process_inbound(said("Go."), options).await
    });
    began.notified().await;
    let _busy = deferred(kit, "held", "busy").await;
    held.entered("held").await;

    let close = spawn_close(kit);
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

/// Nor does its broadcast count it among its failures, as Python's
/// `failed_count`, the length of the failures its targets left.
#[tokio::test(start_paused = true)]
async fn a_turn_the_close_cuts_is_no_failure_of_its_broadcast() {
    let mock = Arc::new(MockTelemetryProvider::default());
    let provider: Arc<dyn TelemetryProvider> = Arc::<MockTelemetryProvider>::clone(&mock);
    let telemetry = TelemetryConfig {
        provider: Some(provider),
        ..TelemetryConfig::default()
    };
    let kit = RoomKit::new(RoomKitOptions {
        telemetry: Some(telemetry),
        ..RoomKitOptions::with_store(InMemoryStore::default())
    });
    let held = Holding::new("ws", [("held", Hold::UntilReleased)]);
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(held.clone()).unwrap();
    let options = CreateRoomOptions {
        room_id: Some(room("busy")),
        ..Default::default()
    };
    kit.create_room(options).await.unwrap();
    for id in ["sms", "ws"] {
        kit.attach_channel(&room("busy"), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }

    cut_in(&kit, &held, false).await;

    let spans = mock.completed_spans();
    let broadcast = spans
        .iter()
        .find(|span| span.name == "framework.broadcast" && span.room_id.as_deref() == Some("r"))
        .unwrap();
    assert_eq!(broadcast.attributes["failed_count"], json!(0));
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

/// A hook under the lock, on the text `closer`, that closes the kit from its
/// own work, which the close spares, and goes on holding the lock.
fn closing_hook(kit: &RoomKit<InMemoryStore>, inside: &Arc<Notify>, go: &Arc<Notify>) {
    let (weak, inside, go) = (kit.downgrade(), Arc::clone(inside), Arc::clone(go));
    let hook = move |event: RoomEvent, _context| {
        let (weak, inside, go) = (weak.clone(), Arc::clone(&inside), Arc::clone(&go));
        let ours = text_of(&event.content) == "closer";
        async move {
            if ours {
                let kit = weak.upgrade().unwrap();
                inside.notify_one();
                go.notified().await;
                drop(tokio::spawn(roomkit::keep_context(async move {
                    drop(kit.close().await);
                })));
                pending::<()>().await;
            }
            Ok(HookResult::allow(AllowOptions::default()))
        }
    };
    let options = HookOptions {
        name: "closer".to_owned(),
        ..Default::default()
    };
    kit.hook(HookTrigger::BeforeBroadcast, hook, options)
        .unwrap();
}

/// A kit whose transport `g` holds the text `a`, failing it when `refused`
/// says, its send waiting as a hook takes the room's lock to close the kit;
/// the deferred call of `a`, and what releases the hook into its close.
async fn behind_a_lock_the_close_spares(
    refused: &'static [&'static str],
    release_first: bool,
) -> (InboundResult, RoomKit<InMemoryStore>, Instant) {
    let gate = Failing::new("g", refused, &["a"]);
    let kit = kit_of(InMemoryStore::default(), &[], &[]).await;
    kit.register_channel(gate.clone()).unwrap();
    open(&kit, &["sms", "g"]).await;
    let (inside, go) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    closing_hook(&kit, &inside, &go);
    let first = deferred(&kit, "a", "r").await;
    gate.entered.notified().await;
    let holder = kit.clone();
    drop(tokio::spawn(async move {
        let options = ProcessInboundOptions {
            room_id: Some(room("r")),
            ..Default::default()
        };
        holder.process_inbound(said("closer"), options).await
    }));
    inside.notified().await;
    if release_first {
        gate.release.notify_one();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    go.notify_one();
    (first, kit, Instant::now())
}

/// When the call ended, whether it reads its target `g`, and how many results
/// its event holds.
async fn ended(
    first: InboundResult,
    kit: &RoomKit<InMemoryStore>,
    started: Instant,
) -> (Duration, bool, usize) {
    let event_id = first.event.as_ref().unwrap().id.clone();
    let result = first.delivery.unwrap().wait().await.unwrap();
    let stored = kit.store().get_event(&event_id).await.unwrap().unwrap();
    (
        started.elapsed(),
        result.delivery_results.contains_key("g"),
        stored.delivery_results.len(),
    )
}

/// A record already waiting behind a lock the close spares is answered at the
/// grace's end: its call reads what it delivered, nothing recorded.
#[tokio::test(start_paused = true)]
async fn a_record_behind_a_lock_the_close_spares_ends_at_the_grace() {
    let (first, kit, started) = behind_a_lock_the_close_spares(&["a"], true).await;
    assert_eq!(
        ended(first, &kit, started).await,
        (Duration::from_secs(5), true, 0)
    );
}

/// A set the grace's end cuts asks that lock past its cut: refused at once,
/// its call reading what it delivered.
#[tokio::test(start_paused = true)]
async fn a_set_cut_at_the_grace_asks_no_lock_the_close_spares() {
    let (first, kit, started) = behind_a_lock_the_close_spares(&[], false).await;
    assert_eq!(
        ended(first, &kit, started).await,
        (Duration::from_secs(5), true, 0)
    );
}
