//! RFC §12.10.4, rule 1 (RMK-1030): admission stops at once. Once
//! `close()` started, every call but a read returns
//! `RoomKitError::KitClosed`, whoever makes it: an outside caller, a hook
//! or a channel's close; the reads answer during the close and after it.
//! Python's `tests/test_kit_close.py`, section 1.
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    Access, HookResult, Metadata, RoomContext, RoomEvent, TextContent, Visibility,
};
use roomkit::{
    ArchiveRoomOptions, AttachChannelOptions, Channel, CheckAllTimersOptions,
    CheckRoomTimersOptions, CloseRoomOptions, CommitEventOptions, ConversationStore,
    CreateRoomOptions, DeleteEventOptions, DetachChannelOptions, GetBindingOptions, GetRoomOptions,
    GetTimelineOptions, HookExecution, HookFuture, HookHandler, HookOptions, HookTrigger,
    InMemoryStore, ListBindingsOptions, ListMembersOptions, ListReadMarkersOptions,
    MarkAllReadOptions, MarkReadOptions, MuteOptions, PauseRoomOptions, PostOptions,
    ProcessInboundOptions, PublishTypingOptions, ReopenRoomOptions, ResumeRoomOptions, RoomKit,
    RoomKitError, SendEventOptions, SetAccessOptions, SetVisibilityOptions, SubscribeRoomOptions,
    ToolCallEvent, UnmuteOptions, UpdateBindingMetadataOptions, UpdateEventOptions, WeakRoomKit,
};
use roomkit_core::ops::ChannelConfig;
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::held::{
    Hold, Holding, a_send_held, closed, closing, into, kit_of, record, result_of, room, said,
    spawn_close, spawn_said, stored,
};
use crate::support::{channel, transport};
use roomkit_core::rules::transcoding::text_of;

/// The kit's reads, each as text: they answer during the close and after
/// it, the store open.
async fn reads<S: ConversationStore>(kit: &RoomKit<S>) -> Vec<String> {
    let room_id = room("r");
    let sms = channel("sms");
    vec![
        format!(
            "{:?}",
            kit.get_room(&room_id, GetRoomOptions::default()).await
        ),
        format!(
            "{:?}",
            kit.get_timeline(&room_id, GetTimelineOptions::default())
                .await
        ),
        format!(
            "{:?}",
            kit.get_binding(&room_id, &sms, GetBindingOptions::default())
                .await
        ),
        format!(
            "{:?}",
            kit.list_bindings(&room_id, ListBindingsOptions::default())
                .await
        ),
        format!(
            "{:?}",
            kit.list_members(&room_id, ListMembersOptions::default())
                .await
        ),
        format!(
            "{:?}",
            kit.get_channel(&sms)
                .map(|channel| channel.channel_id().clone())
        ),
        format!("{:?}", kit.store().get_room(&room_id).await),
        format!(
            "{:?}",
            kit.list_read_markers(&room_id, ListReadMarkersOptions::default())
                .await
        ),
    ]
}

/// RFC §12.10.4: a read answers as before the close, during it and after
/// it (Python's
/// `test_a_read_answers_during_the_close_and_after_it_while_its_store_is_open`,
/// its in-memory store; the kit closes no store of its own here).
#[tokio::test(start_paused = true)]
async fn a_read_answers_during_the_close_and_after_it() {
    let held = Holding::new("ws", [("held", Hold::UntilReleased)]);
    let kit = kit_of(InMemoryStore::default(), &[&held], &["r", "busy"]).await;
    kit.process_inbound(said("hello"), into("r")).await.unwrap();
    a_send_held(&kit, &held, "busy").await;
    let before = reads(&kit).await;
    let close = closing(&kit).await;

    let during = reads(&kit).await;
    held.release.notify_one();
    closed(close).await;

    assert_eq!((during, reads(&kit).await), (before.clone(), before));
}

/// Whether `result` is the refusal of a closing kit.
fn refused<T>(result: Result<T, RoomKitError>) -> bool {
    matches!(result.err(), Some(RoomKitError::KitClosed))
}

/// The kit's calls that run an operation, each made once: whether it was
/// refused.
async fn the_kit_s_operations<S: ConversationStore>(kit: &RoomKit<S>) -> Vec<(&'static str, bool)> {
    let mut calls = the_kit_s_events(kit).await;
    calls.extend(the_kit_s_rooms(kit).await);
    calls.extend(the_kit_s_read_markers(kit).await);
    calls
}

/// The kit's calls that mark a room read, each made once: whether it was
/// refused.
async fn the_kit_s_read_markers<S: ConversationStore>(
    kit: &RoomKit<S>,
) -> Vec<(&'static str, bool)> {
    let r = room("r");
    let sms = channel("sms");
    vec![
        (
            "mark_read",
            refused(
                kit.mark_read(
                    &r,
                    &sms,
                    &"event_1".parse().unwrap(),
                    MarkReadOptions::default(),
                )
                .await,
            ),
        ),
        (
            "publish_typing",
            refused(
                kit.publish_typing(&r, "u1", PublishTypingOptions::default())
                    .await,
            ),
        ),
        (
            "subscribe_room",
            refused(
                kit.subscribe_room(&r, |_| async {}, SubscribeRoomOptions::default())
                    .await,
            ),
        ),
        (
            "mark_all_read",
            refused(
                kit.mark_all_read(&r, &sms, MarkAllReadOptions::default())
                    .await,
            ),
        ),
    ]
}

/// The kit's calls that write events, each made once: whether it was
/// refused.
async fn the_kit_s_events<S: ConversationStore>(kit: &RoomKit<S>) -> Vec<(&'static str, bool)> {
    let r = room("r");
    let sms = channel("sms");
    let event_id = "event_1".parse().unwrap();
    let text = || TextContent::new("x");
    vec![
        (
            "process_inbound",
            refused(kit.process_inbound(said("x"), into("r")).await),
        ),
        (
            "send_event",
            refused(
                kit.send_event(&r, &sms, text(), SendEventOptions::default())
                    .await,
            ),
        ),
        (
            "commit_event",
            refused(
                kit.commit_event(&r, record("x"), CommitEventOptions::default())
                    .await,
            ),
        ),
        (
            "post",
            refused(kit.post(&r, text(), PostOptions::default()).await),
        ),
        (
            "update_event",
            refused(
                kit.update_event(&r, &event_id, UpdateEventOptions::default())
                    .await,
            ),
        ),
        (
            "delete_event",
            refused(
                kit.delete_event(&r, &event_id, DeleteEventOptions::default())
                    .await,
            ),
        ),
    ]
}

/// The kit's calls that change rooms and bindings, each made once: whether
/// it was refused.
async fn the_kit_s_rooms<S: ConversationStore>(kit: &RoomKit<S>) -> Vec<(&'static str, bool)> {
    let r = room("r");
    let sms = channel("sms");
    vec![
        (
            "create_room",
            refused(kit.create_room(CreateRoomOptions::default()).await),
        ),
        (
            "pause_room",
            refused(kit.pause_room(&r, PauseRoomOptions::default()).await),
        ),
        (
            "resume_room",
            refused(kit.resume_room(&r, ResumeRoomOptions::default()).await),
        ),
        (
            "reopen_room",
            refused(kit.reopen_room(&r, ReopenRoomOptions::default()).await),
        ),
        (
            "close_room",
            refused(kit.close_room(&r, CloseRoomOptions::default()).await),
        ),
        (
            "archive_room",
            refused(kit.archive_room(&r, ArchiveRoomOptions::default()).await),
        ),
        (
            "attach_channel",
            refused(
                kit.attach_channel(&r, &channel("ws"), AttachChannelOptions::default())
                    .await,
            ),
        ),
        (
            "detach_channel",
            refused(
                kit.detach_channel(&r, &sms, DetachChannelOptions::default())
                    .await,
            ),
        ),
        (
            "mute",
            refused(kit.mute(&r, &sms, MuteOptions::default()).await),
        ),
        (
            "unmute",
            refused(kit.unmute(&r, &sms, UnmuteOptions::default()).await),
        ),
        (
            "set_access",
            refused(
                kit.set_access(&r, &sms, Access::ReadOnly, SetAccessOptions::default())
                    .await,
            ),
        ),
        (
            "set_visibility",
            refused(
                kit.set_visibility(&r, &sms, Visibility::All, SetVisibilityOptions::default())
                    .await,
            ),
        ),
        (
            "update_binding_metadata",
            refused(
                kit.update_binding_metadata(
                    &r,
                    &sms,
                    Metadata::default(),
                    UpdateBindingMetadataOptions::default(),
                )
                .await,
            ),
        ),
        (
            "check_room_timers",
            refused(
                kit.check_room_timers(&r, CheckRoomTimersOptions::default())
                    .await,
            ),
        ),
        (
            "check_all_timers",
            refused(kit.check_all_timers(CheckAllTimersOptions::default()).await),
        ),
    ]
}

/// A hook of its own type, which lets every event through.
struct Allow;

impl HookHandler for Allow {
    fn call(&self, _event: RoomEvent, _context: RoomContext) -> HookFuture {
        Box::pin(async { Ok(Some(HookResult::allow(AllowOptions::default()))) })
    }
}

/// The kit's registrations, each made once: whether it was refused.
fn the_kit_s_registrations<S: ConversationStore>(kit: &RoomKit<S>) -> Vec<(&'static str, bool)> {
    let late = || HookOptions {
        name: "late".to_owned(),
        ..Default::default()
    };
    let allow =
        |_: RoomEvent, _: RoomContext| async { Ok(HookResult::allow(AllowOptions::default())) };
    let tool_observer = |_: ToolCallEvent, _: RoomContext| async {};
    vec![
        (
            "register_channel",
            refused(kit.register_channel(Holding::new("late", []))),
        ),
        (
            "unregister_channel",
            refused(kit.unregister_channel(&channel("sms"))),
        ),
        (
            "hook",
            refused(kit.hook(HookTrigger::BeforeBroadcast, allow, late())),
        ),
        (
            "hook_handler",
            refused(kit.hook_handler(HookTrigger::BeforeBroadcast, Allow, late())),
        ),
        (
            "hook on a tool trigger",
            refused(kit.hook(
                HookTrigger::OnToolCall,
                tool_observer,
                HookOptions {
                    execution: HookExecution::Async,
                    ..late()
                },
            )),
        ),
    ]
}

/// RFC §12.10.4: every call but a read and `close()` is refused
/// `KitClosed` once the close started, during it and after it, a
/// synchronous one, a channel's registration or a hook's, as one that
/// writes (Python's `test_every_other_call_is_refused_once_the_close_started`).
/// On fefbfac the kit took the calls that run an operation until its seal,
/// and the registrations always.
#[tokio::test(start_paused = true)]
async fn every_other_call_is_refused_once_the_close_started() {
    let held = Holding::new("ws", [("held", Hold::UntilReleased)]);
    let kit = kit_of(InMemoryStore::default(), &[&held], &["r"]).await;
    a_send_held(&kit, &held, "r").await;
    let close = closing(&kit).await;

    let mut during = the_kit_s_operations(&kit).await;
    during.extend(the_kit_s_registrations(&kit));
    held.release.notify_one();
    closed(close).await;
    let mut after = the_kit_s_operations(&kit).await;
    after.extend(the_kit_s_registrations(&kit));

    let all_refused: Vec<(&str, bool)> = during.iter().map(|&(name, _)| (name, true)).collect();
    assert_eq!((during, after), (all_refused.clone(), all_refused));
}

/// A SYNC `BEFORE_BROADCAST` hook on `text` that tells `entered` it
/// began, waits for `go`, then sends an event into `r`, noting what that
/// call came to in `answered`.
fn sending_hook(
    kit: &RoomKit,
    text: &'static str,
    entered: &Arc<Notify>,
    go: &Arc<Notify>,
    answered: &Arc<Mutex<Vec<String>>>,
) {
    let weak = kit.downgrade();
    let (entered, go, answered) = (Arc::clone(entered), Arc::clone(go), Arc::clone(answered));
    let spec = HookOptions {
        name: "late".to_owned(),
        ..Default::default()
    };
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _context| {
            let (weak, entered, go, answered) = (
                weak.clone(),
                Arc::clone(&entered),
                Arc::clone(&go),
                Arc::clone(&answered),
            );
            let ours = text_of(&event.content) == text;
            async move {
                if ours && let Some(kit) = weak.upgrade() {
                    entered.notify_one();
                    go.notified().await;
                    let content = TextContent::new("from the hook");
                    let sent = kit
                        .send_event(
                            &room("r"),
                            &channel("sms"),
                            content,
                            SendEventOptions::default(),
                        )
                        .await;
                    answered.lock().unwrap().push(format!("{:?}", sent.err()));
                }
                Ok(HookResult::allow(AllowOptions::default()))
            }
        },
        spec,
    )
    .unwrap();
}

/// RFC §12.10.4: a hook still running as the kit closes calls into the
/// kit: refused, while the call it runs for commits within the grace, its
/// delivery dropped, `kit_closed` (Python's
/// `test_a_call_from_a_hook_in_the_close_s_grace_is_refused`). On fefbfac
/// the hook's call was taken.
#[tokio::test(start_paused = true)]
async fn a_call_from_a_hook_in_the_close_s_grace_is_refused() {
    let held = Holding::new("ws", []);
    let kit = kit_of(InMemoryStore::default(), &[&held], &["r"]).await;
    let (entered, go, answered) = (Arc::default(), Arc::default(), Arc::default());
    sending_hook(&kit, "in hook", &entered, &go, &answered);
    let call = spawn_said(&kit, "in hook", "r");
    entered.notified().await;
    let close = spawn_close(&kit);
    tokio::task::yield_now().await;
    go.notify_one();
    closed(close).await;
    let result = result_of(call).await;
    let answered = answered.lock().unwrap().clone();

    assert_eq!(
        (
            answered,
            result.cancellation_reason.as_deref(),
            stored(&kit, "r").await,
            held.sent(),
        ),
        (
            vec!["Some(KitClosed)".to_owned()],
            Some("kit_closed"),
            vec!["in hook".to_owned()],
            Vec::<String>::new(),
        )
    );
}

/// RFC §12.10.4: an observer still running as the kit closes commits a
/// record: refused (Python's
/// `test_a_call_an_observer_makes_as_the_kit_closes_is_refused`).
#[tokio::test(start_paused = true)]
async fn a_call_an_observer_makes_as_the_kit_closes_is_refused() {
    let kit = kit_of(InMemoryStore::default(), &[], &["r"]).await;
    let (entered, go) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let answered = Arc::new(Mutex::new(Vec::new()));
    let (weak, gates, noted) = (
        kit.downgrade(),
        (Arc::clone(&entered), Arc::clone(&go)),
        Arc::clone(&answered),
    );
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |_event: RoomEvent, _context| {
            let (weak, (entered, go), noted) = (weak.clone(), gates.clone(), Arc::clone(&noted));
            async move {
                entered.notify_one();
                go.notified().await;
                if let Some(kit) = weak.upgrade() {
                    let committed = kit
                        .commit_event(&room("r"), record("audit"), CommitEventOptions::default())
                        .await;
                    noted.lock().unwrap().push(format!("{:?}", committed.err()));
                }
            }
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "audit".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    kit.process_inbound(said("hi"), into("r")).await.unwrap();
    entered.notified().await;
    let close = spawn_close(&kit);
    tokio::task::yield_now().await;
    go.notify_one();
    closed(close).await;
    let answered = answered.lock().unwrap().clone();

    assert_eq!(
        (answered, stored(&kit, "r").await),
        (vec!["Some(KitClosed)".to_owned()], vec!["hi".to_owned()])
    );
}

/// A channel whose close sends a last message into `r`, noting what that
/// call came to.
#[derive(Clone, Default)]
struct SaysByeAsItCloses {
    kit: Arc<Mutex<Option<WeakRoomKit>>>,
    answered: Arc<Mutex<Vec<String>>>,
}

impl SaysByeAsItCloses {
    /// The configuration the kit keeps of the channel.
    fn config() -> ChannelConfig {
        transport("bye")
    }
}

impl Channel for SaysByeAsItCloses {
    fn channel_id(&self) -> roomkit::model::ChannelId {
        Self::config().channel_id.clone()
    }

    fn channel_type(&self) -> roomkit::model::ChannelType {
        Self::config().channel_type.clone()
    }

    fn category(&self) -> roomkit::model::ChannelCategory {
        Self::config().category
    }

    fn direction(&self) -> roomkit::model::ChannelDirection {
        Self::config().direction
    }

    fn capabilities(&self) -> roomkit::model::ChannelCapabilities {
        Self::config().capabilities.clone()
    }

    fn provider_name(&self) -> Option<String> {
        Self::config().provider_name.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        Self::config().recipient_key.clone()
    }

    fn supports_streaming_delivery(&self) -> bool {
        Self::config().supports_streaming_delivery
    }

    fn address_config(&self) -> (roomkit::ChannelAddressing, Option<roomkit::CountryCode>) {
        let config = Self::config();
        let addressing = roomkit::ChannelAddressing {
            addressing: config.addressing,
            recipient_key: config.recipient_key.clone(),
            chat_key: config.chat_key.clone(),
            replies_to_sender: false,
        };
        (addressing, config.country_code.clone())
    }

    fn close(&self) -> impl Future<Output = ()> + Send {
        let this = self.clone();
        async move {
            let kit = this
                .kit
                .lock()
                .unwrap()
                .clone()
                .and_then(|kit| kit.upgrade());
            if let Some(kit) = kit {
                let bye = TextContent::new("bye");
                let sent = kit
                    .send_event(
                        &room("r"),
                        &channel("bye"),
                        bye,
                        SendEventOptions::default(),
                    )
                    .await;
                this.answered
                    .lock()
                    .unwrap()
                    .push(format!("{:?}", sent.err()));
            }
        }
    }

    fn handle_inbound(
        &self,
        message: roomkit::model::InboundMessage,
        context: RoomContext,
    ) -> impl std::future::Future<Output = Result<RoomEvent, roomkit::ChannelError>> + Send {
        std::future::ready(Ok(roomkit::default_inbound_event(&message, &context)))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: roomkit::model::ChannelBinding,
        _context: RoomContext,
    ) -> impl std::future::Future<
        Output = Result<roomkit::model::ChannelOutput, roomkit::ChannelError>,
    > + Send {
        std::future::ready(Ok(roomkit::model::ChannelOutput::empty()))
    }
}

/// RFC §12.10.4: a channel's close sends a last message into its room:
/// the call is refused, nothing stored (Python's
/// `test_a_call_a_channel_s_close_makes_is_refused`). On fefbfac it was
/// refused too, the kit sealed before its channels close.
#[tokio::test(start_paused = true)]
async fn a_call_a_channel_s_close_makes_is_refused() {
    let kit = kit_of(InMemoryStore::default(), &[], &["r"]).await;
    let bye = SaysByeAsItCloses::default();
    *bye.kit.lock().unwrap() = Some(kit.downgrade());
    kit.register_channel(bye.clone()).unwrap();
    kit.attach_channel(&room("r"), &channel("bye"), AttachChannelOptions::default())
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(60), kit.close())
        .await
        .unwrap()
        .unwrap();

    let answered = bye.answered.lock().unwrap().clone();
    assert_eq!(
        (answered, stored(&kit, "r").await),
        (vec!["Some(KitClosed)".to_owned()], Vec::<String>::new())
    );
}

/// RMK-1016: calls arriving with no gap do not hold the close open: they
/// are refused, and the close returns with its grace (Python's
/// `test_the_close_returns_while_calls_keep_coming`). On fefbfac they were
/// taken until the seal.
#[tokio::test(start_paused = true)]
async fn the_close_returns_while_calls_keep_coming() {
    let held = Holding::new("ws", [("held", Hold::ForEver)]);
    let kit = kit_of(InMemoryStore::default(), &[&held], &["r"]).await;
    a_send_held(&kit, &held, "r").await;
    let stop = Arc::new(AtomicBool::new(false));
    let refusals = Arc::new(Mutex::new(0_usize));
    let producers: Vec<_> = (0..4)
        .map(|_| {
            let (kit, stop, refusals) = (kit.clone(), Arc::clone(&stop), Arc::clone(&refusals));
            tokio::spawn(async move {
                while !stop.load(Ordering::SeqCst) {
                    let options = ProcessInboundOptions {
                        defer_delivery: true,
                        ..into("r")
                    };
                    if refused(kit.process_inbound(said("traffic"), options).await) {
                        *refusals.lock().unwrap() += 1;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let started = Instant::now();
    let close = tokio::time::timeout(Duration::from_secs(60), kit.close()).await;
    let took = started.elapsed();
    stop.store(true, Ordering::SeqCst);
    for producer in producers {
        producer.await.unwrap();
    }

    assert_eq!(
        (
            close.is_ok(),
            took < Duration::from_secs(8),
            *refusals.lock().unwrap() > 0
        ),
        (true, true, true)
    );
}

/// A transport whose delivery, once let go, sends into its room and fails
/// with what that call returned.
#[derive(Clone)]
struct Relaying {
    kit: WeakRoomKit,
    entered: Arc<Notify>,
    go: Arc<Notify>,
}

impl Relaying {
    /// The configuration the kit keeps of the channel.
    fn config() -> ChannelConfig {
        transport("relay")
    }
}

impl Channel for Relaying {
    fn channel_id(&self) -> roomkit::model::ChannelId {
        Self::config().channel_id.clone()
    }

    fn channel_type(&self) -> roomkit::model::ChannelType {
        Self::config().channel_type.clone()
    }

    fn category(&self) -> roomkit::model::ChannelCategory {
        Self::config().category
    }

    fn direction(&self) -> roomkit::model::ChannelDirection {
        Self::config().direction
    }

    fn capabilities(&self) -> roomkit::model::ChannelCapabilities {
        Self::config().capabilities.clone()
    }

    fn provider_name(&self) -> Option<String> {
        Self::config().provider_name.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        Self::config().recipient_key.clone()
    }

    fn supports_streaming_delivery(&self) -> bool {
        Self::config().supports_streaming_delivery
    }

    fn address_config(&self) -> (roomkit::ChannelAddressing, Option<roomkit::CountryCode>) {
        let config = Self::config();
        let addressing = roomkit::ChannelAddressing {
            addressing: config.addressing,
            recipient_key: config.recipient_key.clone(),
            chat_key: config.chat_key.clone(),
            replies_to_sender: false,
        };
        (addressing, config.country_code.clone())
    }

    fn deliver(
        &self,
        event: RoomEvent,
        _binding: roomkit::model::ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<roomkit::model::ChannelOutput, roomkit::ChannelError>> + Send
    {
        let this = self.clone();
        async move {
            this.entered.notify_one();
            this.go.notified().await;
            let kit = this.kit.upgrade().ok_or(RoomKitError::KitClosed)?;
            let content = TextContent::new(format!("relayed {}", text_of(&event.content)));
            kit.send_event(
                &event.room_id,
                &channel("sms"),
                content,
                SendEventOptions::default(),
            )
            .await?;
            Ok(roomkit::model::ChannelOutput::default())
        }
    }

    fn handle_inbound(
        &self,
        message: roomkit::model::InboundMessage,
        context: RoomContext,
    ) -> impl std::future::Future<Output = Result<RoomEvent, roomkit::ChannelError>> + Send {
        std::future::ready(Ok(roomkit::default_inbound_event(&message, &context)))
    }
}

/// RFC §12.10.4, §5.13: a delivery that fails with the refusal a closing
/// kit returns reads it as Python's delivery reads `KitClosedError`: failed,
/// its code `kit_closed`, retryable, what `RoomKitError::code()` and
/// `retryable()` give. On 01c8420 its code was `ChannelError`.
#[tokio::test(start_paused = true)]
async fn a_send_meeting_the_close_s_refusal_reads_its_code() {
    let kit = kit_of(InMemoryStore::default(), &[], &["r"]).await;
    let relay = Relaying {
        kit: kit.downgrade(),
        entered: Arc::default(),
        go: Arc::default(),
    };
    kit.register_channel(relay.clone()).unwrap();
    kit.attach_channel(
        &room("r"),
        &channel("relay"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let call = spawn_said(&kit, "Hi.", "r");
    relay.entered.notified().await;
    let close = closing(&kit).await;
    relay.go.notify_one();
    closed(close).await;

    let result = result_of(call).await;
    let failed = result
        .delivery_results
        .get(&channel("relay"))
        .map(|delivered| {
            let error = delivered.error.as_ref();
            (
                delivered.status,
                error.map(|error| (error.code.clone(), error.retryable)),
            )
        });
    assert_eq!(
        failed,
        Some((
            roomkit::model::DeliveryResultStatus::Failed,
            Some(("kit_closed".to_owned(), true))
        ))
    );
}

/// RFC §12.10.4: `unsubscribe_room`, a teardown call, returns `false`
/// doing nothing once the close started, the close ending every
/// subscription (Python's `_TEARDOWN`, `unsubscribe_room` answering
/// `False`).
#[tokio::test(start_paused = true)]
async fn an_unsubscribe_once_the_close_started_returns_false() {
    let held = Holding::new("ws", [("held", Hold::UntilReleased)]);
    let kit = kit_of(InMemoryStore::default(), &[&held], &["r"]).await;
    let id = kit
        .subscribe_room(&room("r"), |_| async {}, SubscribeRoomOptions::default())
        .await
        .unwrap();
    a_send_held(&kit, &held, "r").await;
    let close = closing(&kit).await;

    let during = kit.unsubscribe_room(&id).await;
    held.release.notify_one();
    closed(close).await;

    assert_eq!((during, kit.unsubscribe_room(&id).await), (false, false));
}
