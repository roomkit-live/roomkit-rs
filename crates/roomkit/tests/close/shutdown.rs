//! RFC §12.10.4: the kit's one shutdown, as Python's `TestOneShutdown`.

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::RoomId;
use roomkit::{Channel, CreateRoomOptions, GetRoomOptions, RoomKit, RoomKitError, RoomKitOptions};
use roomkit_core::ops::ChannelConfig;
use tokio::sync::Notify;

use crate::store::StuckStore;
use crate::support::{BOUND, Witness, channel, transport};

/// A channel whose close waits until released, counting its closes; given
/// a kit, it closes that kit from its own close.
#[derive(Clone, Default)]
struct GatedClose {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    closes: Arc<AtomicUsize>,
    finished: Arc<AtomicBool>,
    closes_the_kit: Arc<Mutex<Option<RoomKit>>>,
}

impl GatedClose {
    /// The configuration the kit keeps of the channel.
    fn config() -> ChannelConfig {
        transport("sms1")
    }
}

impl Channel for GatedClose {
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
            this.closes.fetch_add(1, Ordering::SeqCst);
            this.entered.notify_one();
            this.release.notified().await;
            let kit = this.closes_the_kit.lock().unwrap().clone();
            if let Some(kit) = kit {
                kit.close().await.unwrap();
            }
            this.finished.store(true, Ordering::SeqCst);
        }
    }

    fn handle_inbound(
        &self,
        message: roomkit::model::InboundMessage,
        context: roomkit::model::RoomContext,
    ) -> impl std::future::Future<Output = Result<roomkit::model::RoomEvent, roomkit::ChannelError>> + Send
    {
        std::future::ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: roomkit::model::RoomEvent,
        _binding: roomkit::model::ChannelBinding,
        _context: roomkit::model::RoomContext,
    ) -> impl std::future::Future<
        Output = Result<roomkit::model::ChannelOutput, roomkit::ChannelError>,
    > + Send {
        std::future::ready(Ok(roomkit::model::ChannelOutput::empty()))
    }
}

/// RFC §12.10.4: a close called while another runs, its caller still
/// waiting or its future dropped, waits for the same shutdown, which runs
/// to its end, and both return once it ended; a close once it ended
/// returns at once; the channels close once.
#[tokio::test(start_paused = true)]
async fn a_close_during_another_returns_once_the_shutdown_ends() {
    let mut cases = Vec::new();
    for first_dropped in [false, true] {
        let kit = RoomKit::new(RoomKitOptions::default());
        let gated = GatedClose::default();
        kit.register_channel(gated.clone()).unwrap();
        let closes = || {
            let kit = kit.clone();
            tokio::spawn(async move { kit.close().await })
        };
        let first = closes();
        tokio::time::timeout(BOUND, gated.entered.notified())
            .await
            .unwrap();
        if first_dropped {
            first.abort();
        }
        let mut second = closes();
        let early = tokio::time::timeout(Duration::from_secs(1), &mut second).await;
        gated.release.notify_one();
        let waited = early.is_err();
        let second_ended = waited
            && tokio::time::timeout(BOUND, second)
                .await
                .is_ok_and(|ended| ended.unwrap().is_ok());
        let first_ended = match tokio::time::timeout(BOUND, first).await.unwrap() {
            Ok(answered) => answered.is_ok(),
            Err(dropped) => dropped.is_cancelled(),
        };
        let later = tokio::time::timeout(BOUND, kit.close()).await;
        cases.push((
            first_dropped,
            waited,
            first_ended && second_ended,
            later.is_ok_and(|closed| closed.is_ok()),
            gated.closes.load(Ordering::SeqCst),
            gated.finished.load(Ordering::SeqCst),
        ));
    }
    assert_eq!(
        cases,
        [
            (false, true, true, true, 1, true),
            (true, true, true, true, 1, true)
        ]
    );
}

/// RFC §12.10.4: a close called from the shutdown's own work, a channel's
/// close, returns at once rather than wait for itself.
#[tokio::test]
async fn a_close_from_the_shutdown_s_own_work_returns_at_once() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let gated = GatedClose::default();
    *gated.closes_the_kit.lock().unwrap() = Some(kit.clone());
    gated.release.notify_one();
    kit.register_channel(gated.clone()).unwrap();
    tokio::time::timeout(BOUND, kit.close())
        .await
        .unwrap()
        .unwrap();
    assert!(gated.finished.load(Ordering::SeqCst));
}

/// A channel whose close panics, or notes that it closed.
#[derive(Clone)]
struct Closing {
    declaration: ChannelConfig,
    panics: bool,
    closed: Arc<AtomicBool>,
}

impl Closing {
    fn new(id: &str, panics: bool) -> Self {
        Self {
            declaration: transport(id),
            panics,
            closed: Arc::default(),
        }
    }
}

impl Channel for Closing {
    fn channel_id(&self) -> roomkit::model::ChannelId {
        self.declaration.channel_id.clone()
    }

    fn channel_type(&self) -> roomkit::model::ChannelType {
        self.declaration.channel_type.clone()
    }

    fn category(&self) -> roomkit::model::ChannelCategory {
        self.declaration.category
    }

    fn direction(&self) -> roomkit::model::ChannelDirection {
        self.declaration.direction
    }

    fn capabilities(&self) -> roomkit::model::ChannelCapabilities {
        self.declaration.capabilities.clone()
    }

    fn provider_name(&self) -> Option<String> {
        self.declaration.provider_name.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        self.declaration.recipient_key.clone()
    }

    fn supports_streaming_delivery(&self) -> bool {
        self.declaration.supports_streaming_delivery
    }

    fn address_config(&self) -> (roomkit::ChannelAddressing, Option<roomkit::CountryCode>) {
        let config = &self.declaration;
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
            assert!(!this.panics, "this channel's close panics");
            this.closed.store(true, Ordering::SeqCst);
        }
    }

    fn handle_inbound(
        &self,
        message: roomkit::model::InboundMessage,
        context: roomkit::model::RoomContext,
    ) -> impl std::future::Future<Output = Result<roomkit::model::RoomEvent, roomkit::ChannelError>> + Send
    {
        std::future::ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: roomkit::model::RoomEvent,
        _binding: roomkit::model::ChannelBinding,
        _context: roomkit::model::RoomContext,
    ) -> impl std::future::Future<
        Output = Result<roomkit::model::ChannelOutput, roomkit::ChannelError>,
    > + Send {
        std::future::ready(Ok(roomkit::model::ChannelOutput::empty()))
    }
}

/// RFC §12.10.4: a channel whose close fails (panics) stops neither the
/// channels after it nor is reported as a success: every close, the first
/// included, returns the failure naming the channel, and the shutdown
/// never runs again, as Python's `ExceptionGroup` is raised to each caller
/// (`test_a_close_after_a_failed_one_raises_the_same_failure`). It panicked
/// the shutdown's task and left channel `b` open on 1d8796c
/// (tasks/probes/rmk1017-review-A/review_probe_1d8796c.out).
#[tokio::test]
async fn a_close_after_a_failed_one_returns_the_same_failure() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let (a, b) = (Closing::new("a", true), Closing::new("b", false));
    kit.register_channel(a).unwrap();
    kit.register_channel(b.clone()).unwrap();
    let first = tokio::time::timeout(BOUND, kit.close()).await.unwrap();
    let later = tokio::time::timeout(BOUND, kit.close()).await.unwrap();
    let failed = || RoomKitError::ExceptionGroup {
        failures: vec![(channel("a"), "this channel's close panics".to_owned())],
    };
    assert_eq!(
        (first, later, b.closed.load(Ordering::SeqCst)),
        (Err(failed()), Err(failed()), true)
    );
}

/// A channel whose `close` panics in the call itself, before it returns
/// its future, or notes that it closed.
#[derive(Clone)]
struct EagerPanic {
    declaration: ChannelConfig,
    panics: bool,
    closed: Arc<AtomicBool>,
}

impl Channel for EagerPanic {
    fn channel_id(&self) -> roomkit::model::ChannelId {
        self.declaration.channel_id.clone()
    }

    fn channel_type(&self) -> roomkit::model::ChannelType {
        self.declaration.channel_type.clone()
    }

    fn category(&self) -> roomkit::model::ChannelCategory {
        self.declaration.category
    }

    fn direction(&self) -> roomkit::model::ChannelDirection {
        self.declaration.direction
    }

    fn capabilities(&self) -> roomkit::model::ChannelCapabilities {
        self.declaration.capabilities.clone()
    }

    fn provider_name(&self) -> Option<String> {
        self.declaration.provider_name.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        self.declaration.recipient_key.clone()
    }

    fn supports_streaming_delivery(&self) -> bool {
        self.declaration.supports_streaming_delivery
    }

    fn address_config(&self) -> (roomkit::ChannelAddressing, Option<roomkit::CountryCode>) {
        let config = &self.declaration;
        let addressing = roomkit::ChannelAddressing {
            addressing: config.addressing,
            recipient_key: config.recipient_key.clone(),
            chat_key: config.chat_key.clone(),
            replies_to_sender: false,
        };
        (addressing, config.country_code.clone())
    }

    fn close(&self) -> impl Future<Output = ()> + Send {
        assert!(
            !self.panics,
            "this channel's close panics before its future"
        );
        let closed = Arc::clone(&self.closed);
        async move { closed.store(true, Ordering::SeqCst) }
    }

    fn handle_inbound(
        &self,
        message: roomkit::model::InboundMessage,
        context: roomkit::model::RoomContext,
    ) -> impl std::future::Future<Output = Result<roomkit::model::RoomEvent, roomkit::ChannelError>> + Send
    {
        std::future::ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: roomkit::model::RoomEvent,
        _binding: roomkit::model::ChannelBinding,
        _context: roomkit::model::RoomContext,
    ) -> impl std::future::Future<
        Output = Result<roomkit::model::ChannelOutput, roomkit::ChannelError>,
    > + Send {
        std::future::ready(Ok(roomkit::model::ChannelOutput::empty()))
    }
}

/// RFC §12.10.4: a channel whose `close` panics before it returns its
/// future fails as one whose future panics: the channels after it close,
/// and every close returns the failure. On 9dfe09b the panic came before
/// the catch, killing the shutdown's task: channel `b` was never closed and
/// both closes read `ShutdownCancelled` (`rmk1017-lightreview/
/// lr_probe_p7_head.out`).
#[tokio::test]
async fn a_channel_close_that_panics_before_its_future_fails_the_same_way() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let channel_of = |id, panics| EagerPanic {
        declaration: transport(id),
        panics,
        closed: Arc::default(),
    };
    let b = channel_of("b", false);
    kit.register_channel(channel_of("a", true)).unwrap();
    kit.register_channel(b.clone()).unwrap();
    let first = tokio::time::timeout(BOUND, kit.close()).await.unwrap();
    let later = tokio::time::timeout(BOUND, kit.close()).await.unwrap();
    let failed = || RoomKitError::ExceptionGroup {
        failures: vec![(
            channel("a"),
            "this channel's close panics before its future".to_owned(),
        )],
    };
    assert_eq!(
        (first, later, b.closed.load(Ordering::SeqCst)),
        (Err(failed()), Err(failed()), true)
    );
}

/// A channel whose close never ends, saying when it began.
#[derive(Clone, Default)]
struct HangingClose {
    entered: Arc<Notify>,
}

impl HangingClose {
    /// The configuration the kit keeps of the channel.
    fn config() -> ChannelConfig {
        transport("hang")
    }
}

impl Channel for HangingClose {
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
        let entered = Arc::clone(&self.entered);
        async move {
            entered.notify_one();
            std::future::pending::<()>().await;
        }
    }

    fn handle_inbound(
        &self,
        message: roomkit::model::InboundMessage,
        context: roomkit::model::RoomContext,
    ) -> impl std::future::Future<Output = Result<roomkit::model::RoomEvent, roomkit::ChannelError>> + Send
    {
        std::future::ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: roomkit::model::RoomEvent,
        _binding: roomkit::model::ChannelBinding,
        _context: roomkit::model::RoomContext,
    ) -> impl std::future::Future<
        Output = Result<roomkit::model::ChannelOutput, roomkit::ChannelError>,
    > + Send {
        std::future::ready(Ok(roomkit::model::ChannelOutput::empty()))
    }
}

/// RFC §12.10.4: a shutdown cut itself, its runtime ending under it as a
/// process's end cancels every task, answers a later caller with an error
/// saying the shutdown did not finish, as Python's `RoomKitError`
/// (`test_a_close_after_its_shutdown_was_cut_says_so`); it returned `()`
/// on 1d8796c.
#[test]
fn a_close_after_its_shutdown_was_cut_says_so() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let hanging = HangingClose::default();
    kit.register_channel(hanging.clone()).unwrap();
    let first = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    first.block_on(async {
        tokio::spawn({
            let kit = kit.clone();
            async move { kit.close().await }
        });
        hanging.entered.notified().await;
    });
    drop(first);
    let second = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let later = second.block_on(async { tokio::time::timeout(BOUND, kit.close()).await });
    assert_eq!(later.unwrap(), Err(RoomKitError::ShutdownCancelled));
}

/// RFC §12.10.4: the kit's close closes its store once, after its
/// channels, as Python's close releases its store (decided 2026-10-31); a
/// store with nothing to release, as the in-memory one, still serves a
/// read after it, as Python's (`tasks/probes/step5-lot5/py_kit_closes_store.out`).
#[tokio::test]
async fn the_close_closes_the_store_once_after_the_channels() {
    let witness = Witness::new();
    let store = StuckStore::default().closing_after(&witness.closed);
    let kit = RoomKit::new(RoomKitOptions::with_store(store));
    kit.register_channel(witness.clone()).unwrap();
    let room = RoomId::new("room-1").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.close().await.unwrap();
    kit.close().await.unwrap();
    let read = kit.get_room(&room, GetRoomOptions::default()).await;
    let closes = kit.store().closes.lock().unwrap().clone();
    assert_eq!((closes, read.map(|room| room.id)), (vec![true], Ok(room)));
}
