//! RFC §12.10.4: the kit's one shutdown, as Python's `TestOneShutdown`.

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::{Channel, ChannelDeclaration, KitError, RoomKit, RoomKitOptions};
use tokio::sync::Notify;

use crate::support::{BOUND, channel, transport};

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

impl Channel for GatedClose {
    fn declaration(&self) -> ChannelDeclaration {
        transport("sms1")
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
    declaration: ChannelDeclaration,
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
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn close(&self) -> impl Future<Output = ()> + Send {
        let this = self.clone();
        async move {
            assert!(!this.panics, "this channel's close panics");
            this.closed.store(true, Ordering::SeqCst);
        }
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
    let failed = || KitError::ChannelsFailedToClose {
        failures: vec![(channel("a"), "this channel's close panics".to_owned())],
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

impl Channel for HangingClose {
    fn declaration(&self) -> ChannelDeclaration {
        transport("hang")
    }

    fn close(&self) -> impl Future<Output = ()> + Send {
        let entered = Arc::clone(&self.entered);
        async move {
            entered.notify_one();
            std::future::pending::<()>().await;
        }
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
    assert_eq!(later.unwrap(), Err(KitError::ShutdownCancelled));
}
