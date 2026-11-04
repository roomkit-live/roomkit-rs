//! RFC §10.1 "Explicit delivery cancellation", §6.7: a cut call's streaming
//! target, as Python's `DeliveryCascade.cancel_and_wait` leaves it. Python
//! cancels the reader at once: its `finally` runs to its end, however long,
//! the `timeout` bounding the waiter only, and `wait()` reads the whole
//! record. A Rust future has no `finally` to run once dropped: the target is
//! handed the stream's end, and what it does past it stands for it.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventContent, InboundMessage, RoomContext, RoomEvent, TextContent,
};
use serde_json::{Value, json};
use tokio::time::Instant;

use super::support::{ReadingOn, Step, Streaming, Stuck, Timed, bind, kit_with, room_id};
use crate::turns::observe::Carried;
use crate::{
    CancelOptions, Channel, ChannelError, DeliveryHandle, HookExecution, HookOptions, HookTrigger,
    InboundResult, ProcessInboundOptions, RoomKit, RoomKitError,
};

/// A deferred call into a room whose agent's stream is `steps` and whose
/// streaming target reads it as `target` does.
async fn called(steps: Vec<Step>, target: &ReadingOn) -> (RoomKit, DeliveryHandle) {
    let kit = room(steps, target).await;
    let handle = deferred(&kit).await;
    (kit, handle)
}

/// A room whose agent's stream is `steps` and whose streaming target reads it
/// as `target` does.
async fn room(steps: Vec<Step>, target: &ReadingOn) -> RoomKit {
    let kit = kit_with(Streaming::scripted(steps)).await;
    bind(&kit, Stuck::reading_on(target)).await;
    kit
}

/// A deferred call into `kit`'s room: its handle.
async fn deferred(kit: &RoomKit) -> DeliveryHandle {
    let message = InboundMessage::new(
        ChannelId::new("sms").unwrap(),
        "+15550001",
        TextContent::new("Hi"),
    );
    let result = kit
        .process_inbound(
            message,
            ProcessInboundOptions {
                room_id: Some(room_id()),
                defer_delivery: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    result.delivery.expect("a handle")
}

/// A stream the cut finds under way: the turn wrote its record, then holds.
fn holding() -> Vec<Step> {
    vec![
        Step::Write("cited", json!(3)),
        Step::Text("Partial"),
        Step::Hold,
    ]
}

/// A stream that ends: the target reads the end, the turn over.
fn ending() -> Vec<Step> {
    vec![
        Step::Write("cited", json!(3)),
        Step::Text("Hi"),
        Step::Completed,
    ]
}

fn with_timeout(timeout: Duration) -> CancelOptions {
    CancelOptions {
        timeout,
        ..CancelOptions::default()
    }
}

/// What a caller reads of its turns: the record's `cited`, its agent's end.
fn read(result: &InboundResult) -> (Option<Value>, Option<Value>) {
    let record = &result.response_metadata;
    let end = record
        .get("turns")
        .and_then(|turns| turns.get("ai")?.get("loop_end_reason").cloned());
    (record.get("cited"), end)
}

fn cited_and(end: &str) -> (Option<Value>, Option<Value>) {
    (Some(json!(3)), Some(json!(end)))
}

/// Whether `elapsed` is `expected`, on paused time, give or take the timers'
/// rounding: nothing else takes time.
fn at(elapsed: Duration, expected: Duration) -> bool {
    elapsed >= expected && elapsed <= expected + Duration::from_millis(10)
}

// Python: a target's `finally` of 0.2 s, cancel(): 0.202 s, kept=['flushed'];
// of 6 s, cancel(timeout=30): 6.03 s, cited=3, turns present. No grace and no
// bound of the framework's cuts the target's work or answers before it.
#[tokio::test(start_paused = true)]
async fn a_cut_target_s_work_runs_to_its_end_and_its_caller_reads_the_whole_record() {
    for (flush, timeout) in [
        (Duration::from_millis(200), Duration::from_secs(5)),
        (Duration::from_secs(6), Duration::from_secs(30)),
    ] {
        let target = ReadingOn::flushing(flush);
        let (kit, handle) = called(holding(), &target).await;
        target.read.notified().await;
        let start = Instant::now();
        let cut = handle.cancel(with_timeout(timeout)).await.unwrap();
        let elapsed = start.elapsed();
        let kept = target.kept.lock().unwrap().clone();
        kit.close().await.unwrap();
        assert!(at(elapsed, flush), "{elapsed:?} for a flush of {flush:?}");
        assert_eq!(kept, ["cut", "flushed"]);
        assert_eq!(read(&cut), cited_and("cancelled"));
    }
}

// Python: cancel(timeout=0.001): TimeoutError at 0.001 s, the cleanup goes on,
// wait(): 0.2 s later, cited=3.
#[tokio::test(start_paused = true)]
async fn a_cancel_past_its_timeout_leaves_the_target_working_and_wait_reads_the_record() {
    let flush = Duration::from_millis(200);
    let target = ReadingOn::flushing(flush);
    let (kit, handle) = called(holding(), &target).await;
    target.read.notified().await;
    let start = Instant::now();
    let cancel = handle.cancel(with_timeout(Duration::from_millis(1))).await;
    let timed_out_at = start.elapsed();
    let kept_at_the_timeout = target.kept.lock().unwrap().clone();
    let waited = handle.wait().await.unwrap();
    let elapsed = start.elapsed();
    let kept = target.kept.lock().unwrap().clone();
    kit.close().await.unwrap();
    assert!(matches!(cancel, Err(RoomKitError::Timeout)), "{cancel:?}");
    assert!(
        at(timed_out_at, Duration::from_millis(1)),
        "{timed_out_at:?}"
    );
    assert_eq!(kept_at_the_timeout, ["cut"]);
    assert!(at(elapsed, flush), "{elapsed:?}");
    assert_eq!(kept, ["cut", "flushed"]);
    assert_eq!(read(&waited), cited_and("cancelled"));
}

// Python: the cancel reaches the turn and the target's `finally` together:
// a tool stuck in the turn is not left running for as long as the target works.
#[tokio::test(start_paused = true)]
async fn a_cut_target_s_work_does_not_hold_the_cut_of_its_turn() {
    let target = ReadingOn::flushing(Duration::from_secs(6));
    let kit = room(holding(), &target).await;
    let timed = Timed::on(&kit, Instant::now());
    let handle = deferred(&kit).await;
    target.read.notified().await;
    let cancelling = tokio::spawn({
        let handle = handle.clone();
        async move { handle.cancel(CancelOptions::default()).await }
    });
    target.past_the_end.notified().await;
    tokio::time::sleep(Duration::from_millis(1)).await;
    let crossed = timed.count(Carried::Control, "cancel");
    drop(cancelling);
    kit.close().await.unwrap();
    assert_eq!(crossed, 1);
}

// Python: a target past its stream's end, blocked at an await: the cancel lands
// there, 0.000 s, cited=3, turns completed. A Rust future is dropped there.
#[tokio::test(start_paused = true)]
async fn a_target_blocked_past_its_stream_s_end_does_not_hold_its_caller() {
    let target = ReadingOn::blocked();
    let (kit, handle) = called(ending(), &target).await;
    target.past_the_end.notified().await;
    let start = Instant::now();
    let cut = handle
        .cancel(with_timeout(Duration::from_secs(30)))
        .await
        .unwrap();
    let elapsed = start.elapsed();
    kit.close().await.unwrap();
    assert_eq!(elapsed, Duration::ZERO);
    assert_eq!(read(&cut), cited_and("completed"));
    assert_eq!(cut.cancellation_reason.as_deref(), Some("caller_cancelled"));
}

// Python: a target past the real end works until the cancel lands in that work,
// its `finally` too: 0.000 s, kept=[].
#[tokio::test(start_paused = true)]
async fn a_target_working_past_its_stream_s_end_is_cut_where_it_waits() {
    let target = ReadingOn::flushing(Duration::from_secs(1));
    let (kit, handle) = called(ending(), &target).await;
    target.past_the_end.notified().await;
    let start = Instant::now();
    handle.cancel(CancelOptions::default()).await.unwrap();
    let elapsed = start.elapsed();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let kept = target.kept.lock().unwrap().clone();
    kit.close().await.unwrap();
    assert_eq!(elapsed, Duration::ZERO);
    assert_eq!(kept, Vec::<&str>::new());
}

/// Past a `cancel` of 1 ms that timed out, the kit's close and the handle's
/// wait: how long the close took, the wait's end, what the target kept.
async fn closed_past_a_cancel(target: &ReadingOn) -> (Duration, InboundResult, Vec<&'static str>) {
    let (kit, handle) = called(holding(), target).await;
    target.read.notified().await;
    let cancel = handle
        .cancel(CancelOptions {
            reason: "stopped".to_owned(),
            timeout: Duration::from_millis(1),
        })
        .await;
    assert!(matches!(cancel, Err(RoomKitError::Timeout)), "{cancel:?}");
    let start = Instant::now();
    kit.close().await.unwrap();
    let closed_in = start.elapsed();
    let waited = handle.wait().await.unwrap();
    let kept = target.kept.lock().unwrap().clone();
    (closed_in, waited, kept)
}

// Python: cancel(reason="stopped", timeout=0.001), close(), wait(), a target's
// `finally` of 0.2 s: the close finds the cascade cancelled and leaves it to
// end: close 0.2 s, kept=['flushed'], reason=stopped, cited=3.
#[tokio::test(start_paused = true)]
async fn a_close_past_a_cancel_lets_the_target_finish_and_keeps_the_cancel_s_reason() {
    let flush = Duration::from_millis(200);
    let (closed_in, waited, kept) = closed_past_a_cancel(&ReadingOn::flushing(flush)).await;
    assert!(at(closed_in, Duration::from_millis(199)), "{closed_in:?}");
    assert_eq!(kept, ["cut", "flushed"]);
    assert_eq!(waited.cancellation_reason.as_deref(), Some("stopped"));
    assert_eq!(read(&waited), cited_and("cancelled"));
}

// Python: a `close()` with no cancel before it cuts the cascade itself; a target's
// `finally` of 0.2 s runs within its grace: close 0.203 s, kept=['flushed'],
// reason=kit_closed, cited=3.
#[tokio::test(start_paused = true)]
async fn a_close_finding_a_target_reading_lets_it_finish() {
    let flush = Duration::from_millis(200);
    let target = ReadingOn::flushing(flush);
    let (kit, handle) = called(holding(), &target).await;
    target.read.notified().await;
    let start = Instant::now();
    kit.close().await.unwrap();
    let closed_in = start.elapsed();
    let waited = handle.wait().await.unwrap();
    let kept = target.kept.lock().unwrap().clone();
    assert!(at(closed_in, flush), "{closed_in:?}");
    assert_eq!(kept, ["cut", "flushed"]);
    assert_eq!(waited.cancellation_reason.as_deref(), Some("kit_closed"));
    assert_eq!(read(&waited), cited_and("cancelled"));
}

// Python's `record_error`: a target that fails on the close's cut: its ON_ERROR
// fires once, and the call reads `kit_closed`, no error (RFC 12.10.4).
#[tokio::test(start_paused = true)]
async fn a_target_that_fails_on_the_close_s_cut_leaves_the_call_no_error() {
    let target = ReadingOn::failing();
    let kit = room(holding(), &target).await;
    let errors = Arc::new(Mutex::new(0));
    let counted = Arc::clone(&errors);
    kit.hook(
        HookTrigger::OnError,
        move |_event: RoomEvent, _context| {
            *counted.lock().unwrap() += 1;
            async {}
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "errors".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let handle = deferred(&kit).await;
    target.read.notified().await;
    kit.close().await.unwrap();
    let waited = handle.wait().await.unwrap();
    assert_eq!(waited.error, None);
    assert_eq!(waited.cancellation_reason.as_deref(), Some("kit_closed"));
    assert_eq!(*errors.lock().unwrap(), 1);
}

// Python: a `finally` that never ends: close takes its grace and its unwind, 7 s,
// and wait() never returns. A future is dropped at the grace's end, nothing to
// unwind, and its read stored what it had: the wait reads the whole record.
#[tokio::test(start_paused = true)]
async fn a_close_past_a_cancel_drops_a_blocked_target_at_its_grace_s_end() {
    let (closed_in, waited, kept) = closed_past_a_cancel(&ReadingOn::blocked()).await;
    assert!(at(closed_in, Duration::from_secs(5)), "{closed_in:?}");
    assert_eq!(kept, ["cut"]);
    assert_eq!(waited.cancellation_reason.as_deref(), Some("stopped"));
    assert_eq!(read(&waited), cited_and("cancelled"));
}

/// A streaming transport with the default `deliver_stream`, which buffers the
/// text and delivers it at the stream's end: what it was delivered.
#[derive(Clone, Default)]
struct Buffering {
    delivered: Arc<Mutex<Vec<String>>>,
}

impl Channel for Buffering {
    fn channel_id(&self) -> ChannelId {
        ChannelId::new("ws").unwrap()
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::WEBSOCKET
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Transport
    }

    fn direction(&self) -> ChannelDirection {
        ChannelDirection::Bidirectional
    }

    fn supports_streaming_delivery(&self) -> bool {
        true
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(crate::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        if let EventContent::Text(text) = &event.content {
            self.delivered.lock().unwrap().push(text.body.clone());
        }
        std::future::ready(Ok(ChannelOutput::empty()))
    }
}

// Python: Channel.deliver_stream's default delivers what it buffered at the
// stream's end or on its failure; a cancel goes through it, nothing delivered.
#[tokio::test(start_paused = true)]
async fn the_default_target_cut_delivers_nothing_it_buffered() {
    let target = Buffering::default();
    let kit = kit_with(Streaming::scripted(holding())).await;
    bind(&kit, target.clone()).await;
    let handle = deferred(&kit).await;
    tokio::time::sleep(Duration::from_millis(1)).await;
    handle.cancel(CancelOptions::default()).await.unwrap();
    let delivered = target.delivered.lock().unwrap().clone();
    kit.close().await.unwrap();
    assert!(
        delivered.iter().all(|text| text != "Partial"),
        "the text buffered was delivered: {delivered:?}"
    );
}
