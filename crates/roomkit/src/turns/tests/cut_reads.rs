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
use roomkit_core::model::hook::{AllowOptions, BlockOptions};
use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventContent, HookResult, InboundMessage, RoomContext, RoomEvent, TextContent,
};
use roomkit_core::ops::{CheckTimers, EffectId, KitConfig, Next, Operation, SequentialEnvironment};
use serde_json::{Value, json};
use tokio::time::Instant;

use super::support::{
    ReadingOn, Step, Streaming, Stuck, Timed, Waiting, bind, kit_with, of_type, room_id, says,
};
use crate::turns::observe::{Carried, Fate};
use crate::{
    CancelOptions, Channel, ChannelError, DeliveryHandle, HookOptions, HookTrigger, InboundResult,
    ProcessInboundOptions, RoomKit, RoomKitError,
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
    deferred_saying(kit, "Hi").await
}

/// A deferred call into `kit`'s room, Alice writing `text` by SMS: its
/// handle.
async fn deferred_saying(kit: &RoomKit, text: &str) -> DeliveryHandle {
    let message = InboundMessage::new(
        ChannelId::new("sms").unwrap(),
        "+15550001",
        TextContent::new(text),
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

/// A message that never leaves its side.
const UNSENT: Fate = Fate {
    refused: None,
    unsent: true,
    lost: false,
    duplicated: false,
};

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
    wait_under_room_lock(&kit, &handle).await;
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
    assert!(handle.done());
}

// Python: tests/test_delivery_cancellation.py::test_cancel_timeout_reports_incomplete_cleanup_and_can_be_joined
async fn wait_under_room_lock(kit: &RoomKit, handle: &DeliveryHandle) {
    let section = crate::context::Section::enter(&kit.inner.locks, &room_id())
        .await
        .unwrap();
    section
        .scope(async {
            assert!(
                crate::context::ExecutionContext::current()
                    .lease(&room_id())
                    .is_some()
            );
            let partial = tokio::time::timeout(Duration::from_millis(10), handle.wait())
                .await
                .unwrap()
                .unwrap();
            // Under the lock wait returns the original commit snapshot;
            // the final cancellation reason is filled by the later join.
            assert!(!partial.blocked);
            assert_eq!(
                partial
                    .event
                    .map(|event| roomkit_core::rules::transcoding::text_of(&event.content)),
                Some("Hi".to_owned())
            );
            assert!(!handle.done());
        })
        .await;
}

// Python: tests/test_turns_cleanup.py::test_a_cancelled_cascade_reports_its_cleanup_incomplete_while_an_end_is_owed
// A cut turn over for its owner at its cancel bound, its executor's end never
// come, leaves what the executor cleans up not known to be done (RFC
// §10.6.8): cancel(timeout=30) raises TimeoutError at the bound, and each
// wait() after it the same (lanes.py _cleanup, wait_drained). The cancel is
// lost on its way, as an executor stuck in a blocking call never reads it
// (RMK-1328).
#[tokio::test(start_paused = true)]
async fn a_cut_whose_turn_s_end_is_owed_reports_its_cleanup_incomplete() {
    let target = ReadingOn::flushing(Duration::ZERO);
    let kit = room(holding(), &target).await;
    stuck_executor(&kit);
    let handle = deferred(&kit).await;
    target.read.notified().await;
    let start = Instant::now();
    let cancel = handle.cancel(with_timeout(Duration::from_secs(30))).await;
    let elapsed = start.elapsed();
    let waits = [handle.wait().await, handle.wait().await];
    kit.close().await.unwrap();
    assert!(at(elapsed, Duration::from_secs(5)), "{elapsed:?}");
    assert!(matches!(cancel, Err(RoomKitError::Timeout)), "{cancel:?}");
    for waited in waits {
        assert!(matches!(waited, Err(RoomKitError::Timeout)), "{waited:?}");
    }
}

// Python: core/mixins/lane_execution.py reads an awaited call's streams through
// _read_streams, which returns at the cut: the cleanup's TimeoutError reaches
// no caller (lanes.py _cleanup_ended logs it). The call reads its result, its
// turn's end owed (RMK-1328).
#[tokio::test(start_paused = true)]
async fn an_awaited_call_its_key_cut_reads_its_result_while_its_turn_s_end_is_owed() {
    let target = ReadingOn::flushing(Duration::ZERO);
    let kit = room(holding(), &target).await;
    stuck_executor(&kit);
    let mut message = InboundMessage::new(
        ChannelId::new("sms").unwrap(),
        "+15550001",
        TextContent::new("Hi"),
    );
    message.delivery_key = Some("key-1".to_owned());
    let options = ProcessInboundOptions {
        room_id: Some(room_id()),
        ..Default::default()
    };
    let call = tokio::spawn({
        let kit = kit.clone();
        async move { kit.process_inbound(message, options).await }
    });
    target.read.notified().await;
    let cut = kit
        .cancel_delivery("key-1", crate::CancelDeliveryOptions::default())
        .await;
    let called = call.await.unwrap();
    kit.close().await.unwrap();
    assert_eq!(cut, 1);
    let called = called.unwrap();
    assert_eq!(
        called.cancellation_reason.as_deref(),
        Some("caller_cancelled")
    );
}

// Python: lanes.py _cleanup reads end_owed on the cascade's streams only: a
// turn cut before its output, never read, leaves the cleanup done, and
// cancel() returns the result (RMK-1328).
#[tokio::test(start_paused = true)]
async fn a_cut_whose_turn_had_not_answered_reads_its_result_with_its_end_owed() {
    let kit = kit_with(Waiting::new(Duration::from_secs(3600))).await;
    stuck_executor(&kit);
    let handle = deferred(&kit).await;
    // Paused time: the turn runs before the cut.
    tokio::time::sleep(Duration::from_millis(10)).await;
    let cancel = handle.cancel(with_timeout(Duration::from_secs(30))).await;
    let waited = handle.wait().await;
    kit.close().await.unwrap();
    let cancel = cancel.unwrap();
    assert_eq!(
        cancel.cancellation_reason.as_deref(),
        Some("caller_cancelled")
    );
    assert!(waited.is_ok(), "{waited:?}");
}

// Python: the kit's close cancels every cascade in flight (core/_closing.py),
// and wait() joins a cancelled cascade's cleanup (models/delivery.py,
// lanes.py wait_drained): a turn read when the close cut it, its end owed,
// fails the wait with TimeoutError (RMK-1328).
#[tokio::test(start_paused = true)]
async fn a_wait_past_the_kit_s_close_whose_turn_s_end_is_owed_times_out() {
    let target = ReadingOn::flushing(Duration::ZERO);
    let kit = room(holding(), &target).await;
    kit.inner.turns.set_timeouts(crate::turns::TurnTimeouts {
        cancel: Duration::from_millis(50),
        ..Default::default()
    });
    stuck_executor(&kit);
    let handle = deferred(&kit).await;
    target.read.notified().await;
    kit.close().await.unwrap();
    let waits = [handle.wait().await, handle.wait().await];
    for waited in waits {
        assert!(matches!(waited, Err(RoomKitError::Timeout)), "{waited:?}");
    }
}

/// A room whose two agents, `ai` then `ai2`, answer with a stream that ends,
/// and whose streaming target reads one of them to its end, then works on
/// past it as `target` does, the other answered and not read yet.
async fn two_answers(target: &ReadingOn) -> RoomKit {
    let kit = room(ending(), target).await;
    bind(&kit, Streaming::scripted(ending()).named("ai2")).await;
    kit
}

// Python: tests/test_turns_cleanup.py::test_a_cancelled_cascade_waits_for_the_end_of_a_stream_it_never_read
// The cut finds one stream read to its end, its target flushing past it, and
// the other answered and never read. Python's cleanup cuts the target where it
// waits, then closes the unread stream and waits for its end with no bound of
// its own (lanes.py _cleanup, the owner's close_stream): its executor deaf to
// the close, cancel(timeout=1) raises TimeoutError at 1 s, whether the call's
// delivery is over or goes on, a transport still delivering the answer; its
// executor ending it, cancel returns at once (RMK-1442).
#[tokio::test(start_paused = true)]
async fn a_cancelled_cascade_waits_for_the_end_of_a_stream_it_never_read() {
    for (stuck, delivering) in [(true, false), (true, true), (false, false)] {
        let target = ReadingOn::flushing(Duration::from_millis(100));
        let kit = two_answers(&target).await;
        if delivering {
            bind(&kit, Stuck::holding_answers().named("ws2")).await;
        }
        if stuck {
            stuck_executor(&kit);
        }
        let handle = deferred(&kit).await;
        target.past_the_end.notified().await;
        let start = Instant::now();
        let cancel = handle.cancel(with_timeout(Duration::from_secs(1))).await;
        let elapsed = start.elapsed();
        kit.close().await.unwrap();
        if stuck {
            assert!(
                matches!(cancel, Err(RoomKitError::Timeout)),
                "delivering: {delivering}, {cancel:?}"
            );
            assert!(at(elapsed, Duration::from_secs(1)), "{elapsed:?}");
        } else {
            let reason = cancel.unwrap().cancellation_reason;
            assert_eq!(reason.as_deref(), Some("caller_cancelled"));
            assert!(at(elapsed, Duration::ZERO), "{elapsed:?}");
        }
    }
}

// Python: core/mixins/lane_execution.py hands its cascade a delivery set's
// streams once the set ran (add_streams), and a cascade cancelled by then drops
// them unread (drop_unread_streams): its cleanup closes none of them nor waits
// for their end. A cut while a transport still delivers the message finds both
// agents answered and returns at once, its executor deaf to the close (RMK-1442).
#[tokio::test(start_paused = true)]
async fn a_cut_while_its_delivery_set_runs_waits_for_no_stream_the_set_answered() {
    let kit = kit_with(Streaming::scripted(ending())).await;
    bind(&kit, Streaming::scripted(ending()).named("ai2")).await;
    bind(&kit, Stuck::holding_messages()).await;
    stuck_executor(&kit);
    let handle = deferred(&kit).await;
    // Paused time: both agents answer while the transport holds the set.
    tokio::time::sleep(Duration::from_millis(10)).await;
    let start = Instant::now();
    let cancel = handle.cancel(with_timeout(Duration::from_secs(1))).await;
    let elapsed = start.elapsed();
    kit.close().await.unwrap();
    let reason = cancel.unwrap().cancellation_reason;
    assert_eq!(reason.as_deref(), Some("caller_cancelled"));
    assert!(at(elapsed, Duration::ZERO), "{elapsed:?}");
}

/// An awaited call into `kit`'s room, its delivery keyed `key-1`, spawned.
fn awaited(kit: &RoomKit) -> tokio::task::JoinHandle<Result<InboundResult, RoomKitError>> {
    let mut message = InboundMessage::new(
        ChannelId::new("sms").unwrap(),
        "+15550001",
        TextContent::new("Hi"),
    );
    message.delivery_key = Some("key-1".to_owned());
    let options = ProcessInboundOptions {
        room_id: Some(room_id()),
        ..Default::default()
    };
    tokio::spawn({
        let kit = kit.clone();
        async move { kit.process_inbound(message, options).await }
    })
}

// Python: DeliveryCascade.cancel_turns cuts each stream before cancel_delivery
// returns. The driver cannot run between the call below and the wire read on
// this current-thread runtime: no scheduler delay decides the assertion.
#[tokio::test]
async fn cancelling_by_key_signals_the_read_turn_before_the_driver_runs_again() {
    for reason in ["caller_cancelled", "requested_stop"] {
        cancellation_crosses_before_returning(reason).await;
    }
}

async fn cancellation_crosses_before_returning(reason: &str) {
    let target = ReadingOn::flushing(Duration::ZERO);
    let kit = room(holding(), &target).await;
    let controls = cancel_controls(&kit);
    let call = awaited(&kit);
    target.read.notified().await;

    let count = kit
        .cancel_delivery(
            "key-1",
            crate::CancelDeliveryOptions {
                reason: reason.to_owned(),
                ..Default::default()
            },
        )
        .await;
    let crossed = controls.lock().unwrap().clone();

    call.await.unwrap().unwrap();
    kit.close().await.unwrap();
    assert_eq!(count, 1);
    assert_eq!(
        crossed,
        vec![json!(reason)],
        "the cut must cross with its reason before cancel_delivery returns"
    );
}

fn cancel_controls(kit: &RoomKit) -> Arc<Mutex<Vec<Value>>> {
    let controls = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&controls);
    kit.inner.turns.set_tap(Arc::new(move |carried, raw| {
        if carried == Carried::Control && raw.contains(of_type("cancel")) {
            let message: Value = serde_json::from_str(raw).unwrap();
            seen.lock()
                .unwrap()
                .push(message["message"]["reason"].clone());
        }
    }));
    controls
}

// Python: DeliveryCascade.cancel keeps its reason for work handed over
// after the cut (test_a_turn_being_handed_over_is_cut_as_it_arrives).
#[tokio::test]
async fn a_turn_submitted_after_its_delivery_was_cut_receives_the_reason() {
    let agent = Waiting::new(Duration::ZERO);
    let kit = kit_with(agent.clone()).await;
    let controls = cancel_controls(&kit);
    let cancel = crate::delivery_keys::CancelRequest::default();
    cancel.request("before_submission");
    let (turn, _following) = submitted_turn(&kit, |turn| cancel.follows(&turn)).await;
    // The driver observes CancelRequest instead of reading an Output from
    // this cut. Observe its actual end and unfinished work, not a reader
    // invoked outside that delivery-cancellation context.
    tokio::time::timeout(Duration::from_secs(5), turn.ended())
        .await
        .unwrap();
    assert!(!turn.canceller().owed_end().is_owed());
    assert!(
        agent
            .log
            .lock()
            .unwrap()
            .iter()
            .all(|(what, _)| *what != "answered")
    );
    // Late registration cuts only B. C is submitted without that request
    // and must produce its response normally on the same executor.
    let (mut independent, ()) = submitted_turn(&kit, drop).await;
    let (answer, streams) = independent.output(kit.inner.turns.cutting()).await.unwrap();
    assert!(answer.responded);
    assert_eq!(answer.response_events.len(), 1);
    assert!(!streams);
    assert_eq!(
        agent
            .log
            .lock()
            .unwrap()
            .iter()
            .filter(|(what, _)| *what == "answered")
            .count(),
        1
    );
    kit.close().await.unwrap();
    assert_eq!(*controls.lock().unwrap(), vec![json!("before_submission")]);
}

/// An effect id, the first an operation issues: any id serves here.
fn effect() -> EffectId {
    let mut operation = CheckTimers::new(Arc::new(KitConfig::new()), room_id(), None);
    match operation.poll(&SequentialEnvironment::default()).next {
        Next::Effects(effects) => effects.first().unwrap().id,
        Next::Waiting | Next::Ready(_) => panic!("a timer check asks for the lock first"),
    }
}

// RFC §10.1 "Explicit delivery cancellation": a turn being handed to its
// executor is cut, never left running (RMK-1679). The run gives a call up as
// its driver's `give_up` does, the turn it awaits cut, its task aborted, its
// effect answered, while that call, polled on another worker, has not
// submitted its turn yet: an abort lands at the call's next await only, past
// the submission. The turn submitted then is cut as it arrives, its channel
// never runs it, and the run holds it to join its end: with the reason of the
// delivery's cut when one was asked for, as a turn handed over after that cut
// receives it, the run's own otherwise.
#[tokio::test]
async fn a_turn_its_given_up_call_still_submits_is_cut_as_it_arrives() {
    for (asked, reason) in [
        (None, "cancelled"),
        (Some("caller_cancelled"), "caller_cancelled"),
    ] {
        let agent = Waiting::new(Duration::ZERO);
        let kit = kit_with(agent.clone()).await;
        let controls = cancel_controls(&kit);
        let (run, _adoptions) = crate::context::RunShared::new();
        let call = effect();
        let cut = Arc::new(crate::delivery_keys::CancelRequest::default());
        run.calling(call);
        run.cancels_with(call, Arc::clone(&cut));
        if let Some(asked) = asked {
            cut.request(asked);
        }
        assert!(run.cut_turn(call).is_none(), "no turn submitted yet");
        run.answered(call);
        let (_turn, _following) = submitted_turn(&kit, |turn| run.turn_submitted(call, turn)).await;
        let late = run.turns_cut_late();
        assert_eq!(late.len(), 1, "the run joins the turn cut late");
        for turn in late {
            tokio::time::timeout(Duration::from_secs(5), turn.ended())
                .await
                .unwrap();
        }
        kit.close().await.unwrap();
        assert_eq!(agent.ends(), Vec::<&str>::new());
        assert_eq!(*controls.lock().unwrap(), vec![json!(reason)]);
    }
}

#[tokio::test]
async fn a_cancelled_turn_s_end_is_not_an_executor_loss() {
    let kit = kit_with(Streaming::scripted(holding())).await;
    let (mut turn, ()) = submitted_turn(&kit, drop).await;
    let (_, streams) = turn.output(kit.inner.turns.cutting()).await.unwrap();
    assert!(streams);
    assert!(turn.next_item().await.unwrap().is_ok());

    // The control crosses and End arrives before the driver stops reading.
    // This is the interleaving between the two branches of a biased select.
    turn.canceller().cancel("requested_stop");
    turn.ended().await;
    let end = turn.next_item().await;
    kit.close().await.unwrap();
    assert!(
        end.is_none(),
        "a cancelled End is not executor_lost: {end:?}"
    );
}

async fn submitted_turn<G>(
    kit: &RoomKit,
    submitted: impl FnOnce(crate::turns::Canceller) -> G,
) -> (crate::turns::OwnerTurn, G) {
    let room = kit
        .get_room(&room_id(), crate::GetRoomOptions::default())
        .await
        .unwrap();
    let binding = kit
        .get_binding(
            &room_id(),
            &super::support::channel("ai"),
            crate::GetBindingOptions::default(),
        )
        .await
        .unwrap();
    let event = RoomEvent::new(
        room_id(),
        roomkit_core::model::EventSource::new(super::support::channel("sms"), ChannelType::SMS),
        TextContent::new("Hi"),
    );
    let host: Arc<dyn crate::turn::TurnKit> = Arc::new(Arc::downgrade(&kit.inner));
    kit.inner
        .turns
        .submit(&host, &binding, event, RoomContext::new(room), submitted)
        .unwrap()
}

/// Cuts `call`, an awaited call [`awaited`] spawned, by its key: how many
/// deliveries the cut reached, and what the call returned within 1 s, if it
/// did, its `cancellation_reason`.
async fn cut_by_its_key(
    kit: &RoomKit,
    call: tokio::task::JoinHandle<Result<InboundResult, RoomKitError>>,
) -> (usize, Option<Option<String>>) {
    let cut = kit
        .cancel_delivery("key-1", crate::CancelDeliveryOptions::default())
        .await;
    let called = tokio::time::timeout(Duration::from_secs(1), call).await;
    let returned = called
        .ok()
        .map(|called| called.unwrap().unwrap().cancellation_reason);
    (cut, returned)
}

// Python: tests/test_turns_cleanup.py::test_an_awaited_call_cut_waits_for_the_end_of_a_stream_it_never_read
// Python's awaited call reads its streams in a task of its cascade, whose
// `finally` closes every stream of the cascade, those never read included,
// each waiting for its end (inbound_streaming.py _process_streaming_responses):
// cut by its key while its target flushes past one stream, the other never
// read, its executor deaf to the close, the call has not returned 1 s after;
// its executor ending the turn, it returns at once (RMK-1442).
#[tokio::test(start_paused = true)]
async fn an_awaited_call_cut_waits_for_the_end_of_a_stream_it_never_read() {
    let mut seen = Vec::new();
    for stuck in [true, false] {
        let target = ReadingOn::flushing(Duration::from_millis(100));
        let kit = two_answers(&target).await;
        if stuck {
            stuck_executor(&kit);
        }
        let call = awaited(&kit);
        target.past_the_end.notified().await;
        seen.push(cut_by_its_key(&kit, call).await);
        kit.close().await.unwrap();
    }
    let cancelled = Some(Some("caller_cancelled".to_owned()));
    assert_eq!(seen, [(1, None), (1, cancelled)]);
}

// Python: tests/test_turns_cleanup.py::test_an_awaited_call_cut_while_an_answer_s_set_runs_waits_for_its_unread_stream
// Cut before its streams are read, an agent's answer still delivered and the
// streaming agent's stream queued behind that set, Python's awaited call
// waits for its cascade's cleanup (lane_execution.py _finish_cascade,
// wait_drained), which closes that stream with no bound (RMK-1442).
#[tokio::test(start_paused = true)]
async fn an_awaited_call_cut_while_an_answer_s_set_runs_waits_for_its_unread_stream() {
    let mut seen = Vec::new();
    for stuck in [true, false] {
        let kit = kit_with(Waiting::new(Duration::ZERO)).await;
        bind(&kit, Streaming::scripted(ending()).named("ai2")).await;
        bind(&kit, Stuck::holding_answers()).await;
        if stuck {
            stuck_executor(&kit);
        }
        let call = awaited(&kit);
        // Paused time: ai answered, its answer's delivery held; ai2's stream
        // queued behind that set.
        tokio::time::sleep(Duration::from_millis(10)).await;
        seen.push(cut_by_its_key(&kit, call).await);
        kit.close().await.unwrap();
    }
    let cancelled = Some(Some("caller_cancelled".to_owned()));
    assert_eq!(seen, [(1, None), (1, cancelled)]);
}

// Python: tests/test_turns_cleanup.py::test_a_handle_whose_unread_stream_is_closing_is_not_done
// Past a cancel that timed out, the cleanup still closing a stream the call
// never read, its executor deaf: Python's handle is not done, a second
// cancel(timeout=1) raises TimeoutError at 1 s, and wait() does not return
// (models/delivery.py: done reads the cascade drained, wait joins its
// cleanup), whether the call's run is over or went on and ended (RMK-1442).
#[tokio::test(start_paused = true)]
async fn a_handle_whose_unread_stream_is_closing_is_not_done() {
    for delivering in [false, true] {
        let target = ReadingOn::flushing(Duration::from_millis(100));
        let kit = two_answers(&target).await;
        if delivering {
            let held = Stuck::holding_answers_for(Duration::from_secs(3));
            bind(&kit, held.named("ws2")).await;
        }
        stuck_executor(&kit);
        let handle = deferred(&kit).await;
        target.past_the_end.notified().await;
        let first = handle.cancel(with_timeout(Duration::from_secs(1))).await;
        assert!(matches!(first, Err(RoomKitError::Timeout)), "{first:?}");
        // The transport's delivery ends: the run is over, the closing runs on.
        tokio::time::sleep(Duration::from_secs(5)).await;
        let done = handle.done();
        let start = Instant::now();
        let second = handle.cancel(with_timeout(Duration::from_secs(1))).await;
        let elapsed = start.elapsed();
        let waited = tokio::time::timeout(Duration::from_secs(30), handle.wait()).await;
        kit.close().await.unwrap();
        assert!(!done, "delivering: {delivering}");
        assert!(
            matches!(second, Err(RoomKitError::Timeout)),
            "delivering: {delivering}, {second:?}"
        );
        assert!(at(elapsed, Duration::from_secs(1)), "{elapsed:?}");
        assert!(waited.is_err(), "delivering: {delivering}, {waited:?}");
    }
}

/// What the cancel of an adopted call's handle returned, read by the hook that
/// made it: its `cancellation_reason`, or `None` for a `Timeout`, and when.
type Cancelled = Arc<Mutex<Option<(Option<Option<String>>, Duration)>>>;

/// Registers an off-lock hook whose body, on `hello`, makes a deferred call
/// into the room, adopted beside the event, then cancels its handle within
/// 1 s once `target` read one stream to its end.
fn adopting(kit: &RoomKit, target: &ReadingOn) -> Cancelled {
    adopting_within(kit, target, Duration::from_secs(1))
}

/// [`adopting`], the handle cancelled within `bound`.
fn adopting_within(kit: &RoomKit, target: &ReadingOn, bound: Duration) -> Cancelled {
    let past_the_end = Arc::clone(&target.past_the_end);
    adopting_until(kit, past_the_end, Some(bound), false).0
}

/// The handle of the call an off-lock hook made, kept for the test.
type Kept = Arc<Mutex<Option<DeliveryHandle>>>;

/// Registers an off-lock hook whose body, on `hello`, makes a deferred call
/// into the room, adopted beside the event, then cancels its handle within
/// `bound` once `target` read the first item of its stream, and blocks
/// `hello`: nothing more is delivered. The handle is kept.
fn cutting_its_read(kit: &RoomKit, target: &ReadingOn, bound: Duration) -> (Cancelled, Kept) {
    adopting_until(kit, Arc::clone(&target.read), Some(bound), true)
}

/// [`cutting_its_read`], the handle kept uncancelled.
fn keeping_its_read(kit: &RoomKit, target: &ReadingOn) -> Kept {
    adopting_until(kit, Arc::clone(&target.read), None, true).1
}

/// The hook of [`adopting`]: the handle kept, cancelled within `bound`, if
/// any, once `told`, `hello` blocked after when `blocks`.
fn adopting_until(
    kit: &RoomKit,
    told: Arc<tokio::sync::Notify>,
    bound: Option<Duration>,
    blocks: bool,
) -> (Cancelled, Kept) {
    let (cancelled, kept) = (Cancelled::default(), Kept::default());
    let (kit_of_hook, seen, keeps) = (kit.clone(), Arc::clone(&cancelled), Arc::clone(&kept));
    let body = move |event: RoomEvent, _: RoomContext| {
        let (kit, seen, told) = (kit_of_hook.clone(), Arc::clone(&seen), Arc::clone(&told));
        let keeps = Arc::clone(&keeps);
        async move {
            let hello = matches!(&event.content, EventContent::Text(text) if text.body == "hello");
            if !hello {
                return Ok(HookResult::allow(AllowOptions::default()));
            }
            let handle = deferred_saying(&kit, "side").await;
            told.notified().await;
            *keeps.lock().unwrap() = Some(handle.clone());
            if let Some(bound) = bound {
                let start = Instant::now();
                let cancel = handle.cancel(with_timeout(bound)).await;
                let reason = match cancel {
                    Ok(result) => Some(result.cancellation_reason),
                    Err(RoomKitError::Timeout) => None,
                    Err(error) => panic!("{error:?}"),
                };
                *seen.lock().unwrap() = Some((reason, start.elapsed()));
            }
            Ok::<_, crate::HookError>(if blocks {
                // Nothing more to deliver: the test is the side call's.
                HookResult::block("adopted", BlockOptions::default())
            } else {
                HookResult::allow(AllowOptions::default())
            })
        }
    };
    let options = HookOptions {
        name: "adopts".to_owned(),
        needs_lock: false,
        ..Default::default()
    };
    kit.hook(HookTrigger::BeforeBroadcast, body, options)
        .unwrap();
    (cancelled, kept)
}

// Python: tests/test_turns_cleanup.py::test_an_adopted_call_s_handle_waits_for_the_end_of_a_stream_it_never_read
// A deferred call an off-lock hook's body makes is adopted beside the event
// with a cascade of its own (core/mixins/inbound.py _defer_inbound_delivery):
// cut by its handle while its target flushes past one stream, the other never
// read, Python's cancel closes that stream and waits for its end; its
// executor deaf to the close, cancel(timeout=1) raises TimeoutError at 1 s;
// its executor ending the turn, cancel returns at once (RMK-1442).
#[tokio::test(start_paused = true)]
async fn an_adopted_call_s_handle_waits_for_the_end_of_a_stream_it_never_read() {
    let mut seen = Vec::new();
    for stuck in [true, false] {
        let target = ReadingOn::flushing(Duration::from_millis(100));
        let kit = two_answers(&target).await;
        if stuck {
            stuck_executor(&kit);
        }
        let cancelled = adopting(&kit, &target);
        let said = tokio::time::timeout(Duration::from_secs(5), says(&kit, "hello")).await;
        said.unwrap().unwrap();
        kit.close().await.unwrap();
        let (reason, elapsed) = cancelled.lock().unwrap().take().expect("cancelled");
        seen.push((reason, elapsed));
    }
    let [(stuck, stuck_at), (ends, ends_at)] = seen.try_into().unwrap();
    assert_eq!(
        (stuck, ends),
        (None, Some(Some("caller_cancelled".to_owned())))
    );
    assert!(at(stuck_at, Duration::from_secs(1)), "{stuck_at:?}");
    assert!(at(ends_at, Duration::ZERO), "{ends_at:?}");
}

// RFC §10.1, §10.6.8 (RMK-1442 review): the turn of the stream an adopted
// call's cut left unread is over for its owner without its executor's end
// during the closing (its executor deaf and silent), the cleanup incomplete:
// its handle's cancel reads Timeout well within its bound, as a deferred call's.
#[tokio::test(start_paused = true)]
async fn an_adopted_call_whose_unread_turn_s_end_is_owed_times_out() {
    let target = ReadingOn::flushing(Duration::from_millis(100));
    let kit = two_answers(&target).await;
    stuck_and_silent(&kit);
    let bound = Duration::from_secs(60);
    let cancelled = adopting_within(&kit, &target, bound);
    let said = tokio::time::timeout(Duration::from_secs(100), says(&kit, "hello")).await;
    said.unwrap().unwrap();
    kit.close().await.unwrap();
    let (reason, elapsed) = cancelled.lock().unwrap().take().expect("cancelled");
    assert_eq!(reason, None, "Timeout");
    assert!(elapsed < bound, "{elapsed:?}");
}

// Python: tests/test_turns_cleanup.py::test_an_adopted_call_whose_read_turn_s_end_is_owed_times_out
// A deferred call an off-lock hook's body makes is adopted beside the event
// with a cascade of its own: cut by its handle while its target reads its
// agent's stream, its executor deaf to the cut, the turn is over for its owner
// at its cancel bound without its executor's end, the cleanup incomplete
// (lanes.py _cleanup reads end_owed on every stream of the cascade): Python's
// cancel raises TimeoutError at that bound, well within its own timeout, as a
// deferred call's, and so does every wait after it; its executor ending the
// turn, the cancel and the waits return at once (RMK-1510).
#[tokio::test(start_paused = true)]
async fn an_adopted_call_whose_read_turn_s_end_is_owed_times_out() {
    let mut seen = Vec::new();
    for stuck in [true, false] {
        let target = ReadingOn::flushing(Duration::ZERO);
        let kit = room(holding(), &target).await;
        if stuck {
            stuck_executor(&kit);
        }
        let (cancelled, kept) = cutting_its_read(&kit, &target, Duration::from_secs(30));
        let said = tokio::time::timeout(Duration::from_secs(100), says(&kit, "hello")).await;
        said.unwrap().unwrap();
        let handle = kept.lock().unwrap().take().expect("a handle");
        let waits = [handle.wait().await, handle.wait().await];
        kit.close().await.unwrap();
        let waits = waits.map(|waited| match waited {
            Ok(result) => Some(result.cancellation_reason),
            Err(RoomKitError::Timeout) => None,
            Err(error) => panic!("{error:?}"),
        });
        seen.push((cancelled.lock().unwrap().take().expect("cancelled"), waits));
    }
    let [
        ((stuck, stuck_at), stuck_waits),
        ((ends, ends_at), ends_waits),
    ] = seen.try_into().unwrap();
    let cancelled = Some(Some("caller_cancelled".to_owned()));
    assert_eq!(
        (stuck, stuck_waits, ends, ends_waits),
        (
            None,
            [None, None],
            cancelled.clone(),
            [cancelled.clone(), cancelled]
        )
    );
    assert!(at(stuck_at, Duration::from_secs(5)), "{stuck_at:?}");
    assert!(at(ends_at, Duration::ZERO), "{ends_at:?}");
}

// Python: tests/test_turns_cleanup.py::test_a_wait_past_the_kit_s_close_on_an_adopted_call_whose_turn_s_end_is_owed_times_out
// The kit's close cancels every cascade in flight, an adopted call's own among
// them (core/_closing.py Cascades.cut): a turn its target read when the close
// cut it, its end owed, fails every wait on its handle with TimeoutError, as a
// deferred call's (RMK-1510).
#[tokio::test(start_paused = true)]
async fn a_wait_past_the_kit_s_close_on_an_adopted_call_whose_turn_s_end_is_owed_times_out() {
    let target = ReadingOn::flushing(Duration::ZERO);
    let kit = room(holding(), &target).await;
    kit.inner.turns.set_timeouts(crate::turns::TurnTimeouts {
        cancel: Duration::from_millis(50),
        ..Default::default()
    });
    stuck_executor(&kit);
    let kept = keeping_its_read(&kit, &target);
    // Blocked once its hook kept the handle, the call returns while the
    // adopted call's turn reads on (RMK-1512).
    tokio::time::timeout(Duration::from_secs(5), says(&kit, "hello"))
        .await
        .unwrap()
        .unwrap();
    let handle = kept.lock().unwrap().take().expect("a handle");
    kit.close().await.unwrap();
    let waits = [handle.wait().await, handle.wait().await];
    for waited in waits {
        assert!(matches!(waited, Err(RoomKitError::Timeout)), "{waited:?}");
    }
}

/// Makes `kit`'s turns run on an executor stuck in a blocking call, which
/// sends no heartbeat either: its owner takes it for lost.
fn stuck_and_silent(kit: &RoomKit) {
    kit.inner.turns.set_fault(Arc::new(|carried, raw: &str| {
        let stuck = [of_type("cancel"), of_type("close")];
        let control = carried == Carried::Control && stuck.iter().any(|marks| raw.contains(marks));
        let silent = carried == Carried::Frame && raw.contains(of_type("heartbeat"));
        if control || silent {
            UNSENT
        } else {
            Fate::default()
        }
    }));
}

/// Makes `kit`'s turns run on an executor stuck in a blocking call: it reads
/// neither a turn's cut nor its reader's close, and the turn's end never
/// comes.
fn stuck_executor(kit: &RoomKit) {
    kit.inner.turns.set_fault(Arc::new(|carried, raw: &str| {
        let stuck = [of_type("cancel"), of_type("close")];
        if carried == Carried::Control && stuck.iter().any(|marks| raw.contains(marks)) {
            UNSENT
        } else {
            Fate::default()
        }
    }));
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
    assert_eq!(kept, ["dropped"]);
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

// Python: a target's own `await kit.close()`, reading or past a cancel's end: 0.0 s,
// and the code past it runs.
#[tokio::test(start_paused = true)]
async fn a_target_s_own_close_returns_to_it() {
    for (reading, cancels, expected) in [
        (true, false, vec!["closed", "cut"]),
        (false, true, vec!["cut", "closed"]),
    ] {
        let target = ReadingOn::closing(reading);
        let kit = room(holding(), &target).await;
        *target.closes.lock().unwrap() = Some(kit.clone());
        let handle = deferred(&kit).await;
        target.read.notified().await;
        let start = Instant::now();
        if cancels {
            handle.cancel(CancelOptions::default()).await.unwrap();
        }
        kit.close().await.unwrap();
        let elapsed = start.elapsed();
        let kept = target.kept.lock().unwrap().clone();
        assert_eq!(elapsed, Duration::ZERO, "reading: {reading}");
        assert_eq!(kept, expected);
    }
}

// Python: a `finally` that never ends: close takes its grace and its unwind, 7 s,
// and wait() never returns. A future is dropped at the grace's end, nothing to
// unwind, and its read stored what it had: the wait reads the whole record.
#[tokio::test(start_paused = true)]
async fn a_close_past_a_cancel_drops_a_blocked_target_at_its_grace_s_end() {
    let (closed_in, waited, kept) = closed_past_a_cancel(&ReadingOn::blocked()).await;
    assert!(at(closed_in, Duration::from_secs(5)), "{closed_in:?}");
    assert_eq!(kept, ["cut", "dropped"]);
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
