//! RFC §10.6.8, §12.10.4 (RMK-1017, N8): a call whose run the close
//! aborts at its grace's end while a turn's stream is open, kept unread
//! or read, reads the turn's end, as Python's, whose grace cuts the
//! turn's reader: `kit_closed` once a row of it was stored and never
//! delivered, nothing more once its rows were delivered, and the turn
//! link's `backend_closed` when nothing of it was stored. N8's shape: the
//! witness takes about the grace to deliver "One.", the agent, asked with
//! it, writes "Thinking" then calls a tool of 30 s, and the kit is closed
//! from outside at 0.1 s, so that the lanes go idle just before or after
//! the grace ends at 5.1 s.

use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{HookResult, RoomId};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, HookExecution, HookSpec, HookTrigger, RoomKit,
    RoomKitOptions, text_of,
};

use crate::drain::{Left, attach_left, texts, told};
use crate::store::SlowCommits;
use crate::support::{
    Witness, channel, kit_on, kit_with, room_id, spawn_message, stored, stored_in,
};

/// What a call came to, `(cancellation_reason, "type: message")` of its
/// result, and what the room stores once the close returned.
type Row = (Option<(Option<String>, Option<String>)>, Vec<String>);

/// N8 once: the witness taking `witness_ms` to deliver "One.".
async fn n8(witness_ms: u64) -> Row {
    let witness = Witness::slow("One.", Duration::from_millis(witness_ms));
    let kit = kit_with(&witness).await;
    attach_left(&kit, Left::Tool).await;
    let one = spawn_message(&kit, "One.", room_id());
    tokio::time::sleep(Duration::from_millis(100)).await;
    tokio::time::timeout(Duration::from_secs(60), kit.close())
        .await
        .unwrap()
        .unwrap();
    (told(one).await, stored(&kit).await)
}

/// The turn cut once the lanes were idle: what it said stored, its
/// delivery dropped.
fn said() -> Row {
    (
        Some((Some("kit_closed".to_owned()), None)),
        texts(&["One.", "Thinking", "[Tool call: slow]", "[Tool call: slow]"]),
    )
}

/// The turn's stream left open by the abort, nothing of it stored.
fn closed() -> Row {
    (
        Some((
            None,
            Some("TurnLinkError: backend_closed: the turn backend closed".to_owned()),
        )),
        texts(&["One."]),
    )
}

/// RFC §10.6.8 (RMK-1017, light review 6, fix 3): a run the close aborts
/// keeping the stream of a turn it never read carries that turn's
/// `backend_closed`, the turn still open as the kit closes. N8's race at
/// the grace's edge, made sure: One's set leaves its place at 1 s, the
/// write of its room's delivery cursor taking 10 s, so its turn's stream,
/// answered and kept, is not read yet when the grace ends at 5.1 s, and
/// the run is aborted there. Python's reader of a turn `turns.close`
/// abandons fails the same row `TurnLinkError`, nothing of the turn
/// stored (`rmk1017-lightreview5/n8_py.out`, tool, 5099 ms). On c4cc718
/// the call read no reason and no error.
#[tokio::test(start_paused = true)]
async fn a_turn_kept_unread_at_the_abort_fails_backend_closed() {
    let store = SlowCommits::advancing(room_id(), Duration::from_secs(10));
    let witness = Witness::slow("One.", Duration::from_secs(1));
    let kit = kit_on(RoomKit::new(RoomKitOptions::with_store(store)), &witness).await;
    attach_left(&kit, Left::Tool).await;
    let one = spawn_message(&kit, "One.", room_id());
    tokio::time::sleep(Duration::from_millis(100)).await;
    tokio::time::timeout(Duration::from_secs(60), kit.close())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((told(one).await, stored(&kit).await), closed());
}

/// N8 on paused time, once per witness: the lanes idle before the grace
/// ends cut the turn then; at 5100 ms and past it the grace's end leaves
/// the turn's stream open, kept as One's set still delivers, and the call
/// reads the turn's `backend_closed`, as Python's reads its
/// `TurnLinkError` there (`rmk1017-fix8/py_n8_reason.out`, 5 of 5 runs),
/// where it read `lane_cancelled` (7a0da49).
#[tokio::test(start_paused = true)]
async fn n8_at_the_grace_s_end() {
    let mut rows = Vec::new();
    for witness_ms in [5095, 5099, 5100, 5200] {
        rows.push((witness_ms, n8(witness_ms).await));
    }
    assert_eq!(
        rows,
        [
            (5095, said()),
            (5099, said()),
            (5100, closed()),
            (5200, closed())
        ]
    );
}

/// Whether `row` is the turn cut once the lanes were idle: what it said
/// stored, "Thinking" at least, its delivery dropped.
fn cut_once_idle(row: &Row) -> bool {
    let (told, stored) = row;
    *told == Some((Some("kit_closed".to_owned()), None))
        && stored.get(..2) == Some(&texts(&["One.", "Thinking"])[..])
}

/// RFC §10.6.8 (RMK-1017, light review 8, H2): a row of the turn's
/// stream stored and delivered before the abort reads no reason. A SYNC
/// hook on the tool call's row outlives the close (its timeout 600 s, it
/// sleeps 400 s): "Go." and "Thinking" are stored and delivered, the run
/// is aborted with the read under way and the tool call's row in its
/// hook, and the call reads no reason and no error, as Python's, whose
/// call reads the same once every row was delivered
/// (`rmk1017-lightreview8/py_hook.out`). On 065bf3c it read `kit_closed`,
/// the delivered "Thinking" counted as a row stored and never delivered.
#[tokio::test(start_paused = true)]
async fn a_row_delivered_before_the_abort_reads_no_reason() {
    let witness = Witness::new();
    let kit = kit_with(&witness).await;
    let mut spec = HookSpec::new(
        "slow-row",
        HookTrigger::BeforeBroadcast,
        HookExecution::Sync,
    );
    spec.timeout = Duration::from_secs(600);
    kit.hook(spec, move |event, _context| {
        let slow = text_of(&event.content).starts_with("[Tool call");
        async move {
            if slow {
                tokio::time::sleep(Duration::from_secs(400)).await;
            }
            Ok(HookResult::allow())
        }
    })
    .unwrap();
    attach_left(&kit, Left::Tool).await;
    let go = spawn_message(&kit, "Go.", room_id());
    tokio::time::sleep(Duration::from_millis(100)).await;
    tokio::time::timeout(Duration::from_mins(15), kit.close())
        .await
        .unwrap()
        .unwrap();
    let call = told(go).await;
    tokio::time::sleep(Duration::from_secs(500)).await;
    assert_eq!(
        (call, stored(&kit).await, witness.delivered()),
        (
            Some((None, None)),
            texts(&["Go.", "Thinking"]),
            texts(&["Go.", "Thinking"])
        )
    );
}

/// RFC §10.6.8 (RMK-1017, light review 8, DET): a read under way at the
/// grace's end, made sure. `room_2`'s witness takes 30 s to deliver
/// "Two.", so the lanes stay busy past the grace and no turn is cut once
/// they are idle; in `room_1` the witness takes 1 s on "One.", so the
/// agent's stream is read from 1 s, after the close (0.1 s) took its
/// snapshot, and the grace's end aborts One's run with the read under
/// way. An agent that says nothing: One reads the turn link's
/// `backend_closed`, nothing of the turn stored; one that wrote: what it
/// said stored and never delivered, `kit_closed`; Two's delivery cut
/// running reads `lane_cancelled`, as Python's in all three rows, its
/// `TurnLinkError` `executor_lost` there
/// (`rmk1017-lightreview8/py_det.out`). Without the read's mark, with a
/// read that returned as the close drained unmarked, and on 7a0da49, the
/// silent agent's call read no reason and no error.
#[tokio::test(start_paused = true)]
async fn a_read_under_way_at_the_grace_s_end_reads_the_turn_s_end() {
    let mut rows = Vec::new();
    for left in [Left::Output, Left::Stream, Left::Tool] {
        let witness = Witness::slow("One.", Duration::from_secs(1));
        let kit = kit_with(&witness).await;
        let w2 = Witness::slow("Two.", Duration::from_secs(30)).named("w2");
        kit.register_channel(w2.clone()).unwrap();
        let room_2 = RoomId::new("room_2").unwrap();
        kit.create_room(CreateRoomOptions {
            room_id: Some(room_2.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
        for id in ["sms", "w2"] {
            kit.attach_channel(&room_2, &channel(id), AttachChannelOptions::default())
                .await
                .unwrap();
        }
        attach_left(&kit, left).await;
        let two = spawn_message(&kit, "Two.", room_2.clone());
        let one = spawn_message(&kit, "One.", room_id());
        tokio::time::sleep(Duration::from_millis(100)).await;
        tokio::time::timeout(Duration::from_secs(60), kit.close())
            .await
            .unwrap()
            .unwrap();
        rows.push((
            left,
            (told(one).await, stored(&kit).await),
            (told(two).await, stored_in(&kit, &room_2).await),
        ));
    }
    let two = || {
        (
            Some((Some("lane_cancelled".to_owned()), None)),
            texts(&["Two."]),
        )
    };
    let said = |stored: &[&str]| (Some((Some("kit_closed".to_owned()), None)), texts(stored));
    assert_eq!(
        rows,
        [
            (Left::Output, closed(), two()),
            (Left::Stream, said(&["One.", "Think"]), two()),
            (
                Left::Tool,
                said(&["One.", "Thinking", "[Tool call: slow]", "[Tool call: slow]"]),
                two()
            ),
        ]
    );
}

/// N8 on threads, eight runs at a time, 40 at 5100 ms: from 5096 to 5100
/// ms each call reads the turn cut once the lanes were idle, or its stream
/// left open by the grace's end, never no reason and no error; at 5200
/// ms, One's set still delivering at the grace's end, the turn's
/// `backend_closed`, as Python's `TurnLinkError`
/// (`rmk1017-fix8/py_n8_reason.out`). On 7a0da49 runs at 5100 ms read no
/// reason and no error, their turn's stream read as the abort came, or
/// its rows taken before the cut and their sets never placed in the
/// lane, and most at 5100 and 5200 ms read `lane_cancelled`
/// (`rmk1017-fix7/lr6c_head.out`, `rmk1017-fix8/conc_*.out`). On demand:
/// it takes about 26 s of real time and catches its races only
/// statistically; the paused tests above carry the suite. Run it with
/// `cargo test -p roomkit --test close -- --ignored
/// grace::n8_at_the_grace_s_end_on_threads`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a race caught statistically in 26 s of real time, run on demand"]
async fn n8_at_the_grace_s_end_on_threads() {
    let mut wrong = Vec::new();
    for batch in 0..5 {
        let mut witnesses = vec![5100; 8];
        if batch == 0 {
            witnesses.extend([5096, 5097, 5098, 5099, 5200, 5200]);
        }
        let runs: Vec<_> = witnesses
            .into_iter()
            .map(|witness_ms| (witness_ms, tokio::spawn(n8(witness_ms))))
            .collect();
        for (witness_ms, run) in runs {
            let row = run.await.unwrap();
            let allowed = row == closed() || (witness_ms < 5200 && cut_once_idle(&row));
            if !allowed {
                wrong.push((witness_ms, row));
            }
        }
    }
    assert_eq!(wrong, []);
}
