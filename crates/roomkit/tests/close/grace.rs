//! RFC §10.6.8, §12.10.4 (RMK-1017, N8): a call whose run the close
//! aborts at its grace's end while a turn's stream is open, kept unread
//! or read, reads the turn's end, as Python's, whose grace cuts the
//! turn's reader: `kit_closed` once a row of it was stored, else the turn
//! link's `backend_closed`. N8's shape: the witness takes about the grace
//! to deliver "One.", the agent, asked with it, writes "Thinking" then
//! calls a tool of 30 s, and the kit is closed from outside at 0.1 s, so
//! that the lanes go idle just before or after the grace ends at 5.1 s.

use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::{RoomKit, RoomKitOptions};

use crate::drain::{Left, attach_left, texts, told};
use crate::store::SlowCommits;
use crate::support::{Witness, kit_on, kit_with, room_id, spawn_message, stored};

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

/// N8 on threads, eight runs at a time, 40 at 5100 ms: from 5096 to 5100
/// ms each call reads the turn cut once the lanes were idle, or its stream
/// left open by the grace's end, never no reason and no error; at 5200
/// ms, One's set still delivering at the grace's end, the turn's
/// `backend_closed`, as Python's `TurnLinkError`
/// (`rmk1017-fix8/py_n8_reason.out`). On 7a0da49 runs at 5100 ms read no
/// reason and no error, their turn's stream read as the abort came, or
/// its rows taken before the cut and their sets never placed in the
/// lane, and most at 5100 and 5200 ms read `lane_cancelled`
/// (`rmk1017-fix7/lr6c_head.out`, `rmk1017-fix8/conc_*.out`). About 26 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
