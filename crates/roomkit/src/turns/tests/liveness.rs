//! RFC §10.6.8, liveness both ways, on tokio's paused clock with the RFC's
//! default bounds: a heartbeat each way every 2 seconds, 10 seconds of
//! silence before a side is lost, 5 seconds for a cancelled turn to end.

use std::time::Duration;

use pretty_assertions::assert_eq;
use tokio::time::Instant;

use super::support::{Timed, Waiting, fault, kit_with, of_type, says};
use crate::turns::TurnTimeouts;
use crate::turns::observe::{Carried, Fate};

const UNSENT: Fate = Fate {
    refused: false,
    unsent: true,
    lost: false,
    duplicated: false,
};

/// The error type and the message of `result`'s turn error, in Python's
/// `TurnLinkError` words (RMK-1106).
fn failure(result: &crate::InboundResult) -> Option<(String, String)> {
    let error = result.error.as_ref()?;
    Some((error.error_type.clone(), error.message.clone()))
}

/// A turn busy for 30 seconds, three times `heartbeat_timeout`, still
/// beating, ends as it would have: the framework sets no timeout around
/// `on_event()` (RFC §13.6).
#[tokio::test(start_paused = true)]
async fn a_busy_turn_past_the_heartbeat_timeout_ends_normally() {
    let agent = Waiting::new(Duration::from_secs(30));
    let kit = kit_with(agent.clone()).await;
    let started = Instant::now();
    let result = says(&kit, "Hi").await.unwrap();
    assert_eq!(
        (
            result.error,
            started.elapsed() >= Duration::from_secs(30),
            agent.ends()
        ),
        (None, true, vec!["answered"])
    );
}

/// An executor whose heartbeats never reach its owner is lost once the
/// owner heard nothing for `heartbeat_timeout`: the turn fails
/// `executor_lost`, and the executor, told so, cuts its work.
#[tokio::test(start_paused = true)]
async fn dropped_executor_heartbeats_fail_the_turn_executor_lost() {
    let agent = Waiting::new(Duration::from_secs(60));
    let kit = kit_with(agent.clone()).await;
    fault(&kit, Carried::Frame, of_type("heartbeat"), UNSENT);
    let started = Instant::now();
    let result = says(&kit, "Hi").await.unwrap();
    let failed_after = started.elapsed();
    crate::testing::idle(&kit).await;
    assert_eq!(
        (
            failure(&result),
            (Duration::from_secs(10)..=Duration::from_secs(14)).contains(&failed_after),
            agent.ends()
        ),
        (
            Some((
                "TurnLinkError".to_owned(),
                "The turn ended without its executor: executor_lost".to_owned()
            )),
            true,
            vec!["dropped"]
        )
    );
}

/// An owner whose heartbeats never reach its executor is lost to it once
/// it heard nothing for `heartbeat_timeout`: the executor cuts the turn
/// `owner_lost` and sends nothing more, so the owner, hearing nothing
/// either, fails it `executor_lost`.
#[tokio::test(start_paused = true)]
async fn dropped_owner_heartbeats_cut_the_turn_owner_lost() {
    let agent = Waiting::new(Duration::from_secs(60));
    let kit = kit_with(agent.clone()).await;
    fault(&kit, Carried::Control, of_type("heartbeat"), UNSENT);
    let started = Instant::now();
    let result = says(&kit, "Hi").await.unwrap();
    let failed_after = started.elapsed();
    let dropped_at = agent.log.lock().unwrap().first().map(|(_, at)| *at);
    assert_eq!(
        (
            failure(&result),
            dropped_at.is_some_and(|at| (Duration::from_secs(10)..=Duration::from_secs(14)).contains(&at)),
            failed_after > Duration::from_secs(20),
            agent.ends()
        ),
        (
            Some(("TurnLinkError".to_owned(), "The turn ended without its executor: executor_lost".to_owned())),
            true,
            true,
            vec!["dropped"]
        )
    );
}

/// A turn whose cancel its executor never answers (the cancel lost on its
/// way, as an executor stuck in a blocking call would never read it) is
/// given up at `cancel_timeout`: the owner stops beating and releases the
/// turn, whose frames queue nowhere from then on, and the release reaching
/// the executor at once, in memory, it cuts the turn as one whose owner
/// was lost (RFC §10.6.8).
#[tokio::test(start_paused = true)]
async fn an_unanswered_cancel_is_given_up_at_the_cancel_timeout_and_late_frames_dropped() {
    let agent = Waiting::new(Duration::from_secs(60));
    let kit = kit_with(agent.clone()).await;
    fault(&kit, Carried::Control, of_type("cancel"), UNSENT);
    let started = Instant::now();
    let timed = Timed::on(&kit, started);
    // The caller leaves once the turn runs: its turn is cut (RFC §10.1
    // "Explicit delivery cancellation").
    let gone = tokio::time::timeout(Duration::from_secs(1), says(&kit, "Hi")).await;
    crate::testing::idle(&kit).await;
    let given_up = Duration::from_secs(1) + Duration::from_secs(5);
    let late_frames = timed.after(Carried::Frame, given_up);
    let late_controls = timed.after(Carried::Control, given_up);
    let beats_before = timed.count(Carried::Frame, "heartbeat");
    let dropped_at = agent.log.lock().unwrap().first().map(|(_, at)| *at);
    assert_eq!(
        (
            gone.is_err(),
            late_frames,
            late_controls,
            beats_before >= 2,
            // Cut as the release reached it.
            dropped_at
                .is_some_and(|at| (given_up..given_up + Duration::from_secs(1)).contains(&at)),
        ),
        (true, Vec::new(), Vec::new(), true, true)
    );
}

/// RFC §10.6.3: a message lost on its way fails the turn `frame_lost` at
/// the next one; a duplicate is dropped.
#[tokio::test(start_paused = true)]
async fn a_lost_frame_fails_the_turn_frame_lost_and_a_duplicate_is_dropped() {
    let lost = Fate {
        refused: false,
        unsent: false,
        lost: true,
        duplicated: false,
    };
    let duplicated = Fate {
        refused: false,
        unsent: false,
        lost: false,
        duplicated: true,
    };
    let agent = Waiting::new(Duration::from_secs(5));
    let kit = kit_with(agent.clone()).await;
    fault(&kit, Carried::Frame, of_type("accepted"), duplicated);
    let doubled = says(&kit, "Hi").await.unwrap();
    fault(&kit, Carried::Frame, of_type("heartbeat"), lost);
    let gapped = says(&kit, "Again").await.unwrap();
    assert_eq!(
        (failure(&doubled), failure(&gapped)),
        (
            None,
            Some((
                "TurnLinkError".to_owned(),
                "Message 2 of the turn was lost".to_owned()
            ))
        )
    );
}

/// A turn whose work panics fails at once, as a call that panicked fails
/// its effect: its executor tells the owner as its task unwinds, rather
/// than leave the owner to learn it from its silence.
#[tokio::test(start_paused = true)]
async fn a_turn_whose_work_panics_fails_at_once() {
    let kit = kit_with(super::support::Panicking::new()).await;
    let started = Instant::now();
    let result = says(&kit, "Hi").await.unwrap();
    assert_eq!(
        (
            result.error.map(|error| (error.error_type, error.message)),
            started.elapsed() < Duration::from_secs(1)
        ),
        (
            Some(("Panic".to_owned(), "the turn's work panicked".to_owned())),
            true
        )
    );
}

/// RFC §10.6.8: an executor that never says it took the turn, its
/// `accepted` and its beats lost, fails the turn at `accept_timeout`, here
/// 4 seconds, before `heartbeat_timeout` would; the executor it was handed
/// to is lost, not missing (`executor_lost`).
#[tokio::test(start_paused = true)]
async fn a_turn_never_accepted_fails_at_the_accept_timeout() {
    let agent = Waiting::new(Duration::from_secs(60));
    let kit = kit_with(agent.clone()).await;
    crate::testing::set_turn_timeouts(
        &kit,
        TurnTimeouts {
            accept: Duration::from_secs(4),
            ..Default::default()
        },
    );
    kit.inner
        .turns
        .set_fault(std::sync::Arc::new(|carried, raw: &str| {
            let silenced = raw.contains(of_type("accepted")) || raw.contains(of_type("heartbeat"));
            if carried == Carried::Frame && silenced {
                UNSENT
            } else {
                Fate::default()
            }
        }));
    let started = Instant::now();
    let result = says(&kit, "Hi").await.unwrap();
    let failed_after = started.elapsed();
    assert_eq!(
        (
            failure(&result),
            (Duration::from_secs(4)..Duration::from_secs(7)).contains(&failed_after)
        ),
        (
            Some((
                "TurnLinkError".to_owned(),
                "The turn ended without its executor: executor_lost".to_owned()
            )),
            true
        )
    );
}

/// RFC §10.6.8 `backend_closed`: once the kit's turn backend closed, a turn
/// is refused at once, its reason its own, not a lost executor's.
#[tokio::test(start_paused = true)]
async fn a_turn_on_a_closed_backend_fails_backend_closed() {
    let agent = Waiting::new(Duration::from_secs(1));
    let kit = kit_with(agent.clone()).await;
    kit.inner.turns.close();
    let result = says(&kit, "Hi").await.unwrap();
    assert_eq!(
        (failure(&result), agent.ends()),
        (
            Some((
                "TurnLinkError".to_owned(),
                "The turn ended: its turn backend closed".to_owned()
            )),
            Vec::new()
        )
    );
}

/// RFC §10.6.5: a turn's `on_event()` runs whole within its turn and in
/// the execution context of the call that submitted it, the work it does
/// before its future included: it reads its turn, and a task it spawns
/// there with `keep_context`, posting into the room, acts for the turn, so
/// the turn awaiting it answers. (Run before its future's first poll, that
/// work stood outside the turn's context: the post waited for the room's
/// lane the turn held, for ever.)
#[tokio::test(start_paused = true)]
async fn the_work_on_event_does_before_its_future_is_the_turn_s() {
    let agent = super::support::Eager::new();
    let kit = kit_with(agent.clone()).await;
    *agent.kit.lock().unwrap() = Some(kit.clone());
    let answered = tokio::time::timeout(Duration::from_secs(60), says(&kit, "Hi")).await;
    agent.kit.lock().unwrap().take();
    assert_eq!(
        (
            answered.map(|result| result.unwrap().error),
            agent.turned.lock().unwrap().clone()
        ),
        (Ok(None), vec![true])
    );
}
