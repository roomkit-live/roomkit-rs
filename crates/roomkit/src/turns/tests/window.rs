//! RFC §10.6.4, the stream pulled, never pushed: a reader that stops
//! pulling holds no unbounded queue.

use std::sync::atomic::Ordering;
use std::time::Duration;

use pretty_assertions::assert_eq;
use tokio::time::Instant;

use super::support::{Streaming, Stuck, Timed, bind, kit_with, says};
use crate::turns::observe::Carried;
use crate::turns::pulls::PULL_WINDOW;

/// A streaming target that reads one delta of an answer of ten thousand,
/// then stops pulling: the executor reads ahead within the window the
/// owner granted, 64 deltas, and waits there, the turn alive and beating,
/// until the kit's shutdown cuts it.
#[tokio::test(start_paused = true)]
async fn a_reader_that_stops_pulling_holds_the_executor_at_its_window() {
    let agent = Streaming::new(10_000);
    let kit = kit_with(agent.clone()).await;
    bind(&kit, Stuck::new(1)).await;
    let timed = Timed::on(&kit, Instant::now());
    let call = tokio::time::timeout(Duration::from_secs(60), says(&kit, "Talk"));
    let still_reading = call.await.is_err();
    let produced = agent.produced.load(Ordering::SeqCst);
    let items = timed.count(Carried::Frame, "item");
    let pulls = timed.count(Carried::Control, "pull");
    kit.shutdown().await;
    assert_eq!(
        (still_reading, produced, items, pulls),
        (true, window(), window(), 1)
    );
}

fn window() -> usize {
    usize::try_from(PULL_WINDOW).unwrap()
}
