//! Work that starts in the order it was handed over, whatever the runtime's
//! workers: the Python implementation creates its tasks on one event loop,
//! which runs their first steps in the order they were created, and the
//! orders that follow from it (a status bus's entries, a change's
//! `ON_EVENT_DELETED` hooks, RFC §10.3) hold on a multi-thread runtime too.

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::{Mutex, MutexGuard, PoisonError};

use tokio::sync::oneshot;

/// A line of work: each piece handed over polls only once the piece handed
/// over before it was first polled. Only the starts are ordered: once
/// started, each piece runs at its own pace, beside the others.
#[derive(Debug, Default)]
pub(crate) struct StartOrder {
    /// Told once the last piece handed over was first polled, or dropped
    /// unstarted.
    last: Mutex<Option<oneshot::Receiver<()>>>,
}

impl StartOrder {
    /// `work`, to start once the piece handed over before it started: its
    /// place is taken now, at the call.
    pub(crate) fn in_turn<F: Future>(&self, work: F) -> impl Future<Output = F::Output> + use<F> {
        let (started, starting) = oneshot::channel();
        let previous = lock(&self.last).replace(starting);
        async move {
            if let Some(previous) = previous {
                // An error is the piece before dropped unstarted: nothing to
                // wait for.
                let _ = previous.await;
            }
            first_polled(work, started).await
        }
    }
}

/// Runs `work`, telling `started` once its first poll returned: it ran to
/// its first wait, or to its end. A panic in that poll drops `started`,
/// which tells it too.
async fn first_polled<T>(work: impl Future<Output = T>, started: oneshot::Sender<()>) -> T {
    let mut work = pin!(work);
    let mut started = Some(started);
    poll_fn(move |cx| {
        let polled = work.as_mut().poll(cx);
        if let Some(started) = started.take() {
            let _ = started.send(());
        }
        polled
    })
    .await
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    /// Pieces handed over in order start in that order on several workers,
    /// each waiting at its first poll for nothing the others do.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pieces_start_in_the_order_they_were_handed_over_on_several_workers() {
        let order = StartOrder::default();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let handles: Vec<_> = (0..200)
            .map(|piece| {
                let seen = Arc::clone(&seen);
                tokio::spawn(order.in_turn(async move {
                    lock(&seen).push(piece);
                    tokio::task::yield_now().await;
                }))
            })
            .collect();
        for handle in handles {
            tokio::time::timeout(Duration::from_secs(10), handle)
                .await
                .unwrap()
                .unwrap();
        }
        assert_eq!(*lock(&seen), (0..200).collect::<Vec<_>>());
    }

    /// A piece dropped before it started holds up none after it.
    #[tokio::test]
    async fn a_piece_dropped_unstarted_holds_up_none_after_it() {
        let order = StartOrder::default();
        let dropped = order.in_turn(async {});
        let next = order.in_turn(async { 7 });
        drop(dropped);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(10), next)
                .await
                .unwrap(),
            7
        );
    }
}
