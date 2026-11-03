//! A strategy's delegations run at once (RFC §19.7.3, §19.7.4): a parallel
//! team's workers and a loop's parallel reviewers. Python's
//! `core/task_utils.py` `gather_or_cut`.

use std::future::Future;
use std::pin::Pin;
use std::task::Poll;

use crate::error::RoomKitError;

/// Every one of `works`, run together in this task, its context kept:
/// their outputs in order once all succeeded, or the first error to come,
/// the others then dropped, which cuts them, each delegation posting its
/// end (RMK-1279). Each pass polls every work still going before the error
/// answers, so that every work has started, as each of Python's tasks
/// takes its first step before its waiter resumes.
pub(crate) async fn gather_or_cut<T, F>(works: Vec<F>) -> Result<Vec<T>, RoomKitError>
where
    F: Future<Output = Result<T, RoomKitError>>,
{
    let mut works: Vec<Pin<Box<F>>> = works.into_iter().map(Box::pin).collect();
    let mut outputs: Vec<Option<T>> = works.iter().map(|_| None).collect();
    let mut failed: Option<RoomKitError> = None;
    std::future::poll_fn(|context| {
        let mut pending = false;
        for (work, output) in works.iter_mut().zip(outputs.iter_mut()) {
            if output.is_some() {
                continue;
            }
            match work.as_mut().poll(context) {
                Poll::Ready(Ok(done)) => *output = Some(done),
                Poll::Ready(Err(error)) => {
                    failed.get_or_insert(error);
                }
                Poll::Pending => pending = true,
            }
        }
        if failed.is_some() || !pending {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    if let Some(error) = failed {
        drop(works);
        return Err(error);
    }
    Ok(outputs.into_iter().flatten().collect())
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::gather_or_cut;
    use crate::error::RoomKitError;

    type Work = Pin<Box<dyn Future<Output = Result<&'static str, RoomKitError>>>>;

    /// Sets its flag once dropped: a work cut where it waits.
    struct Cut(Arc<AtomicBool>);

    impl Drop for Cut {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    /// A work that starts, then waits for ever: its flags say whether it
    /// started and whether it was cut.
    fn slow() -> (Work, Arc<AtomicBool>, Arc<AtomicBool>) {
        let (started, cut) = (
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        );
        let (on_start, waiting) = (Arc::clone(&started), Cut(Arc::clone(&cut)));
        let work: Work = Box::pin(async move {
            let _waiting = waiting;
            on_start.store(true, Ordering::SeqCst);
            std::future::pending().await
        });
        (work, started, cut)
    }

    fn raising(message: &'static str) -> Work {
        Box::pin(async move { Err(RoomKitError::Value(message.to_owned())) })
    }

    // Python: tests/orchestration/test_supervisor_strategy.py::TestParallelWorkerFailure::test_the_first_error_cuts_the_other_workers
    // Python: tests/orchestration/test_loop_strategy.py::TestReviewParallel::test_the_first_error_cuts_the_other_reviewers
    #[tokio::test]
    async fn the_first_error_cuts_the_others_each_started_first() {
        for raising_first in [false, true] {
            let (work, started, cut) = slow();
            let works = if raising_first {
                vec![raising("boom"), work]
            } else {
                vec![work, raising("boom")]
            };
            // Bounded: a gather that waited for every work would never end.
            let gathered = tokio::time::timeout(Duration::from_secs(5), gather_or_cut(works))
                .await
                .unwrap_or_else(|_| panic!("the gather waited for the slow work"));
            assert!(matches!(gathered, Err(RoomKitError::Value(message)) if message == "boom"));
            assert!(
                started.load(Ordering::SeqCst),
                "the slow work never started"
            );
            assert!(cut.load(Ordering::SeqCst), "the slow work was left running");
        }
    }

    // Python: core/task_utils.py gather_or_cut, its results in order, none for no work
    #[tokio::test]
    async fn the_outputs_come_in_order_and_none_for_no_work() -> Result<(), RoomKitError> {
        let works: Vec<Work> = vec![Box::pin(async { Ok("a") }), Box::pin(async { Ok("b") })];
        assert_eq!(gather_or_cut(works).await?, ["a", "b"]);
        assert_eq!(gather_or_cut(Vec::<Work>::new()).await?, Vec::<&str>::new());
        Ok(())
    }
}
