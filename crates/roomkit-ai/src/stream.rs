//! Streams written as async code, pulled by their reader, and the
//! concurrency of a round's calls, both within the task that reads them.
//!
//! - [`items`] turns an async producer into a [`Stream`]: the producer runs
//!   only while the stream is polled, and each item it hands over waits
//!   until the reader took it, so the producer never runs ahead of its
//!   reader (RFC §10.6.4: the reader sets the pace).
//! - [`join_all`] runs futures concurrently in the current task: what they
//!   read of the task (the turn's host, the call's execution context)
//!   stays theirs, as no task is spawned.

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use futures_core::Stream;

/// Where a producer hands the items of its stream.
#[derive(Debug)]
pub(crate) struct Yielder<T> {
    slot: Arc<Mutex<Option<T>>>,
}

impl<T: Unpin> Yielder<T> {
    /// Hands `item` to the stream's reader, and returns once the reader
    /// asks for the next one.
    pub(crate) fn give(&self, item: T) -> impl Future<Output = ()> + '_ {
        Give {
            slot: &self.slot,
            item: Some(item),
        }
    }
}

/// The hand-over of one item: pending once, while the reader takes it.
struct Give<'a, T> {
    slot: &'a Mutex<Option<T>>,
    item: Option<T>,
}

impl<T: Unpin> Future for Give<'_, T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<()> {
        let give = self.get_mut();
        match give.item.take() {
            // The stream returns the item at once and polls the producer
            // again for the next one: no wake is needed.
            Some(item) => {
                *give.slot.lock().unwrap_or_else(PoisonError::into_inner) = Some(item);
                Poll::Pending
            }
            None => Poll::Ready(()),
        }
    }
}

/// A stream of the items a producer hands over (see [`items`]).
pub(crate) struct Items<'a, T> {
    producer: Option<Pin<Box<dyn Future<Output = ()> + Send + 'a>>>,
    slot: Arc<Mutex<Option<T>>>,
}

impl<T> std::fmt::Debug for Items<'_, T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Items")
            .field("running", &self.producer.is_some())
            .finish_non_exhaustive()
    }
}

/// The stream of the items `produce` hands to its [`Yielder`], in order,
/// ending when `produce` returns.
pub(crate) fn items<'a, T, F, Fut>(produce: F) -> Items<'a, T>
where
    T: Send + Unpin + 'a,
    F: FnOnce(Yielder<T>) -> Fut,
    Fut: Future<Output = ()> + Send + 'a,
{
    let slot = Arc::new(Mutex::new(None));
    let yielder = Yielder {
        slot: Arc::clone(&slot),
    };
    Items {
        producer: Some(Box::pin(produce(yielder))),
        slot,
    }
}

impl<T> Items<'_, T> {
    fn take(&self) -> Option<T> {
        self.slot
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }
}

impl<T> Stream for Items<'_, T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<T>> {
        let this = self.get_mut();
        let Some(producer) = this.producer.as_mut() else {
            return Poll::Ready(None);
        };
        match producer.as_mut().poll(context) {
            Poll::Ready(()) => {
                this.producer = None;
                Poll::Ready(this.take())
            }
            Poll::Pending => match this.take() {
                Some(item) => Poll::Ready(Some(item)),
                None => Poll::Pending,
            },
        }
    }
}

/// The next item of `stream`, `None` once it ended.
pub(crate) async fn next<S: Stream + Unpin + ?Sized>(stream: &mut S) -> Option<S::Item> {
    poll_fn(|context| Pin::new(&mut *stream).poll_next(context)).await
}

/// A future of [`join_all`], and its output once it is ready.
type Running<F> = (Pin<Box<F>>, Option<<F as Future>::Output>);

/// The outputs of `futures`, in their order, run concurrently in the
/// current task.
pub(crate) async fn join_all<F: Future>(futures: Vec<F>) -> Vec<F::Output> {
    let mut running: Vec<Running<F>> = futures
        .into_iter()
        .map(|future| (Box::pin(future), None))
        .collect();
    poll_fn(|context| {
        let mut done = true;
        for (future, output) in &mut running {
            if output.is_none() {
                match future.as_mut().poll(context) {
                    Poll::Ready(ready) => *output = Some(ready),
                    Poll::Pending => done = false,
                }
            }
        }
        if done { Poll::Ready(()) } else { Poll::Pending }
    })
    .await;
    running
        .into_iter()
        .filter_map(|(_, output)| output)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use pretty_assertions::assert_eq;

    use super::*;

    #[tokio::test]
    async fn a_producer_s_items_come_in_order_and_the_stream_ends_with_it() {
        let mut stream = items(|out: Yielder<u32>| async move {
            for item in [1, 2, 3] {
                out.give(item).await;
            }
        });
        let mut read = Vec::new();
        while let Some(item) = next(&mut stream).await {
            read.push(item);
        }
        assert_eq!(read, [1, 2, 3]);
    }

    /// RFC §10.6.4: the producer never runs ahead of its reader.
    #[tokio::test]
    async fn a_producer_goes_on_only_once_its_item_was_read_and_the_next_asked() {
        let produced = Arc::new(AtomicUsize::new(0));
        let counting = Arc::clone(&produced);
        let mut stream = items(move |out: Yielder<usize>| async move {
            for item in 0..3 {
                counting.fetch_add(1, Ordering::SeqCst);
                out.give(item).await;
            }
        });
        let first = next(&mut stream).await;
        let after_first = produced.load(Ordering::SeqCst);
        let second = next(&mut stream).await;
        assert_eq!(
            (first, after_first, second, produced.load(Ordering::SeqCst)),
            (Some(0), 1, Some(1), 2)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn futures_joined_run_together_and_keep_their_order() {
        let slow = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            "slow"
        };
        let fast = async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            "fast"
        };
        let started = tokio::time::Instant::now();
        let futures: Vec<Pin<Box<dyn Future<Output = &str>>>> =
            vec![Box::pin(slow), Box::pin(fast)];
        let outputs = join_all(futures).await;
        assert_eq!(
            (outputs, started.elapsed()),
            (vec!["slow", "fast"], Duration::from_millis(20))
        );
    }
}
