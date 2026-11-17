//! A standalone stream still owns its unreported external calls after being dropped.
use super::*;
use roomkit_ai::memory::{MemoryProvider, MemoryResult, RetrieveOptions};
use std::pin::Pin;
use std::sync::atomic::AtomicBool;

/// Channel-owned memory is closed after calls; the model provider is the host's.
pub(super) struct OwnedMemory(pub(super) Arc<AtomicBool>);
impl MemoryProvider for OwnedMemory {
    fn retrieve<'a>(
        &'a self,
        _: &'a roomkit::model::RoomId,
        _: &'a RoomEvent,
        _: &'a RoomContext,
        _: RetrieveOptions,
    ) -> Pin<Box<dyn Future<Output = Result<MemoryResult, roomkit::ChannelError>> + Send + 'a>>
    {
        Box::pin(std::future::ready(Ok(MemoryResult::default())))
    }
    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.0.store(true, Ordering::SeqCst);
        Box::pin(std::future::ready(()))
    }
}

// Python: test_external_call_cut.py cancellation cleanup; native standalone tracking
// also proves that a retained callback can close its own channel without self-wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_standalone_call_is_reported_before_resources_close() {
    for streaming in [false, true] {
        for own_close in [false, true] {
            cleanup(streaming, own_close).await;
        }
    }
}

async fn cleanup(streaming: bool, own_close: bool) {
    let handler = Arc::new(Handler {
        hold: true,
        ..Handler::default()
    });
    let closed = Arc::new(AtomicBool::new(false));
    let agent = AiChannel::new(
        "ai".parse().unwrap(),
        model(streaming, AiToolCall::new("c1", "remote")),
        AiChannelOptions {
            memory: Some(Arc::new(OwnedMemory(Arc::clone(&closed)))),
            external_tool_handler: Some(Arc::<Handler>::clone(&handler)),
            ..AiChannelOptions::default()
        },
    )
    .unwrap();
    if own_close {
        let own_channel = agent.clone();
        *handler.close.lock().unwrap() = Some(Box::pin(async move {
            own_channel.close().await;
        }));
    }
    let task = tokio::spawn(turn(agent.clone()));
    bounded(handler.entered.notified()).await;
    assert_eq!(agent.active_turns(), 1);
    task.abort();
    assert!(bounded(task).await.unwrap_err().is_cancelled());
    bounded(handler.cancelled.notified()).await;
    assert_eq!(
        agent.active_turns(),
        0,
        "cleanup is not an addressable AI loop"
    );
    if own_close {
        bounded(handler.closed.notified()).await;
    } else {
        // Poll close itself, not merely a spawned task. It must join the
        // registered cleanup that is still held in its callback.
        let close = agent.close();
        tokio::pin!(close);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut close)
                .await
                .is_err()
        );
        assert!(handler.reports.lock().unwrap().is_empty());
        assert!(
            !closed.load(Ordering::SeqCst),
            "owned memory stays open while cleanup is held"
        );
        handler.release.notify_one();
        bounded(close).await;
        assert!(closed.load(Ordering::SeqCst));
        assert_eq!(*handler.reports.lock().unwrap(), ["cancelled"]);
        return;
    }
    handler.release.notify_one();
    bounded(async {
        loop {
            if !handler.reports.lock().unwrap().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(*handler.reports.lock().unwrap(), ["cancelled"]);
    bounded(agent.close()).await;
}
