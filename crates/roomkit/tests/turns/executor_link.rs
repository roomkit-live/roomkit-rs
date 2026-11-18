//! An application's transport serves a built-in executor without Redis or a store.
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use roomkit::testing::{EchoAgent, EchoAgentOptions};
use roomkit::turns::wire::{Control, Frame};
use roomkit::turns::{ExecutorLink, TurnFuture, TurnRequest};
use roomkit::{RoomKitError, TurnExecutor, TurnExecutorOptions};
use serde_json::Value;
use tokio::sync::{Mutex, Notify, mpsc};

struct Link {
    request: TurnRequest,
    seq: AtomicU64,
    frames: mpsc::UnboundedSender<Frame>,
    controls: Mutex<mpsc::UnboundedReceiver<Control>>,
    read: Notify,
    hold_accepted: bool,
    write: Notify,
    released: Notify,
}

impl ExecutorLink for Link {
    fn request(&self) -> &TurnRequest {
        &self.request
    }
    fn receive(&self) -> TurnFuture<'_, Result<Control, RoomKitError>> {
        Box::pin(async {
            let control = self
                .controls
                .lock()
                .await
                .recv()
                .await
                .ok_or(RoomKitError::KitClosed)?;
            self.read.notify_one();
            Ok(control)
        })
    }
    fn send(&self, frame: Frame) -> TurnFuture<'_, Result<u64, RoomKitError>> {
        Box::pin(async move {
            let held = if self.hold_accepted {
                matches!(frame, Frame::Accepted { .. })
            } else {
                matches!(frame, Frame::End)
            };
            self.frames.send(frame).unwrap();
            if held {
                self.write.notified().await;
            }
            Ok(self.seq.fetch_add(1, Ordering::AcqRel) + 1)
        })
    }
    fn turn_ended(&self) {
        self.released.notify_one();
    }
}

fn link(
    hold_accepted: bool,
) -> (
    Arc<Link>,
    mpsc::UnboundedReceiver<Frame>,
    mpsc::UnboundedSender<Control>,
) {
    let fixture = include_str!("../fixtures/turns/python-case1.jsonl");
    let row: Value = serde_json::from_str(fixture.lines().next().unwrap()).unwrap();
    let request: TurnRequest = serde_json::from_value(row["message"].clone()).unwrap();
    let (frames, arrived) = mpsc::unbounded_channel();
    let (send, controls) = mpsc::unbounded_channel();
    let link = Arc::new(Link {
        request,
        seq: AtomicU64::new(0),
        frames,
        controls: Mutex::new(controls),
        read: Notify::new(),
        hold_accepted,
        write: Notify::new(),
        released: Notify::new(),
    });
    (link, arrived, send)
}

#[tokio::test]
async fn a_real_worker_serves_an_application_link_until_its_terminal_frame_is_written() {
    let (link, mut arrived, _controls) = link(false);
    let worker = TurnExecutor::new(
        |id| {
            std::future::ready(Ok(Some(EchoAgent::new(
                id,
                "from my worker",
                EchoAgentOptions::default(),
            ))))
        },
        TurnExecutorOptions {
            executor_id: Some("mine".into()),
            ..TurnExecutorOptions::default()
        },
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        worker.take(Arc::<Link>::clone(&link)).unwrap();
        assert!(matches!(arrived.recv().await, Some(Frame::Accepted{executor_id}) if executor_id=="mine"));
        let Some(Frame::Output{output,..}) = arrived.recv().await else {panic!("the built channel did not answer")};
        assert!(output.responded);
        assert!(matches!(arrived.recv().await, Some(Frame::End)));
        let mut close = Box::pin(worker.close());
        let status = std::future::poll_fn(|cx| std::task::Poll::Ready(close.as_mut().poll(cx))).await;
        assert!(status.is_pending(),"the worker closed before the End write finished");
        link.write.notify_one();
        close.await;
        link.released.notified().await;
    }).await.unwrap();
}

#[tokio::test]
async fn a_blocked_frame_write_still_reads_a_cancel_from_the_application_backend() {
    let (link, mut arrived, controls) = link(true);
    let build = Arc::new(Notify::new());
    let worker = TurnExecutor::new(
        {
            let build = Arc::clone(&build);
            move |id| {
                let build = Arc::clone(&build);
                async move {
                    build.notified().await;
                    Ok(Some(EchoAgent::new(
                        id,
                        "late",
                        EchoAgentOptions::default(),
                    )))
                }
            }
        },
        TurnExecutorOptions::default(),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        worker.take(Arc::<Link>::clone(&link)).unwrap();
        assert!(matches!(arrived.recv().await, Some(Frame::Accepted { .. })));
        controls
            .send(Control::Cancel {
                reason: "stop".into(),
            })
            .unwrap();
        link.read.notified().await;
        link.write.notify_one();
        assert!(matches!(arrived.recv().await, Some(Frame::End)));
        link.released.notified().await;
        build.notify_one();
        worker.close().await;
    })
    .await
    .unwrap();
}

pub(super) mod reach_owner;
