//! Tool hooks run in their turn's captured context, not the connection's.

use roomkit::{ToolCallReport, TurnHost, keep_context};
use std::sync::{Mutex, PoisonError};
use tokio::sync::{mpsc, oneshot};

pub(super) type Pending = oneshot::Receiver<()>;
type Message = (ToolCallReport, oneshot::Sender<()>);

pub(super) struct Reports {
    sender: Option<mpsc::UnboundedSender<Message>>,
    waiting: Mutex<Option<(TurnHost, mpsc::UnboundedReceiver<Message>)>>,
}

impl Reports {
    pub(super) fn new(host: Option<TurnHost>) -> Self {
        let (sender, waiting) = if let Some(host) = host {
            let (sender, receiver) = mpsc::unbounded_channel::<Message>();
            (Some(sender), Some((host, receiver)))
        } else {
            (None, None)
        };
        Self {
            sender,
            waiting: Mutex::new(waiting),
        }
    }

    /// The stream's first read installs its delivery context. Capturing at
    /// `on_event` would lose that lane and its cascade budget. The worker
    /// holds no sender to its own queue and outlives a cut callback.
    pub(super) fn start(&self) {
        let waiting = self
            .waiting
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some((host, mut receiver)) = waiting {
            let worker_host = host.clone();
            tokio::spawn(keep_context(host.external_agent_context(async move {
                while let Some((report, done)) = receiver.recv().await {
                    worker_host.report_external_tool_call(&report).await;
                    let _gone = done.send(());
                }
            })));
        }
    }

    /// Enqueued before a callback can be cancelled: its once-only report
    /// survives the callback or stream disappearing, as Python shields it.
    pub(super) fn send(&self, report: ToolCallReport) -> Pending {
        let (done, pending) = oneshot::channel();
        if let Some(sender) = &self.sender {
            let _gone = sender.send((report, done));
        }
        pending
    }
}
