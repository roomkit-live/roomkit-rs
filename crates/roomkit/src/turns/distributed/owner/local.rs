//! An executor on the same backend takes the turn through the in-memory link,
//! as Python's Redis backend carries a turn placed in its own process.
use std::future::pending;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{Mutex, watch};

use crate::RoomKitError;
use crate::turns::DistributedTurnBackend;
use crate::turns::distributed::Worker;
use crate::turns::pipe::PipeReceiver;
use crate::turns::wire::{Control, Frame};
use crate::turns::{
    CarriedRequest, ExecutorLink, MemoryTurn, TurnContext, TurnFuture, TurnRequest, in_memory,
};

impl DistributedTurnBackend {
    /// The backend owns the turn before the worker can start. If closing
    /// refuses admission, dropping `taken` releases the unsubmitted handover.
    pub(super) fn submit_here(
        &self,
        worker: Worker,
        request: CarriedRequest,
        memory: &Arc<MemoryTurn>,
        controls: PipeReceiver<Control>,
        taken: impl FnOnce() + Send + 'static,
    ) {
        let owner = Arc::clone(memory);
        let closing = self.0.draining.subscribe();
        if !self.dispatch(async move {
            if *closing.borrow() {
                drop(taken);
                super::refuse(&owner, &super::super::routes::closed());
                return;
            }
            let link = crate::turns::executor::parse(request)
                .map_err(super::invalid)
                .map(|request| Arc::new(LocalLink::new(request, &owner, controls, closing)));
            let admitted =
                link.and_then(|link| worker.take(Arc::<LocalLink>::clone(&link)).map(|()| link));
            match admitted {
                Ok(link) => {
                    taken();
                    link.ended().await;
                    owner.frames.shut();
                }
                Err(error) => {
                    drop(taken);
                    super::refuse(&owner, &error);
                }
            }
        }) {
            super::refuse(memory, &super::super::routes::closed());
        }
    }
}

/// The executor's end of a turn its owner submitted on this same backend: the
/// in-memory link over the owner's pipes, cut when the backend closes.
struct LocalLink {
    link: Arc<dyn ExecutorLink>,
    owner: Arc<MemoryTurn>,
    /// The backend's close, and whether it already cut the turn.
    closing: Mutex<(watch::Receiver<bool>, bool)>,
    /// The last number given to a frame, from one.
    sent: AtomicU64,
    /// Whether the executor's task finished, its frames written.
    ended: watch::Sender<bool>,
}

impl LocalLink {
    fn new(
        request: TurnRequest,
        owner: &Arc<MemoryTurn>,
        controls: PipeReceiver<Control>,
        closing: watch::Receiver<bool>,
    ) -> Self {
        Self {
            link: in_memory::executor_end(request, Arc::clone(owner), controls),
            owner: Arc::clone(owner),
            closing: Mutex::new((closing, false)),
            sent: AtomicU64::new(0),
            ended: watch::Sender::new(false),
        }
    }

    /// Returns once the executor's task finished and its frames are written.
    async fn ended(&self) {
        let mut ended = self.ended.subscribe();
        drop(ended.wait_for(|ended| *ended).await);
    }
}

impl ExecutorLink for LocalLink {
    fn request(&self) -> &TurnRequest {
        self.link.request()
    }

    fn receive(&self) -> TurnFuture<'_, Result<Control, RoomKitError>> {
        Box::pin(async move {
            let mut closing = self.closing.lock().await;
            let (draining, cut) = &mut *closing;
            if *cut {
                // The owner gave the link up: it sends nothing more.
                return pending().await;
            }
            tokio::select! {
                biased;
                // The backend closes: the owner gives the link up at once, and
                // the turn is cut as its owner's cancel would cut it, taking no
                // number of the owner's.
                () = async { drop(draining.wait_for(|started| *started).await); } => {
                    *cut = true;
                    self.owner.frames.shut();
                    Ok(Control::Cancel { reason: "backend_closed".to_owned() })
                }
                control = self.link.receive() => control,
            }
        })
    }

    fn send(&self, frame: Frame) -> TurnFuture<'_, Result<u64, RoomKitError>> {
        Box::pin(async move {
            let seq = self.sent.fetch_add(1, Ordering::AcqRel).saturating_add(1);
            let written = self.link.send(frame).await?;
            // A link its owner gave up drops the frame: the backend still
            // drains the admitted worker, its later frames the closed link's.
            Ok(if self.owner.frames.is_open() {
                written
            } else {
                seq
            })
        })
    }

    fn turn_context(&self) -> TurnContext {
        self.link.turn_context()
    }

    fn stream_context(&self) -> Option<TurnContext> {
        self.link.stream_context()
    }

    fn turn_ended(&self) {
        self.ended.send_replace(true);
    }
}

#[cfg(test)]
mod context_tests;
