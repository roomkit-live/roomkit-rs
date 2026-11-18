//! An executor on the same backend receives the turn through the in-memory pipes.
use std::sync::Arc;

use crate::turns::executor::WorkerOrigin;
use crate::turns::pipe::PipeReceiver;
use crate::turns::wire::Control;
use crate::turns::{CarriedRequest, MemoryTurn, WorkerTurn};
use crate::{RedisTurnBackend, TurnExecutor};

impl RedisTurnBackend {
    /// The backend owns the bridge before the worker can start. If closing
    /// refuses admission, dropping `taken` releases the unsubmitted handover.
    pub(super) fn submit_here(
        &self,
        worker: TurnExecutor,
        request: CarriedRequest,
        memory: &Arc<MemoryTurn>,
        controls: PipeReceiver<Control>,
        taken: impl FnOnce() + Send + 'static,
    ) {
        let owner = Arc::clone(memory);
        let closing = self.0.draining.subscribe();
        let port = self.local_owner();
        if !self.dispatch(async move {
            if *closing.borrow() {
                drop(taken);
                super::refuse(&owner, &super::super::routes::closed());
                return;
            }
            let origin = WorkerOrigin {
                context: owner.turn_context(),
                waits_for_writes: true,
                owner: Some(port),
            };
            match worker.take_from(request, origin) {
                Ok(turn) => {
                    taken();
                    carry(turn, &owner, controls, closing).await;
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

async fn carry(
    mut worker: WorkerTurn,
    owner: &MemoryTurn,
    mut controls: PipeReceiver<Control>,
    mut closing: tokio::sync::watch::Receiver<bool>,
) {
    let mut cut = false;
    loop {
        tokio::select! {
            biased;
            () = async { drop(closing.wait_for(|value| *value).await); }, if !cut => {
                // The owner gives up the link immediately; the backend still drains
                // the admitted worker. Its later frames belong to the closed link.
                owner.frames.shut();
                worker.cut_for_shutdown();
                cut = true;
            }
            control = controls.next_raw(), if !cut => match control {
                Some(raw) => worker.local_control(raw, owner),
                None => break,
            },
            frame = worker.next_frame() => match frame {
                Some(raw) => owner.frames.deliver_raw(raw),
                None => break,
            },
        }
    }
    owner.frames.shut();
}

#[cfg(test)]
mod context_tests;
