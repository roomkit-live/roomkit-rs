//! An executor on the same backend receives the turn through the in-memory pipes.
use std::sync::Arc;

use crate::turns::executor::WorkerOrigin;
use crate::turns::pipe::PipeReceiver;
use crate::turns::wire::Control;
use crate::turns::{CarriedRequest, MemoryTurn, WorkerTurn};
use crate::{RedisTurnBackend, RoomKitError, TurnExecutor};

impl RedisTurnBackend {
    pub(super) fn take_here(
        &self,
        worker: &TurnExecutor,
        request: CarriedRequest,
        memory: &MemoryTurn,
    ) -> Result<WorkerTurn, RoomKitError> {
        if *self.0.draining.borrow() {
            return Err(super::super::routes::closed());
        }
        let origin = WorkerOrigin {
            context: memory.turn_context(),
            waits_for_writes: true,
            owner: Some(self.local_owner()),
        };
        worker.take_from(request, origin)
    }

    pub(super) fn serve_here(
        &self,
        mut worker: WorkerTurn,
        memory: &Arc<MemoryTurn>,
        mut controls: PipeReceiver<Control>,
    ) {
        let owner = Arc::clone(memory);
        let mut closing = self.0.draining.subscribe();
        if !self.dispatch(async move {
            let mut cut = false;
            loop {
                tokio::select! {
                    () = async { drop(closing.wait_for(|value| *value).await); }, if !cut => {
                        worker.cut_for_shutdown();
                        cut = true;
                    }
                    control = controls.next_raw() => match control {
                        Some(raw) => worker.local_control(raw, &owner),
                        None => break,
                    },
                    frame = worker.next_frame() => match frame {
                        Some(raw) => owner.frames.deliver_raw(raw),
                        None => break,
                    },
                }
            }
            owner.frames.shut();
        }) {
            memory.frames.shut();
        }
    }
}

#[cfg(test)]
mod context_tests;
