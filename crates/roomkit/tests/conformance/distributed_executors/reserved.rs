//! The kit's turns submitted to the executor a step reserved (README: the
//! `executor.reserve` call, `expect.reserved_turns`).
//!
//! While a step reserved an executor, the kit's turn backend submits each of
//! the kit's turns to that executor alone (`TurnBackend::submit_to`), as a
//! host that places its turns by rules of its own does (RFC §10.6.7), and
//! records what the submission came to: the reason it failed with, none when
//! it returned the turn's link, and whether the request was marked handed
//! once it returned. With none reserved, the turns are placed as usual.
//!
//! Wrapped, the Redis backend is the kit's application transport rather
//! than its native one, as for a killed owner: the owner's side drives the
//! same public link either way, as Python's kit does.

use std::sync::{Arc, Mutex};

use roomkit::RoomKitError;
use roomkit::turns::{
    ChannelControl, ChannelResult, ExecutorInfo, ExecutorsListener, RoomOwner, ServedExecutor,
    TurnBackend, TurnBackendHandle, TurnFuture, TurnLink, TurnRequest, TurnTimeouts,
};
use serde_json::{Value, json};

use crate::identity::RunSlot;
use crate::run::lock;

/// The kit's turn backend, and the executor its turns are submitted to.
pub(super) struct Reserved {
    inner: TurnBackendHandle,
    run: RunSlot,
    executor: Mutex<Option<ExecutorInfo>>,
}

impl Reserved {
    pub(super) fn new(inner: TurnBackendHandle, run: RunSlot) -> Self {
        Self {
            inner,
            run,
            executor: Mutex::default(),
        }
    }

    /// Reserves `executor` for the kit's turns from now, or none again.
    pub(super) fn reserve(&self, executor: Option<ExecutorInfo>) {
        *lock(&self.executor) = executor;
    }

    /// Records a turn submitted to the executor `id`, and what it came to.
    fn record(&self, id: &str, room: &str, error: Option<&RoomKitError>, handed: bool) {
        let Some(run) = self.run.run() else {
            return;
        };
        let reason = match error {
            None => Value::Null,
            Some(RoomKitError::TurnLink { reason, .. }) => Value::String(reason.clone()),
            Some(other) => Value::String(other.to_string()),
        };
        lock(&run.observed)
            .reserved_turns
            .entry(id.to_owned())
            .or_default()
            .push(json!({"room_id": room, "error": reason, "handed": handed}));
    }
}

impl TurnBackend for Reserved {
    fn submit(
        &self,
        request: TurnRequest,
    ) -> TurnFuture<'_, Result<Arc<dyn TurnLink>, RoomKitError>> {
        Box::pin(async move {
            let Some(executor) = lock(&self.executor).clone() else {
                return self.inner.submit(request).await;
            };
            // A clone shares the request's mark: read once the call returned.
            let witness = request.clone();
            let id = executor.executor_id.clone();
            let submitted = self.inner.submit_to(request, executor).await;
            self.record(
                &id,
                witness.room_id.as_str(),
                submitted.as_ref().err(),
                witness.handed(),
            );
            submitted
        })
    }

    fn serve(&self, executor: Arc<dyn ServedExecutor>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        self.inner.serve(executor)
    }

    fn control(
        &self,
        message: ChannelControl,
    ) -> TurnFuture<'_, Result<Option<ChannelResult>, RoomKitError>> {
        self.inner.control(message)
    }

    fn attend(&self, owner: Arc<dyn RoomOwner>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        self.inner.attend(owner)
    }

    fn claim_presence<'a>(
        &'a self,
        channel_id: &'a str,
        room_id: &'a str,
        online: Option<bool>,
    ) -> Option<TurnFuture<'a, Result<bool, RoomKitError>>> {
        TurnBackend::claim_presence(&*self.inner, channel_id, room_id, online)
    }

    fn watch_executors(&self, listener: ExecutorsListener) {
        self.inner.watch_executors(listener);
    }

    fn executors(&self) -> Option<TurnFuture<'_, Result<Vec<ExecutorInfo>, RoomKitError>>> {
        TurnBackend::executors(&*self.inner)
    }

    fn executors_now(&self) -> Option<TurnFuture<'_, Result<Vec<ExecutorInfo>, RoomKitError>>> {
        self.inner.executors_now()
    }

    fn timeouts(&self) -> Option<TurnTimeouts> {
        self.inner.timeouts()
    }

    fn close(&self) -> TurnFuture<'_, Result<(), RoomKitError>> {
        self.inner.close()
    }
}
