//! Every owner submits here: the kit's room driver as any direct caller.
use super::{Delivered, PendingHandover, RedisTurnBackend, invalid};
use crate::RoomKitError;
use crate::turns::pipe::PipeReceiver;
use crate::turns::redis::routes::TurnRoute;
use crate::turns::redis::{routes, transport::RedisLink};
use crate::turns::wire::{Control, Frame};
use crate::turns::{CarriedRequest, ExecutorInfo, TurnLink, TurnRequest};
use std::sync::Arc;

impl RedisTurnBackend {
    /// Submit a turn to a locally served worker or another Redis process.
    ///
    /// # Errors
    /// Returns a placement or transport error if no executor accepts the request.
    pub async fn submit(&self, request: TurnRequest) -> Result<Arc<dyn TurnLink>, RoomKitError> {
        self.deliver(request, None).await
    }

    /// Submit a turn to the one executor `executor` names, Python's
    /// `submit_to`: by its `executor_id`, and by its `node` when given. That
    /// executor takes it as it takes a turn `submit` places, and holds the
    /// room once it did; no other is tried (RFC §10.6.7).
    ///
    /// # Errors
    /// `no_executor` when the executor does not take the turn: nothing was
    /// handed, and the request is not marked so. Any other error after the
    /// hand-over leaves the turn's outcome unknown: it is handed to no other.
    pub async fn submit_to(
        &self,
        request: TurnRequest,
        executor: ExecutorInfo,
    ) -> Result<Arc<dyn TurnLink>, RoomKitError> {
        self.deliver(request, Some(executor)).await
    }

    /// Hand `request` to the executor `only` names, else to the one its
    /// room's turns go to, and read its link back.
    async fn deliver(
        &self,
        request: TurnRequest,
        only: Option<ExecutorInfo>,
    ) -> Result<Arc<dyn TurnLink>, RoomKitError> {
        self.ensure_open()?;
        let raw = serde_json::to_string(&request).map_err(|error| invalid(error.to_string()))?;
        let carried = CarriedRequest::from_wire(&raw).map_err(invalid)?;
        let (link, controls) = RedisLink::new(request, self.timeouts());
        let memory = &link.memory;
        let marker = link.request().handover.clone();
        let mark = |handed| marker.mark(handed);
        let delivery = match &only {
            Some(only) => self.hand_to(carried, memory, mark, only).await?,
            None => self.hand_over(carried, memory, mark).await?,
        };
        match delivery {
            Delivered::Here(worker, request) => {
                let (taken, admitted) = tokio::sync::oneshot::channel();
                self.submit_here(worker, *request, memory, controls, move || {
                    marker.mark(true);
                    let _receiver = taken.send(());
                });
                if admitted.await.is_err() {
                    if let (_, Frame::Error { error }) = link.receive().await? {
                        return Err(crate::turns::rebuild_kit_error(&error));
                    }
                    return Err(routes::closed());
                }
            }
            Delivered::Elsewhere(peer, route) => self.forward(&peer, route, &link, controls)?,
        }
        Ok(Arc::new(link))
    }

    /// Carry the turn's messages between its owner's `link` and `peer`, the
    /// process that took it; one it cannot carry is let go there.
    fn forward(
        &self,
        peer: &str,
        route: TurnRoute,
        link: &RedisLink,
        controls: PipeReceiver<Control>,
    ) -> Result<(), RoomKitError> {
        let mut pending = PendingHandover::new(self, peer, &link.memory.turn_id);
        let backend = self.clone();
        let destination = peer.to_owned();
        let read_end = Arc::clone(&link.read_end);
        let memory = Arc::clone(&link.memory);
        if !self.dispatch(async move {
            backend
                .forward_owner(destination, memory, controls, route, Some(read_end))
                .await;
        }) {
            return Err(routes::closed());
        }
        pending.armed = false;
        Ok(())
    }
}
