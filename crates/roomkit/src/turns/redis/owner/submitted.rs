//! A direct caller submits on the same placement and numbered bridge as the room driver.
use super::{Delivered, PendingHandover, RedisTurnBackend, invalid};
use crate::RoomKitError;
use crate::turns::redis::{routes, transport::RedisLink};
use crate::turns::wire::Frame;
use crate::turns::{CarriedRequest, TurnLink, TurnRequest};
use std::sync::Arc;

impl RedisTurnBackend {
    /// Submit a turn to a locally served worker or another Redis process.
    ///
    /// # Errors
    /// Returns a placement or transport error if no executor accepts the request.
    pub async fn submit(&self, request: TurnRequest) -> Result<Arc<dyn TurnLink>, RoomKitError> {
        self.ensure_open()?;
        let raw = serde_json::to_string(&request).map_err(|error| invalid(error.to_string()))?;
        let carried = CarriedRequest::from_wire(&raw).map_err(invalid)?;
        let (link, controls) = RedisLink::new(request, self.timeouts());
        let memory = &link.memory;
        let marker = link.request().handover.clone();
        let delivery = self
            .hand_over(carried, memory, |handed| marker.mark(handed))
            .await?;
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
            Delivered::Elsewhere(peer, route) => {
                let mut pending = PendingHandover::new(self, &peer, &memory.turn_id);
                let backend = self.clone();
                let destination = peer.clone();
                let memory = Arc::clone(memory);
                if !self.dispatch(async move {
                    backend
                        .forward_owner(destination, memory, controls, route)
                        .await;
                }) {
                    return Err(routes::closed());
                }
                pending.armed = false;
            }
        }
        Ok(Arc::new(link))
    }
}
