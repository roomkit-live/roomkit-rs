//! The distributed backend as an application-facing turn transport.
use super::DistributedTurnBackend;
use crate::RoomKitError;
use crate::turns::wire::{HostResult, OwnerMessage};
use crate::turns::{
    ChannelControl, ChannelResult, ExecutorInfo, RoomOwner, ServedExecutor, TurnBackend,
    TurnFuture, TurnLink, TurnRequest, TurnTimeouts,
};
use std::sync::Arc;
mod link;
pub(super) use link::DistributedLink;

impl TurnBackend for DistributedTurnBackend {
    fn submit(
        &self,
        request: TurnRequest,
    ) -> TurnFuture<'_, Result<Arc<dyn TurnLink>, RoomKitError>> {
        Box::pin(self.submit(request))
    }

    fn submit_to(
        &self,
        request: TurnRequest,
        executor: ExecutorInfo,
    ) -> TurnFuture<'_, Result<Arc<dyn TurnLink>, RoomKitError>> {
        Box::pin(self.submit_to(request, executor))
    }

    fn serve(&self, executor: Arc<dyn ServedExecutor>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(self.serve_erased(executor))
    }

    fn control(
        &self,
        message: ChannelControl,
    ) -> TurnFuture<'_, Result<Option<ChannelResult>, RoomKitError>> {
        Box::pin(async move { self.control_remote(&message).await })
    }

    fn attend(&self, owner: Arc<dyn RoomOwner>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async move {
            self.ensure_open()?;
            let own = owner.executor();
            {
                let mut port = crate::turns::lock(&self.0.owner);
                if port.is_none() {
                    if let Some(worker) = own {
                        self.reserve_owner(&worker);
                    }
                    *port = Some(owner);
                }
            }
            DistributedTurnBackend::attend(self).await
        })
    }

    fn claim_presence<'a>(
        &'a self,
        channel_id: &'a str,
        room_id: &'a str,
        online: Option<bool>,
    ) -> Option<TurnFuture<'a, Result<bool, RoomKitError>>> {
        Some(Box::pin(self.claim_presence(channel_id, room_id, online)))
    }

    fn watch_executors(&self, listener: crate::turns::ExecutorsListener) {
        self.0.watchers.watch(listener);
    }

    fn executors(&self) -> Option<TurnFuture<'_, Result<Vec<ExecutorInfo>, RoomKitError>>> {
        Some(Box::pin(self.executors()))
    }
    fn timeouts(&self) -> Option<TurnTimeouts> {
        Some(self.timeouts())
    }
    fn close(&self) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(self.close())
    }
}

pub(super) async fn deliver_to_owner(
    owner: Arc<dyn RoomOwner>,
    message: OwnerMessage,
) -> Option<HostResult> {
    match message {
        OwnerMessage::RoomCall(call) => {
            let call_id = call.call_id.clone();
            Some(
                owner
                    .room_call(call)
                    .await
                    .unwrap_or_else(|error| HostResult {
                        call_id,
                        result: serde_json::Value::Null,
                        error: Some(crate::turns::error::kit_error(&error)),
                        record_changes: None,
                    }),
            )
        }
        OwnerMessage::RoomPublication(publication) => {
            if let Err(error) = owner.room_publication(publication).await {
                tracing::debug!(%error, "a room publication did not reach its owner");
            }
            None
        }
    }
}
