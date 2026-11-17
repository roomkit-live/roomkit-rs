//! A caller owns a queued legacy request until the writer accepts it.
//!
//! rmcp owns its send tasks separately from the response future. A local
//! marker therefore connects the caller's lifetime to the writer's rendezvous;
//! it never changes JSON parameters or the SDK's result parsing.

use std::sync::{Arc, Mutex};

use rmcp::model::{
    CancelledNotification, CancelledNotificationParam, ClientJsonRpcMessage, ClientNotification,
    ClientRequest, GetExtensions, RequestId, ServerResult,
};
use rmcp::service::{Peer, PeerRequestOptions, RoleClient, ServiceError};
use tokio::sync::oneshot;

#[derive(Clone, Copy, Default)]
enum Phase {
    #[default]
    Queued,
    Sent,
    Abandoned,
}

#[derive(Clone, Default)]
struct Marker(Arc<Mutex<Phase>>);

struct Caller {
    marker: Marker,
    peer: Peer<RoleClient>,
    id: Option<RequestId>,
    runtime: Option<tokio::runtime::Handle>,
}

pub(crate) async fn send(
    peer: &Peer<RoleClient>,
    mut request: ClientRequest,
) -> Result<ServerResult, ServiceError> {
    let mut caller = Caller {
        marker: Marker::default(),
        peer: peer.clone(),
        id: None,
        runtime: tokio::runtime::Handle::try_current().ok(),
    };
    request.extensions_mut().insert(caller.marker.clone());
    let handle = peer
        .send_request_with_option(request, PeerRequestOptions::no_options())
        .await?;
    caller.id = Some(handle.id.clone());
    let result = handle.await_response().await;
    caller.id = None;
    result
}

impl Drop for Caller {
    fn drop(&mut self) {
        let mut phase = self
            .marker
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(*phase, Phase::Queued) {
            *phase = Phase::Abandoned;
        }
        drop(phase);
        let Some(id) = self.id.take() else {
            return;
        };
        let mut notification =
            CancelledNotification::new(CancelledNotificationParam::new(Some(id), None));
        notification.extensions.insert(self.marker.clone());
        let peer = self.peer.clone();
        let Some(runtime) = &self.runtime else {
            return;
        };
        runtime.spawn(async move {
            let _notified = peer
                .send_notification(ClientNotification::CancelledNotification(notification))
                .await;
        });
    }
}

/// Python removes its response waiter without a wire notification on abandonment.
/// Acknowledge only our marked cleanup so rmcp releases its local responder.
pub(super) fn local_cancellation(message: &ClientJsonRpcMessage) -> bool {
    let ClientJsonRpcMessage::Notification(notification) = message else {
        return false;
    };
    notification
        .notification
        .extensions()
        .get::<Marker>()
        .is_some()
}

pub(super) fn accept(message: &ClientJsonRpcMessage, accepted: oneshot::Sender<()>) -> bool {
    let marker = match message {
        ClientJsonRpcMessage::Request(request) => request.request.extensions().get::<Marker>(),
        ClientJsonRpcMessage::Response(_)
        | ClientJsonRpcMessage::Notification(_)
        | ClientJsonRpcMessage::Error(_) => None,
    };
    let Some(marker) = marker else {
        return accepted.send(()).is_ok();
    };
    let mut phase = marker
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if matches!(*phase, Phase::Abandoned) {
        let _accepted = accepted.send(());
        return false;
    }
    if accepted.send(()).is_err() {
        *phase = Phase::Abandoned;
        return false;
    }
    *phase = Phase::Sent;
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::model::CustomRequest;

    #[test]
    fn an_abandoned_send_cannot_be_accepted_even_without_a_caller_marker() {
        for marked in [false, true] {
            let mut request = CustomRequest::new("host/read", None);
            if marked {
                request.extensions.insert(Marker::default());
            }
            let message = ClientJsonRpcMessage::request(
                ClientRequest::CustomRequest(request),
                RequestId::Number(1),
            );
            let (accepted, received) = oneshot::channel();
            drop(received);
            assert!(!accept(&message, accepted));
        }
    }

    #[test]
    fn an_unmarked_cancellation_is_still_a_wire_notification() {
        let notification = CancelledNotification::new(CancelledNotificationParam::new(
            Some(RequestId::Number(7)),
            Some("explicit".into()),
        ));
        let message = ClientJsonRpcMessage::notification(
            ClientNotification::CancelledNotification(notification),
        );
        assert!(!local_cancellation(&message));
        let (accepted, received) = oneshot::channel();
        assert!(accept(&message, accepted));
        assert_eq!(received.blocking_recv(), Ok(()));
    }
}
