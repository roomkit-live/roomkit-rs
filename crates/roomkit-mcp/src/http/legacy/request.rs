//! A caller owns a queued legacy request until the writer accepts it.
//!
//! rmcp owns its send tasks separately from the response future. A local
//! marker therefore connects the caller's lifetime to the writer's rendezvous;
//! it never changes JSON parameters or the SDK's result parsing.
//!
//! A caller that gives up on its request once rmcp holds it, at its deadline
//! or dropped, tells the server with `notifications/cancelled`, whether the
//! request was posted or not, as Python's SDK does for a request whose write
//! began. The cancellation joins the writer's queue behind what waits there
//! already; the writer skips the request when it had not taken it.

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use rmcp::model::{
    CancelledNotification, CancelledNotificationParam, ClientJsonRpcMessage, ClientNotification,
    ClientRequest, GetExtensions, RequestId, ServerResult,
};
use rmcp::service::{Peer, PeerRequestOptions, RoleClient, ServiceError};
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::writer::Queue;

/// How long a cancellation waits for the writer at most, as Python's SDK
/// bounds that courtesy write: past it, the cancellation is not sent.
const CANCELLATION_WAIT: Duration = Duration::from_secs(5);

/// The reason a cancellation gives, Python's: its provider cancels a call it
/// gives up on as its caller.
const CANCELLED: &str = "caller cancelled";

#[derive(Clone, Copy, Default)]
enum Phase {
    #[default]
    Queued,
    Sent,
    Abandoned,
}

#[derive(Clone, Default)]
struct Marker(Arc<Mutex<Phase>>);

/// A request its caller gave up on, which the writer tells the server of.
pub(super) struct Cancellation {
    id: RequestId,
    since: Instant,
}

impl Cancellation {
    /// The `notifications/cancelled` the writer posts; none once the
    /// cancellation waited past [`CANCELLATION_WAIT`] for the writer.
    pub(super) fn notification(self) -> Option<ClientJsonRpcMessage> {
        if self.since.elapsed() > CANCELLATION_WAIT {
            tracing::warn!(
                target: "roomkit::mcp",
                id = %self.id,
                "MCP cancellation not sent: the legacy writer stayed busy"
            );
            return None;
        }
        let params = CancelledNotificationParam::new(Some(self.id), Some(CANCELLED.into()));
        Some(ClientJsonRpcMessage::notification(
            ClientNotification::CancelledNotification(CancelledNotification::new(params)),
        ))
    }
}

struct Caller {
    marker: Marker,
    peer: Peer<RoleClient>,
    writer: Queue,
    /// The request while its caller waits for the answer.
    id: Option<RequestId>,
    runtime: Option<tokio::runtime::Handle>,
}

fn lock(marker: &Marker) -> MutexGuard<'_, Phase> {
    marker
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub(crate) async fn send(
    peer: &Peer<RoleClient>,
    writer: &Queue,
    mut request: ClientRequest,
) -> Result<ServerResult, ServiceError> {
    let mut caller = Caller {
        marker: Marker::default(),
        peer: peer.clone(),
        writer: writer.clone(),
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
        let mut phase = lock(&self.marker);
        if matches!(*phase, Phase::Queued) {
            *phase = Phase::Abandoned;
        }
        drop(phase);
        let Some(id) = self.id.take() else {
            return;
        };
        self.writer.cancel(Cancellation {
            id: id.clone(),
            since: Instant::now(),
        });
        // rmcp releases its responder once it sends a cancellation: this
        // marked one stays local, the writer posting the server's.
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

/// rmcp's cancellation of a request a caller gave up on stays local: it only
/// releases rmcp's responder. The writer posts the server's
/// ([`Cancellation`]).
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
    let mut phase = lock(marker);
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

    // Python's SDK: `_ABANDON_WRITE_TIMEOUT`, its bound on the courtesy
    // cancellation's write.
    #[tokio::test(start_paused = true)]
    async fn a_cancellation_that_waited_past_its_bound_is_not_sent() {
        let since = Instant::now();
        tokio::time::advance(CANCELLATION_WAIT).await;
        let id = RequestId::Number(3);
        let sent = Cancellation {
            id: id.clone(),
            since,
        }
        .notification()
        .map(|message| serde_json::to_value(message).unwrap());
        assert_eq!(
            sent,
            Some(
                serde_json::json!({"jsonrpc":"2.0","method":"notifications/cancelled",
                "params":{"requestId":3,"reason":"caller cancelled"}})
            )
        );
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(Cancellation { id, since }.notification().is_none());
    }
}
