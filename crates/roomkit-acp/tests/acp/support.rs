//! A client that records what the agent sends it, and the texts of its
//! updates.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_client_protocol_schema::v1::{
    ContentBlock, RequestPermissionOutcome, RequestPermissionRequest, RequestPermissionResponse,
    SelectedPermissionOutcome, SessionNotification, SessionUpdate,
};
use roomkit_acp::{AcpClient, AcpError};
use tokio::sync::Notify;

/// How long a test waits for what should happen at once.
pub(crate) const PATIENCE: Duration = Duration::from_secs(10);

/// A client that records the updates it is sent and the permissions it is
/// asked, and answers each permission with `allow-once` once it has taken
/// an update saying `waiting`.
#[derive(Default)]
pub(crate) struct Recorder {
    updates: Mutex<Vec<SessionNotification>>,
    permissions: Mutex<Vec<RequestPermissionRequest>>,
    waiting: Notify,
}

impl Recorder {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// This recorder, as the client a connection is given.
    pub(crate) fn client(self: &Arc<Self>) -> Arc<dyn AcpClient> {
        let client: Arc<Self> = Arc::clone(self);
        client
    }

    /// The texts of the message chunks taken, in order.
    pub(crate) fn texts(&self) -> Vec<String> {
        self.updates
            .lock()
            .unwrap()
            .iter()
            .filter_map(|notification| text_of(&notification.update))
            .collect()
    }

    pub(crate) fn permissions(&self) -> Vec<RequestPermissionRequest> {
        self.permissions.lock().unwrap().clone()
    }
}

impl AcpClient for Recorder {
    fn session_update(
        &self,
        notification: SessionNotification,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let waiting = text_of(&notification.update).as_deref() == Some("waiting");
            self.updates.lock().unwrap().push(notification);
            if waiting {
                self.waiting.notify_one();
            }
        })
    }

    fn request_permission(
        &self,
        request: RequestPermissionRequest,
    ) -> Pin<Box<dyn Future<Output = Result<RequestPermissionResponse, AcpError>> + Send + '_>>
    {
        Box::pin(async move {
            self.permissions.lock().unwrap().push(request);
            // Answered only once the reader went on past the request.
            tokio::time::timeout(PATIENCE, self.waiting.notified())
                .await
                .map_err(|_late| AcpError::Value("the reader stopped".to_owned()))?;
            Ok(RequestPermissionResponse::new(
                RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new("allow-once")),
            ))
        })
    }
}

/// The text of a message chunk, none for any other update.
fn text_of(update: &SessionUpdate) -> Option<String> {
    let SessionUpdate::AgentMessageChunk(chunk) = update else {
        return None;
    };
    let ContentBlock::Text(text) = &chunk.content else {
        return None;
    };
    Some(text.text.clone())
}
