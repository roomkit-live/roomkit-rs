//! The channel's client: what the agent calls back into, routed to the turn
//! prompting its session.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

use agent_client_protocol_schema::v1::{
    ContentBlock, PermissionOptionKind, RequestPermissionOutcome, RequestPermissionRequest,
    RequestPermissionResponse, SelectedPermissionOutcome, SessionNotification, SessionUpdate,
};
use tokio::sync::mpsc;
use tokio::task::AbortHandle;

use crate::client::AcpClient;
use crate::error::AcpError;

/// The turns prompting a session, by the session's id: where the agent's
/// updates for that session go.
#[derive(Default)]
pub(super) struct Turns {
    by_session: Mutex<HashMap<String, Registered>>,
}

impl Turns {
    fn lock(&self) -> MutexGuard<'_, HashMap<String, Registered>> {
        self.by_session
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The turn prompting `session` from now on, which takes its text.
    pub(super) fn register(
        &self,
        session: &str,
        text: mpsc::UnboundedSender<String>,
        prompt: AbortHandle,
        activity: Arc<AtomicBool>,
    ) {
        self.lock().insert(
            session.to_owned(),
            Registered {
                text,
                prompt,
                activity,
            },
        );
    }

    /// Stops each prompt in flight, as Python's close cancels its runners:
    /// its turn ends quietly, marked interrupted. The sessions they
    /// prompted.
    pub(super) fn stop(&self) -> Vec<String> {
        self.lock()
            .iter()
            .map(|(session, turn)| {
                turn.prompt.abort();
                session.clone()
            })
            .collect()
    }

    /// The turn holding `text` stops prompting `session`; a later turn
    /// registered under the same session keeps its own.
    pub(super) fn unregister(&self, session: &str, text: &mpsc::UnboundedSender<String>) {
        let mut turns = self.lock();
        if turns
            .get(session)
            .is_some_and(|turn| turn.text.same_channel(text))
        {
            turns.remove(session);
        }
    }

    fn text(&self, session: &str, text: String) {
        if let Some(turn) = self.lock().get(session) {
            // A turn whose stream went takes nothing more.
            let _gone = turn.text.send(text);
        }
    }

    fn activity(&self, session: &str) {
        if let Some(turn) = self.lock().get(session) {
            turn.activity.store(true, Ordering::Release);
        }
    }
}

/// A turn prompting a session: where its text goes, and its prompt.
struct Registered {
    text: mpsc::UnboundedSender<String>,
    prompt: AbortHandle,
    activity: Arc<AtomicBool>,
}

/// The client the channel opens its transport with.
pub(super) struct ChannelClient {
    pub(super) turns: Arc<Turns>,
}

impl AcpClient for ChannelClient {
    fn session_update(
        &self,
        notification: SessionNotification,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let session = notification.session_id.0.to_string();
            self.turns.activity(&session);
            // What the agent says goes to the turn prompting its session;
            // the other updates are the next lots' (thinking, tools, plans).
            if let SessionUpdate::AgentMessageChunk(chunk) = notification.update
                && let ContentBlock::Text(text) = chunk.content
                && !text.text.is_empty()
            {
                self.turns.text(&session, text.text);
            }
        })
    }

    fn request_permission(
        &self,
        request: RequestPermissionRequest,
    ) -> Pin<Box<dyn Future<Output = Result<RequestPermissionResponse, AcpError>> + Send + '_>>
    {
        Box::pin(async move {
            self.turns.activity(&request.session_id.0);
            Ok(rejected(&request))
        })
    }
}

/// The answer to a permission request with no external tool handler to
/// decide it, as Python's `_permission_response` refuses it: the agent's
/// `reject_once` option, else its `reject_always`, else `cancelled`.
fn rejected(request: &RequestPermissionRequest) -> RequestPermissionResponse {
    let reject = [
        PermissionOptionKind::RejectOnce,
        PermissionOptionKind::RejectAlways,
    ]
    .iter()
    .find_map(|kind| request.options.iter().find(|option| option.kind == *kind));
    let outcome = match reject {
        Some(option) => RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(
            option.option_id.clone(),
        )),
        None => RequestPermissionOutcome::Cancelled,
    };
    RequestPermissionResponse::new(outcome)
}
