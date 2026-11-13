//! Tool callbacks run in their turn's captured context, not the connection's.

use roomkit::model::{RoomId, ToolCall};
use roomkit::{ToolCallReport, TurnHost, keep_context};
use roomkit_ai::ExternalToolHandler;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::{mpsc, oneshot};

use crate::channel::permissions::{self, Decision};

pub(super) type Pending = oneshot::Receiver<()>;
type Message = (ToolCallReport, bool, oneshot::Sender<()>);
type Permission = (ToolCall, String, oneshot::Sender<Decision>);

struct Waiting {
    host: Option<TurnHost>,
    reports: mpsc::UnboundedReceiver<Message>,
    permissions: mpsc::UnboundedReceiver<Permission>,
}

pub(super) struct Reports {
    sender: mpsc::UnboundedSender<Message>,
    permission: mpsc::UnboundedSender<Permission>,
    waiting: Mutex<Option<Waiting>>,
    handler: Option<Arc<dyn ExternalToolHandler>>,
    room: RoomId,
}

impl Reports {
    pub(super) fn new(
        host: Option<TurnHost>,
        handler: Option<Arc<dyn ExternalToolHandler>>,
        room: RoomId,
    ) -> Self {
        let (sender, reports) = mpsc::unbounded_channel();
        let (permission, permissions) = mpsc::unbounded_channel();
        Self {
            sender,
            permission,
            waiting: Mutex::new(Some(Waiting {
                host,
                reports,
                permissions,
            })),
            handler,
            room,
        }
    }

    /// Captured on the first stream read, when delivery admission and depth exist.
    pub(super) fn start(&self) {
        let Some(waiting) = self
            .waiting
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        else {
            return;
        };
        let Waiting {
            host,
            reports,
            permissions,
        } = waiting;
        spawn(
            host.as_ref(),
            report_loop(
                host.clone(),
                self.handler.clone(),
                self.room.clone(),
                reports,
            ),
        );
        spawn(
            host.as_ref(),
            permission_loop(self.handler.clone(), self.room.clone(), permissions),
        );
    }

    pub(super) fn send(&self, report: ToolCallReport, observe_only: bool) -> Pending {
        let (done, pending) = oneshot::channel();
        let _gone = self.sender.send((report, observe_only, done));
        pending
    }

    pub(super) async fn decide(&self, call: ToolCall, session: &str) -> Decision {
        if self.handler.is_none() {
            return Decision::default();
        }
        let (done, pending) = oneshot::channel();
        let _gone = self.permission.send((call, session.to_owned(), done));
        pending.await.unwrap_or_default()
    }
}

async fn report_loop(
    host: Option<TurnHost>,
    handler: Option<Arc<dyn ExternalToolHandler>>,
    room: RoomId,
    mut reports: mpsc::UnboundedReceiver<Message>,
) {
    while let Some((report, observe_only, done)) = reports.recv().await {
        super::handler::report(
            host.as_ref(),
            handler.as_deref(),
            &room,
            report,
            observe_only,
        )
        .await;
        let _gone = done.send(());
    }
}

async fn permission_loop(
    handler: Option<Arc<dyn ExternalToolHandler>>,
    room: RoomId,
    mut permissions: mpsc::UnboundedReceiver<Permission>,
) {
    while let Some((call, session, mut done)) = permissions.recv().await {
        let (handler, room) = (handler.clone(), room.clone());
        // Independent requests can wait concurrently. Dropping the
        // connection request cancels its decision; ending a turn does not.
        tokio::spawn(keep_context(async move {
            tokio::select! {
                () = done.closed() => {},
                decision = permissions::decide(handler.as_deref(), &call, &session, Some(room)) => {
                    let _gone = done.send(decision);
                }
            }
        }));
    }
}

fn spawn(host: Option<&TurnHost>, future: impl Future<Output = ()> + Send + 'static) {
    if let Some(host) = host {
        tokio::spawn(keep_context(host.external_agent_context(future)));
    } else {
        // The standalone stream owns a record too, even without a TurnHost.
        tokio::spawn(keep_context(future));
    }
}
