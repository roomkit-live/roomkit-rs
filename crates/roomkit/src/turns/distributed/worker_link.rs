//! The executor's end of a turn a room owner on another node submitted:
//! Python's `RedisExecutorLink`. The executor reads and writes it through the
//! public `ExecutorLink` (RFC §10.6.3), as it does any transport's.
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{Mutex, watch};

use super::DistributedTurnBackend;
use super::routes::TurnRoute;
use super::stream::Message;
use crate::RoomKitError;
use crate::turns::pipe::{MAX_MESSAGE_BYTES, MessageOrder};
use crate::turns::wire::{Control, ControlEnvelope, Envelope, Frame};
use crate::turns::{ExecutorLink, TurnFuture, TurnRequest};

#[cfg(test)]
#[cfg(feature = "redis")]
mod tests;

/// Where the link stands between its owner's node and its executor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    /// The handover is not answered yet: no frame crosses before its answer.
    Handing,
    /// Frames and controls cross.
    Open,
    /// The owner let go of the turn, or the backend gave it up: nothing
    /// more crosses.
    Closed,
}

pub(super) struct WorkerLink {
    request: TurnRequest,
    backend: DistributedTurnBackend,
    /// The node of the owner that submitted the turn.
    peer: String,
    controls: Mutex<Controls>,
    /// The last number given to a frame, from one.
    sent: AtomicU64,
    stage: watch::Sender<Stage>,
    /// Whether the executor's task finished, its frames written.
    ended: watch::Sender<bool>,
}

/// The owner's controls of the turn, as its node sends them.
struct Controls {
    route: TurnRoute,
    order: MessageOrder,
    draining: watch::Receiver<bool>,
    /// Whether the backend's close already cut the turn.
    cut: bool,
}

impl WorkerLink {
    pub(super) fn new(
        request: TurnRequest,
        backend: &DistributedTurnBackend,
        peer: &str,
        route: TurnRoute,
    ) -> Self {
        Self {
            request,
            backend: backend.clone(),
            peer: peer.to_owned(),
            controls: Mutex::new(Controls {
                route,
                order: MessageOrder::default(),
                draining: backend.0.draining.subscribe(),
                cut: false,
            }),
            sent: AtomicU64::new(0),
            stage: watch::Sender::new(Stage::Handing),
            ended: watch::Sender::new(false),
        }
    }

    /// The handover is answered: the turn's frames may follow its answer.
    pub(super) fn open(&self) {
        self.stage.send_if_modified(|stage| {
            let handing = *stage == Stage::Handing;
            if handing {
                *stage = Stage::Open;
            }
            handing
        });
    }

    /// Nothing more crosses: the executor's turn stops, its owner learning it
    /// from the turn's silence.
    pub(super) fn close(&self) {
        self.stage.send_replace(Stage::Closed);
    }

    /// Returns once the executor's task finished and its frames are written.
    pub(super) async fn ended(&self) {
        let mut ended = self.ended.subscribe();
        drop(ended.wait_for(|ended| *ended).await);
    }

    /// The control `raw` carries, when it is the next one: read within its
    /// bound, for this turn and in order, as a pipe reads its own; `None` for
    /// a duplicate.
    fn admitted(
        &self,
        order: &mut MessageOrder,
        raw: &str,
    ) -> Result<Option<Control>, RoomKitError> {
        if raw.len() > MAX_MESSAGE_BYTES {
            return Err(invalid(format!(
                "{} bytes, past the bound of {MAX_MESSAGE_BYTES}",
                raw.len()
            )));
        }
        let envelope: ControlEnvelope =
            serde_json::from_str(raw).map_err(|error| invalid(error.to_string()))?;
        if envelope.turn_id != self.request.turn_id {
            return Err(invalid(format!(
                "a message of turn {} on the link of turn {}",
                envelope.turn_id, self.request.turn_id
            )));
        }
        let next = order
            .admit(envelope.seq)
            .map_err(|lost| crate::turns::TurnError::link(&lost).into_kit_error())?;
        Ok(next.then_some(envelope.message))
    }
}

impl ExecutorLink for WorkerLink {
    fn request(&self) -> &TurnRequest {
        &self.request
    }

    fn receive(&self) -> TurnFuture<'_, Result<Control, RoomKitError>> {
        Box::pin(async move {
            let mut stage = self.stage.subscribe();
            let mut controls = self.controls.lock().await;
            loop {
                let Controls {
                    route,
                    order,
                    draining,
                    cut,
                } = &mut *controls;
                tokio::select! {
                    _ = stage.wait_for(|stage| *stage == Stage::Closed) => {
                        return Err(super::routes::closed());
                    }
                    // The backend closes: the turn is cut as its owner's cancel
                    // would cut it, taking no number of the owner's, and its end
                    // is still sent (Python's `RedisExecutorLink.cut`).
                    () = async { drop(draining.wait_for(|started| *started).await); }, if !*cut => {
                        *cut = true;
                        return Ok(Control::Cancel { reason: "backend_closed".to_owned() });
                    }
                    message = route.inbox.recv() => match message {
                        Some(message) if message.kind == "control" => {
                            if let Some(control) = self.admitted(order, &message.body)? {
                                return Ok(control);
                            }
                        }
                        // The owner let go of the turn.
                        _ => {
                            self.close();
                            return Err(super::routes::closed());
                        }
                    },
                }
            }
        })
    }

    fn send(&self, frame: Frame) -> TurnFuture<'_, Result<u64, RoomKitError>> {
        Box::pin(async move {
            let seq = self.sent.fetch_add(1, Ordering::AcqRel).saturating_add(1);
            let mut stage = self.stage.subscribe();
            let reached = stage.wait_for(|stage| *stage != Stage::Handing).await;
            if reached.map_or(true, |stage| *stage == Stage::Closed) {
                return Ok(seq);
            }
            let raw = serde_json::to_string(&Envelope {
                turn_id: self.request.turn_id.as_str(),
                seq,
                message: &frame,
            })
            .map_err(|error| invalid(error.to_string()))?;
            let body = crate::turns::wire::peer_error_payload(raw).map_err(invalid)?;
            let id = &self.request.turn_id;
            let message = Message::new("frame", self.backend.node_id(), body, id, "");
            self.backend.post(&self.peer, &message).await?;
            Ok(seq)
        })
    }

    fn turn_ended(&self) {
        self.ended.send_replace(true);
    }
}

fn invalid(why: String) -> RoomKitError {
    RoomKitError::TurnLink {
        reason: "frame_invalid".into(),
        message: Some(why),
    }
}
