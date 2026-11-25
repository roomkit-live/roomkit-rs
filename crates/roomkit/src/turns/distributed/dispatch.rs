//! Dispatch a Redis request to the executor hosted in this process.
use std::sync::Arc;

use super::DistributedTurnBackend;
use super::stream::Message;
use super::worker_link::WorkerLink;
use crate::RoomKitError;
use crate::turns::CarriedRequest;
use crate::turns::wire::{ChannelControl, ChannelControlEnvelope};

/// Gives a turn up when the job that holds it ends or is aborted.
struct Held(Arc<WorkerLink>);

impl Drop for Held {
    fn drop(&mut self) {
        self.0.close();
    }
}

impl DistributedTurnBackend {
    pub(super) fn handle(&self, msg: &Message) -> Result<(), RoomKitError> {
        if *self.0.draining.borrow() {
            return self.refuse_closing(msg);
        }
        let executor = self.receiving_worker(&msg.target, &msg.sender);
        match (msg.kind.as_str(), executor) {
            ("request", Some(worker)) if !worker.closed() => self.accept_request(msg, worker),
            ("channel", Some(worker)) if !worker.closed() => self.accept_channel(msg, worker),
            ("channel", None) => {
                let missing = RoomKitError::TurnLink {
                    reason: "no_executor".into(),
                    message: Some(format!("Executor {} no longer serves", msg.target)),
                };
                self.channel_error(msg, &missing, None)
            }
            ("request" | "channel", _) => self.answer(msg, "refused".into()),
            _ => self.owner_message(msg),
        }
    }

    fn channel_error(
        &self,
        msg: &Message,
        error: &RoomKitError,
        executor_id: Option<String>,
    ) -> Result<(), RoomKitError> {
        let result = crate::turns::ChannelResult {
            result: serde_json::Value::Null,
            error: Some(crate::turns::error::kit_error(error)),
            room_state: None,
            executor_id,
        };
        self.answer(
            msg,
            serde_json::to_string(&result).map_err(|e| invalid(e.to_string()))?,
        )
    }

    fn accept_request(
        &self,
        msg: &Message,
        worker: super::process::Recipient,
    ) -> Result<(), RoomKitError> {
        let carried = CarriedRequest::from_wire(&msg.body).map_err(invalid)?;
        let request = crate::turns::executor::parse(carried).map_err(invalid)?;
        let Some(worker) = worker.for_channel(&request.channel_id) else {
            return self.answer(msg, "refused".into());
        };
        let id = request.turn_id.clone();
        let Some(route) = self.route_turn(&id, &msg.sender, true) else {
            return self.answer(msg, "refused".into());
        };
        let backend = self.clone();
        let message = msg.clone();
        let admitted = self.dispatch(async move {
            // What the turn's work reaches its owner by once the turn ended: the
            // executor asks the port this backend gave it when it served it.
            drop(backend.remember_owner(&id, &message.sender));
            let link = Arc::new(WorkerLink::new(request, &backend, &message.sender, route));
            // A close owns the turn through its final write: this job lives as
            // long as the turn's frames cross, and gives the turn up if aborted.
            let held = Held(Arc::clone(&link));
            let taken = worker.take(Arc::<WorkerLink>::clone(&link)).is_ok();
            let reply = if taken { "taken" } else { "refused" };
            if let Err(error) = backend.answer(&message, reply.into()) {
                tracing::warn!(%error, "Sending the turn handover result failed");
            } else if taken {
                link.open();
                link.ended().await;
            }
            drop(held);
        });
        if !admitted {
            return self.answer(msg, "refused".into());
        }
        Ok(())
    }

    fn accept_channel(
        &self,
        msg: &Message,
        worker: super::process::Recipient,
    ) -> Result<(), RoomKitError> {
        let control: ChannelControlEnvelope = match serde_json::from_str(&msg.body) {
            Ok(control) => control,
            Err(error) => {
                return self.channel_error(
                    msg,
                    &RoomKitError::Value(error.to_string()),
                    Some(msg.target.clone()),
                );
            }
        };
        let channel = match &control.message {
            ChannelControl::ChannelCall(call) => &call.channel_id,
            ChannelControl::RoomAttached { channel_id, .. }
            | ChannelControl::RoomDetached { channel_id, .. }
            | ChannelControl::CommittedEvent { channel_id, .. }
            | ChannelControl::ChannelReleased { channel_id } => channel_id,
        };
        let Some(worker) = worker.for_channel(channel) else {
            return self.answer(msg, "refused".into());
        };
        let backend = self.clone();
        let message = msg.clone();
        let admitted = self.dispatch(async move {
            let body = match worker.control(control.message).await {
                Ok(answer) => super::control::encode_answer(answer),
                Err(error) => Ok(serde_json::json!({
                    "type":"channel_result", "result":null, "room_state":null,
                    "executor_id":worker.executor_id(),"error":crate::turns::record_of(&error)
                })
                .to_string()),
            };
            if let Ok(body) = body
                && let Err(error) = backend.answer(&message, body)
            {
                tracing::warn!(%error, "Sending the channel control result failed");
            }
        });
        if !admitted {
            return self.answer(msg, "refused".into());
        }
        Ok(())
    }

    pub(super) fn refuse_closing(&self, msg: &Message) -> Result<(), RoomKitError> {
        match msg.kind.as_str() {
            "request" | "channel" => self.answer(msg, "refused".into()),
            "owner" => self.owner_error(msg, &super::routes::closed()),
            _ => Ok(()),
        }
    }

    /// The receiver has a question reference even when it could not read the call id.
    pub(super) fn owner_error(
        &self,
        msg: &Message,
        error: &RoomKitError,
    ) -> Result<(), RoomKitError> {
        let result = crate::turns::wire::RoomCallResult {
            call_id: String::new(),
            result: serde_json::Value::Null,
            error: Some(crate::turns::error::kit_error(error)),
            record_changes: None,
        };
        self.answer(
            msg,
            serde_json::to_string(&result).map_err(|e| invalid(e.to_string()))?,
        )
    }
}

fn invalid(why: String) -> RoomKitError {
    RoomKitError::TurnLink {
        reason: "frame_invalid".into(),
        message: Some(why),
    }
}
