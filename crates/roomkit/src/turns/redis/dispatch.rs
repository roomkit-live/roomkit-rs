//! Dispatch a Redis request to the executor hosted in this process.
use super::RedisTurnBackend;
use super::routes::TurnRoute;
use super::stream::Message;
use crate::RoomKitError;
use crate::turns::wire::ChannelControlEnvelope;
use crate::turns::{CarriedRequest, WorkerTurn};

impl RedisTurnBackend {
    pub(super) fn handle(&self, msg: &Message) -> Result<(), RoomKitError> {
        if *self.0.draining.borrow() {
            return self.refuse_closing(msg);
        }
        let executor = self.shared_worker(&msg.target);
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

    fn accept_request(&self, msg: &Message, worker: super::Worker) -> Result<(), RoomKitError> {
        let carried = CarriedRequest::from_wire(&msg.body).map_err(invalid)?;
        let id = crate::turns::wire::validate_rest(&carried.rest)
            .map_err(invalid)?
            .turn_id;
        let Some(route) = self.route_turn(&id, &msg.sender, true) else {
            return self.answer(msg, "refused".into());
        };
        let backend = self.clone();
        let message = msg.clone();
        let admitted = self.dispatch(async move {
            let turn = worker.native().take_from(
                carried,
                crate::turns::executor::WorkerOrigin {
                    owner: Some(crate::turns::room_owner::WorkerOwner::Redis(
                        backend.remember_owner(&id, &message.sender),
                    )),
                    ..Default::default()
                },
            );
            let reply = if turn.is_ok() { "taken" } else { "refused" };
            if let Err(error) = backend.answer(&message, reply.into()) {
                tracing::warn!(%error, "Sending the turn handover result failed");
            } else if let Ok(turn) = turn {
                backend
                    .forward_worker(&message.sender, &id, turn, route)
                    .await;
            }
        });
        if !admitted {
            return self.answer(msg, "refused".into());
        }
        Ok(())
    }

    fn accept_channel(&self, msg: &Message, worker: super::Worker) -> Result<(), RoomKitError> {
        let control: ChannelControlEnvelope = match serde_json::from_str(&msg.body) {
            Ok(control) => control,
            Err(error) => {
                return self.channel_error(
                    msg,
                    &RoomKitError::Value(error.to_string()),
                    Some(worker.executor_id().to_owned()),
                );
            }
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

    async fn forward_worker(
        &self,
        peer: &str,
        id: &str,
        mut turn: WorkerTurn,
        mut route: TurnRoute,
    ) {
        let mut draining = self.0.draining.subscribe();
        let mut cut = false;
        let mut write = super::outbox::PendingWrite::default();
        loop {
            let result = tokio::select! {
                () = async { drop(draining.wait_for(|started| *started).await); }, if !cut => {
                    turn.cut_for_shutdown();
                    cut = true;
                    continue;
                }
                control = route.inbox.recv() => match control {
                    Some(msg) if msg.kind == "control" => { turn.control(msg.body); continue; }
                    _ => break,
                },
                raw = turn.next_frame(), if write.idle() => {
                    let Some(body) = raw else { break };
                    let Ok(body) = crate::turns::wire::peer_error_payload(body) else { break };
                    let msg = Message::new("frame", self.node_id(), body, id, "");
                    write.start(self, peer, &msg)
                }
                result = write.finished() => result,
            };
            if let Err(error) = result {
                tracing::warn!(%error, "Sending the turn frame failed");
                break;
            }
        }
    }
}

fn invalid(why: String) -> RoomKitError {
    RoomKitError::TurnLink {
        reason: "frame_invalid".into(),
        message: Some(why),
    }
}
