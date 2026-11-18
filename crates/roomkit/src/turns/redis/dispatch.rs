//! Dispatch a Redis request to the executor hosted in this process.
use super::RedisTurnBackend;
use super::routes::TurnRoute;
use super::stream::Message;
use crate::RoomKitError;
use crate::turns::wire::ChannelControlEnvelope;
use crate::turns::{CarriedRequest, WorkerTurn, lock};

impl RedisTurnBackend {
    pub(super) async fn handle(&self, msg: &Message) -> Result<(), RoomKitError> {
        if *self.0.draining.borrow() {
            return self.refuse_closing(msg).await;
        }
        let executor = lock(&self.0.serving).workers.get(&msg.target).cloned();
        match msg.kind.as_str() {
            "request" | "channel" => match executor.filter(|w| !w.closed()) {
                Some(worker) if msg.kind == "request" => self.accept_request(msg, worker).await,
                Some(worker) => self.accept_channel(msg, worker).await,
                None => self.answer(msg, "refused".into()).await,
            },
            _ => self.owner_message(msg).await,
        }
    }

    async fn accept_request(
        &self,
        msg: &Message,
        worker: crate::TurnExecutor,
    ) -> Result<(), RoomKitError> {
        let carried = CarriedRequest::from_wire(&msg.body).map_err(invalid)?;
        let id = crate::turns::wire::validate_rest(&carried.rest)
            .map_err(invalid)?
            .turn_id;
        let Some(route) = self.route_turn(&id, &msg.sender, true) else {
            return self.answer(msg, "refused".into()).await;
        };
        let backend = self.clone();
        let message = msg.clone();
        let admitted = self.dispatch(async move {
            let turn = worker.take_from(carried, Some(backend.remember_owner(&message.sender)));
            let reply = if turn.is_ok() { "taken" } else { "refused" };
            if let Err(error) = backend.answer(&message, reply.into()).await {
                tracing::warn!(%error, "Sending the turn handover result failed");
            } else if let Ok(turn) = turn {
                backend
                    .forward_worker(&message.sender, &id, turn, route)
                    .await;
            }
        });
        if !admitted {
            return self.answer(msg, "refused".into()).await;
        }
        Ok(())
    }

    async fn accept_channel(
        &self,
        msg: &Message,
        worker: crate::TurnExecutor,
    ) -> Result<(), RoomKitError> {
        let control: ChannelControlEnvelope =
            serde_json::from_str(&msg.body).map_err(|e| invalid(e.to_string()))?;
        let backend = self.clone();
        let message = msg.clone();
        let admitted = self.dispatch(async move {
            let body = match worker.control(control).await {
                Ok(Some(answer)) => serde_json::to_string(&answer),
                Ok(None) => Ok(r#"{"type":"channel_result","result":null,"room_state":null,"error":null,"executor_id":null}"#.to_owned()),
                Err(error) => Ok(serde_json::json!({
                    "type":"channel_result", "result":null, "room_state":null,
                    "executor_id":worker.executor_id(),"error":crate::turns::record_of(&error)
                }).to_string()),
            };
            if let Ok(body) = body && let Err(error) = backend.answer(&message, body).await {
                tracing::warn!(%error, "Sending the channel control result failed");
            }
        });
        if !admitted {
            return self.answer(msg, "refused".into()).await;
        }
        Ok(())
    }

    pub(super) async fn refuse_closing(&self, msg: &Message) -> Result<(), RoomKitError> {
        let body = match msg.kind.as_str() {
            "request" | "channel" => "refused".into(),
            "owner" => {
                let result = crate::turns::wire::RoomCallResult {
                    call_id: String::new(),
                    result: serde_json::Value::Null,
                    error: Some(crate::turns::error::kit_error(&super::routes::closed())),
                    record_changes: None,
                };
                serde_json::to_string(&result).map_err(|e| invalid(e.to_string()))?
            }
            _ => return Ok(()),
        };
        self.answer(msg, body).await
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
