//! Dispatch a Redis request to the executor hosted in this process.
use super::RedisTurnBackend;
use super::routes::TurnRoute;
use super::stream::Message;
use crate::RoomKitError;
use crate::turns::wire::ChannelControlEnvelope;
use crate::turns::{CarriedRequest, WorkerTurn, lock};

impl RedisTurnBackend {
    pub(super) async fn handle(&self, msg: &Message) -> Result<(), RoomKitError> {
        let executor = lock(&self.0.serving).workers.get(&msg.target).cloned();
        match msg.kind.as_str() {
            "request" => {
                let Some(worker) = executor.filter(|w| !w.closed()) else {
                    return self.answer(msg, "refused".to_owned()).await;
                };
                let carried = CarriedRequest::from_wire(&msg.body).map_err(invalid)?;
                let request = crate::turns::wire::validate_rest(&carried.rest).map_err(invalid)?;
                let id = request.turn_id;
                let Some(route) = self.route_turn(&id, &msg.sender, true) else {
                    return self.answer(msg, "refused".into()).await;
                };
                let turn = worker.take(carried)?;
                let backend = self.clone();
                let peer = msg.sender.clone();
                self.answer(msg, "taken".into()).await?;
                tokio::spawn(async move {
                    backend.forward_worker(&peer, &id, turn, route).await;
                });
                Ok(())
            }
            "channel" => {
                let Some(worker) = executor.filter(|w| !w.closed()) else {
                    return self.answer(msg, "refused".into()).await;
                };
                let control: ChannelControlEnvelope =
                    serde_json::from_str(&msg.body).map_err(|e| invalid(e.to_string()))?;
                let backend = self.clone();
                let message = msg.clone();
                tokio::spawn(async move {
                    let body = match worker.control(control).await {
                        Ok(Some(answer)) => serde_json::to_string(&answer),
                        Ok(None) => {
                            Ok(r#"{"type":"channel_result","result":null,"room_state":null,"error":null,"executor_id":null}"#.to_owned())
                        }
                        Err(error) => Ok(
                            serde_json::json!({"type":"channel_result","result":null, "room_state":null,"executor_id":worker.executor_id(),
                            "error":crate::turns::record_of(&error)})
                            .to_string(),
                        ),
                    };
                    if let Ok(body) = body
                        && let Err(error) = backend.answer(&message, body).await
                    {
                        tracing::warn!(%error, "Sending the channel control result failed");
                    }
                });
                Ok(())
            }
            _ => self.owner_message(msg).await,
        }
    }

    async fn forward_worker(
        &self,
        peer: &str,
        id: &str,
        mut turn: WorkerTurn,
        mut route: TurnRoute,
    ) {
        loop {
            tokio::select! {
                control = route.inbox.recv() => match control {
                    Some(msg) if msg.kind == "control" => turn.control(msg.body),
                    _ => break,
                },
                raw = turn.next_frame() => {
                    let Some(body) = raw else { break };
                    let Ok(body) = crate::turns::wire::peer_error_payload(body) else { break; };
                    let msg = Message::new("frame", self.node_id(), body, id, "");
                    if let Err(error) = self.post(peer, &msg).await {
                        tracing::warn!(%error, "Sending the turn frame failed");
                        break;
                    }
                }
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
