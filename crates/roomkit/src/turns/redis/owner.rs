//! Deliver a declaration-only channel's turn, and forward its numbered messages.
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::RoomKitError;
use crate::turns::pipe::{LinkFailure, PipeReceiver};
use crate::turns::submission::Handover;
use crate::turns::wire::{Control, Frame, RequestRest};
use crate::turns::{CarriedRequest, MemoryTurn};

use super::RedisTurnBackend;
use super::routes::TurnRoute;
use super::stream::Message;

impl RedisTurnBackend {
    pub(in crate::turns) async fn submit_remote(
        &self,
        carried: CarriedRequest,
        memory: Arc<MemoryTurn>,
        controls: PipeReceiver<Control>,
        mut handover: Handover,
    ) {
        let result = self.hand_over(carried, &memory, &mut handover).await;
        drop(handover);
        match result {
            Ok((peer, route)) => self.forward_owner(peer, memory, controls, route).await,
            Err(error) => {
                drop(memory.frames.put(&Frame::Error {
                    error: crate::turns::error::kit_error(&error),
                }));
                drop(memory.frames.put(&Frame::End));
            }
        }
    }

    async fn hand_over(
        &self,
        carried: CarriedRequest,
        memory: &Arc<MemoryTurn>,
        handover: &mut Handover,
    ) -> Result<(String, TurnRoute), RoomKitError> {
        self.attend().await?;
        let rest: RequestRest =
            crate::turns::wire::validate_rest(&carried.rest).map_err(invalid)?;
        let body = carried.to_wire().map_err(invalid)?;
        let mut refused = BTreeSet::new();
        loop {
            let target = self
                .place(
                    rest.channel_id.as_str(),
                    rest.room_id.as_str(),
                    &rest.room_hooks,
                    rest.context.room.organization_id.as_deref(),
                    &refused,
                    true,
                )
                .await?;
            let route = self
                .route_turn(&memory.turn_id, &target.node, false)
                .ok_or_else(|| {
                    if *self.0.draining.borrow() {
                        super::routes::closed()
                    } else {
                        invalid("turn already submitted".into())
                    }
                })?;
            handover.handed = true;
            let answer = self
                .ask(
                    &target.node,
                    "request",
                    body.clone(),
                    &target.executor_id,
                    self.timeouts().accept,
                )
                .await;
            match answer.as_deref() {
                Ok("taken") => {
                    self.watch_process(&target, memory);
                    return Ok((target.node, route));
                }
                Ok("refused") => {
                    handover.handed = false;
                    refused.insert(target.member());
                }
                _ => {
                    let release = Message::new(
                        "released",
                        self.node_id(),
                        memory.turn_id.to_string(),
                        &memory.turn_id,
                        "",
                    );
                    drop(self.post(&target.node, &release).await);
                    return Err(answer
                        .err()
                        .unwrap_or_else(|| invalid("invalid turn handover answer".into())));
                }
            }
        }
    }

    async fn release_turn(&self, peer: &str, turn: &str) {
        let release = Message::new("released", self.node_id(), turn.to_owned(), turn, "");
        drop(self.post(peer, &release).await);
    }

    async fn forward_owner(
        &self,
        peer: String,
        memory: Arc<MemoryTurn>,
        mut controls: PipeReceiver<Control>,
        mut route: TurnRoute,
    ) {
        let mut write = super::outbox::PendingWrite::default();
        loop {
            let result = tokio::select! {
                msg = route.inbox.recv() => {
                    let Some(msg) = msg else { memory.frames.shut(); return; };
                    memory.frames.deliver_raw(msg.body);
                    continue;
                }
                raw = controls.next_raw(), if write.idle() => {
                    let Some(body) = raw else {break};
                    let body = match crate::turns::wire::peer_error_payload(body) {
                        Ok(body) => body,
                        Err(why) => {
                            memory.frames.end(LinkFailure::Invalid(why));
                            break;
                        }
                    };
                    let msg = Message::new("control", self.node_id(), body, &memory.turn_id, "");
                    write.start(self, &peer, &msg)
                }
                result = write.finished() => result,
            };
            if let Err(error) = result {
                tracing::warn!(%error, "Sending the turn control failed");
                memory.frames.end(LinkFailure::WriteRefused);
                break;
            }
        }
        self.release_turn(&peer, &memory.turn_id).await;
    }
}

fn invalid(reason: String) -> RoomKitError {
    RoomKitError::TurnLink {
        reason: "frame_invalid".into(),
        message: Some(reason),
    }
}

type OwnerReply = crate::channel::BoxFuture<'static, Option<crate::turns::wire::RoomCallResult>>;
pub(super) type OwnerPort =
    Arc<dyn Fn(crate::turns::wire::OwnerMessageEnvelope) -> OwnerReply + Send + Sync>;

impl RedisTurnBackend {
    pub(in crate::turns) fn set_owner(
        &self,
        turns: &Arc<crate::turns::Turns>,
        kit: &Arc<dyn crate::turn::TurnKit>,
    ) {
        let turns = Arc::downgrade(turns);
        let kit = Arc::clone(kit);
        *crate::turns::lock(&self.0.owner) = Some(Arc::new(move |message| {
            let turns = turns.upgrade();
            let kit = Arc::clone(&kit);
            Box::pin(async move { turns?.receive_owner_message(kit, message).await })
        }));
    }

    pub(super) fn owner_message(&self, incoming: &Message) -> Result<(), RoomKitError> {
        use crate::turns::wire::{OwnerMessage, OwnerMessageEnvelope, RoomCallResult};
        let parsed: OwnerMessageEnvelope =
            serde_json::from_str(&incoming.body).map_err(|e| invalid(e.to_string()))?;
        let owner = crate::turns::lock(&self.0.owner).clone();
        let backend = self.clone();
        let message = incoming.clone();
        let admitted = self.dispatch(async move {
            let result = if let Some(owner) = owner {
                owner(parsed).await
            } else if let OwnerMessage::RoomCall(call) = parsed.message {
                Some(RoomCallResult {
                    call_id: call.call_id,
                    result: serde_json::Value::Null,
                    error: Some(crate::turns::error::one(
                        "builtins:RuntimeError",
                        "No room owner is reached outside a turn on this backend".into(),
                    )),
                    record_changes: None,
                })
            } else {
                None
            };
            if message.kind == "owner"
                && let Some(result) = result
                && let Ok(body) = serde_json::to_string(&result)
                && let Err(error) = backend.answer(&message, body)
            {
                tracing::warn!(%error, "Sending the room call result failed");
            }
        });
        if !admitted {
            return self.refuse_closing(incoming);
        }
        Ok(())
    }
}

#[cfg(test)]
mod control_failure;
