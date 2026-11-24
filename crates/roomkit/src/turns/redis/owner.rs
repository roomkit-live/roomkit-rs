//! Deliver a submitted turn, and forward its numbered messages.
mod local;
mod submitted;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::Worker;
use crate::RoomKitError;
use crate::turns::pipe::{LinkFailure, PipeReceiver};
use crate::turns::wire::{Control, Frame, RequestRest};
use crate::turns::{CarriedRequest, MemoryTurn};

use super::RedisTurnBackend;
use super::placement::Placed;
use super::registry::Target;
use super::routes::TurnRoute;
use super::stream::Message;

enum Delivered {
    Here(Worker, Box<CarriedRequest>),
    Elsewhere(String, TurnRoute),
}

impl RedisTurnBackend {
    async fn hand_over(
        &self,
        carried: CarriedRequest,
        memory: &Arc<MemoryTurn>,
        mut mark: impl FnMut(bool) + Send,
    ) -> Result<Delivered, RoomKitError> {
        self.attend().await?;
        let rest: RequestRest =
            crate::turns::wire::validate_rest(&carried.rest).map_err(invalid)?;
        let mut wire = None;
        let mut refused = BTreeSet::new();
        loop {
            let target = self
                .place(
                    &rest.channel_id,
                    rest.room_id.as_str(),
                    &rest.room_hooks,
                    rest.context.room.organization_id.as_deref(),
                    &refused,
                    true,
                )
                .await?;
            let target = match target {
                Placed::Here(worker) if worker.closed() => {
                    refused.insert(format!("{}:{}", self.node_id(), worker.executor_id()));
                    continue;
                }
                Placed::Here(worker) => {
                    return Ok(Delivered::Here(worker, Box::new(carried)));
                }
                Placed::Elsewhere(target) => target,
            };
            let body = wire
                .get_or_insert_with(|| carried.to_wire())
                .as_ref()
                .map_err(|why| invalid(why.clone()))?;
            if let Some(delivered) = self
                .hand_elsewhere(&target, memory, &mut mark, body)
                .await?
            {
                return Ok(delivered);
            }
            refused.insert(target.member());
        }
    }

    async fn hand_elsewhere(
        &self,
        target: &Target,
        memory: &Arc<MemoryTurn>,
        mark: &mut (impl FnMut(bool) + Send),
        body: &str,
    ) -> Result<Option<Delivered>, RoomKitError> {
        let route = self
            .route_turn(&memory.turn_id, &target.node, false)
            .ok_or_else(|| {
                if *self.0.draining.borrow() {
                    super::routes::closed()
                } else {
                    invalid("turn already submitted".into())
                }
            })?;
        mark(true);
        let mut pending = PendingHandover::new(self, &target.node, &memory.turn_id);
        let answer = self
            .ask(
                &target.node,
                "request",
                body.to_owned(),
                &target.executor_id,
                self.timeouts().accept,
            )
            .await;
        pending.armed = false;
        match answer.as_deref() {
            Ok("taken") => {
                self.watch_process(target, memory);
                Ok(Some(Delivered::Elsewhere(target.node.clone(), route)))
            }
            Ok("refused") => {
                mark(false);
                Ok(None)
            }
            _ => {
                self.release_turn(&target.node, &memory.turn_id).await;
                Err(answer
                    .err()
                    .unwrap_or_else(|| invalid("invalid turn handover answer".into())))
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
        read_end: Option<Arc<AtomicBool>>,
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
                memory.frames.end(LinkFailure::FrameLost);
                break;
            }
        }
        if read_end.is_none_or(|read| !read.load(Ordering::Acquire)) {
            self.release_turn(&peer, &memory.turn_id).await;
        }
    }
}

/// Dropping a pending submission also lets the remote worker go if it received the request.
struct PendingHandover<'a> {
    backend: &'a RedisTurnBackend,
    peer: &'a str,
    turn: &'a str,
    armed: bool,
    runtime: tokio::runtime::Handle,
}

impl<'a> PendingHandover<'a> {
    fn new(backend: &'a RedisTurnBackend, peer: &'a str, turn: &'a str) -> Self {
        Self {
            backend,
            peer,
            turn,
            armed: true,
            runtime: tokio::runtime::Handle::current(),
        }
    }
}

impl Drop for PendingHandover<'_> {
    fn drop(&mut self) {
        if self.armed {
            // A Send submission can be dropped from a thread outside the runtime.
            let _runtime = self.runtime.enter();
            let release = Message::new(
                "released",
                self.backend.node_id(),
                self.turn.to_owned(),
                self.turn,
                "",
            );
            if let Err(error) = self.backend.post_unconfirmed(self.peer, &release) {
                tracing::debug!(%error, "could not release a cancelled Redis handover");
            }
        }
    }
}

fn refuse(memory: &MemoryTurn, error: &RoomKitError) {
    drop(memory.frames.put(&Frame::Error {
        error: crate::turns::error::kit_error(error),
    }));
    drop(memory.frames.put(&Frame::End));
}

fn invalid(reason: String) -> RoomKitError {
    RoomKitError::TurnLink {
        reason: "frame_invalid".into(),
        message: Some(reason),
    }
}

impl RedisTurnBackend {
    pub(super) fn owner_message(&self, incoming: &Message) -> Result<(), RoomKitError> {
        use crate::turns::wire::{OwnerMessage, OwnerMessageEnvelope, RoomCallResult};
        let parsed: OwnerMessageEnvelope = match serde_json::from_str(&incoming.body) {
            Ok(envelope) => envelope,
            Err(error) => {
                return self.owner_error(incoming, &RoomKitError::Value(error.to_string()));
            }
        };
        let owner = crate::turns::lock(&self.0.owner).clone();
        let backend = self.clone();
        let message = incoming.clone();
        let admitted = self.dispatch_incoming(incoming.kind == "work", async move {
            let result = if let Some(owner) = owner {
                super::transport::deliver_to_owner(owner, parsed.message).await
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
