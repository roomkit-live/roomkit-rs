//! Per-room verbs carried to the local executor and answered as JSON
//! (RFC §10.6.3), including changed state on a failed verb.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use roomkit_core::model::{Metadata, Timestamp};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::Turns;

#[cfg(any(test, feature = "redis"))]
mod submission;
use super::error::{TurnError, kit_error, turn_failure};
use super::observe::Carried;
use super::wire::{ChannelControl, ChannelControlEnvelope, PreviousTurn, WireError};
use crate::RoomKitError;
use crate::channel::{ChannelError, DynChannel};
#[cfg(feature = "redis")]
use submission::SubmittedRemoteCall;

/// The owner's local knowledge of whether this particular attempt reached an
/// executor. The wire copy starts with a new marker.
#[derive(Clone, Debug, Default)]
pub(crate) struct Handover(Arc<AtomicBool>);

impl PartialEq for Handover {
    fn eq(&self, other: &Self) -> bool {
        self.handed() == other.handed()
    }
}

impl Handover {
    fn handed(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct ChannelCall {
    pub(crate) call_id: String,
    pub(crate) channel_id: roomkit_core::model::ChannelId,
    pub(crate) room_id: roomkit_core::model::RoomId,
    pub(crate) verb: String,
    #[serde(default)]
    pub(crate) arguments: Metadata,
    pub(crate) organization_id: Option<String>,
    #[serde(default)]
    pub(crate) room_state: Metadata,
    pub(crate) previous_turn: Option<PreviousTurn>,
    #[serde(skip)]
    pub(crate) handover: Handover,
}

impl ChannelCall {
    /// The transport has handed the call to an executor.
    #[cfg(any(feature = "redis", test))]
    pub(crate) fn hand_over(&self) {
        self.handover.0.store(true, Ordering::Release);
    }

    /// The executor explicitly refused this attempt without applying it.
    #[cfg(any(feature = "redis", test))]
    pub(crate) fn refused(&self) {
        self.handover.0.store(false, Ordering::Release);
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename = "channel_result")]
pub(crate) struct ChannelResult {
    #[serde(default)]
    pub(crate) result: Value,
    pub(crate) error: Option<WireError>,
    pub(crate) room_state: Option<Metadata>,
    pub(crate) executor_id: Option<String>,
}

impl Turns {
    pub(crate) async fn channel_call(
        &self,
        mut call: ChannelCall,
        channel: Arc<dyn DynChannel>,
        room_created_at: Timestamp,
    ) -> Result<ChannelResult, RoomKitError> {
        if self.is_closed() {
            return Err(TurnError::link_reason("backend_closed").into_kit_error());
        }
        #[cfg(feature = "redis")]
        if let Some(backend) = self.remote_for_channel(Some(&*channel)) {
            return self
                .remote_channel_call(backend, call, room_created_at)
                .await;
        }
        if self.executor_lifecycle.is_closed() {
            return Err(
                TurnError::new("no_executor", "the kit's local executor has closed")
                    .into_kit_error(),
            );
        }
        let (call, raw, key, stale) = {
            let mut records = self.records();
            // The records of another room of the id, deleted since and
            // created again, name no previous turn (RFC §17.2).
            records.mark_room(&call.room_id, room_created_at);
            call.previous_turn = records.get(&call.channel_id, &call.room_id).cloned();
            // Serialize before changing the owner's record: an unsent call
            // must leave the previous turn untouched.
            let submitted = PreviousTurn::after(&call.call_id, call.previous_turn.as_ref(), None);
            let envelope = ChannelControlEnvelope {
                message: ChannelControl::ChannelCall(Box::new(call)),
            };
            let raw = serde_json::to_string(&envelope).map_err(|e| invalid(&e))?;
            let parsed: ChannelControlEnvelope =
                serde_json::from_str(&raw).map_err(|e| invalid(&e))?;
            let ChannelControl::ChannelCall(call) = parsed.message else {
                unreachable!()
            };
            records.put(&call.channel_id, &call.room_id, submitted);
            records.mark_room(&call.room_id, room_created_at);
            let key = (call.channel_id.clone(), call.room_id.clone());
            let stale = self.rooms.enter(
                &channel,
                key.clone(),
                &call.call_id,
                call.previous_turn.as_ref(),
            );
            (call, raw, key, stale)
        };
        self.observers().tell(Carried::ChannelControl, &raw);
        // From here cancellation leaves the call as the previous entry.
        let ready = self
            .rooms
            .ready(&*channel, &key, call.room_state.clone(), stale)
            .await;
        let answer = self
            .answer_call(&*channel, &call, ready, &self.executor_id)
            .await;
        let raw = serde_json::to_string(&answer).map_err(|e| invalid(&e))?;
        self.observers().tell(Carried::ChannelResult, &raw);
        let answer: ChannelResult = serde_json::from_str(&raw).map_err(|e| invalid(&e))?;
        if let Some(executor) = &answer.executor_id {
            self.took(&call.channel_id, &call.room_id, &call.call_id, executor);
        }
        Ok(answer)
    }

    pub(crate) fn forget_worker_room(
        &self,
        channel_id: &roomkit_core::model::ChannelId,
        room_id: &roomkit_core::model::RoomId,
    ) {
        self.rooms.forget(channel_id, room_id);
    }

    pub(crate) async fn worker_call(
        &self,
        channel: &Arc<dyn DynChannel>,
        call: &ChannelCall,
        executor_id: &str,
    ) -> ChannelResult {
        let key = (call.channel_id.clone(), call.room_id.clone());
        let stale = self.rooms.enter(
            channel,
            key.clone(),
            &call.call_id,
            call.previous_turn.as_ref(),
        );
        let ready = self
            .rooms
            .ready(&**channel, &key, call.room_state.clone(), stale)
            .await;
        self.answer_call(&**channel, call, ready, executor_id).await
    }

    async fn answer_call(
        &self,
        channel: &dyn DynChannel,
        call: &ChannelCall,
        ready: Result<(), ChannelError>,
        executor_id: &str,
    ) -> ChannelResult {
        let before = ready.and_then(|()| {
            self.call_states
                .track(channel, &call.room_id, &call.call_id)
        });
        let (result, room_state) = match before {
            Ok(before) => {
                let result = channel
                    .channel_call(
                        call.room_id.clone(),
                        call.verb.clone(),
                        call.arguments.clone(),
                    )
                    .await;
                match before.changed(channel) {
                    Ok(changed) => (result, changed),
                    Err(error) => (Err(error), None),
                }
            }
            Err(error) => (Err(error), None),
        };
        let (result, error) = match result {
            Ok(result) => (result, None),
            Err(error) => {
                let record = error
                    .downcast_ref::<RoomKitError>()
                    .map_or_else(|| turn_failure(&*error), kit_error);
                (Value::Null, Some(record))
            }
        };
        ChannelResult {
            result,
            error,
            room_state,
            executor_id: Some(executor_id.to_owned()),
        }
    }
}

fn invalid(error: &serde_json::Error) -> RoomKitError {
    RoomKitError::Value(format!(
        "The channel control cannot cross the turn backend: {error}"
    ))
}

#[cfg(feature = "redis")]
impl Turns {
    async fn remote_channel_call(
        &self,
        backend: &super::redis::RedisTurnBackend,
        mut call: ChannelCall,
        created_at: Timestamp,
    ) -> Result<ChannelResult, RoomKitError> {
        let submission = SubmittedRemoteCall::new(self, &mut call, created_at);
        let message = ChannelControl::ChannelCall(Box::new(call));
        let answer = backend.control_remote(&message).await?.ok_or_else(|| {
            TurnError::new(
                "frame_invalid",
                "the executor did not answer the channel call",
            )
            .into_kit_error()
        })?;
        submission.complete(&answer);
        Ok(answer)
    }
}
