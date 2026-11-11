//! Per-room verbs carried to the local executor and answered as JSON
//! (RFC §10.6.3), including changed state on a failed verb.

use std::sync::Arc;

use roomkit_core::model::Metadata;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::Turns;
use super::error::{TurnError, kit_error, turn_failure};
use super::observe::Carried;
use super::wire::{ChannelControl, ChannelControlEnvelope, PreviousTurn, WireError};
use crate::RoomKitError;
use crate::channel::{ChannelError, DynChannel};

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
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename = "channel_result")]
pub(crate) struct ChannelResult {
    pub(crate) result: Value,
    pub(crate) error: Option<WireError>,
    pub(crate) room_state: Option<Metadata>,
    pub(crate) executor_id: String,
}

impl Turns {
    pub(crate) async fn channel_call(
        &self,
        mut call: ChannelCall,
        channel: Arc<dyn DynChannel>,
    ) -> Result<ChannelResult, RoomKitError> {
        if self.is_closed() {
            return Err(TurnError::link_reason("backend_closed").into_kit_error());
        }
        if self.executor_lifecycle.is_closed() {
            return Err(
                TurnError::new("no_executor", "the kit's local executor has closed")
                    .into_kit_error(),
            );
        }
        let (call, raw, key, stale) = {
            let mut records = self.records();
            call.previous_turn = records.get(&call.channel_id, &call.room_id).cloned();
            // Serialize before changing the owner's record: an unsent call
            // must leave the previous turn untouched.
            let envelope = ChannelControlEnvelope {
                message: ChannelControl::ChannelCall(Box::new(call)),
            };
            let raw = serde_json::to_string(&envelope).map_err(|e| invalid(&e))?;
            let parsed: ChannelControlEnvelope =
                serde_json::from_str(&raw).map_err(|e| invalid(&e))?;
            let ChannelControl::ChannelCall(call) = parsed.message else {
                unreachable!()
            };
            records.put(
                &call.channel_id,
                &call.room_id,
                PreviousTurn {
                    turn_id: call.call_id.clone(),
                    executor_id: None,
                },
            );
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
        let answer = self.answer_call(&*channel, &call, ready).await;
        let raw = serde_json::to_string(&answer).map_err(|e| invalid(&e))?;
        self.observers().tell(Carried::ChannelResult, &raw);
        let answer: ChannelResult = serde_json::from_str(&raw).map_err(|e| invalid(&e))?;
        self.took(
            &call.channel_id,
            &call.room_id,
            &call.call_id,
            &answer.executor_id,
        );
        Ok(answer)
    }

    async fn answer_call(
        &self,
        channel: &dyn DynChannel,
        call: &ChannelCall,
        ready: Result<(), ChannelError>,
    ) -> ChannelResult {
        let before = ready.and_then(|()| channel.room_state(&call.room_id));
        let (result, room_state) = match before {
            Ok(before) => {
                let result = channel
                    .channel_call(
                        call.room_id.clone(),
                        call.verb.clone(),
                        call.arguments.clone(),
                    )
                    .await;
                match channel.room_state(&call.room_id) {
                    Ok(after) => (result, (after != before).then_some(after)),
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
            executor_id: self.executor_id.clone(),
        }
    }
}

fn invalid(error: &serde_json::Error) -> RoomKitError {
    RoomKitError::Value(format!(
        "The channel control cannot cross the turn backend: {error}"
    ))
}
