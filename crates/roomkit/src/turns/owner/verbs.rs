//! The verbs a turn's host asks, run on the owner (RFC §10.6.5): `send_event`,
//! `TurnHost`'s one verb, during its turn as a `host_call`, after it as a
//! `room_call`.
//!
//! A host acts for its turn's room, in that room's organization: a verb
//! that names another room, or another organization, is refused, and so is
//! an argument the verb does not take. During the turn, the event is at
//! least as deep as the turn's response (RFC §8.3) and draws on the budget
//! of the delivery the turn's reader runs, as the owner's own code would.
//! After it, the verb is work the turn began, accepted only from the
//! executor of the room's last turn of the channel, for that very turn, and
//! a new arrival at the depth its arguments name.

use std::sync::Arc;

use roomkit_core::model::{DirectEvent, RoomId};
use serde_json::Value;

use super::Facts;
use crate::turn::TurnKit;
use crate::turns::codec::decode_send_event;
use crate::turns::error::{kit_error, one};
use crate::turns::records::RoomRecords;
use crate::turns::wire::{
    CallContext, OwnerMessage, RoomCallResult, SEND_EVENT, WireError, host_result_kind,
};

/// The refusal of a verb the host may not ask.
const PERMISSION_ERROR: &str = "PermissionError";

/// The refusal of a verb or arguments the owner does not know, and of a
/// verb for another room or organization than the turn's.
const VALUE_ERROR: &str = "ValueError";

/// Runs `verb` for the turn `facts` names, during the turn: its result as
/// data, or what it raised.
pub(super) async fn serve(
    kit: &Arc<dyn TurnKit>,
    facts: &Facts,
    verb: &str,
    arguments: serde_json::Map<String, Value>,
    context: &CallContext,
) -> Result<Value, Box<WireError>> {
    if verb != SEND_EVENT {
        return Err(refusal(
            VALUE_ERROR,
            format!("No verb {verb:?} is served by the room owner"),
        ));
    }
    if !context.room_id.is_empty() && context.room_id != facts.room_id.as_str() {
        return Err(refusal(
            PERMISSION_ERROR,
            format!(
                "A call of the turn in room {} named room {}",
                facts.room_id, context.room_id
            ),
        ));
    }
    let (room_id, organization_id, mut event) =
        decode_send_event(arguments).map_err(|why| refusal(VALUE_ERROR, why))?;
    within_its_room(facts, &room_id, organization_id.as_deref())?;
    event.chain_depth = event
        .chain_depth
        .max(context.chain_depth)
        .max(facts.response_depth);
    send(kit, room_id, event).await
}

/// Refuses a verb that names another room, or another organization, than
/// the turn's, as the Python implementation's owner refuses it
/// (`ValueError`).
fn within_its_room(
    facts: &Facts,
    room_id: &RoomId,
    organization_id: Option<&str>,
) -> Result<(), Box<WireError>> {
    if room_id != &facts.room_id {
        return Err(refusal(
            VALUE_ERROR,
            format!(
                "A turn's host acts for its room {:?}, not for {room_id}",
                facts.room_id.as_str()
            ),
        ));
    }
    if let Some(organization_id) = organization_id
        && Some(organization_id) != facts.organization_id.as_deref()
    {
        return Err(refusal(
            VALUE_ERROR,
            format!("A turn's host acts in its room's organization, not in {organization_id:?}"),
        ));
    }
    Ok(())
}

/// Injects `event` into `room_id`: the event as stored, or the kit's error.
async fn send(
    kit: &Arc<dyn TurnKit>,
    room_id: RoomId,
    event: DirectEvent,
) -> Result<Value, Box<WireError>> {
    match kit.send_event(room_id, event).await {
        Ok(stored) => serde_json::to_value(stored).map_err(|error| {
            refusal(
                "TypeError",
                format!("the stored event cannot cross: {error}"),
            )
        }),
        Err(error) => Err(Box::new(kit_error(&error))),
    }
}

/// Answers a verb an executor made after its turn ended (RFC §10.6.3
/// `room_call`), as the room's records name its last turn of the channel.
pub(crate) async fn answer_room_call(
    kit: &Arc<dyn TurnKit>,
    records: impl FnOnce() -> Option<(String, Option<String>)>,
    message: OwnerMessage,
) -> RoomCallResult {
    let OwnerMessage::RoomCall {
        call_id,
        channel_id,
        room_id,
        executor_id,
        turn_id,
        verb,
        arguments,
        ..
    } = message;
    let answered = async {
        match records() {
            Some((last, Some(took))) if took == executor_id && last == turn_id => {}
            Some((last, Some(took))) if took == executor_id => {
                return Err(refusal(
                    PERMISSION_ERROR,
                    format!(
                        "turn {turn_id} is no longer the room's last of {channel_id}: {last} is"
                    ),
                ));
            }
            _ => {
                return Err(refusal(
                    PERMISSION_ERROR,
                    format!(
                        "executor {executor_id} did not take the last turn of {channel_id} in room {room_id}"
                    ),
                ));
            }
        }
        if verb != SEND_EVENT {
            return Err(refusal(
                VALUE_ERROR,
                format!("No verb {verb:?} is served between turns"),
            ));
        }
        let (named, organization_id, event) =
            decode_send_event(arguments).map_err(|why| refusal(VALUE_ERROR, why))?;
        if named != room_id || organization_id.is_some() {
            return Err(refusal(
                PERMISSION_ERROR,
                format!(
                    "work of a turn in room {room_id} writes to its room only, for its room's organization"
                ),
            ));
        }
        send(kit, room_id, event).await
    };
    let (result, error) = match answered.await {
        Ok(result) => (result, None),
        Err(error) => {
            tracing::warn!(target: "roomkit::turns", call = %call_id, error = %error.message, "a verb made after its turn is refused");
            (Value::Null, Some(*error))
        }
    };
    RoomCallResult {
        kind: host_result_kind(),
        call_id,
        result,
        error,
    }
}

/// The record of the turn the room's records name for a channel.
pub(crate) fn last_turn(
    records: &RoomRecords,
    channel_id: &roomkit_core::model::ChannelId,
    room_id: &RoomId,
) -> Option<(String, Option<String>)> {
    records
        .get(channel_id, room_id)
        .map(|record| (record.turn_id.clone(), record.executor_id.clone()))
}

fn refusal(kind: &str, message: String) -> Box<WireError> {
    Box::new(one(kind, message))
}
