//! The verbs a turn's host asks, run on the owner (RFC §10.6.5):
//! `send_event`, `post_status`, `update_room_metadata` and
//! `add_observation`, during its turn as a `host_call`, after it as a
//! `room_call`.
//!
//! A host acts for its turn's room, in that room's organization: a verb
//! that names another room, or another organization, is refused, as the
//! Python implementation refuses it (`ValueError`), and so is an argument
//! the verb does not take. During the turn, the event is at least as deep
//! as the turn's response (RFC §8.3) and draws on the budget of the
//! delivery the turn's reader runs, as the owner's own code would. After
//! it, the verb is work the turn began, accepted only from the executor of
//! the room's last turn of the channel, for that very turn, and a new
//! arrival at the depth its arguments name, in the room's organization as
//! the store holds it (Python's `call_between_turns`).

use std::sync::Arc;

use roomkit_core::model::{DirectEvent, Metadata, RoomId};
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::Facts;
use crate::orchestration::StatusEntry;
use crate::turn::TurnKit;
use crate::turns::codec::decode_send_event;
use crate::turns::error::{kit_error, one};
use crate::turns::records::RoomRecords;
use crate::turns::wire::{
    ADD_OBSERVATION, AddObservation, BETWEEN_TURNS, CallContext, OwnerMessage, POST_STATUS,
    PostStatus, RoomCallResult, SEND_EVENT, UPDATE_ROOM_METADATA, UpdateRoomMetadata, WireError,
    host_result_kind,
};

/// The refusal of a verb the host may not ask.
const PERMISSION_ERROR: &str = "PermissionError";

/// The refusal of a verb or arguments the owner does not know, and of a
/// verb for another room or organization than the turn's.
const VALUE_ERROR: &str = "ValueError";

/// The action of a task's entries, which the framework alone posts:
/// Python's `TASK_ACTION`.
const TASK_ACTION: &str = "task";

/// The metadata key of a worker run's entries, the framework's alone:
/// Python's `RUN_ENTRY`.
const RUN_ENTRY: &str = "task_run";

/// What a verb acts for: its turn's room and organization, and the depth an
/// event it sends is at least while the turn runs (`None` once it is over).
struct Scope<'a> {
    room_id: &'a RoomId,
    organization_id: Option<&'a str>,
    floor: Option<u32>,
}

/// Runs `verb` for the turn `facts` names, during the turn: its result as
/// data, or what it raised.
pub(super) async fn serve(
    kit: &Arc<dyn TurnKit>,
    facts: &Facts,
    verb: &str,
    arguments: serde_json::Map<String, Value>,
    context: &CallContext,
) -> Result<Value, Box<WireError>> {
    if !context.room_id.is_empty() && context.room_id != facts.room_id.as_str() {
        return Err(refusal(
            PERMISSION_ERROR,
            format!(
                "A call of the turn in room {} named room {}",
                facts.room_id, context.room_id
            ),
        ));
    }
    let scope = Scope {
        room_id: &facts.room_id,
        organization_id: facts.organization_id.as_deref(),
        floor: Some(context.chain_depth.max(facts.response_depth)),
    };
    run_verb(kit, &scope, verb, arguments).await
}

/// Runs `verb` with `arguments` for `scope`: its result as data, or what it
/// raised.
async fn run_verb(
    kit: &Arc<dyn TurnKit>,
    scope: &Scope<'_>,
    verb: &str,
    arguments: Metadata,
) -> Result<Value, Box<WireError>> {
    match verb {
        SEND_EVENT => {
            let (room_id, organization_id, mut event) =
                decode_send_event(arguments).map_err(|why| invalid(verb, &why))?;
            within_its_room(scope, &room_id, organization_id.as_deref())?;
            if let Some(floor) = scope.floor {
                event.chain_depth = event.chain_depth.max(floor);
            }
            send(kit, room_id, event).await
        }
        POST_STATUS => post_status(kit, scope, decoded(verb, arguments)?).map(|()| Value::Null),
        UPDATE_ROOM_METADATA => {
            let args: UpdateRoomMetadata = decoded(verb, arguments)?;
            within_its_room(scope, &args.room_id, args.organization_id.as_deref())?;
            let organization_id = scope.organization_id.map(str::to_owned);
            let room = kit
                .update_room_metadata(args.room_id, args.metadata, organization_id)
                .await
                .map_err(|error| Box::new(kit_error(&error)))?;
            to_data(&room)
        }
        ADD_OBSERVATION => {
            let args: AddObservation = decoded(verb, arguments)?;
            within_its_room(scope, &args.observation.room_id, None)?;
            kit.add_observation(args.observation)
                .await
                .map_err(|error| Box::new(kit_error(&error)))?;
            Ok(Value::Null)
        }
        _ => Err(refusal(
            VALUE_ERROR,
            format!("No verb {verb:?} is served by the room owner"),
        )),
    }
}

/// Posts a host's status on the kit's bus (RFC §19.8): it names its turn's
/// room, never another, and it is never one the framework alone posts, a
/// task's or a worker run's entry, as the Python implementation refuses
/// them.
fn post_status(
    kit: &Arc<dyn TurnKit>,
    scope: &Scope<'_>,
    args: PostStatus,
) -> Result<(), Box<WireError>> {
    if args.action == TASK_ACTION {
        return Err(refusal(
            VALUE_ERROR,
            format!("A turn's host posts no {TASK_ACTION:?} status: the framework does"),
        ));
    }
    let mut metadata = args.metadata.unwrap_or_default();
    if metadata.contains_key(RUN_ENTRY) {
        return Err(refusal(
            VALUE_ERROR,
            format!("A turn's host posts no {RUN_ENTRY:?} status: the framework does"),
        ));
    }
    let named = metadata
        .entry("room_id")
        .or_insert_with(|| Value::from(scope.room_id.as_str()));
    if named.as_str() != Some(scope.room_id.as_str()) {
        return Err(other_room(scope, &named.to_string()));
    }
    let entry = StatusEntry::posted(
        args.agent_id,
        args.action,
        args.status,
        args.detail,
        Some(metadata),
    );
    kit.post_status(entry)
        .map_err(|error| Box::new(kit_error(&error)))
}

/// A verb's arguments, read as the record of `verb`; an argument the record
/// does not name, or one that does not read, refuses it.
fn decoded<T: DeserializeOwned>(verb: &str, arguments: Metadata) -> Result<T, Box<WireError>> {
    serde_json::from_value(Value::Object(arguments))
        .map_err(|error| invalid(verb, &error.to_string()))
}

/// The refusal of `verb`'s arguments, as Python's owner words it.
fn invalid(verb: &str, why: &str) -> Box<WireError> {
    refusal(
        VALUE_ERROR,
        format!("The arguments of verb {verb:?} are not valid: {why}"),
    )
}

/// `value` as data, the verb's result.
fn to_data<T: serde::Serialize>(value: &T) -> Result<Value, Box<WireError>> {
    serde_json::to_value(value).map_err(|error| {
        refusal(
            "TypeError",
            format!("the verb's result cannot cross: {error}"),
        )
    })
}

/// Refuses a verb that names another room, or another organization, than
/// the turn's, as the Python implementation's owner refuses it.
fn within_its_room(
    scope: &Scope<'_>,
    room_id: &RoomId,
    organization_id: Option<&str>,
) -> Result<(), Box<WireError>> {
    if room_id != scope.room_id {
        return Err(other_room(scope, room_id.as_str()));
    }
    if let Some(organization_id) = organization_id
        && Some(organization_id) != scope.organization_id
    {
        return Err(refusal(
            VALUE_ERROR,
            format!("A turn's host acts in its room's organization, not in {organization_id:?}"),
        ));
    }
    Ok(())
}

/// The refusal of a verb for `named`, another room than the turn's.
fn other_room(scope: &Scope<'_>, named: &str) -> Box<WireError> {
    refusal(
        VALUE_ERROR,
        format!(
            "A turn's host acts for its room {:?}, not for {named}",
            scope.room_id.as_str()
        ),
    )
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
        if !BETWEEN_TURNS.contains(&verb.as_str()) {
            return Err(refusal(
                VALUE_ERROR,
                format!("{verb:?} needs a turn: it is not served between turns"),
            ));
        }
        let organization_id = room_organization(kit, &room_id).await?;
        // Work its turn began, which starts fresh once the turn is over
        // (RFC §8.3): no depth of the turn floors what it does.
        let scope = Scope {
            room_id: &room_id,
            organization_id: organization_id.as_deref(),
            floor: None,
        };
        run_verb(kit, &scope, &verb, arguments).await
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

/// The organization of `room_id`, which a verb after its turn acts in:
/// a room the store does not hold refuses it, as Python's owner refuses it.
async fn room_organization(
    kit: &Arc<dyn TurnKit>,
    room_id: &RoomId,
) -> Result<Option<String>, Box<WireError>> {
    match kit.room(room_id.clone()).await {
        Ok(Some(room)) => Ok(room.organization_id),
        Ok(None) => Err(refusal(
            VALUE_ERROR,
            format!("No room {:?}", room_id.as_str()),
        )),
        Err(error) => Err(Box::new(kit_error(&error))),
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
