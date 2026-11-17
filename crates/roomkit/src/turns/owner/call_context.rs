//! A host callback's independent owner-side tool scope and response record.

use std::sync::Arc;

use roomkit_core::model::{ChannelId, Metadata, ResponseMetadata, Room};
use serde_json::Value;

use super::{Facts, verbs};
use crate::error::repr;
use crate::turn::TurnKit;
use crate::turns::DeclaredVerb;
use crate::turns::error::{channel_error, kit_error, one};
use crate::turns::wire::{CallContext, RecordChanges, WireError};
use crate::{
    OwnerToolContext, OwnerToolIdentity, OwnerToolScope, RoomKitError, TurnRecord,
    within_tool_call_scope, within_tool_turn_depth,
};

pub(super) struct HostAnswer {
    pub(super) outcome: Result<Value, Box<WireError>>,
    pub(super) record_changes: Option<RecordChanges>,
}

impl HostAnswer {
    pub(super) fn failed(error: WireError) -> Self {
        Self {
            outcome: Err(Box::new(error)),
            record_changes: None,
        }
    }
}

pub(super) async fn serve(
    kit: &Arc<dyn TurnKit>,
    facts: &Facts,
    verb: &str,
    arguments: Metadata,
    context: &CallContext,
) -> HostAnswer {
    if matches!(verb, "room_tasks" | "room_vision" | "room_context") {
        return HostAnswer {
            outcome: verbs::serve(kit, facts, verb, arguments, context).await,
            record_changes: None,
        };
    }
    if let Some(error) = invalid_context(facts, context) {
        return HostAnswer::failed(error);
    }
    let scope = Arc::new(OwnerToolScope::new(owner_context(facts, context)));
    let record = context
        .response_metadata
        .clone()
        .map(ResponseMetadata::from);
    let work = async {
        let served = Box::pin(verbs::serve(kit, facts, verb, arguments, context));
        match record.as_ref() {
            Some(record) => {
                let held = TurnRecord::default();
                held.adopt(record.clone());
                held.scope(served).await
            }
            None => crate::turn_record::without_turn_record(served).await,
        }
    };
    let depth = context.chain_depth.max(facts.response_depth);
    let outcome = crate::turn::without_turn_host(within_tool_call_scope(
        scope,
        within_tool_turn_depth(depth, work),
    ))
    .await;
    let record_changes =
        context
            .response_metadata
            .as_ref()
            .zip(record)
            .and_then(|(carried, worked)| {
                let (written, removed) = worked.call_changes(carried);
                (!written.is_empty() || !removed.is_empty())
                    .then_some(RecordChanges { written, removed })
            });
    HostAnswer {
        outcome,
        record_changes,
    }
}

fn invalid_context(facts: &Facts, context: &CallContext) -> Option<WireError> {
    if (context.room_id.is_empty() || context.room_id == facts.room_id.as_str())
        && (context.channel_id.is_empty() || context.channel_id == facts.channel_id.as_str())
    {
        return None;
    }
    Some(one(
        "roomkit.core.exceptions:UnservedToolCallError",
        format!(
            "A call of the turn in room {} named room {} and channel {}",
            repr(facts.room_id.as_str()),
            repr(&context.room_id),
            repr(&context.channel_id),
        ),
    ))
}

fn owner_context(facts: &Facts, context: &CallContext) -> OwnerToolContext {
    OwnerToolContext {
        room_id: Some(facts.room_id.clone()),
        room: (facts.room.id == facts.room_id).then(|| Arc::clone(&facts.room)),
        actor_id: context.actor_id.clone(),
        requester: context.requester.clone(),
        allowed_names: context
            .allowed_names
            .clone()
            .map(|names| names.into_iter().collect()),
        chain_depth: context.chain_depth.max(facts.response_depth),
        call: Some(OwnerToolIdentity {
            room_id: Some(facts.room_id.clone()),
            channel_id: Some(facts.channel_id.clone()),
            tool_call_id: context.tool_call_id.clone(),
        }),
    }
}

/// Invoke inside the scope, including synchronous work constructing the future.
pub(super) async fn invoke(
    verb: DeclaredVerb,
    arguments: Metadata,
) -> Result<Value, Box<WireError>> {
    verb.call(arguments).await.map_err(|error| {
        Box::new(match error.downcast_ref::<RoomKitError>() {
            Some(error) => kit_error(error),
            None => channel_error(error.as_ref()),
        })
    })
}

pub(super) async fn between_turns(
    verb: DeclaredVerb,
    room: Room,
    channel_id: ChannelId,
    arguments: Metadata,
) -> Result<Value, Box<WireError>> {
    let scope = OwnerToolScope::new(OwnerToolContext {
        room_id: Some(room.id.clone()),
        call: Some(OwnerToolIdentity {
            room_id: Some(room.id.clone()),
            channel_id: Some(channel_id),
            tool_call_id: String::new(),
        }),
        room: Some(Arc::new(room)),
        ..OwnerToolContext::default()
    });
    let work = within_tool_call_scope(
        Arc::new(scope),
        within_tool_turn_depth(0, invoke(verb, arguments)),
    );
    crate::turn::without_turn_host(crate::turn_record::without_turn_record(work)).await
}
