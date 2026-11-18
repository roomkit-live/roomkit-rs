//! An owner callback's independent tool scope and response record.

use std::sync::Arc;

use roomkit_core::model::{ChannelId, Metadata, ResponseMetadata, Room};
use serde_json::Value;

use super::{Facts, verbs};
use crate::error::repr;
use crate::turn::{OwnerCall, OwnerToolAnswer, TurnKit};
use crate::turns::DeclaredVerb;
use crate::turns::error::{channel_error, kit_error, one};
use crate::turns::wire::{CallContext, RecordChanges, WireError};
use crate::{
    OwnerToolContext, OwnerToolIdentity, OwnerToolScope, RoomKitError, ToolCallScope, TurnRecord,
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
    let run = async {
        if let Some((client, request)) = &facts.client {
            let host = client.host.as_ref().ok_or_else(|| {
                Box::new(one(
                    "ValueError",
                    format!("No verb {} is served by the room owner", repr(verb)),
                ))
            })?;
            host.call(request, verb, arguments)
                .await
                .map_err(|error| Box::new(client.errors.record(error.as_ref())))
        } else {
            verbs::serve(kit, facts, verb, arguments, context).await
        }
    };
    if matches!(verb, "room_tasks" | "room_vision" | "room_context") {
        return HostAnswer {
            outcome: run.await,
            record_changes: None,
        };
    }
    if let Some(error) = invalid_context(facts, context) {
        return HostAnswer::failed(error);
    }
    let served = scoped(facts, context, run).await;
    HostAnswer {
        outcome: served.outcome,
        record_changes: served.record_changes,
    }
}

pub(super) struct ToolAnswer {
    pub(super) outcome: OwnerToolAnswer,
    pub(super) error: Option<WireError>,
    pub(super) structured_content: Option<Metadata>,
    pub(super) record_changes: Option<RecordChanges>,
}

impl From<OwnerToolAnswer> for ToolAnswer {
    fn from(outcome: OwnerToolAnswer) -> Self {
        Self {
            outcome,
            error: None,
            structured_content: None,
            record_changes: None,
        }
    }
}

pub(super) async fn serve_tool(
    kit: &Arc<dyn TurnKit>,
    facts: &Facts,
    call: crate::turns::wire::ToolCall,
) -> ToolAnswer {
    let context = call.context.clone();
    if let Some(error) = invalid_context(facts, &context) {
        return OwnerToolAnswer::Unserved(error.message).into();
    }
    let run = async {
        if let Some((client, request)) = &facts.client {
            let Some(tools) = &client.tools else {
                return (
                    OwnerToolAnswer::Unserved(format!(
                        "No tool {} is served by the room owner",
                        repr(&call.name)
                    )),
                    None,
                );
            };
            return match tools.serve(request, call).await {
                Ok(value) => (OwnerToolAnswer::Served(value), None),
                Err(error) => {
                    let record = client.errors.record(error.as_ref());
                    let outcome = match record.kind.rsplit(':').next().unwrap_or(&record.kind) {
                        "UnservedToolCallError" => {
                            OwnerToolAnswer::Unserved(record.message.clone())
                        }
                        "ToolRefusedError" => OwnerToolAnswer::Refused(record.message.clone()),
                        _ => OwnerToolAnswer::Failed {
                            kind: record.kind.clone(),
                            message: record.message.clone(),
                        },
                    };
                    (outcome, Some(record))
                }
            };
        }
        let native = OwnerCall {
            room_id: facts.room_id.clone(),
            room_metadata: Arc::clone(&facts.room_metadata),
            channel_id: facts.channel_id.clone(),
            tool_call_id: context.tool_call_id.clone(),
            arguments: call.arguments,
            requester: context.requester.clone(),
        };
        (kit.serve_owner_tool(call.name, native).await, None)
    };
    let served = scoped(facts, &context, run).await;
    let (outcome, error) = served.outcome;
    let structured_content = matches!(outcome, OwnerToolAnswer::Served(_))
        .then_some(served.structured_content)
        .flatten();
    ToolAnswer {
        outcome,
        error,
        structured_content,
        record_changes: served.record_changes,
    }
}

struct Served<T> {
    outcome: T,
    record_changes: Option<RecordChanges>,
    structured_content: Option<Metadata>,
}

async fn scoped<T>(
    facts: &Facts,
    context: &CallContext,
    work: impl Future<Output = T>,
) -> Served<T> {
    let scope = Arc::new(OwnerToolScope::new(owner_context(facts, context)));
    let record = context
        .response_metadata
        .clone()
        .map(ResponseMetadata::from);
    let served = async {
        let work = Box::pin(work);
        match record.as_ref() {
            Some(record) => {
                let held = TurnRecord::default();
                held.adopt(record.clone());
                held.scope(work).await
            }
            None => crate::turn_record::without_turn_record(work).await,
        }
    };
    let depth = context.chain_depth.max(facts.response_depth);
    let outcome = crate::turn::without_turn_host(within_tool_call_scope(
        Arc::<OwnerToolScope>::clone(&scope),
        within_tool_turn_depth(depth, served),
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
    Served {
        outcome,
        record_changes,
        structured_content: scope.structured_content(),
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
    error_classes: crate::turns::ErrorClasses,
    verb: DeclaredVerb,
    arguments: Metadata,
) -> Result<Value, Box<WireError>> {
    verb.call(arguments).await.map_err(|error| {
        Box::new(match error.downcast_ref::<RoomKitError>() {
            Some(error) => kit_error(error),
            None => error_classes.encode_with(error.as_ref(), || channel_error(error.as_ref())),
        })
    })
}

pub(super) async fn between_turns(
    error_classes: crate::turns::ErrorClasses,
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
        within_tool_turn_depth(0, invoke(error_classes, verb, arguments)),
    );
    crate::turn::without_turn_host(crate::turn_record::without_turn_record(work)).await
}
