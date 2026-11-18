//! Snapshot one owner call, then merge its answer before returning or raising.

use roomkit_core::model::Metadata;
use serde_json::Value;

use crate::turn::OwnerToolAnswer;
use crate::turns::ErrorClasses;
use crate::turns::wire::{CallContext, RecordChanges, WireError};
use crate::{RoomKitError, current_tool_call_scope, current_turn_record};

pub(super) fn call_context(floor: u32) -> CallContext {
    let scope = current_tool_call_scope().map(|scope| scope.owner_context());
    let call = scope.as_ref().and_then(|scope| scope.call.as_ref());
    let allowed_names = scope.as_ref().and_then(|scope| {
        scope.allowed_names.as_ref().map(|names| {
            let mut names: Vec<_> = names.iter().cloned().collect();
            names.sort();
            names
        })
    });
    CallContext {
        tool_call_id: call.map_or_else(String::new, |call| call.tool_call_id.clone()),
        room_id: scope
            .as_ref()
            .and_then(|scope| scope.room_id.as_ref())
            .map_or_else(String::new, ToString::to_string),
        channel_id: call
            .and_then(|call| call.channel_id.as_ref())
            .map_or_else(String::new, ToString::to_string),
        chain_depth: scope.as_ref().map_or(floor, |scope| scope.chain_depth),
        actor_id: scope.as_ref().and_then(|scope| scope.actor_id.clone()),
        allowed_names,
        requester: scope.and_then(|scope| scope.requester),
        response_metadata: current_turn_record().map(|record| record.to_metadata()),
    }
}

pub(super) struct HostReply {
    pub(super) result: Value,
    pub(super) error: Option<WireError>,
    pub(super) record_changes: Option<RecordChanges>,
}

impl HostReply {
    pub(super) fn finish(
        self,
        carried: Option<&Metadata>,
        error_classes: &ErrorClasses,
    ) -> Result<Value, RoomKitError> {
        take_changes(carried, self.record_changes);
        match self.error {
            Some(error) => Err(error_classes.rebuild(&error)),
            None => Ok(self.result),
        }
    }
}

/// The wire fields kept until the waiting executor call resumes in its own scope.
pub(super) struct ToolReply {
    pub(super) outcome: OwnerToolAnswer,
    pub(super) structured_content: Option<Metadata>,
    pub(super) record_changes: Option<RecordChanges>,
}

impl From<OwnerToolAnswer> for ToolReply {
    fn from(outcome: OwnerToolAnswer) -> Self {
        Self {
            outcome,
            structured_content: None,
            record_changes: None,
        }
    }
}

impl ToolReply {
    pub(super) fn finish(self, carried: Option<&Metadata>) -> OwnerToolAnswer {
        take_changes(carried, self.record_changes);
        if matches!(
            self.outcome,
            OwnerToolAnswer::Served(_) | OwnerToolAnswer::Parts(_)
        ) && let (Some(copy), Some(scope)) = (self.structured_content, current_tool_call_scope())
        {
            scope.set_structured_content(copy);
        }
        self.outcome
    }
}

fn take_changes(carried: Option<&Metadata>, changes: Option<RecordChanges>) {
    // Read the live record at resumption: a hook may have replaced the
    // turn's record while this call waited for its owner.
    if let (Some(carried), Some(changes), Some(record)) = (carried, changes, current_turn_record())
    {
        record.apply_call_changes(carried, &changes.written, &changes.removed);
    }
}
