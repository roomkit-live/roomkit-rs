//! The `delegate_task` tool kit (roomkit-specs `conformance/README.md`,
//! "Calls"; RFC §23.4): `delegate_handler` builds the implementation's
//! `DelegateHandler` under a name the later calls use; `setup_delegation`
//! serves the tool on an agent, whose model then calls it; `delegate.handle`
//! is the handler's own call, its `as` label naming the task's id.

use std::sync::Arc;

use roomkit::model::{ChannelId, Metadata, RoomId};
use roomkit::tasks::{CompletedTaskCache, DelegateHandler, DelegateHandlerOptions};
use roomkit_ai::tasks::{SetupDelegationOptions, build_delegate_tool, setup_delegation};
use serde_json::Value;

use crate::calls::{Called, Unreadable, dump, field, finish, optional};
use crate::run::{Run, lock};

/// The arguments of the calls, for the shape check.
pub(crate) fn arguments(name: &str) -> Option<&'static [&'static str]> {
    match name {
        "delegate_handler" => Some(&[
            "handler",
            "notify",
            "default_share_channels",
            "cache",
            "serialize_per_room",
            "max_conversation",
        ]),
        "setup_delegation" => Some(&["agent_id", "handler", "targets"]),
        "delegate.handle" => Some(&["handler", "room_id", "calling_agent_id", "arguments"]),
        _ => None,
    }
}

/// Whether this module makes the call `name`.
pub(crate) fn handles(name: &str) -> bool {
    arguments(name).is_some()
}

/// The handler the run keeps under the name the call's `handler` gives.
fn handler(run: &Run, spec: &Value) -> Result<DelegateHandler<crate::Store>, Unreadable> {
    let name = field::<String>(spec, "handler")?;
    lock(&run.delegate_handlers)
        .get(&name)
        .cloned()
        .ok_or_else(|| format!("no delegate handler {name:?}"))
}

/// Makes the call `name`; returns its outcome and, for a call on the
/// handler, the id of the task it names.
pub(crate) async fn call(
    run: &Arc<Run>,
    name: &str,
    spec: &Value,
) -> Result<(Called, Option<String>), Unreadable> {
    Ok(match name {
        "delegate_handler" => {
            let defaults = DelegateHandlerOptions::<crate::Store>::default();
            let options = DelegateHandlerOptions {
                notify: optional(spec, "notify")?,
                default_share_channels: optional(spec, "default_share_channels")?,
                cache: optional::<bool>(spec, "cache")?
                    .unwrap_or_default()
                    .then(CompletedTaskCache::default),
                serialize_per_room: optional(spec, "serialize_per_room")?
                    .unwrap_or(defaults.serialize_per_room),
                max_conversation: optional(spec, "max_conversation")?
                    .unwrap_or(defaults.max_conversation),
                ..defaults
            };
            let built = DelegateHandler::new(run.kit(), options);
            let named = field::<String>(spec, "handler")?;
            lock(&run.delegate_handlers).insert(named, built);
            (Ok(Value::Null), None)
        }
        "setup_delegation" => {
            let agent = field::<ChannelId>(spec, "agent_id")?;
            let targets: Option<Vec<(String, Option<String>)>> = optional(spec, "targets")?;
            let options = SetupDelegationOptions {
                tool: targets.map(|targets| build_delegate_tool(&targets)),
            };
            let outcome = setup_delegation(agent, &handler(run, spec)?, options);
            (finish(run, name, outcome, |()| Value::Null), None)
        }
        _ => {
            let (room, calling): (RoomId, ChannelId) =
                (field(spec, "room_id")?, field(spec, "calling_agent_id")?);
            let arguments: Metadata = field(spec, "arguments")?;
            let outcome = handler(run, spec)?
                .handle(&room, &calling, &arguments)
                .await;
            let id = outcome.as_ref().ok().and_then(|answer| {
                answer
                    .get("task_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            });
            (finish(run, name, outcome, dump), id)
        }
    })
}
