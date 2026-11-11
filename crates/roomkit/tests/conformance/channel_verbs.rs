//! The scripted channel's local per-room verbs.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit::model::{ChannelId, Metadata, RoomId};
use roomkit::{ChannelError, ClassedError, RoomKitError};
use serde_json::Value;

use crate::run::{Run, Scope};
use crate::scripted::ScriptedFailure;

pub(crate) struct Verbs {
    specs: Value,
    kept: Mutex<HashMap<RoomId, Metadata>>,
}

impl Verbs {
    pub(crate) fn new(specs: Value) -> Self {
        Self {
            specs,
            kept: Mutex::default(),
        }
    }

    fn kept(&self) -> MutexGuard<'_, HashMap<RoomId, Metadata>> {
        self.kept.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) async fn call(
        &self,
        run: Option<Arc<Run>>,
        channel_id: &ChannelId,
        room_id: RoomId,
        verb: String,
        arguments: Metadata,
    ) -> Result<Value, ChannelError> {
        let Some(spec) = self.specs.get(&verb) else {
            return Err(RoomKitError::Value(format!(
                "Channel '{channel_id}' has no per-room verb '{verb}'"
            ))
            .into());
        };
        if let Some(gate) = spec["gate"].as_str()
            && let Some(run) = &run
        {
            run.gates.hold(gate).await;
        }
        if spec["keeps"] == true {
            self.kept()
                .entry(room_id.clone())
                .or_default()
                .extend(arguments);
        }
        if spec["forgets"] == true {
            self.forget(&room_id);
        }
        if let Some(run) = &run {
            for action in spec["do"].as_array().into_iter().flatten() {
                run.run_action(action.clone(), Scope::in_room(room_id.as_str()))
                    .await;
            }
        }
        if let Some(failure) = spec["fail"].as_str() {
            return Err(ClassedError::new(
                "ScriptedFailureError",
                ScriptedFailure(failure.to_owned()),
            )
            .into());
        }
        if spec["keeps"] == true || spec["reads"] == true {
            return Ok(Value::Object(self.state(&room_id)));
        }
        Ok(spec.get("result").cloned().unwrap_or(Value::Null))
    }

    pub(crate) fn state(&self, room_id: &RoomId) -> Metadata {
        self.kept().get(room_id).cloned().unwrap_or_default()
    }

    pub(crate) fn restore(&self, room_id: &RoomId, state: Metadata) {
        if !state.is_empty() {
            self.kept().entry(room_id.clone()).or_insert(state);
        }
    }

    pub(crate) fn forget(&self, room_id: &RoomId) {
        self.kept().remove(room_id);
    }
}

pub(crate) async fn call(
    run: &Arc<Run>,
    spec: &Value,
) -> Result<crate::calls::Called, crate::calls::Unreadable> {
    use crate::calls::{dump, field, finish, optional};
    let options = roomkit::ChannelCallOptions {
        organization_id: spec["organization_id"].as_str().map(str::to_owned),
        arguments: optional::<Metadata>(spec, "arguments")?.unwrap_or_default(),
    };
    let outcome = run
        .kit_of(spec)?
        .channel_call(
            &field(spec, "channel_id")?,
            &field(spec, "room_id")?,
            &field::<String>(spec, "verb")?,
            options,
        )
        .await;
    Ok(finish(run, "channel_call", outcome, dump))
}
