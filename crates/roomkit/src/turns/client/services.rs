//! Services the standalone owner consults when constructing and serving a turn.
use super::{OwnerEvents, OwnerHost, OwnerTools, TurnClientOptions};
use crate::telemetry::TelemetryProvider;
use crate::turns::{ErrorClasses, ServedExecutor, lock};
use roomkit_core::model::{ChannelBinding, ChannelCategory, RoomContext, RoomEvent, RoomId};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

pub(in crate::turns) struct Services {
    pub(in crate::turns) executor: Option<Arc<dyn ServedExecutor>>,
    pub(in crate::turns) tools: Option<Arc<dyn OwnerTools>>,
    pub(in crate::turns) host: Option<Arc<dyn OwnerHost>>,
    pub(in crate::turns) events: Option<Arc<dyn OwnerEvents>>,
    pub(in crate::turns) errors: ErrorClasses,
    pub(in crate::turns) telemetry: Option<Arc<dyn TelemetryProvider>>,
    hooks: Mutex<HashMap<RoomId, Vec<String>>>,
}

impl fmt::Debug for Services {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnerServices")
            .field("errors", &self.errors)
            .finish_non_exhaustive()
    }
}

impl Services {
    pub(super) fn new(o: TurnClientOptions) -> Self {
        Self {
            executor: o.executor,
            tools: o.tools,
            host: o.host,
            events: o.events,
            errors: o.error_classes,
            telemetry: o.telemetry,
            hooks: Mutex::default(),
        }
    }
    pub(in crate::turns) fn trace(&self) -> Option<std::collections::BTreeMap<String, String>> {
        let id = crate::telemetry::get_current_span().filter(|id| !id.is_empty())?;
        let mut carrier = self
            .telemetry
            .as_ref()
            .map(|t| t.inject_context(&id))
            .unwrap_or_default();
        carrier.insert("span_id".into(), id);
        Some(carrier)
    }
    pub(super) fn enable(&self, room: &RoomId, name: &str) {
        let mut hooks = lock(&self.hooks);
        let names = hooks.entry(room.clone()).or_default();
        if !names.iter().any(|n| n == name) {
            names.push(name.to_owned());
        }
        if let Some(own) = &self.executor {
            own.apply_room_hooks(room, names.clone());
        }
    }
    pub(super) fn disable(&self, room: &RoomId, name: &str) -> bool {
        let mut hooks = lock(&self.hooks);
        let Some(names) = hooks.get_mut(room) else {
            return false;
        };
        let Some(i) = names.iter().position(|n| n == name) else {
            return false;
        };
        names.remove(i);
        if let Some(own) = &self.executor {
            own.apply_room_hooks(room, names.clone());
        }
        if names.is_empty() {
            hooks.remove(room);
        }
        true
    }
    pub(in crate::turns) fn hook_names(&self, room: &RoomId) -> Vec<String> {
        lock(&self.hooks).get(room).cloned().unwrap_or_default()
    }
    pub(in crate::turns) async fn channel_rows(
        &self,
        binding: &ChannelBinding,
        event: &RoomEvent,
        context: &RoomContext,
    ) -> Vec<RoomEvent> {
        let Some(host) = &self.host else {
            return Vec::new();
        };
        if binding.category != ChannelCategory::Intelligence
            || self
                .executor
                .as_ref()
                .is_some_and(|w| w.hosts_channel(&binding.channel_id))
        {
            return Vec::new();
        }
        let first = crate::history::distributed::history_start(event, context);
        match host
            .channel_rows(&event.room_id, &binding.channel_id, first)
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                tracing::warn!(%error,room_id=%event.room_id,channel_id=%binding.channel_id,"could not read the channel's rows for its turn");
                Vec::new()
            }
        }
    }
}
