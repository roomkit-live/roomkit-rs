//! The kit's telemetry: Python's `RoomKit(telemetry=…)`, `kit.telemetry`.

use std::collections::BTreeSet;
use std::sync::Arc;

use roomkit_core::model::{
    ChannelId, DirectEvent, IdentityResult, InboundMessage, Metadata, RoomId,
};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::HookTrigger;
use serde_json::Value;

use super::{Inner, RoomKit};
use crate::error::RoomKitError;
use crate::inbound::InboundResult;
use crate::tasks::DelegatedTask;
use crate::telemetry::{
    Attr, KitTelemetry, NoopTelemetryProvider, OpenSpan, SpanKind, StartSpanOptions,
    TelemetryConfig, TelemetryProvider,
};

impl<S> RoomKit<S> {
    /// Python `telemetry`: the provider the kit reports to, the one
    /// [`RoomKitOptions::telemetry`](crate::RoomKitOptions::telemetry)
    /// gave, else a [`NoopTelemetryProvider`].
    #[must_use]
    pub fn telemetry(&self) -> Arc<dyn TelemetryProvider> {
        Arc::clone(&self.inner.telemetry.provider)
    }
}

/// A hook trigger's value, as Python's `str(trigger)`.
fn trigger_name(trigger: HookTrigger) -> String {
    serde_json::to_value(trigger)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_default()
}

/// Ends a sync hook's span as its `outcome` reads: `ok` with the action it
/// returned; `error` with what it raised, `timeout`, or
/// `invalid return type`.
pub(crate) fn end_hook_span<P>(span: Option<OpenSpan>, outcome: &HookOutcome<P>) {
    let Some(span) = span else {
        return;
    };
    match outcome {
        HookOutcome::Returned(result) => {
            let mut attributes = Metadata::new();
            let action = serde_json::to_value(result.action).unwrap_or_default();
            attributes.insert(Attr::HOOK_RESULT.to_owned(), action);
            span.end(attributes);
        }
        HookOutcome::Raised(error) => {
            span.fail_error(&error.error_type, &error.message, Metadata::new());
        }
        HookOutcome::TimedOut => span.fail("timeout", Metadata::new()),
        HookOutcome::Invalid { .. } => span.fail("invalid return type", Metadata::new()),
    }
}

/// Ends a `framework.route` span, the room found, if any.
pub(crate) fn end_route_span(span: OpenSpan, room_id: Option<&RoomId>) {
    let room = room_id.map(ToString::to_string).unwrap_or_default();
    let mut attributes = Metadata::new();
    attributes.insert(Attr::ROOM_ID.to_owned(), Value::from(room));
    span.end(attributes);
}

/// Ends a `framework.identity_resolution` span with the resolver's status.
pub(crate) fn end_identity_span(span: OpenSpan, result: &IdentityResult) {
    let mut attributes = Metadata::new();
    let status = serde_json::to_value(result.status).unwrap_or_default();
    attributes.insert("identity_status".to_owned(), status);
    span.end(attributes);
}

/// Ends a `framework.inbound` span as the call's `result` reads: `blocked`
/// and `deferred` on its end, the room the message reached.
pub(super) fn end_inbound_span(
    span: OpenSpan,
    result: &Result<InboundResult, RoomKitError>,
    deferred: bool,
) {
    let mut attributes = Metadata::new();
    attributes.insert("deferred".to_owned(), Value::from(deferred));
    match result {
        Ok(result) => {
            attributes.insert("blocked".to_owned(), Value::from(result.blocked));
            if let Some(event) = &result.event {
                let room = Value::from(event.room_id.as_str());
                attributes.insert(Attr::ROOM_ID.to_owned(), room);
            }
            span.end(attributes);
        }
        Err(error) => span.fail_error(error.class_name(), error.to_string(), attributes),
    }
}

/// Ends an async hook's span as its `outcome` reads: as a sync hook's, the
/// action left out, as Python's.
pub(crate) fn end_observer_span<P>(span: Option<OpenSpan>, outcome: &HookOutcome<P>) {
    let Some(span) = span else {
        return;
    };
    match outcome {
        HookOutcome::Returned(_) => span.end(Metadata::new()),
        HookOutcome::Raised(error) => {
            span.fail_error(&error.error_type, &error.message, Metadata::new());
        }
        HookOutcome::TimedOut => span.fail("timeout", Metadata::new()),
        HookOutcome::Invalid { .. } => span.fail("invalid return type", Metadata::new()),
    }
}

/// The kit's telemetry of `config`, its metadata handed to its provider, and
/// the hook triggers that open no span.
pub(super) fn telemetry_of(config: Option<TelemetryConfig>) -> (KitTelemetry, BTreeSet<String>) {
    let config = config.unwrap_or_default();
    let provider = config
        .provider
        .unwrap_or_else(|| Arc::new(NoopTelemetryProvider));
    if !config.metadata.is_empty() {
        provider.set_metadata(config.metadata);
    }
    let telemetry = KitTelemetry {
        provider,
        sensitive: config.record_sensitive_data,
    };
    (telemetry, config.suppressed_hook_triggers)
}

impl<S> Inner<S> {
    /// The span of the hook `name` on `trigger` in `room_id`, Python's
    /// `hook.sync.<name>` and `hook.async.<name>`; `None` for a trigger the
    /// telemetry's config suppresses.
    pub(crate) fn hook_span(
        &self,
        kind: SpanKind,
        name: &str,
        trigger: HookTrigger,
        room_id: &RoomId,
    ) -> Option<OpenSpan> {
        let trigger = trigger_name(trigger);
        if self.suppressed_hook_triggers.contains(&trigger) {
            return None;
        }
        let name = if name.is_empty() { "unnamed" } else { name };
        let mut attributes = Metadata::new();
        attributes.insert(Attr::HOOK_NAME.to_owned(), Value::from(name));
        attributes.insert(Attr::HOOK_TRIGGER.to_owned(), Value::from(trigger));
        let options = StartSpanOptions {
            room_id: Some(room_id.to_string()),
            attributes,
            ..StartSpanOptions::default()
        };
        Some(OpenSpan::start(
            &self.telemetry,
            kind,
            &format!("{kind}.{name}"),
            options,
        ))
    }

    /// A span's `sender_id`, an address: none unless the kit records what
    /// can hold personal data (Python's `sensitive`).
    fn sender_attributes(&self, sender_id: &str) -> Metadata {
        let mut attributes = Metadata::new();
        if self.telemetry.sensitive {
            attributes.insert("sender_id".to_owned(), Value::from(sender_id));
        }
        attributes
    }

    /// The span of a call to `process_inbound`, Python's `framework.inbound`,
    /// and, for a call that names its room, its `framework.route` read: the
    /// router's own runs in the driver (`route_span`).
    pub(crate) fn inbound_span(
        &self,
        message: &InboundMessage,
        room_id: Option<&RoomId>,
    ) -> OpenSpan {
        let mut attributes = self.sender_attributes(&message.sender_id);
        if let Some(session) = message.metadata.get("voice_session_id") {
            attributes.insert(Attr::SESSION_ID.to_owned(), session.clone());
        }
        let span = OpenSpan::start(
            &self.telemetry,
            SpanKind::InboundPipeline,
            "framework.inbound",
            StartSpanOptions {
                channel_id: Some(message.channel_id.to_string()),
                room_id: room_id.map(ToString::to_string),
                attributes,
                ..StartSpanOptions::default()
            },
        );
        if let Some(room_id) = room_id {
            let route = self.route_span_under(
                Some(span.id().to_owned()),
                &message.channel_id,
                &message.sender_id,
            );
            end_route_span(route, Some(room_id));
        }
        span
    }

    /// The span of a router's answer, Python's `framework.route`.
    pub(crate) fn route_span(&self, channel_id: &ChannelId, sender_id: &str) -> OpenSpan {
        self.route_span_under(None, channel_id, sender_id)
    }

    fn route_span_under(
        &self,
        parent_id: Option<String>,
        channel_id: &ChannelId,
        sender_id: &str,
    ) -> OpenSpan {
        let attributes = self.sender_attributes(sender_id);
        OpenSpan::start(
            &self.telemetry,
            SpanKind::InboundPipeline,
            "framework.route",
            StartSpanOptions {
                parent_id,
                channel_id: Some(channel_id.to_string()),
                attributes,
                ..StartSpanOptions::default()
            },
        )
    }

    /// The span of the resolver's answer, Python's
    /// `framework.identity_resolution`.
    pub(crate) fn identity_span(&self, room_id: &RoomId, channel_id: &ChannelId) -> OpenSpan {
        OpenSpan::start(
            &self.telemetry,
            SpanKind::InboundPipeline,
            "framework.identity_resolution",
            StartSpanOptions {
                room_id: Some(room_id.to_string()),
                channel_id: Some(channel_id.to_string()),
                ..StartSpanOptions::default()
            },
        )
    }

    /// The span of an injected event, Python's `framework.send_event`.
    pub(crate) fn send_event_span(&self, room_id: &RoomId, event: &DirectEvent) -> OpenSpan {
        let mut attributes = Metadata::new();
        let event_type = serde_json::to_value(event.event_type).unwrap_or_default();
        attributes.insert("event_type".to_owned(), event_type);
        OpenSpan::start(
            &self.telemetry,
            SpanKind::InboundPipeline,
            "framework.send_event",
            StartSpanOptions {
                room_id: Some(room_id.to_string()),
                channel_id: Some(event.channel_id.to_string()),
                attributes,
                ..StartSpanOptions::default()
            },
        )
    }

    /// The span of a delegation, Python's `delegation.inline` and
    /// `delegation.background`.
    pub(crate) fn delegation_span(&self, task: &DelegatedTask, wait: bool) -> OpenSpan {
        let mode = if wait { "inline" } else { "background" };
        let mut attributes = Metadata::new();
        for (key, value) in [
            (Attr::DELEGATION_TASK_ID, task.id.as_str()),
            (Attr::DELEGATION_WORKER_ID, task.agent_id.as_str()),
            (Attr::DELEGATION_CHILD_ROOM_ID, task.child_room_id.as_str()),
            (
                Attr::DELEGATION_PARENT_ROOM_ID,
                task.parent_room_id.as_str(),
            ),
            (Attr::DELEGATION_MODE, mode),
        ] {
            attributes.insert(key.to_owned(), Value::from(value));
        }
        OpenSpan::start(
            &self.telemetry,
            SpanKind::Delegation,
            &format!("delegation.{mode}"),
            StartSpanOptions {
                room_id: Some(task.parent_room_id.to_string()),
                channel_id: Some(task.agent_id.to_string()),
                attributes,
                ..StartSpanOptions::default()
            },
        )
    }

    /// Closes the telemetry provider, last of all, as Python's close: on a
    /// thread of the runtime's, a provider's export being bounded by itself
    /// and never run on the kit's tasks.
    pub(super) async fn close_telemetry(&self) {
        let telemetry = Arc::clone(&self.telemetry.provider);
        if let Err(error) = tokio::task::spawn_blocking(move || telemetry.close()).await {
            tracing::warn!(target: "roomkit::telemetry", %error, "closing the telemetry provider failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use crate::telemetry::{MockTelemetryProvider, TelemetryConfig, TelemetryProvider};
    use crate::{RoomKit, RoomKitOptions};

    fn kit_of(telemetry: Option<TelemetryConfig>) -> RoomKit {
        RoomKit::new(RoomKitOptions {
            telemetry,
            ..RoomKitOptions::default()
        })
    }

    // Python: tests/test_telemetry.py::TestRoomKitWiring::test_default_noop_telemetry
    #[tokio::test]
    async fn a_kit_s_provider_is_noop_by_default() {
        let kit = kit_of(None);
        assert_eq!(kit.telemetry().name(), "noop");
        kit.close().await.unwrap();
    }

    // Python: tests/test_telemetry.py::TestRoomKitWiring::test_provider_direct, test_config_with_provider
    #[tokio::test]
    async fn a_kit_s_provider_is_the_one_given_alone_or_in_a_config() {
        let mock = Arc::new(MockTelemetryProvider::default());
        let given: Arc<dyn TelemetryProvider> = mock;
        let alone = kit_of(Some(Arc::clone(&given).into()));
        assert!(Arc::ptr_eq(&alone.telemetry(), &given));
        alone.close().await.unwrap();
        let in_config = kit_of(Some(TelemetryConfig {
            provider: Some(Arc::clone(&given)),
            suppressed_hook_triggers: BTreeSet::new(),
            ..TelemetryConfig::default()
        }));
        assert!(Arc::ptr_eq(&in_config.telemetry(), &given));
        in_config.close().await.unwrap();
    }

    // Python: tests/test_telemetry.py::TestRoomKitWiring::test_config_without_provider_falls_back_to_noop
    #[tokio::test]
    async fn a_config_with_no_provider_means_noop() {
        let kit = kit_of(Some(TelemetryConfig::default()));
        assert_eq!(kit.telemetry().name(), "noop");
        kit.close().await.unwrap();
    }
}
