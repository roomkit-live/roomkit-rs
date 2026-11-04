//! The kit's telemetry: Python's `RoomKit(telemetry=…)`, `kit.telemetry`.

use std::sync::Arc;

use roomkit_core::model::{ChannelId, EventType, IdentityResult, InboundMessage, Metadata, RoomId};
use serde_json::Value;

use super::{Inner, RoomKit};
use crate::error::RoomKitError;
use crate::inbound::InboundResult;
use crate::tasks::DelegatedTask;
use crate::telemetry::{
    Attr, KitTelemetry, LateSpan, NoopTelemetryProvider, OpenSpan, SpanKind, StartSpanOptions,
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

/// Ends a `framework.route` span, the room found.
pub(crate) fn end_route_span(span: OpenSpan, room_id: &RoomId) {
    let mut attributes = Metadata::new();
    attributes.insert(Attr::ROOM_ID.to_owned(), Value::from(room_id.as_str()));
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

/// The kit's telemetry of `config`, its metadata handed to its provider.
pub(super) fn telemetry_of(config: Option<TelemetryConfig>) -> KitTelemetry {
    let config = config.unwrap_or_default();
    let provider = config
        .provider
        .unwrap_or_else(|| Arc::new(NoopTelemetryProvider));
    if !config.metadata.is_empty() {
        provider.set_metadata(config.metadata);
    }
    KitTelemetry {
        provider,
        sensitive: config.record_sensitive_data,
        suppressed: Arc::new(config.suppressed_hook_triggers),
    }
}

impl<S> Inner<S> {
    /// The span of a call to `process_inbound`, Python's `framework.inbound`.
    pub(crate) fn inbound_span(
        &self,
        message: &InboundMessage,
        sender_id: &str,
        room_id: Option<&RoomId>,
    ) -> OpenSpan {
        let mut attributes = self.telemetry.sender_attributes(sender_id);
        if let Some(session) = message.metadata.get("voice_session_id") {
            attributes.insert(Attr::SESSION_ID.to_owned(), session.clone());
        }
        OpenSpan::start(
            &self.telemetry,
            SpanKind::InboundPipeline,
            "framework.inbound",
            StartSpanOptions {
                channel_id: Some(message.channel_id.to_string()),
                room_id: room_id.map(ToString::to_string),
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

    /// The span of an injected event, Python's `framework.send_event`: it
    /// opens as the injection goes in, the refusals before it behind.
    pub(crate) fn send_event_span(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        event_type: EventType,
    ) -> LateSpan {
        let mut attributes = Metadata::new();
        let event_type = serde_json::to_value(event_type).unwrap_or_default();
        attributes.insert("event_type".to_owned(), event_type);
        LateSpan::new(
            &self.telemetry,
            SpanKind::InboundPipeline,
            "framework.send_event",
            StartSpanOptions {
                room_id: Some(room_id.to_string()),
                channel_id: Some(channel_id.to_string()),
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
