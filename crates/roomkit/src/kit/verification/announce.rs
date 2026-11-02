//! What a verification's transitions announce (RFC §11.7.7, "Every
//! transition emits its framework event"): `verification_requested`,
//! `verification_completed` and `verification_ended` (RFC §8.2), and the
//! ASYNC hooks `ON_VERIFICATION_COMPLETED` and `ON_VERIFICATION_ENDED`, fired
//! in the request's room (RFC §9.2).

use roomkit_core::model::{
    ChannelId, ChannelType, EventContent, EventSource, EventStatus, EventType, IdentityId,
    Metadata, RoomEvent, SystemContent, Timestamp, VerificationRequest, VerificationRequestStatus,
    Visibility,
};
use roomkit_core::rules::hook_registry::HookTrigger;
use serde_json::{Value, json};

use super::state::{ENDED_REQUEST, ENDED_VERIFICATION};
use crate::events::FrameworkEvent;
use crate::kit::RoomKit;
use crate::store::ConversationStore;

impl<S: ConversationStore> RoomKit<S> {
    /// Emits the framework event `event_type` of `request`, in its room and
    /// on its channel, with the end's `reason` and what ended when it ends
    /// (Python's `_emit_verification_event`).
    pub(super) async fn announce_request(
        &self,
        event_type: &str,
        request: &VerificationRequest,
        end: Option<(&str, &str)>,
    ) {
        let mut data = Metadata::new();
        data.insert("identity_id".to_owned(), json!(request.identity_id));
        data.insert("request_id".to_owned(), json!(request.id));
        data.insert("method".to_owned(), json!(request.method));
        data.insert("expires_at".to_owned(), json!(request.expires_at));
        if let Some((reason, ended)) = end {
            data.insert("reason".to_owned(), json!(reason));
            data.insert("ended".to_owned(), json!(ended));
        }
        let event = FrameworkEvent {
            r#type: event_type.to_owned(),
            room_id: Some(request.room_id.clone()),
            channel_id: Some(request.channel_id.clone()),
            event_id: None,
            timestamp: self.inner.environment().now(),
            data,
        };
        self.emit_verification_event(event).await;
    }

    /// Emits `verification_ended` for the verification of `identity_id`
    /// that ended for `reason`, in no room: a verification belongs to the
    /// identity (RFC §11.7.1).
    pub(super) async fn announce_verification_end(&self, identity_id: &IdentityId, reason: &str) {
        let mut data = Metadata::new();
        data.insert("identity_id".to_owned(), json!(identity_id));
        data.insert("reason".to_owned(), json!(reason));
        data.insert("ended".to_owned(), json!(ENDED_VERIFICATION));
        let event = FrameworkEvent {
            r#type: "verification_ended".to_owned(),
            room_id: None,
            channel_id: None,
            event_id: None,
            timestamp: self.inner.environment().now(),
            data,
        };
        self.emit_verification_event(event).await;
    }

    /// Announces the open request a new request or a revocation took over:
    /// cancelled, or expired when its time had passed, an end the hooks hear
    /// (Python's `_announce_superseded`).
    pub(super) async fn announce_superseded(&self, request: &VerificationRequest) {
        if request.status != VerificationRequestStatus::Expired {
            let end = Some(("cancelled", ENDED_REQUEST));
            self.announce_request("verification_ended", request, end)
                .await;
            return;
        }
        let end = Some(("expired", ENDED_REQUEST));
        self.announce_request("verification_ended", request, end)
            .await;
        self.verification_hook(
            HookTrigger::OnVerificationEnded,
            request,
            end_data("expired", ENDED_REQUEST),
        )
        .await;
    }

    /// Fires `trigger`'s hooks in the room `request` was opened in, ASYNC,
    /// on a system event whose data names the identity, the request and its
    /// channel beside `data` (Python's `_verification_hook` through
    /// `_fire_lifecycle_hook`).
    pub(super) async fn verification_hook(
        &self,
        trigger: HookTrigger,
        request: &VerificationRequest,
        extra: Metadata,
    ) {
        let mut data = Metadata::new();
        data.insert("identity_id".to_owned(), json!(request.identity_id));
        data.insert("organization_id".to_owned(), json!(request.organization_id));
        data.insert("request_id".to_owned(), json!(request.id));
        data.insert("channel_id".to_owned(), json!(request.channel_id));
        data.extend(extra);
        let code = serde_json::to_value(trigger)
            .ok()
            .and_then(|code| code.as_str().map(str::to_owned));
        let content = SystemContent {
            body: format!("Verification of {}", request.identity_id),
            code,
            data,
        };
        let environment = self.inner.environment();
        let mut event = RoomEvent::at(
            environment.new_event_id(),
            request.room_id.clone(),
            EventType::System,
            EventSource::new(ChannelId::system(), ChannelType::SYSTEM),
            EventContent::System(content),
            environment.now(),
        );
        event.status = EventStatus::Delivered;
        event.visibility = Visibility::Internal;
        self.inner.observe_event(trigger, event).await;
    }

    /// Emits `event` to the kit's handlers and waits for them, as Python's
    /// `_emit_framework_event` awaits them.
    async fn emit_verification_event(&self, event: FrameworkEvent) {
        if let Some(handled) = self.inner.events.emit(&self.inner.tasks, event) {
            handled.wait().await;
        }
    }
}

/// The data an end's hook adds: its reason and what ended.
pub(super) fn end_data(reason: &str, ended: &str) -> Metadata {
    let mut data = Metadata::new();
    data.insert("reason".to_owned(), json!(reason));
    data.insert("ended".to_owned(), json!(ended));
    data
}

/// The data a completion's hook adds: the method and the verification's
/// expiry, in the serialized form.
pub(super) fn completion_data(method: Option<&str>, expires_at: Option<Timestamp>) -> Metadata {
    let mut data = Metadata::new();
    data.insert("method".to_owned(), json!(method));
    data.insert(
        "expires_at".to_owned(),
        expires_at.map_or(Value::Null, |expires| json!(expires)),
    );
    data
}
