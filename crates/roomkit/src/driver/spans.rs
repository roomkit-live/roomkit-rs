//! The `framework.broadcast` spans of a run: each delivery set of the run
//! from its first target's call to the set leaving its place in the room's
//! lane, as Python's `EventRouter.broadcast` opens one around a set.

use std::collections::HashMap;

use roomkit_core::model::{EventId, Metadata, RoomEvent};
use roomkit_core::ops::{ChannelOutcome, ChannelRequest, EffectId, HoldRequest, Outcome, Request};
use serde_json::Value;

use crate::telemetry::{Attr, KitTelemetry, OpenSpan, SpanKind, StartSpanOptions};

/// A delivery set's span, and what its end reports.
struct Broadcast {
    span: OpenSpan,
    targets: u64,
    delivered: u64,
    failed: u64,
}

/// What a call of a set counts as, once answered.
#[derive(Clone, Copy)]
enum Call {
    OnEvent,
    Deliver,
}

pub(super) struct RunSpans {
    telemetry: KitTelemetry,
    /// The events the run committed, until their set is over: what a set's
    /// span reads of its event.
    committed: HashMap<EventId, (String, String, Option<String>)>,
    open: HashMap<EventId, Broadcast>,
    calls: HashMap<EffectId, (EventId, Call)>,
}

/// The `voice_session_id` an event's metadata carries, if any.
fn voice_session(metadata: &Metadata) -> Option<String> {
    metadata
        .get("voice_session_id")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

impl RunSpans {
    pub(super) fn new(telemetry: KitTelemetry) -> Self {
        Self {
            telemetry,
            committed: HashMap::new(),
            open: HashMap::new(),
            calls: HashMap::new(),
        }
    }

    /// Notes what `request` means to the run's sets; the id of the
    /// `framework.broadcast` span the call of `effect` runs under, when it
    /// is a set's.
    #[expect(
        clippy::wildcard_enum_match_arm,
        reason = "a set's span reads a set's calls and its leaving alone, whatever else a run asks"
    )]
    pub(super) fn requested(&mut self, effect: EffectId, request: &Request) -> Option<String> {
        match request {
            Request::Channel(ChannelRequest::OnEvent { event, .. }) => {
                self.called(effect, event, Call::OnEvent);
            }
            Request::Channel(ChannelRequest::Deliver { event, .. }) => {
                self.called(effect, event, Call::Deliver);
            }
            Request::Hold(HoldRequest::LeaveLane { event_id, .. }) => self.left(event_id),
            _ => return None,
        }
        let (event_id, _) = self.calls.get(&effect)?;
        Some(self.open.get(event_id)?.span.id().to_owned())
    }

    /// Remembers `event`, which the run commits, for its set's span.
    pub(super) fn committing(&mut self, event: &RoomEvent) {
        self.committed.insert(
            event.id.clone(),
            (
                event.room_id.to_string(),
                event.source.channel_id.to_string(),
                voice_session(&event.metadata),
            ),
        );
    }

    fn called(&mut self, effect: EffectId, event: &RoomEvent, call: Call) {
        let (room, source, session) = (
            event.room_id.to_string(),
            event.source.channel_id.to_string(),
            voice_session(&event.metadata),
        );
        self.opened(&event.id, room, source, session);
        if matches!(call, Call::OnEvent)
            && let Some(broadcast) = self.open.get_mut(&event.id)
        {
            broadcast.targets += 1;
        }
        self.calls.insert(effect, (event.id.clone(), call));
    }

    fn opened(
        &mut self,
        event_id: &EventId,
        room: String,
        source: String,
        session: Option<String>,
    ) {
        if self.open.contains_key(event_id) {
            return;
        }
        let mut attributes = Metadata::new();
        attributes.insert(Attr::CHANNEL_ID.to_owned(), Value::String(source));
        let span = OpenSpan::start(
            &self.telemetry,
            SpanKind::Broadcast,
            "framework.broadcast",
            StartSpanOptions {
                room_id: Some(room),
                session_id: session,
                attributes,
                ..StartSpanOptions::default()
            },
        );
        self.open.insert(
            event_id.clone(),
            Broadcast {
                span,
                targets: 0,
                delivered: 0,
                failed: 0,
            },
        );
    }

    /// Counts the answer to the call of `effect`, if it is a set's.
    pub(super) fn answered(&mut self, effect: EffectId, outcome: &Outcome) {
        let Some((event_id, call)) = self.calls.remove(&effect) else {
            return;
        };
        let Some(broadcast) = self.open.get_mut(&event_id) else {
            return;
        };
        let failed = matches!(outcome, Outcome::Failed(_)) || is_refused(outcome);
        match (call, failed) {
            (_, true) => broadcast.failed += 1,
            (Call::Deliver, false) => broadcast.delivered += 1,
            (Call::OnEvent, false) => {}
        }
    }

    /// The set of `event_id` is over: ends its span, opening one for a set
    /// that had no target, as Python's reports `target_count` 0.
    fn left(&mut self, event_id: &EventId) {
        let known = self.committed.remove(event_id);
        if !self.open.contains_key(event_id) {
            let Some((room, source, session)) = known else {
                return;
            };
            self.opened(event_id, room, source, session);
        }
        if let Some(broadcast) = self.open.remove(event_id) {
            let mut attributes = Metadata::new();
            attributes.insert("target_count".to_owned(), Value::from(broadcast.targets));
            if broadcast.targets > 0 {
                attributes.insert(
                    "delivered_count".to_owned(),
                    Value::from(broadcast.delivered),
                );
                attributes.insert("failed_count".to_owned(), Value::from(broadcast.failed));
            }
            broadcast.span.end(attributes);
        }
    }
}

/// Whether `outcome` is a provider's refusal or a channel's raise.
fn is_refused(outcome: &Outcome) -> bool {
    matches!(
        outcome,
        Outcome::Channel(ChannelOutcome::ProviderDelivery(_) | ChannelOutcome::Raised(_))
    )
}
