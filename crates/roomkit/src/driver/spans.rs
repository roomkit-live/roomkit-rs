//! The spans of a run: its message's `framework.route`, from the router's
//! call to the room step 2 hands the channel, as Python's `_route_to_room`
//! opens one around `_route_and_claim`; and each delivery set's
//! `framework.broadcast`, from its first target's call to its last target's
//! end, as Python's `EventRouter.execute_plan` opens one around a set.

use std::collections::HashMap;
use std::sync::Arc;

use roomkit_core::model::{EventId, Metadata, RoomEvent, RoomId};
use roomkit_core::ops::{ChannelRequest, EffectId, EntryId, Outcome, Request, SpanNote};
use roomkit_core::rules::routing::RouteQuery;
use serde_json::Value;

use crate::kit::end_route_span;
use crate::telemetry::{
    Attr, KitTelemetry, Late, OpenSpan, SpanKind, StartSpanOptions, current_call_goes_in,
};

/// A message's `framework.route`, until its room is known.
struct Routing {
    span: OpenSpan,
    /// The router's call under way, which fails the message when it fails.
    asked: Option<EffectId>,
    /// What failed last on the way to the room: what stopped the run, if
    /// it ends with no room.
    failure: Option<(String, String)>,
}

/// What Python's `_PastTheCapError` reads: the one way to stop short of a
/// room that nothing failed for.
const PAST_THE_CAP: &str = "_PastTheCapError";

/// What Python's `_RoutingTimeoutError` reads: the call's deadline passed
/// on the way to the room.
const ROUTING_TIMEOUT: &str = "_RoutingTimeoutError";

pub(super) struct RunSpans {
    telemetry: KitTelemetry,
    routing: Option<Routing>,
    /// The events the run committed, until their set is over: what a set's
    /// span reads of its event.
    committed: HashMap<EventId, (String, String, Option<String>)>,
    open: HashMap<EventId, OpenSpan>,
    /// The spans, opening late, of the entries the run adopted, until they
    /// go in.
    adopted: HashMap<EntryId, Arc<Late>>,
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
            routing: None,
            committed: HashMap::new(),
            open: HashMap::new(),
            adopted: HashMap::new(),
        }
    }

    /// Notes what `request` means to the run's spans; the id of the
    /// `framework.broadcast` span the call of `effect` runs under, when it
    /// is a set's.
    #[expect(
        clippy::wildcard_enum_match_arm,
        reason = "the spans read a set's calls and a message's way to its room alone, whatever \
                  else a run asks"
    )]
    pub(super) fn requested(&mut self, effect: EffectId, request: &Request) -> Option<String> {
        match request {
            Request::Channel(
                ChannelRequest::OnEvent { event, .. } | ChannelRequest::Deliver { event, .. },
            ) => Some(self.set_of(event)),
            Request::Channel(ChannelRequest::HandleInbound { context, .. }) => {
                self.routed_to(&context.room.id);
                None
            }
            Request::Route { query } => {
                self.routes(effect, query);
                None
            }
            _ => None,
        }
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

    /// Takes what the operation told the run's spans.
    pub(super) fn noted(&mut self, note: SpanNote) {
        match note {
            SpanNote::Injecting { entry: None } => current_call_goes_in(),
            SpanNote::Injecting { entry: Some(entry) } => {
                if let Some(late) = self.adopted.remove(&entry) {
                    late.open();
                }
            }
            SpanNote::SetOver {
                event_id,
                targets,
                delivered,
                failed,
                ..
            } => self.set_over(&event_id, targets, delivered, failed),
        }
    }

    /// The run adopted the entry `id`, whose call's span, if it is one that
    /// opens late, is `late`.
    pub(super) fn adopted(&mut self, id: EntryId, late: Option<Arc<Late>>) {
        if let Some(late) = late {
            self.adopted.insert(id, late);
        }
    }

    /// Reads the answer to the call of `effect`, while the message finds its
    /// room: a router that fails fails the message, a failure of any other
    /// step only once the run stops there.
    #[expect(
        clippy::wildcard_enum_match_arm,
        reason = "any other answer is no failure on the way to the room"
    )]
    pub(super) fn answered(&mut self, effect: EffectId, outcome: &Outcome) {
        let Some(routing) = &mut self.routing else {
            return;
        };
        let failure = match outcome {
            Outcome::Failed(error) => (error.error_type.clone(), error.message.clone()),
            Outcome::DeadlinePassed => (ROUTING_TIMEOUT.to_owned(), String::new()),
            _ => return,
        };
        if routing.asked != Some(effect) {
            routing.failure = Some(failure);
            return;
        }
        if let Some(routing) = self.routing.take() {
            routing
                .span
                .fail_error(failure.0, failure.1, Metadata::new());
        }
    }

    /// The run is over: a message that found no room, past its budget or
    /// past a failure, ends its span in error. A run dropped before this
    /// ends its spans `cancelled`.
    pub(super) fn over(&mut self) {
        if let Some(routing) = self.routing.take() {
            let (class, text) = routing
                .failure
                .unwrap_or_else(|| (PAST_THE_CAP.to_owned(), String::new()));
            routing.span.fail_error(class, text, Metadata::new());
        }
    }

    /// The router is asked: the message's `framework.route` opens, once
    /// however many times step 2 asks.
    fn routes(&mut self, effect: EffectId, query: &RouteQuery) {
        let routing = self.routing.get_or_insert_with(|| Routing {
            span: self
                .telemetry
                .route_span(None, &query.channel_id, &query.sender_id),
            asked: None,
            failure: None,
        });
        routing.asked = Some(effect);
    }

    /// The message's room is known, `room_id`, and handed to its channel.
    fn routed_to(&mut self, room_id: &RoomId) {
        if let Some(routing) = self.routing.take() {
            end_route_span(routing.span, Some(room_id));
        }
    }

    /// The id of the span of `event`'s set, opened with its first call.
    fn set_of(&mut self, event: &RoomEvent) -> String {
        self.opened(
            &event.id,
            event.room_id.to_string(),
            event.source.channel_id.to_string(),
            voice_session(&event.metadata),
        )
    }

    fn opened(
        &mut self,
        event_id: &EventId,
        room: String,
        source: String,
        session: Option<String>,
    ) -> String {
        let telemetry = &self.telemetry;
        let span = self.open.entry(event_id.clone()).or_insert_with(|| {
            let mut attributes = Metadata::new();
            attributes.insert(Attr::CHANNEL_ID.to_owned(), Value::String(source));
            OpenSpan::start(
                telemetry,
                SpanKind::Broadcast,
                "framework.broadcast",
                StartSpanOptions {
                    room_id: Some(room),
                    session_id: session,
                    attributes,
                    ..StartSpanOptions::default()
                },
            )
        });
        span.id().to_owned()
    }

    /// The targets of `event_id`'s set are over: ends its span, opening one
    /// for a set that called nobody, as Python's reports `target_count` 0.
    fn set_over(&mut self, event_id: &EventId, targets: usize, delivered: usize, failed: usize) {
        let known = self.committed.remove(event_id);
        if !self.open.contains_key(event_id) {
            let Some((room, source, session)) = known else {
                return;
            };
            self.opened(event_id, room, source, session);
        }
        let Some(span) = self.open.remove(event_id) else {
            return;
        };
        let mut attributes = Metadata::new();
        attributes.insert("target_count".to_owned(), Value::from(targets));
        if targets > 0 {
            attributes.insert("delivered_count".to_owned(), Value::from(delivered));
            attributes.insert("failed_count".to_owned(), Value::from(failed));
        }
        span.end(attributes);
    }
}
