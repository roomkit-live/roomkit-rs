//! The spans of a run, from what its operation tells them (`SpanNote`): each
//! message's `framework.route`, as Python's `_route_to_room` opens one around
//! the way to its room; and each delivery set's `framework.broadcast`, as
//! Python's `EventRouter.execute_plan` opens one around a set. The spans of
//! an entry the run adopted hang under its caller's, not the run's.

use std::collections::HashMap;
use std::sync::Arc;

use roomkit_core::model::{ChannelId, EventId, Metadata, RoomId};
use roomkit_core::ops::{ChannelRequest, EntryId, Outcome, Request, RouteEnd, SpanNote};
use serde_json::Value;

use super::holds::REENTRANT;
use crate::error::RoomKitError;
use crate::kit::end_route_span;
use crate::telemetry::{
    Attr, KitTelemetry, Late, OpenSpan, SpanKind, StartSpanOptions, current_call_goes_in,
};

/// A message's `framework.route`, until its room is known.
struct Routing {
    span: OpenSpan,
    /// Whether the call's deadline passed on the way.
    timed_out: bool,
}

/// What Python's `ReentrantCallError` reads.
const REENTRANT_CALL: &str = "ReentrantCallError";

/// What Python's `_PastTheCapError` reads.
const PAST_THE_CAP: &str = "_PastTheCapError";
/// The class Python's route span names when no room opened for a routed
/// message is still its own (RMK-1396).
const ROOM_TAKEN: &str = "_RoomTakenError";

/// What Python's `_RoutingTimeoutError` reads.
const ROUTING_TIMEOUT: &str = "_RoutingTimeoutError";

/// The call of an entry the run adopted, as its spans read it.
struct AdoptedCall {
    /// Its span when it opens late, once the entry goes in.
    late: Option<Arc<Late>>,
    /// The span it was made under.
    parent: Option<String>,
}

impl AdoptedCall {
    /// The span what the entry opens hangs under.
    fn span_id(&self) -> Option<String> {
        let late = self.late.as_ref().and_then(|late| late.id());
        late.map(str::to_owned).or_else(|| self.parent.clone())
    }
}

pub(super) struct RunSpans {
    telemetry: KitTelemetry,
    /// Each message's way to its room, by the entry it is, `None` for the
    /// call's own.
    routes: HashMap<Option<EntryId>, Routing>,
    open: HashMap<EventId, OpenSpan>,
    adopted: HashMap<EntryId, AdoptedCall>,
}

impl RunSpans {
    pub(super) fn new(telemetry: KitTelemetry) -> Self {
        Self {
            telemetry,
            routes: HashMap::new(),
            open: HashMap::new(),
            adopted: HashMap::new(),
        }
    }

    /// The id of the `framework.broadcast` span the call of `request` runs
    /// under, when it is a set's.
    #[expect(
        clippy::wildcard_enum_match_arm,
        reason = "a set's calls alone run under a set's span, whatever else a run asks"
    )]
    pub(super) fn requested(&self, request: &Request) -> Option<String> {
        match request {
            Request::Channel(
                ChannelRequest::OnEvent { event, .. } | ChannelRequest::Deliver { event, .. },
            ) => Some(self.open.get(&event.id)?.id().to_owned()),
            _ => None,
        }
    }

    /// Takes what the operation told the run's spans.
    pub(super) fn noted(&mut self, note: SpanNote) {
        match note {
            SpanNote::Injecting { entry: None } => current_call_goes_in(),
            SpanNote::Injecting { entry: Some(entry) } => {
                if let Some(late) = self.adopted.get(&entry).and_then(|call| call.late.as_ref()) {
                    late.open();
                }
            }
            SpanNote::RouteStarts {
                entry,
                channel_id,
                sender_id,
            } => self.route_starts(entry, &channel_id, &sender_id),
            SpanNote::RouteOver { entry, end } => self.route_over(entry, end),
            SpanNote::SetStarts {
                event_id,
                room_id,
                source,
                session,
                entry,
            } => self.set_starts(event_id, &room_id, &source, session, entry),
            SpanNote::SetOver {
                event_id,
                targets,
                delivered,
                failed,
                ..
            } => self.set_over(&event_id, targets, delivered, failed),
            // The caller's tail reads it (`Caller::noted`).
            SpanNote::CascadeFailed { .. } => {}
        }
    }

    /// The run adopted the entry `id`, its call's span opening late if it is
    /// `late`, made under the span `parent`.
    pub(super) fn adopted(&mut self, id: EntryId, late: Option<Arc<Late>>, parent: Option<String>) {
        self.adopted.insert(id, AdoptedCall { late, parent });
    }

    /// The answer to one of the run's calls: its deadline passed on the
    /// message's way to its room.
    pub(super) fn answered(&mut self, outcome: &Outcome) {
        if matches!(outcome, Outcome::DeadlinePassed)
            && let Some(routing) = self.routes.get_mut(&None)
        {
            routing.timed_out = true;
        }
    }

    /// The run is over with a message's way to its room still open: a wait it
    /// refused, `refused`, or its deadline ended it; any other way, its caller
    /// gone, ends the span `cancelled` as it drops.
    pub(super) fn over(&mut self, refused: Option<&RoomId>) {
        for (_, routing) in self.routes.drain() {
            if let Some(room_id) = refused {
                let error = RoomKitError::ReentrantCall {
                    room_id: room_id.clone(),
                };
                routing
                    .span
                    .fail_error(REENTRANT_CALL, error.to_string(), Metadata::new());
            } else if routing.timed_out {
                routing
                    .span
                    .fail_error(ROUTING_TIMEOUT, String::new(), Metadata::new());
            }
        }
    }

    /// The span what the entry `entry` opens hangs under, if it names one;
    /// the current span otherwise.
    fn parent_of(&self, entry: Option<EntryId>) -> Option<String> {
        self.adopted.get(&entry?)?.span_id()
    }

    fn route_starts(&mut self, entry: Option<EntryId>, channel_id: &ChannelId, sender_id: &str) {
        let parent = self.parent_of(entry);
        let span = self.telemetry.route_span(parent, channel_id, sender_id);
        let routing = Routing {
            span,
            timed_out: false,
        };
        self.routes.insert(entry, routing);
    }

    fn route_over(&mut self, entry: Option<EntryId>, end: RouteEnd) {
        let Some(routing) = self.routes.remove(&entry) else {
            return;
        };
        match end {
            RouteEnd::Room(room_id) => end_route_span(routing.span, &room_id),
            RouteEnd::Failed(error) => {
                let error = RoomKitError::from(*error);
                let class = error.class_name();
                // A wait it refused fails its call as the driver names it.
                let class = if class == REENTRANT {
                    REENTRANT_CALL.to_owned()
                } else {
                    class
                };
                routing
                    .span
                    .fail_error(class, error.to_string(), Metadata::new());
            }
            RouteEnd::PastTheBudget => {
                routing
                    .span
                    .fail_error(PAST_THE_CAP, String::new(), Metadata::new());
            }
            RouteEnd::RoomTaken => {
                routing
                    .span
                    .fail_error(ROOM_TAKEN, String::new(), Metadata::new());
            }
        }
    }

    fn set_starts(
        &mut self,
        event_id: EventId,
        room_id: &RoomId,
        source: &ChannelId,
        session: Option<String>,
        entry: Option<EntryId>,
    ) {
        let parent_id = self.parent_of(entry);
        let telemetry = &self.telemetry;
        self.open.entry(event_id).or_insert_with(|| {
            let mut attributes = Metadata::new();
            attributes.insert(
                Attr::CHANNEL_ID.to_owned(),
                Value::String(source.to_string()),
            );
            OpenSpan::start(
                telemetry,
                SpanKind::Broadcast,
                "framework.broadcast",
                StartSpanOptions {
                    parent_id,
                    room_id: Some(room_id.to_string()),
                    session_id: session,
                    attributes,
                    ..StartSpanOptions::default()
                },
            )
        });
    }

    /// The targets of `event_id`'s set are over: ends its span, `target_count`
    /// 0 alone for a set that reaches nobody, as Python's.
    fn set_over(&mut self, event_id: &EventId, targets: usize, delivered: usize, failed: usize) {
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
