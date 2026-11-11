//! Select a discussion's regeneration under the room lock (RFC §13.6,
//! §19.7.5 rule 7). Queuing is separate work after this operation ends:
//! Python's `_door_runs.committed()` no longer lets close cut that work.

use std::sync::Arc;

use super::{InboundError, InboundResult, cascade, deadline, report};
use crate::model::{ErrorRecord, EventId, Room, RoomContext, RoomEvent, RoomId};
use crate::ops::io::{Bound, Io};
use crate::ops::protocol::{EffectId, Operation, Outcome, ProtocolError, Step};
use crate::ops::section::{Failure, Section, SectionWork};
use crate::ops::{
    Abort, Cx, Environment, FrameworkEvent, KitConfig, Notice, Progress, RefusedOperation,
};
use crate::rules::regeneration;

/// The choice made under the lock, before a discussion queues any turn.
#[derive(Clone, Debug, PartialEq)]
pub enum RegenerationSelection {
    /// The room holds no message its agents may answer again.
    Nothing,
    /// The room closed, the named trigger moved, or selection timed out.
    Refused(Box<InboundResult>),
    /// The trigger and history as selection read them.
    Selected {
        /// The stored message, unchanged.
        trigger: Box<RoomEvent>,
        /// The room's context under its selection lock.
        context: Box<RoomContext>,
        /// A failed lock release does not undo selection (RFC §13.5).
        release_error: Option<ErrorRecord>,
    },
}

/// Selects a discussion's regeneration within `process_timeout`, applying
/// due room timers and checking status before the caller's named trigger.
/// Returns before queuing: the caller retains its original discussion and
/// performs that work outside the selection's shutdown lifetime.
#[derive(Debug)]
pub struct SelectRegeneration {
    config: Arc<KitConfig>,
    io: Io,
    room_id: RoomId,
    trigger_id: Option<EventId>,
    section: Option<Section<Selecting>>,
}

impl SelectRegeneration {
    /// Select the newest answerable message, only `trigger_id` when named.
    pub fn new(config: Arc<KitConfig>, room_id: RoomId, trigger_id: Option<EventId>) -> Self {
        Self {
            config,
            io: Io::new(),
            room_id,
            trigger_id,
            section: None,
        }
    }
}

impl Operation for SelectRegeneration {
    type Output = Result<RegenerationSelection, InboundError>;

    fn poll(&mut self, environment: &dyn Environment) -> Step<Self::Output> {
        let mut cx = Cx {
            config: &self.config,
            env: environment,
            io: &mut self.io,
        };
        let section = self.section.get_or_insert_with(|| {
            let bound = Bound::Call(deadline::of_call(&self.config, environment.now()));
            Section::new(self.room_id.clone(), Selecting(self.trigger_id.clone()))
                .waiting_within(bound)
                .reading_within(bound)
        });
        section.start(&mut cx);
        let mut result = None;
        while let Some((effect, outcome)) = cx.io.next_outcome() {
            if section.locking() && outcome == Outcome::DeadlinePassed {
                let selected = refused("process_timeout");
                selected.report_timeout(&mut cx, &self.room_id);
                result = Some(Ok(selected));
                continue;
            }
            if let Progress::Done(ended) = section.accept(&mut cx, effect, outcome) {
                let selected = ended
                    .result
                    .map_err(|error| cascade::failure(error, Some(self.room_id.clone())));
                result = Some(selected.map(|mut selected| {
                    selected.released(ended.release_error);
                    selected.report_timeout(&mut cx, &self.room_id);
                    selected
                }));
            }
        }
        self.io.step(result)
    }

    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.io.complete(id, outcome)
    }

    fn expire(&mut self) -> Vec<EffectId> {
        if let Some(section) = &mut self.section {
            section.cancel_after_timers();
        }
        self.io.expire()
    }
}

impl RegenerationSelection {
    fn released(&mut self, error: Option<ErrorRecord>) {
        match self {
            Self::Selected { release_error, .. } => *release_error = error,
            Self::Refused(result) => result.error = error,
            Self::Nothing => {}
        }
    }

    fn report_timeout(&self, cx: &mut Cx<'_>, room_id: &RoomId) {
        if let Self::Refused(result) = self
            && result.reason.as_deref() == Some("process_timeout")
            && !cx.io.is_expired()
        {
            let timeout = cx
                .config
                .process_timeout
                .map_or(0.0, |value| value.as_secs_f64());
            let mut event =
                FrameworkEvent::process_timeout(Some(room_id.clone()), None, None, timeout);
            if let FrameworkEvent::ProcessTimeout { operation, .. } = &mut event {
                *operation = Some(RefusedOperation::Regenerate);
            }
            cx.io.notify(Notice::FrameworkEvent(event));
        }
    }
}

/// A selection's result before the lock is released. Shared with ordinary
/// regeneration so both paths keep the same refusal priority.
pub(super) fn select(
    cx: &mut Cx<'_>,
    context: Box<RoomContext>,
    trigger_id: Option<&EventId>,
) -> RegenerationSelection {
    let trigger = regeneration::target(&context).cloned();
    if !context.room.status.accepts_events() {
        report::regeneration_refused(cx, &context.room.id, trigger.as_ref(), context.room.status);
        return refused("room_closed");
    }
    if let Some(id) = trigger_id
        && trigger.as_ref().map(|event| &event.id) != Some(id)
    {
        tracing::info!(target: "roomkit::core::inbound", room_id = %context.room.id, event_id = %id,
            "regeneration refused: its trigger is no longer the room's choice");
        return refused("trigger_moved");
    }
    match trigger {
        Some(trigger) => RegenerationSelection::Selected {
            trigger: Box::new(trigger),
            context,
            release_error: None,
        },
        None => RegenerationSelection::Nothing,
    }
}

fn refused(reason: &str) -> RegenerationSelection {
    RegenerationSelection::Refused(Box::new(InboundResult {
        blocked: true,
        reason: Some(reason.to_owned()),
        ..InboundResult::default()
    }))
}

#[derive(Debug)]
struct Selecting(Option<EventId>);

impl SectionWork for Selecting {
    type Output = RegenerationSelection;
    type Error = Abort;

    fn failure(failure: Failure) -> Abort {
        match failure {
            Failure::Store(error) => Abort::Store(error),
            Failure::Lock(error) => Abort::Lock(error),
        }
    }
    fn reads_history(&self) -> bool {
        true
    }
    fn missing(&self, _: &RoomId) -> Result<Self::Output, Abort> {
        Err(Abort::RoomNotFound)
    }
    fn refuses(&self, _: &Room) -> Option<Result<Self::Output, Abort>> {
        None
    }
    fn begin(
        &mut self,
        cx: &mut Cx<'_>,
        context: Box<RoomContext>,
    ) -> Progress<Result<Self::Output, Abort>> {
        Progress::Done(Ok(select(cx, context, self.0.as_ref())))
    }
    fn accept(
        &mut self,
        _: &mut Cx<'_>,
        _: EffectId,
        _: Outcome,
    ) -> Progress<Result<Self::Output, Abort>> {
        Progress::Waiting
    }
    fn awaits(&self, _: EffectId) -> bool {
        false
    }
    fn report(&self, _: &mut Cx<'_>, _: &Result<Self::Output, Abort>) {}
    fn read_expired(&self) -> Option<Result<Self::Output, Abort>> {
        Some(Ok(refused("process_timeout")))
    }
}
