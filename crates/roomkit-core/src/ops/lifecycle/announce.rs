//! The announcement of a room's creation its caller set up first: a room
//! created with a strategy (RFC §19.7), whose `ON_ROOM_CREATED` hooks run
//! once the strategy is installed.

use std::sync::Arc;

use super::LifecycleError;
use super::report::{self, Reported};
use crate::model::RoomId;
use crate::ops::io::Io;
use crate::ops::protocol::{EffectId, Operation, Outcome, ProtocolError, Step, StoreRequest};
use crate::ops::{Cx, Environment, KitConfig};

/// Announces the creation of a room [`CreateRoom::unannounced`] wrote: the
/// room is read as it stands now, with its bindings, then its
/// `ON_ROOM_CREATED` hooks run, awaited, and `room_created` is emitted once
/// they are over, as [`CreateRoom`] announces its own. What a hook raises
/// is the driver's to log, and a driver that cannot run them answers
/// `Failed`, which is logged: the room is announced all the same. Its
/// output is the read's refusal, if the room could not be read.
///
/// [`CreateRoom`]: super::CreateRoom
/// [`CreateRoom::unannounced`]: super::CreateRoom::unannounced
#[derive(Debug)]
pub struct AnnounceRoomCreated {
    config: Arc<KitConfig>,
    io: Io,
    room_id: RoomId,
    loading: Option<EffectId>,
    observing: Option<EffectId>,
    started: bool,
}

impl AnnounceRoomCreated {
    /// The announcement of the creation of `room_id`.
    pub fn new(config: Arc<KitConfig>, room_id: RoomId) -> Self {
        Self {
            config,
            io: Io::new(),
            room_id,
            loading: None,
            observing: None,
            started: false,
        }
    }

    /// The room is read: its hooks run, awaited, its creation announced
    /// once they are over; the output at once when none observes it.
    fn on_loaded(
        &mut self,
        environment: &dyn Environment,
        outcome: Outcome,
    ) -> Option<Result<(), LifecycleError>> {
        let context = match outcome.into_context() {
            Ok(Some(context)) => context,
            Ok(None) => return Some(Err(LifecycleError::RoomNotFound(self.room_id.clone()))),
            Err(error) => return Some(Err(LifecycleError::Store(error))),
        };
        let mut cx = self.cx(environment);
        self.observing = report::report_awaited(&mut cx, Reported::RoomCreated, &context);
        self.observing.is_none().then_some(Ok(()))
    }

    /// The `ON_ROOM_CREATED` hooks are over, what they raised the driver's
    /// to log: `room_created` is emitted.
    fn on_observed(&mut self, environment: &dyn Environment, outcome: Outcome) {
        if let Err(error) = outcome.into_observed() {
            tracing::warn!(
                target: "roomkit::core::lifecycle",
                room_id = %self.room_id,
                error_type = %error.error_type,
                message = %error.message,
                "the ON_ROOM_CREATED hooks could not be run; the room is created all the same"
            );
        }
        let room_id = self.room_id.clone();
        let mut cx = self.cx(environment);
        report::announce(&mut cx, Reported::RoomCreated, &room_id);
    }

    fn cx<'cx>(&'cx mut self, environment: &'cx dyn Environment) -> Cx<'cx> {
        Cx {
            config: &self.config,
            env: environment,
            io: &mut self.io,
        }
    }
}

impl Operation for AnnounceRoomCreated {
    type Output = Result<(), LifecycleError>;

    fn poll(&mut self, environment: &dyn Environment) -> Step<Self::Output> {
        if !self.started {
            self.started = true;
            let request = StoreRequest::LoadContext {
                room_id: self.room_id.clone(),
                recent_events: self.config.recent_events(),
            };
            self.loading = Some(self.io.issue(request));
        }
        let mut result = None;
        while let Some((effect, outcome)) = self.io.next_outcome() {
            if self.loading == Some(effect) {
                self.loading = None;
                result = self.on_loaded(environment, outcome);
            } else if self.observing == Some(effect) {
                self.observing = None;
                self.on_observed(environment, outcome);
                result = Some(Ok(()));
            }
        }
        self.io.step(result)
    }

    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.io.complete(id, outcome)
    }
}
