//! How a call enters its room: a message's steps 1 to 4 (RFC §10.1), or a
//! direct injection's way (RFC §10.5, `direct`), then its off-lock check or
//! its pass.

use super::arrival::{self, Admitted, Arrival};
use super::deadline::{self, Stopped};
use super::{Call, InboundTarget, Machine, Stage};
use crate::model::InboundMessage;
use crate::ops::protocol::{EffectId, Outcome};
use crate::ops::{Cx, Progress};

impl Machine {
    /// Sets the call's deadline (RFC §13.6), then starts the way of its
    /// message or its directly injected event to the room.
    pub(super) fn begin(&mut self, cx: &mut Cx<'_>) {
        let Stage::Start(call) = std::mem::replace(&mut self.stage, Stage::Done) else {
            return;
        };
        self.deadline = deadline::of_call(cx.config, cx.env.now());
        match *call {
            Call::Message {
                target,
                organization_id,
                message,
            } => self.begin_message(cx, target, organization_id, *message),
            Call::Direct {
                room_id,
                organization_id,
                event,
            } => self.begin_direct(cx, room_id, organization_id, *event),
        }
    }

    /// Admits the message before anything is read (`arrival::admit`), then
    /// starts its arrival. A message a delivery's code sends is charged on
    /// the cascade that delivers before its parse, as an adopted one is
    /// (gap G-070).
    pub(super) fn begin_message(
        &mut self,
        cx: &mut Cx<'_>,
        target: InboundTarget,
        organization_id: Option<String>,
        message: InboundMessage,
    ) {
        self.channel_id = Some(message.channel_id.clone());
        self.answers_within.clone_from(&message.response_visibility);
        let budget = self.drawn.then_some(&self.budget);
        let (channel, message) = match arrival::admit(cx.config, message, budget) {
            Ok(Admitted::Message { channel, message }) => (*channel, *message),
            Ok(Admitted::Refused(refused)) => return self.end(cx, Ok(*refused)),
            Err(error) => return self.end(cx, Err(error)),
        };
        let mut arrival = Box::new(Arrival::new(
            target,
            organization_id,
            channel,
            message,
            self.deadline,
        ));
        arrival.start(cx);
        self.stage = Stage::Arriving(arrival);
    }

    /// Takes an answer of the arrival; once it hands the event, the
    /// trigger's off-lock check or its pass starts, in the room the arrival
    /// came into.
    pub(super) fn on_arrival(
        &mut self,
        cx: &mut Cx<'_>,
        mut arrival: Box<Arrival>,
        effect: EffectId,
        outcome: Outcome,
    ) {
        let Progress::Done(arrived) = arrival.accept(cx, effect, outcome) else {
            self.stage = Stage::Arriving(arrival);
            return;
        };
        match arrived {
            Ok(arrived) => {
                self.room_id = Some(arrived.event.room_id.clone());
                self.after_arrival(cx, arrived);
            }
            Err(stopped) => {
                self.room_id = arrival.room().cloned();
                self.stop(cx, stopped);
            }
        }
    }

    /// The call stopped before its pass: it fails, or is refused once past
    /// its deadline, with nothing written.
    pub(super) fn stop(&mut self, cx: &mut Cx<'_>, stopped: Stopped) {
        match stopped {
            Stopped::Failed(error) => self.end(cx, Err(error)),
            Stopped::Expired => self.expire(cx, None),
        }
    }
}
