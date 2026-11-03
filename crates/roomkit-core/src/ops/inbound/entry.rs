//! How a call enters its room: a message's steps 1 to 4 (RFC §10.1), or a
//! direct injection's way (RFC §10.5, `direct`), then its off-lock check or
//! its pass.

use super::arrival::{self, Admitted, Arrival};
use super::deadline::{self, Stopped};
use super::identity::Identified;
use super::{Call, InboundTarget, Machine, MessageAnswers, Stage, result};
use crate::model::InboundMessage;
use crate::ops::io::Bound;
use crate::ops::protocol::{EffectId, Outcome};
use crate::ops::{Cx, Progress};

impl Machine {
    /// Sets the call's deadline (RFC §13.6), then starts the way of its
    /// message or its directly injected event to the room.
    pub(super) fn begin(&mut self, cx: &mut Cx<'_>) {
        let Stage::Start(call) = self.stage.take() else {
            return;
        };
        self.call.bound = Bound::Call(deadline::of_call(cx.config, cx.env.now()));
        match *call {
            Call::Message {
                target,
                organization_id,
                message,
                answers,
            } => self.begin_message(cx, target, organization_id, *message, answers.map(|a| *a)),
            Call::Direct {
                room_id,
                organization_id,
                event,
            } => self.begin_direct(cx, room_id, organization_id, *event),
            Call::Post {
                room_id,
                organization_id,
                post,
            } => self.begin_post(cx, room_id, organization_id, *post),
            Call::Responses { responses } => self.begin_answering(cx, *responses),
            Call::Rerun {
                room_id,
                event_id,
                floor,
            } => self.begin_rerun(cx, room_id, event_id, floor),
            Call::Greeting {
                room_id,
                agent_id,
                text,
            } => self.begin_greeting(cx, room_id, agent_id, text),
        }
    }

    /// Admits the message before anything is read (`arrival::admit`), then
    /// starts its arrival. A message a delivery's code sends into the room
    /// it delivers in, by name, is charged on the cascade that delivers
    /// first thing, as an adopted one is; a routed one, or one into another
    /// room, once step 2 knows its final room (gap G-070).
    pub(super) fn begin_message(
        &mut self,
        cx: &mut Cx<'_>,
        target: InboundTarget,
        organization_id: Option<String>,
        message: InboundMessage,
        answers: Option<MessageAnswers>,
    ) {
        self.call.channel_id = Some(message.channel_id.clone());
        let draw = self.cascade.drawn_on();
        let (channel, message) = match arrival::admit(cx.config, message, draw) {
            Ok(Admitted::Message { channel, message }) => (*channel, *message),
            Ok(Admitted::Refused(refused)) => return self.end(cx, Ok(*refused)),
            Err(error) => return self.end(cx, Err(error)),
        };
        let charge = self.cascade.take_from_delivery();
        let mut arrival = Box::new(
            Arrival::new(target, organization_id, channel, message, self.call.bound)
                .answered(answers)
                .charging(charge),
        );
        arrival.start(cx);
        self.stage.enter(Stage::Arriving(arrival));
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
            self.stage.enter(Stage::Arriving(arrival));
            return;
        };
        match arrived {
            Ok(arrived) => {
                self.call.room_id = Some(arrived.event.room_id.clone());
                if let Some(draw) = arrival.drawn_on() {
                    // Into the delivering room, or a room it opened: its
                    // cascade draws on that delivery's budget too (gap
                    // G-070).
                    self.cascade.draw_on(draw.clone());
                }
                match arrival.take_identified() {
                    Identified::Held(held) => self.hold_for_identity(cx, held, &arrived.event),
                    Identified::Proceed(participation) => {
                        self.call.participation = participation;
                        self.after_arrival(cx, arrived);
                    }
                }
            }
            Err(stopped) => {
                self.call.room_id = arrival.room().cloned();
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
            Stopped::PastBudget => self.end(cx, Ok(result::past_the_budget())),
            Stopped::Refused(refused) => self.end(cx, Ok(*refused)),
        }
    }
}
