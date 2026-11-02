//! An operation run inside another, its effects renamed with the outer
//! operation's ids so that the two never share one. The entries it adopts
//! keep its own ids: an outer operation that nests one which adopts
//! entries adopts none itself. An inner decision is not the outer
//! operation's, and is dropped.
//!
//! The driver keeps a turn's stream, and stops a read, by the outer ids:
//! a `ReadStream` of a turn the inner operation asked, and a `StopReading`
//! of a read it issued, are renamed too (RMK-1185). A turn the inner
//! operation is handed keeps the id it is handed under, which the inner
//! operation never issues ([`Inbound::answering`](crate::ops::inbound::Inbound::answering)).

use std::collections::BTreeMap;

use crate::ops::Environment;
use crate::ops::protocol::{
    ChannelOutcome, ChannelRequest, Delivered, Effect, EffectId, Entry, EntryId, Next, Notice,
    Operation, Outcome, ProtocolError, Request, Settled,
};

/// An inner operation and the outer ids of its outstanding effects.
#[derive(Debug)]
pub(super) struct Nested<O> {
    op: O,
    /// The inner id of each outstanding effect, by its outer id.
    ids: BTreeMap<EffectId, EffectId>,
    /// The outer id of each of the inner operation's turns answered by a
    /// stream not read yet, by its inner id.
    streams: BTreeMap<EffectId, EffectId>,
    /// What the inner operation settled and delivered of the entries it
    /// adopted, not taken yet.
    settled: Vec<Settled>,
    delivered: Vec<Delivered>,
}

impl<O: Operation> Nested<O> {
    pub(super) fn new(op: O) -> Self {
        Self {
            op,
            ids: BTreeMap::new(),
            streams: BTreeMap::new(),
            settled: Vec::new(),
            delivered: Vec::new(),
        }
    }

    /// Polls the inner operation: its effects, renamed with ids taken from
    /// `next`, go to `effects`, its notices to `notices`; its output once
    /// it is over.
    pub(super) fn poll(
        &mut self,
        environment: &dyn Environment,
        next: &mut EffectId,
        effects: &mut Vec<Effect>,
        notices: &mut Vec<Notice>,
    ) -> Option<O::Output> {
        let step = self.op.poll(environment);
        self.settled.extend(step.settled);
        self.delivered.extend(step.delivered);
        let output = match step.next {
            Next::Effects(own) => {
                for mut effect in own {
                    let outer = *next;
                    *next = outer.next();
                    self.ids.insert(outer, effect.id);
                    effect.id = outer;
                    self.rename_the_read_turn(&mut effect.request);
                    effects.push(effect);
                }
                None
            }
            Next::Waiting => None,
            Next::Ready(output) => Some(output),
        };
        notices.extend(
            step.notices
                .into_iter()
                .filter_map(|notice| self.renamed(notice)),
        );
        output
    }

    /// A `ReadStream` of one of the inner operation's turns names that
    /// turn by its outer id, which the driver keeps its stream under.
    fn rename_the_read_turn(&mut self, request: &mut Request) {
        if let Request::Channel(ChannelRequest::ReadStream { turn, .. }) = request
            && let Some(outer) = self.streams.remove(turn)
        {
            *turn = outer;
        }
    }

    /// `notice` as the driver reads it: a `StopReading` names the read by
    /// its outer id, and is dropped once that read is answered.
    fn renamed(&self, notice: Notice) -> Option<Notice> {
        let Notice::StopReading { effect } = notice else {
            return Some(notice);
        };
        self.ids
            .iter()
            .find_map(|(outer, inner)| (*inner == effect).then_some(*outer))
            .map(|outer| Notice::StopReading { effect: outer })
    }

    /// Whether `id` names one of the inner operation's effects.
    pub(super) fn owns(&self, id: EffectId) -> bool {
        self.ids.contains_key(&id)
    }

    /// What the inner operation settled and delivered of its adopted
    /// entries since this was last taken, for the outer step.
    pub(super) fn take_entries(&mut self) -> (Vec<Settled>, Vec<Delivered>) {
        (
            std::mem::take(&mut self.settled),
            std::mem::take(&mut self.delivered),
        )
    }

    /// Hands the inner operation a call the code its effect `origin` runs
    /// made into its room; `entry` back when `origin` is not its own, or
    /// when it does not adopt it.
    pub(super) fn adopt(&mut self, entry: Entry, origin: EffectId) -> Result<EntryId, Box<Entry>> {
        match self.ids.get(&origin) {
            Some(inner) => self.op.adopt(entry, *inner),
            None => Err(Box::new(entry)),
        }
    }

    /// Hands the inner operation the answer to its effect `id`; a turn
    /// answered by a stream is kept by its inner id until its read is
    /// issued.
    pub(super) fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        let inner = self
            .ids
            .remove(&id)
            .ok_or(ProtocolError::UnknownEffect(id))?;
        if matches!(outcome, Outcome::Channel(ChannelOutcome::Streamed(_))) {
            self.streams.insert(inner, id);
        }
        self.op.complete(inner, outcome)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::model::{
        ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelType, EventId,
        EventSource, EventType, Room, RoomContext, RoomEvent, RoomId, TextContent, Timestamp,
    };
    use crate::ops::SequentialEnvironment;
    use crate::ops::protocol::Step;

    /// The turn the inner operation is handed, issued by the outer one.
    fn handed() -> EffectId {
        EffectId::first()
    }

    fn agent() -> ChannelBinding {
        ChannelBinding::at(
            ChannelId::new("ai").unwrap(),
            RoomId::new("room_1").unwrap(),
            ChannelType::AI,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
            Timestamp::UNIX_EPOCH,
        )
    }

    fn task() -> RoomEvent {
        RoomEvent::at(
            EventId::new("evt_1").unwrap(),
            RoomId::new("room_1").unwrap(),
            EventType::Message,
            EventSource::new(ChannelId::new("system").unwrap(), ChannelType::SYSTEM),
            TextContent::new("Find it."),
            Timestamp::UNIX_EPOCH,
        )
    }

    fn read(turn: EffectId) -> Request {
        ChannelRequest::ReadStream {
            turn,
            responder: agent(),
            answers: Box::new(task()),
            target: None,
            correlation_id: "cor_1".to_owned(),
        }
        .into()
    }

    fn effect(id: EffectId, request: Request) -> Effect {
        Effect {
            id,
            request,
            deadline: None,
            locked: None,
            admitted: None,
            budget: None,
            decides: None,
            answers: None,
        }
    }

    /// An inner operation numbered past the turn it is handed, as
    /// `Inbound::answering` is: it asks a turn of its own, reads that
    /// turn's stream and the handed one's, then stops its own read.
    #[derive(Debug, Default)]
    struct Reader {
        polls: u8,
    }

    impl Operation for Reader {
        type Output = ();

        fn poll(&mut self, _environment: &dyn Environment) -> Step<()> {
            self.polls += 1;
            let own_turn = handed().next();
            let own_read = own_turn.next();
            let (notices, next) = match self.polls {
                1 => {
                    let turn = ChannelRequest::OnEvent {
                        binding: agent(),
                        event: task(),
                        context: Box::new(RoomContext::new(Room::at(
                            RoomId::new("room_1").unwrap(),
                            Timestamp::UNIX_EPOCH,
                        ))),
                    };
                    (
                        Vec::new(),
                        Next::Effects(vec![effect(own_turn, turn.into())]),
                    )
                }
                2 => (
                    Vec::new(),
                    Next::Effects(vec![
                        effect(own_read, read(own_turn)),
                        effect(own_read.next(), read(handed())),
                    ]),
                ),
                _ => (
                    vec![Notice::StopReading { effect: own_read }],
                    Next::Waiting,
                ),
            };
            Step {
                notices,
                settled: Vec::new(),
                delivered: Vec::new(),
                decided: None,
                next,
            }
        }

        fn complete(&mut self, _id: EffectId, _outcome: Outcome) -> Result<(), ProtocolError> {
            Ok(())
        }
    }

    /// The outer ids of a run of `Reader` nested past the handed turn,
    /// its turn answered by a stream: what the driver is asked.
    fn run_nested() -> (Vec<Effect>, Vec<Notice>) {
        let environment = SequentialEnvironment::default();
        let mut nested = Nested::new(Reader::default());
        let mut next = handed().next();
        let (mut effects, mut notices) = (Vec::new(), Vec::new());
        nested.poll(&environment, &mut next, &mut effects, &mut notices);
        let turn = effects[0].id;
        let streamed = ChannelOutcome::Streamed(Box::default()).into();
        nested.complete(turn, streamed).unwrap();
        nested.poll(&environment, &mut next, &mut effects, &mut notices);
        nested.poll(&environment, &mut next, &mut effects, &mut notices);
        (effects, notices)
    }

    fn read_turn(effect: &Effect) -> EffectId {
        let Request::Channel(ChannelRequest::ReadStream { turn, .. }) = &effect.request else {
            panic!("not a read: {:?}", effect.request);
        };
        *turn
    }

    // RMK-1185: Python reads every stream its cascade starts
    // (core/mixins/lane_execution.py _finish_cascade).
    #[test]
    fn a_read_of_an_inner_turn_names_the_turn_the_driver_keeps_its_stream_by() {
        let (effects, _) = run_nested();
        assert_eq!(read_turn(&effects[1]), effects[0].id);
    }

    #[test]
    fn a_read_of_a_handed_turn_keeps_the_id_it_was_handed_under() {
        let (effects, _) = run_nested();
        assert_eq!(read_turn(&effects[2]), handed());
    }

    #[test]
    fn a_stopped_read_is_named_by_its_outer_id() {
        let (effects, notices) = run_nested();
        assert_eq!(
            notices,
            [Notice::StopReading {
                effect: effects[1].id
            }]
        );
    }
}
