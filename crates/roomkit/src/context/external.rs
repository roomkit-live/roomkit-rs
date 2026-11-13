//! An external agent's callbacks inherit their caller's depth; they do
//! not run the local AI loop that adds the response's depth.

use roomkit_core::model::RoomId;

use super::ExecutionContext;

impl ExecutionContext {
    /// Keep the stream's live admission and cascade budget, removing only
    /// its own response-depth contribution. Outer turns and hook floors
    /// still constrain calls made by an external agent's observers.
    pub(crate) fn external_agent(mut self, room_id: &RoomId) -> Self {
        let mut frames = self.frames.to_vec();
        if let Some(frame) = frames
            .iter_mut()
            .rev()
            .find(|frame| frame.runs() && frame.work.delivering.as_ref() == Some(room_id))
        {
            frame.work.response_depth = None;
        }
        self.frames = frames.into();
        self
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use roomkit_core::ops::{CheckTimers, KitConfig, Next, Operation, SequentialEnvironment};
    use roomkit_core::rules::chain_depth::{Draw, ReentryBudget};

    use super::super::{Frame, RunShared, Work};
    use super::*;

    // Python: tests/test_channels/test_acp.py::TestACPChannel::
    // test_an_observer_posting_per_tool_call_the_agent_answers_stops.
    // Removing ACP's own floor must neither clear an inherited turn nor
    // detach its observer from the live delivery that bounds its posts.
    #[test]
    fn an_external_agent_keeps_outer_depth_and_the_live_delivery_budget() {
        let room = RoomId::new("room").unwrap();
        let mut check = CheckTimers::new(Arc::new(KitConfig::new()), room.clone(), None);
        let Next::Effects(effects) = check.poll(&SequentialEnvironment::default()).next else {
            panic!("a timer check asks for its lock");
        };
        let effect = effects[0].id;
        let (outer, _outer_queue) = RunShared::new();
        let (own, _own_queue) = RunShared::new();
        outer.calling(effect);
        own.calling(effect);
        let budget = ReentryBudget::new(2);
        let context = ExecutionContext::default()
            .within(Frame::new(
                &outer,
                effect,
                Work {
                    response_depth: Some(4),
                    delivering: Some(room.clone()),
                    ..Work::default()
                },
            ))
            .within(Frame::new(
                &own,
                effect,
                Work {
                    response_depth: Some(7),
                    delivering: Some(room.clone()),
                    admitted: Some(room.clone()),
                    budget: Some(budget.clone()),
                    ..Work::default()
                },
            ));
        let external = context.external_agent(&room);
        assert_eq!(external.turn_depth(), Some(4));
        assert_eq!(external.budget(&room), Some(Draw::Pass(budget)));
        assert!(external.is_admitted_to(&room));
        assert_eq!(external.clone().at_least(6).turn_depth(), Some(6));
        own.answered(effect);
        assert!(external.budget(&room).is_none());
        assert!(!external.is_admitted_to(&room));
        assert_eq!(external.turn_depth(), Some(4));
        outer.answered(effect);
        assert_eq!(external.turn_depth(), None);
    }
}
