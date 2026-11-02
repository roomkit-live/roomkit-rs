//! The answers of turns the caller asked itself, on an event it committed
//! outside the pipeline (RFC §23.3 step 5): a delegated turn's, in a child
//! room a transport is shared into. The Python implementation's
//! `_deliver_answer`, which commits the responses as a room's
//! (`_commit_responses`) and reads the streams by the room's reader
//! (`_finish_cascade`).
//!
//! The call's cascade starts where a delivery set's would end (RFC §10.1
//! step 14): each buffered response re-enters as a pass, through
//! `BEFORE_BROADCAST` and its source's right to write, and is delivered
//! from the room's lane; each stream is read once those are over. The
//! event they answer has no pass of the call's: it is committed already,
//! so the call is decided from its start, and what fails after is its
//! result's error (RFC §13.6).

use super::delivery::{DeliverySet, Placement, Response, SetKind, StreamedTurn};
use super::queues::Lineage;
use super::{Machine, Stage};
use crate::model::{ChannelBinding, RoomContext, RoomEvent};
use crate::ops::Cx;
use crate::ops::protocol::EffectId;
use crate::rules::permissions::DeliveryPlan;

/// What turns the caller asked answered, to commit as a room's responses
/// (RFC §23.3 step 5).
#[derive(Clone, Debug, PartialEq)]
pub struct TurnResponses {
    /// The event the turns answer, as committed.
    pub event: RoomEvent,
    /// The room as the turns were asked: its bindings scope where the
    /// answers may go (RFC §8.5).
    pub context: RoomContext,
    /// Each buffered turn's responder and its response events, in the order
    /// the turns were asked.
    pub buffered: Vec<(ChannelBinding, Vec<RoomEvent>)>,
    /// Each streamed turn's `OnEvent` effect, which the driver keeps the
    /// stream by, and its responder, in the order the turns were asked.
    pub streamed: Vec<(EffectId, ChannelBinding)>,
}

impl Machine {
    /// Queues the responses as the delivery set of their event would have
    /// (RFC §10.1 step 14): each buffered one as a pass, each stream to be
    /// read once the passes and their sets are over. The event's own
    /// turns' streams are the call's, as a trigger's are: they draw no pass
    /// of the budget (RFC §8.3).
    pub(super) fn begin_answering(&mut self, cx: &mut Cx<'_>, responses: TurnResponses) {
        let TurnResponses {
            event,
            context,
            buffered,
            streamed,
        } = responses;
        self.call.room_id = Some(event.room_id.clone());
        self.trigger.decided = true;
        self.stage.enter(Stage::Cascade);
        // The set the caller ran itself, standing for the event's: what its
        // responses answer, within which bindings, in the call's cascade.
        let asked = DeliverySet::new(
            SetKind::Root,
            event,
            DeliveryPlan::default(),
            Box::new(context),
            Placement::Missing,
            self.cascade.budget.clone(),
            Lineage::Call,
        );
        for (responder, events) in buffered {
            self.queue_response(cx, Response { responder, events }, &asked);
        }
        let streams = streamed
            .into_iter()
            .map(|(turn, responder)| StreamedTurn { responder, turn })
            .collect();
        self.queue_streams(streams, &asked);
    }
}
