//! The open human-input requests of a room owner's turns (RFC §10.6.5), as
//! the Python implementation's `OwnerInputs`.
//!
//! A request is answerable from its creation: an answer or a rejection the
//! kit is given for an id it does not know yet is held until its request
//! arrives, or for the hold bound (`input_hold_timeout`, 60 seconds by
//! default), at most [`HELD_KEPT`] at once; and a request answered once is
//! not answered again. The outcome goes to the turn's executor as an
//! `input_answer`; a request whose turn ends is withdrawn.
//!
//! A request belongs to the turn that opened it: an id already open, or
//! already settled, is not opened again, and only its turn closes it. The
//! executor, where the tool waits, settles the request first come first
//! served and closes it here; an answer given here goes to the executor,
//! which says whether it settled the request (`input_taken`), and the
//! kit's door answers that.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::time::Instant;

use super::lock;
use super::wire::{Control, InputOutcome};

/// How many settled request ids the owner remembers, refusing a second
/// answer.
pub(crate) const SETTLED_KEPT: usize = 1024;

/// How many early answers the owner holds at once, the oldest let go
/// first.
pub(crate) const HELD_KEPT: usize = 1024;

/// A turn of the owner's, as the record reads it: where its answers go.
pub(crate) trait InputTurn: Send + Sync {
    /// Sends `control` to the turn's executor: whether it went.
    fn send_control(&self, control: &Control) -> bool;
}

impl InputTurn for super::MemoryTurn {
    fn send_control(&self, control: &Control) -> bool {
        self.controls.put(control).is_ok()
    }
}

/// Whether `a` and `b` are one turn.
fn same(a: &Arc<dyn InputTurn>, b: &Arc<dyn InputTurn>) -> bool {
    std::ptr::addr_eq(Arc::as_ptr(a), Arc::as_ptr(b))
}

/// An outcome the kit was given for a request.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Given {
    request_id: String,
    outcome: InputOutcome,
    value: Option<String>,
    reason: Option<String>,
}

impl Given {
    fn control(&self) -> Control {
        Control::InputAnswer {
            request_id: self.request_id.clone(),
            outcome: self.outcome,
            value: self.value.clone(),
            reason: self.reason.clone(),
        }
    }
}

/// An answer held for a request not open yet.
struct Held {
    outcome: Given,
    until: Instant,
    verdict: oneshot::Sender<bool>,
}

#[derive(Default)]
struct Record {
    open: HashMap<String, Arc<dyn InputTurn>>,
    /// The answers held, oldest first.
    held: VecDeque<(String, Held)>,
    /// The answers sent to a request's executor, awaiting whether they
    /// settled it.
    awaiting: HashMap<String, (Arc<dyn InputTurn>, oneshot::Sender<bool>)>,
    /// The settled request ids, oldest first.
    settled: VecDeque<String>,
    settled_set: HashSet<String>,
}

impl Record {
    fn holds(&self, request_id: &str) -> bool {
        self.held.iter().any(|(id, _)| id == request_id)
    }

    fn take_held(&mut self, request_id: &str) -> Option<Held> {
        let position = self.held.iter().position(|(id, _)| id == request_id)?;
        self.held.remove(position).map(|(_, held)| held)
    }

    fn remember(&mut self, request_id: &str) {
        if self.settled_set.insert(request_id.to_owned()) {
            self.settled.push_back(request_id.to_owned());
        }
        while self.settled.len() > SETTLED_KEPT {
            if let Some(oldest) = self.settled.pop_front() {
                self.settled_set.remove(&oldest);
            }
        }
    }

    /// Lets go of the answers held past their bound: none was taken.
    fn prune(&mut self, now: Instant) {
        let (expired, kept): (VecDeque<_>, VecDeque<_>) = std::mem::take(&mut self.held)
            .into_iter()
            .partition(|(_, held)| held.until <= now);
        self.held = kept;
        for (_, held) in expired {
            let _ = held.verdict.send(false);
        }
    }

    /// Sends `outcome` to `turn`, the verdict awaited from its executor.
    fn deliver(
        &mut self,
        turn: Arc<dyn InputTurn>,
        outcome: &Given,
        verdict: oneshot::Sender<bool>,
    ) {
        self.remember(&outcome.request_id);
        if turn.send_control(&outcome.control()) {
            self.awaiting
                .insert(outcome.request_id.clone(), (turn, verdict));
        } else {
            tracing::warn!(target: "roomkit::turns", request = %outcome.request_id, "the answer to a human-input request could not reach its turn");
            let _ = verdict.send(false);
        }
    }
}

/// The open requests of the owner's turns, and the answers held for ones
/// not open yet.
#[derive(Default)]
pub(crate) struct OwnerInputs {
    record: Mutex<Record>,
}

impl std::fmt::Debug for OwnerInputs {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let record = lock(&self.record);
        formatter
            .debug_struct("OwnerInputs")
            .field("open", &record.open.len())
            .field("held", &record.held.len())
            .finish_non_exhaustive()
    }
}

impl OwnerInputs {
    /// `turn` opened the request `request_id`: an answer held for it is
    /// sent now.
    pub(crate) fn opened(&self, turn: Arc<dyn InputTurn>, request_id: &str) {
        let mut record = lock(&self.record);
        record.prune(Instant::now());
        if record.open.contains_key(request_id) || record.settled_set.contains(request_id) {
            // Another turn's, or settled: a request is opened once.
            return;
        }
        if let Some(held) = record.take_held(request_id) {
            record.deliver(turn, &held.outcome, held.verdict);
            return;
        }
        record.open.insert(request_id.to_owned(), turn);
    }

    /// `turn`'s request `request_id` settled where its tool waits.
    pub(crate) fn closed(&self, turn: &Arc<dyn InputTurn>, request_id: &str) {
        let mut record = lock(&self.record);
        if record
            .open
            .get(request_id)
            .is_some_and(|open| same(open, turn))
        {
            record.open.remove(request_id);
            record.remember(request_id);
        }
    }

    /// Whether the answer sent to `turn`'s request `request_id` settled it.
    pub(crate) fn taken(&self, turn: &Arc<dyn InputTurn>, request_id: &str, taken: bool) {
        let mut record = lock(&self.record);
        let ours = record
            .awaiting
            .get(request_id)
            .is_some_and(|(awaited, _)| same(awaited, turn));
        if let Some((_, verdict)) = ours.then(|| record.awaiting.remove(request_id)).flatten() {
            let _ = verdict.send(taken);
        }
    }

    /// `turn` is over: its open requests are withdrawn, and an answer still
    /// awaiting its verdict was not taken.
    pub(crate) fn turn_ended(&self, turn: &Arc<dyn InputTurn>) {
        let mut record = lock(&self.record);
        let withdrawn: Vec<String> = record
            .open
            .iter()
            .filter(|(_, open)| same(open, turn))
            .map(|(id, _)| id.clone())
            .collect();
        for request_id in withdrawn {
            record.open.remove(&request_id);
            record.remember(&request_id);
        }
        let unanswered: Vec<String> = record
            .awaiting
            .iter()
            .filter(|(_, (awaited, _))| same(awaited, turn))
            .map(|(id, _)| id.clone())
            .collect();
        for request_id in unanswered {
            if let Some((_, verdict)) = record.awaiting.remove(&request_id) {
                let _ = verdict.send(false);
            }
        }
    }

    /// Answers `request_id` with `value`: whether the waiting tool reads
    /// it, an answer for a request not open yet held at most `hold`.
    pub(crate) async fn answer(&self, request_id: &str, value: &str, hold: Duration) -> bool {
        let outcome = Given {
            request_id: request_id.to_owned(),
            outcome: InputOutcome::Answered,
            value: Some(value.to_owned()),
            reason: None,
        };
        self.record_outcome(outcome, hold).await
    }

    /// Rejects `request_id` for `reason`: whether the waiting tool reads
    /// the rejection.
    pub(crate) async fn reject(&self, request_id: &str, reason: &str, hold: Duration) -> bool {
        let outcome = Given {
            request_id: request_id.to_owned(),
            outcome: InputOutcome::Rejected,
            value: None,
            reason: Some(reason.to_owned()),
        };
        self.record_outcome(outcome, hold).await
    }

    /// Sends `outcome` to its request's executor and waits for whether it
    /// settled the request; for a request not open yet, holds it until it
    /// opens, at most `hold`. `false` for a request settled, or answered,
    /// already.
    async fn record_outcome(&self, outcome: Given, hold: Duration) -> bool {
        let (verdict, mut answered) = oneshot::channel();
        match self.send_or_hold(&outcome, verdict, hold) {
            Recorded::Refused => return false,
            Recorded::Sent => return answered.await.unwrap_or(false),
            Recorded::Held => {}
        }
        if let Ok(taken) = tokio::time::timeout(hold, &mut answered).await {
            return taken.unwrap_or(false);
        }
        if lock(&self.record).take_held(&outcome.request_id).is_some() {
            return false;
        }
        // Opened just now: its verdict follows.
        answered.await.unwrap_or(false)
    }

    /// Sends `outcome` to its request's turn, or holds it for a request not
    /// open yet, the oldest held let go past [`HELD_KEPT`].
    fn send_or_hold(
        &self,
        outcome: &Given,
        verdict: oneshot::Sender<bool>,
        hold: Duration,
    ) -> Recorded {
        let mut record = lock(&self.record);
        let now = Instant::now();
        record.prune(now);
        let request_id = outcome.request_id.as_str();
        if record.settled_set.contains(request_id) || record.holds(request_id) {
            return Recorded::Refused;
        }
        if let Some(turn) = record.open.remove(request_id) {
            record.deliver(turn, outcome, verdict);
            return Recorded::Sent;
        }
        let early = Held {
            until: now + hold,
            outcome: outcome.clone(),
            verdict,
        };
        record.held.push_back((outcome.request_id.clone(), early));
        while record.held.len() > HELD_KEPT {
            if let Some((_, oldest)) = record.held.pop_front() {
                let _ = oldest.verdict.send(false);
            }
        }
        Recorded::Held
    }
}

/// What became of an outcome the kit was given.
enum Recorded {
    /// Its request settled, or was answered, already.
    Refused,
    /// Sent to its request's turn, whose verdict follows.
    Sent,
    /// Held for a request not open yet.
    Held,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A turn of the owner's, as the record reads it, keeping what it was
    /// sent.
    #[derive(Default)]
    struct Turn(Mutex<Vec<Control>>);

    impl InputTurn for Turn {
        fn send_control(&self, control: &Control) -> bool {
            lock(&self.0).push(control.clone());
            true
        }
    }

    impl Turn {
        fn values(&self) -> Vec<Option<String>> {
            lock(&self.0)
                .iter()
                .map(|control| {
                    if let Control::InputAnswer { value, .. } = control {
                        value.clone()
                    } else {
                        None
                    }
                })
                .collect()
        }
    }

    fn turn() -> (Arc<Turn>, Arc<dyn InputTurn>) {
        let turn = Arc::new(Turn::default());
        let handle: Arc<dyn InputTurn> = Arc::<Turn>::clone(&turn);
        (turn, handle)
    }

    const HOLD: Duration = Duration::from_secs(60);

    async fn settle() {
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
    }

    // Python: tests/test_turns_inputs.py::test_an_answer_that_comes_before_its_request_is_held_for_it
    #[tokio::test]
    async fn an_answer_that_comes_before_its_request_is_held_for_it() {
        let inputs = Arc::new(OwnerInputs::default());
        let (turn, handle) = turn();
        let answering = tokio::spawn({
            let inputs = Arc::clone(&inputs);
            async move { inputs.answer("p1", "Early", HOLD).await }
        });
        settle().await;
        inputs.opened(Arc::clone(&handle), "p1");
        settle().await;
        assert_eq!(turn.values(), [Some("Early".to_owned())]);
        assert!(!answering.is_finished());
        inputs.taken(&handle, "p1", true);
        assert!(answering.await.unwrap());
    }

    // Python: tests/test_turns_inputs.py::test_an_answer_held_past_its_bound_is_not_taken
    #[tokio::test]
    async fn an_answer_held_past_its_bound_is_not_taken() {
        let inputs = OwnerInputs::default();
        let (turn, handle) = turn();
        assert!(
            !inputs
                .answer("p1", "Early", Duration::from_millis(20))
                .await
        );
        inputs.opened(handle, "p1");
        settle().await;
        assert_eq!(turn.values(), Vec::<Option<String>>::new());
    }

    // Python: tests/test_turns_inputs.py::test_an_answer_the_request_settled_without_says_so
    #[tokio::test]
    async fn an_answer_the_request_settled_without_says_so() {
        let inputs = Arc::new(OwnerInputs::default());
        let (_turn, handle) = turn();
        inputs.opened(Arc::clone(&handle), "p1");
        let answering = tokio::spawn({
            let inputs = Arc::clone(&inputs);
            async move { inputs.answer("p1", "Second", HOLD).await }
        });
        settle().await;
        // Settled first where the tool waits: the executor did not take it.
        inputs.taken(&handle, "p1", false);
        assert!(!answering.await.unwrap());
        assert!(!inputs.answer("p1", "Third", HOLD).await);
    }

    // Python: tests/test_turns_inputs.py::test_an_answer_awaiting_its_verdict_when_the_turn_ends_was_not_taken
    #[tokio::test]
    async fn an_answer_awaiting_its_verdict_when_the_turn_ends_was_not_taken() {
        let inputs = Arc::new(OwnerInputs::default());
        let (_turn, handle) = turn();
        inputs.opened(Arc::clone(&handle), "p1");
        let answering = tokio::spawn({
            let inputs = Arc::clone(&inputs);
            async move { inputs.answer("p1", "Answer", HOLD).await }
        });
        settle().await;
        inputs.turn_ended(&handle);
        assert!(!answering.await.unwrap());
    }

    // Python: tests/test_turns_inputs.py::test_a_request_closed_or_whose_turn_ended_takes_no_answer
    #[tokio::test]
    async fn a_request_closed_or_whose_turn_ended_takes_no_answer() {
        for closed in [true, false] {
            let inputs = OwnerInputs::default();
            let (turn, handle) = turn();
            inputs.opened(Arc::clone(&handle), "p1");
            if closed {
                inputs.closed(&handle, "p1");
            } else {
                inputs.turn_ended(&handle);
            }
            assert!(!inputs.answer("p1", "Late", HOLD).await);
            settle().await;
            assert_eq!(turn.values(), Vec::<Option<String>>::new());
        }
    }

    // Python: tests/test_turns_inputs.py::test_a_request_belongs_to_the_turn_that_opened_it
    #[tokio::test]
    async fn a_request_belongs_to_the_turn_that_opened_it() {
        let inputs = Arc::new(OwnerInputs::default());
        let (owner, owner_handle) = turn();
        let (other, other_handle) = turn();
        inputs.opened(Arc::clone(&owner_handle), "p1");
        // Another turn opening, closing or judging the same id changes
        // nothing.
        inputs.opened(Arc::clone(&other_handle), "p1");
        inputs.closed(&other_handle, "p1");
        let answering = tokio::spawn({
            let inputs = Arc::clone(&inputs);
            async move { inputs.answer("p1", "Mine", HOLD).await }
        });
        settle().await;
        inputs.taken(&other_handle, "p1", false);
        inputs.taken(&owner_handle, "p1", true);
        assert!(answering.await.unwrap());
        assert_eq!(owner.values(), [Some("Mine".to_owned())]);
        assert_eq!(other.values(), Vec::<Option<String>>::new());
    }

    // Python: tests/test_turns_inputs.py::test_early_answers_are_held_up_to_a_bound
    #[tokio::test]
    async fn early_answers_are_held_up_to_a_bound() {
        let inputs = Arc::new(OwnerInputs::default());
        let mut answering = Vec::new();
        for number in 0..=HELD_KEPT {
            let inputs = Arc::clone(&inputs);
            let id = format!("p{number}");
            answering.push(tokio::spawn(
                async move { inputs.answer(&id, &id, HOLD).await },
            ));
        }
        settle().await;
        // The oldest was let go, not taken; the others are still held.
        let oldest = answering.remove(0);
        assert!(!oldest.await.unwrap());
        let (turn, handle) = turn();
        let last = format!("p{HELD_KEPT}");
        inputs.opened(handle, &last);
        assert_eq!(turn.values(), [Some(last)]);
        for task in answering {
            task.abort();
        }
    }
}
