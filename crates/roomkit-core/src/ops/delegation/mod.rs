//! A delegated turn on the trace path (RFC §23.3 steps 4 to 6): the
//! task's description, a system message only the child room's
//! intelligence channels read, committed outside the pipeline, then handed
//! to each of them; what they answer is kept as the room's trace, each row
//! committed as it is, crossing no hook and riding no lane, a streamed
//! answer's rows as its reader ends them, which nobody else reads. The
//! Python implementation's `_broadcast_and_collect` and `_collect_answer`
//! for a child room no transport is shared into.

mod nested;

use std::collections::VecDeque;
use std::sync::Arc;

use self::nested::Nested;
use super::events::{CommitRecord, MutationError};
use super::protocol::{
    ChannelOutcome, ChannelRequest, Delivered, Effect, EffectId, Entry, EntryId, InboundResult,
    Next, Notice, Operation, Outcome, ProtocolError, Request, Settled, Step, StoreOutcome,
    StoreRequest,
};
use super::{Environment, KitConfig};
use crate::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ErrorRecord, EventStatus, Metadata,
    Observation, RoomContext, RoomEvent, RoomId, RoomUpdates, Task,
};
use crate::rules::permissions;

/// What one agent of the child room answered the task with (RFC §23.3
/// step 6): the rows the room kept of its turn, its turn's record, and its
/// failure.
#[derive(Clone, Debug, PartialEq)]
pub struct DelegatedAnswer {
    /// The agent.
    pub channel_id: ChannelId,
    /// Whether its turn answered at all: a turn that raised did not.
    pub responded: bool,
    /// The rows the child room keeps of its turn, as stored, in order.
    pub rows: Vec<RoomEvent>,
    /// Its turn's record (RFC §6.4 "The turn's record").
    pub record: Metadata,
    /// What failed its turn, a stream's failure included.
    pub error: Option<ErrorRecord>,
}

/// Why a delegated turn could not run.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum DelegationError {
    /// The task's description could not be committed.
    #[error(transparent)]
    Commit(#[from] MutationError),
    /// The child room is gone.
    #[error("room {0} does not exist")]
    RoomNotFound(RoomId),
    /// The store failed a read or a write.
    #[error("{}", .0.message)]
    Store(ErrorRecord),
}

/// One delegated turn in a child room, Python's `_broadcast_and_collect`
/// on the trace path. Its output is each answering agent's answer, in the
/// order the room binds them.
#[derive(Debug)]
pub struct DelegatedTurn {
    config: Arc<KitConfig>,
    room_id: RoomId,
    message: Option<Nested<CommitRecord>>,
    /// The description, once committed.
    committed: Option<RoomEvent>,
    next_id: EffectId,
    loading: Option<EffectId>,
    /// The child room as read for the turns, until they are asked.
    loaded: Option<Box<RoomContext>>,
    /// The child room's bindings as read for the turns.
    bindings: Vec<ChannelBinding>,
    /// The turns asked, by their `OnEvent` effect, in the order asked.
    asked: Vec<(EffectId, ChannelBinding, Option<Outcome>)>,
    answers: Vec<DelegatedAnswer>,
    /// The rows waiting for their commit: the answer they belong to, and
    /// the adopted entry each settles.
    rows: VecDeque<(usize, RoomEvent, Option<EntryId>)>,
    committing: Option<(Nested<CommitRecord>, usize, Option<EntryId>)>,
    /// The streams to read, by the turn's effect and the answer's slot.
    streams: VecDeque<(EffectId, usize)>,
    reading: Option<(EffectId, usize)>,
    /// The side effects' writes outstanding.
    persisting: Vec<EffectId>,
    next_entry: EntryId,
    notices: Vec<Notice>,
    settled: Vec<Settled>,
    delivered: Vec<Delivered>,
    failed: Option<DelegationError>,
}

impl DelegatedTurn {
    /// The turn of `room_id`'s agents on `description`, a system message
    /// the caller built, which the operation commits first.
    pub fn new(config: Arc<KitConfig>, room_id: RoomId, description: RoomEvent) -> Self {
        let message = CommitRecord::new(Arc::clone(&config), room_id.clone(), description, None);
        Self {
            config,
            room_id,
            message: Some(Nested::new(message)),
            committed: None,
            next_id: EffectId::first(),
            loading: None,
            loaded: None,
            bindings: Vec::new(),
            asked: Vec::new(),
            answers: Vec::new(),
            rows: VecDeque::new(),
            committing: None,
            streams: VecDeque::new(),
            reading: None,
            persisting: Vec::new(),
            next_entry: EntryId::first(),
            notices: Vec::new(),
            settled: Vec::new(),
            delivered: Vec::new(),
            failed: None,
        }
    }

    fn issue(&mut self, request: impl Into<Request>) -> Effect {
        let id = self.next_id;
        self.next_id = id.next();
        Effect {
            id,
            request: request.into(),
            deadline: None,
            locked: None,
            admitted: None,
            budget: None,
            decides: None,
            answers: None,
        }
    }

    /// Advances the commit of the description; once it is stored, the
    /// room is read for the turns.
    fn advance_message(&mut self, environment: &dyn Environment, effects: &mut Vec<Effect>) {
        let Some(message) = self.message.as_mut() else {
            return;
        };
        match message.poll(environment, &mut self.next_id, effects, &mut self.notices) {
            None => {}
            Some(Ok(stored)) => {
                self.message = None;
                self.committed = Some(stored);
                let recent_events = self.config.recent_events().read_by_the_caller();
                let load = self.issue(StoreRequest::LoadContext {
                    room_id: self.room_id.clone(),
                    recent_events,
                });
                self.loading = Some(load.id);
                effects.push(load);
            }
            Some(Err(error)) => {
                self.message = None;
                self.failed = Some(error.into());
            }
        }
    }

    /// Asks each intelligence channel the description reaches for its
    /// turn, with what of the room it may read (RFC §7.5 rule 8).
    fn ask(&mut self, context: &RoomContext, effects: &mut Vec<Effect>) {
        self.bindings.clone_from(&context.bindings);
        let Some(message) = self.committed.clone() else {
            return;
        };
        for binding in &context.bindings {
            if binding.category != ChannelCategory::Intelligence
                || !permissions::receives(binding, &message)
            {
                continue;
            }
            let request = ChannelRequest::OnEvent {
                binding: binding.clone(),
                event: message.clone(),
                context: Box::new(permissions::context_for(binding, context)),
            };
            let effect = self.issue(request);
            self.asked.push((effect.id, binding.clone(), None));
            effects.push(effect);
        }
    }

    /// Once every turn answered: the buffered answers' rows to commit, in
    /// the order the turns were asked, their side effects to store, then
    /// the streams to read, one at a time.
    fn collect(&mut self, effects: &mut Vec<Effect>) {
        let asked = std::mem::take(&mut self.asked);
        let mut tasks: Vec<Task> = Vec::new();
        let mut observations: Vec<Observation> = Vec::new();
        let mut metadata = Metadata::new();
        for (turn, binding, outcome) in asked {
            let slot = self.answers.len();
            let mut answer = DelegatedAnswer {
                channel_id: binding.channel_id.clone(),
                responded: false,
                rows: Vec::new(),
                record: Metadata::new(),
                error: None,
            };
            match outcome {
                Some(Outcome::Channel(ChannelOutcome::Output(output))) => {
                    let ChannelOutput {
                        responded,
                        response_events,
                        response_metadata,
                        tasks: filed,
                        observations: noted,
                        metadata_updates,
                        error,
                        ..
                    } = *output;
                    answer.responded = responded;
                    answer.record = response_metadata;
                    answer.error = error;
                    for row in response_events {
                        self.rows.push_back((slot, row, None));
                    }
                    tasks.extend(filed);
                    observations.extend(noted);
                    metadata.extend(metadata_updates);
                }
                Some(Outcome::Channel(ChannelOutcome::Streamed(output))) => {
                    answer.responded = true;
                    answer.record.clone_from(&output.response_metadata);
                    tasks.extend(output.tasks);
                    observations.extend(output.observations);
                    metadata.extend(output.metadata_updates);
                    self.streams.push_back((turn, slot));
                }
                // A turn that raised answers nothing, and carries no error to
                // the task: its delivery set logged it, as Python's router
                // does, and kept no output of it.
                Some(Outcome::Channel(ChannelOutcome::Raised(raised))) => {
                    log_raised(&binding.channel_id, &raised.error);
                }
                Some(Outcome::Channel(ChannelOutcome::ProviderDelivery(result))) => {
                    let message = result.error.unwrap_or_default();
                    log_raised(
                        &binding.channel_id,
                        &ErrorRecord::new("ProviderDeliveryError", message),
                    );
                }
                Some(Outcome::Failed(error)) => log_raised(&binding.channel_id, &error),
                Some(_) | None => {}
            }
            self.answers.push(answer);
        }
        if !tasks.is_empty() || !observations.is_empty() {
            let write = self.issue(StoreRequest::PersistSideEffects {
                room_id: self.room_id.clone(),
                tasks,
                observations,
            });
            self.persisting.push(write.id);
            effects.push(write);
        }
        if !metadata.is_empty() {
            let write = self.issue(StoreRequest::UpdateRoom {
                room_id: self.room_id.clone(),
                updates: RoomUpdates {
                    metadata,
                    ..RoomUpdates::default()
                },
            });
            self.persisting.push(write.id);
            effects.push(write);
        }
    }

    /// A row of the trace as the child room stores it: delivered to nobody,
    /// one deeper than the description, which it answers (RFC §23.3 step
    /// 5). A streamed row is given its id and its instant here, as the
    /// pipeline gives a response's.
    fn trace_row(
        &self,
        mut row: RoomEvent,
        streamed: bool,
        environment: &dyn Environment,
    ) -> RoomEvent {
        row.room_id = self.room_id.clone();
        row.status = EventStatus::Delivered;
        if streamed {
            row.id = environment.new_event_id();
            row.created_at = environment.now();
            if let Some(message) = &self.committed {
                row.chain_depth = message.chain_depth.saturating_add(1);
                row.responds_to = Some(message.id.clone());
            }
        }
        row
    }

    /// Advances the row being committed, then starts the next one.
    fn advance_rows(&mut self, environment: &dyn Environment, effects: &mut Vec<Effect>) {
        loop {
            if let Some((commit, slot, entry)) = self.committing.as_mut() {
                let (slot, entry) = (*slot, *entry);
                let Some(stored) =
                    commit.poll(environment, &mut self.next_id, effects, &mut self.notices)
                else {
                    return;
                };
                self.committing = None;
                self.row_stored(slot, entry, stored);
                continue;
            }
            let Some((slot, row, entry)) = self.rows.pop_front() else {
                return;
            };
            let row = self.trace_row(row, entry.is_some(), environment);
            let commit =
                CommitRecord::new(Arc::clone(&self.config), self.room_id.clone(), row, None);
            self.committing = Some((Nested::new(commit), slot, entry));
        }
    }

    /// Records a row's commit, and settles the stream's entry it was.
    fn row_stored(
        &mut self,
        slot: usize,
        entry: Option<EntryId>,
        stored: Result<RoomEvent, MutationError>,
    ) {
        let result = match stored {
            Ok(event) => {
                if let Some(answer) = self.answers.get_mut(slot) {
                    answer.rows.push(event.clone());
                }
                InboundResult {
                    event: Some(event),
                    ..InboundResult::default()
                }
            }
            Err(error) => {
                tracing::warn!(target: "roomkit::tasks", room_id = %self.room_id, %error, "a row of a delegated turn's trace was not stored");
                InboundResult {
                    blocked: true,
                    reason: Some(error.to_string()),
                    ..InboundResult::default()
                }
            }
        };
        if let Some(entry) = entry {
            self.delivered.push(Delivered {
                entry,
                result: result.clone(),
            });
            self.settled.push(Settled {
                entry,
                result: Ok(result),
            });
        }
    }

    /// Starts reading the next stream once the buffered rows are stored.
    fn advance_streams(&mut self, effects: &mut Vec<Effect>) {
        if self.reading.is_some() || self.committing.is_some() || !self.rows.is_empty() {
            return;
        }
        let Some((turn, slot)) = self.streams.pop_front() else {
            return;
        };
        let (Some(message), Some(answer)) = (self.committed.clone(), self.answers.get(slot)) else {
            return;
        };
        let Some(responder) = self.binding_of(&answer.channel_id) else {
            return;
        };
        let read = self.issue(ChannelRequest::ReadStream {
            turn,
            responder,
            answers: Box::new(message),
            target: None,
        });
        self.reading = Some((read.id, slot));
        effects.push(read);
    }

    fn binding_of(&self, channel_id: &ChannelId) -> Option<ChannelBinding> {
        self.bindings
            .iter()
            .find(|binding| &binding.channel_id == channel_id)
            .cloned()
    }

    fn is_over(&self) -> bool {
        self.message.is_none()
            && self.loading.is_none()
            && self.asked.is_empty()
            && self.rows.is_empty()
            && self.committing.is_none()
            && self.streams.is_empty()
            && self.reading.is_none()
            && self.persisting.is_empty()
    }
}

impl Operation for DelegatedTurn {
    type Output = Result<Vec<DelegatedAnswer>, DelegationError>;

    fn poll(&mut self, environment: &dyn Environment) -> Step<Self::Output> {
        let mut effects = Vec::new();
        self.advance_message(environment, &mut effects);
        if let Some(context) = self.loaded.take() {
            self.ask(&context, &mut effects);
        }
        let all_answered =
            !self.asked.is_empty() && self.asked.iter().all(|(_, _, outcome)| outcome.is_some());
        if all_answered {
            self.collect(&mut effects);
        }
        self.advance_rows(environment, &mut effects);
        self.advance_streams(&mut effects);
        let next = if !effects.is_empty() {
            Next::Effects(effects)
        } else if let Some(error) = self.failed.take() {
            Next::Ready(Err(error))
        } else if self.is_over() {
            Next::Ready(Ok(std::mem::take(&mut self.answers)))
        } else {
            Next::Waiting
        };
        Step {
            notices: std::mem::take(&mut self.notices),
            settled: std::mem::take(&mut self.settled),
            delivered: std::mem::take(&mut self.delivered),
            decided: None,
            next,
        }
    }

    #[expect(
        clippy::wildcard_enum_match_arm,
        reason = "an outcome that answers none of the operation's requests is a protocol error, whatever it is"
    )]
    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        if let Some(message) = self.message.as_mut().filter(|message| message.owns(id)) {
            return message.complete(id, outcome);
        }
        if let Some((commit, ..)) = self
            .committing
            .as_mut()
            .filter(|(commit, ..)| commit.owns(id))
        {
            return commit.complete(id, outcome);
        }
        if self.loading == Some(id) {
            self.loading = None;
            match outcome {
                Outcome::Store(StoreOutcome::Context(context)) => self.loaded = Some(context),
                Outcome::Store(StoreOutcome::RoomNotFound) => {
                    self.failed = Some(DelegationError::RoomNotFound(self.room_id.clone()));
                }
                Outcome::Failed(error) => self.failed = Some(DelegationError::Store(error)),
                _ => return Err(unexpected(id, "a room's context")),
            }
            return Ok(());
        }
        if let Some((_, _, answered)) = self.asked.iter_mut().find(|(turn, ..)| *turn == id) {
            *answered = Some(outcome);
            return Ok(());
        }
        if let Some((_, slot)) = self.reading.filter(|(read, _)| *read == id) {
            self.reading = None;
            let error = match outcome {
                Outcome::Channel(ChannelOutcome::Output(output)) => output.error,
                Outcome::Failed(error) => Some(error),
                _ => return Err(unexpected(id, "a stream's end")),
            };
            if let Some(answer) = self.answers.get_mut(slot)
                && answer.error.is_none()
            {
                answer.error = error;
            }
            return Ok(());
        }
        if let Some(position) = self.persisting.iter().position(|write| *write == id) {
            self.persisting.remove(position);
            if let Outcome::Failed(error) = outcome {
                tracing::warn!(target: "roomkit::tasks", room_id = %self.room_id, error = %error.message, "a delegated turn's side effects were not stored");
            }
            return Ok(());
        }
        Err(ProtocolError::UnknownEffect(id))
    }

    /// Adopts each row of the stream being read: committed as the trace's,
    /// then settled with the stored row (RFC §12.2 step 13s).
    #[expect(
        clippy::wildcard_enum_match_arm,
        reason = "any entry but a row of the stream being read is handed back, whatever it is"
    )]
    fn adopt(&mut self, entry: Entry, origin: EffectId) -> Result<EntryId, Box<Entry>> {
        let Some((_, slot)) = self.reading.filter(|(read, _)| *read == origin) else {
            return Err(Box::new(entry));
        };
        match entry {
            Entry::Row { room_id, event, .. } if room_id == self.room_id => {
                let id = self.next_entry;
                self.next_entry = id.next();
                self.rows.push_back((slot, *event, Some(id)));
                Ok(id)
            }
            other => Err(Box::new(other)),
        }
    }
}

/// Logs the raise of `channel_id`'s turn, which the task does not carry.
fn log_raised(channel_id: &ChannelId, error: &ErrorRecord) {
    tracing::warn!(target: "roomkit::tasks", channel = %channel_id, error = %error.message, "a delegated turn raised");
}

/// The protocol error of `outcome`, which does not answer `effect`.
fn unexpected(effect: EffectId, expected: &'static str) -> ProtocolError {
    ProtocolError::UnexpectedOutcome {
        effect,
        expected,
        got: "another outcome",
    }
}
