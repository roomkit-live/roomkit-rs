//! A room's speak queue: who is owed a turn, who gets the next one (RFC
//! §19.7.5). Python's `_queue.py`.
//!
//! Pure state, no I/O: every process that serves the room reads and changes
//! it under the room lock and stores it with the room, the lease naming the
//! one process that gives the turns (rule 16). An instruction's text is never
//! stored: its entry names the process that holds it.

use std::collections::BTreeSet;

use roomkit_core::model::ChannelId;
use roomkit_core::rules::discussion::names::same_name;
use serde::{Deserialize, Serialize};

use super::models::SpeakQueue;

/// The asks an entry keeps: the turn reads only the latest person's and the
/// latest, so a flood of mentions does not grow the stored queue.
pub(crate) const KEPT_ASKS: usize = 8;

/// The askers an entry keeps.
pub(crate) const KEPT_ASKERS: usize = 16;

/// Messages that may wait for a dispatch decision; past them, the oldest
/// asks every candidate, as a decision not taken in time does (rule 18).
pub(crate) const KEPT_PENDING: usize = 16;

/// An event that asked for an agent's turn.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Ask {
    pub(crate) event_id: String,
    pub(crate) depth: u32,
    /// Who asked: an agent's channel id or a person's label.
    pub(crate) asker: String,
    /// The event's place in the room: the latest is read by place, not by
    /// when it was asked (a dispatch decision comes after later messages).
    #[serde(default)]
    pub(crate) index: u64,
    #[serde(default)]
    pub(crate) person: bool,
    /// A person named the agent: the one ask an agent that only listens
    /// takes.
    #[serde(default)]
    pub(crate) named: bool,
}

/// A turn an agent is owed: one for every event that asked for it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Entry {
    pub(crate) agent: ChannelId,
    #[serde(default)]
    pub(crate) asks: Vec<Ask>,
    /// Who asked, and whether a person (else an agent): a person whose label
    /// reads as an agent's channel id is still a person.
    #[serde(default)]
    pub(crate) asked_by: Vec<(String, bool)>,
    #[serde(default)]
    pub(crate) front: bool,
    /// Arrival order among front requests, served first come first served.
    #[serde(default)]
    pub(crate) seq: u64,
    /// The instruction this turn takes as its input, by event id.
    #[serde(default)]
    pub(crate) instruction: Option<String>,
    /// The process holding the instruction's text (rule 16).
    #[serde(default)]
    pub(crate) holder: Option<String>,
    /// The lease was handed to that process for this turn once.
    #[serde(default)]
    pub(crate) handed: bool,
    /// The turn regenerates the answer to the event it was asked by.
    #[serde(default)]
    pub(crate) regenerate: bool,
    /// The turn posts its instruction slot's event, a task's result, as the
    /// agent's message, without a generation (rule 19).
    #[serde(default)]
    pub(crate) post: bool,
    #[serde(default)]
    pub(crate) depth_recorded: bool,
}

impl Entry {
    /// An entry owing `agent` a turn nothing has asked for yet.
    fn new(agent: ChannelId) -> Self {
        Self {
            agent,
            asks: Vec::new(),
            asked_by: Vec::new(),
            front: false,
            seq: 0,
            instruction: None,
            holder: None,
            handed: false,
            regenerate: false,
            post: false,
            depth_recorded: false,
        }
    }

    /// A turn of its own, never merged: an instruction's or a regenerated
    /// answer's, given even while the discussion waits or the agent listens.
    pub(crate) fn own_turn(&self) -> bool {
        self.instruction.is_some() || self.regenerate
    }

    /// Merge `ask` into the turn, keeping only what the turn reads.
    pub(crate) fn add(&mut self, ask: Ask) {
        let asker = (ask.asker.clone(), ask.person);
        if !ask.asker.is_empty() && !self.asked_by.contains(&asker) {
            self.asked_by.push(asker);
            let past = self.asked_by.len().saturating_sub(KEPT_ASKERS);
            self.asked_by.drain(..past);
        }
        self.asks.push(ask);
        if self.asks.len() <= KEPT_ASKS {
            return;
        }
        let latest_person = latest(self.asks.iter().filter(|ask| ask.person)).cloned();
        let past = self.asks.len() - KEPT_ASKS;
        self.asks.drain(..past);
        if let Some(person) = latest_person
            && !self.asks.contains(&person)
            && let Some(first) = self.asks.first_mut()
        {
            *first = person;
        }
    }

    /// The event the turn answers: the latest person's message among the
    /// events that asked for it, else the latest of them.
    pub(crate) fn answered(&self) -> Option<&Ask> {
        latest(self.asks.iter().filter(|ask| ask.person)).or_else(|| latest(self.asks.iter()))
    }

    /// Who asked for the turn, and whether a person, in order.
    pub(crate) fn askers(&self) -> &[(String, bool)] {
        &self.asked_by
    }
}

/// The ask of the latest event by its place in the room, the last asked
/// among those of one place.
fn latest<'a>(asks: impl Iterator<Item = &'a Ask>) -> Option<&'a Ask> {
    asks.max_by_key(|ask| ask.index)
}

/// A person's message waiting for the dispatch policy's decision (rule 18).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Pending {
    pub(crate) event_id: String,
    pub(crate) depth: u32,
    /// The person, as the transcript labels them.
    pub(crate) asker: String,
    pub(crate) index: u64,
    /// The agents that asked the person, then `everyone` in its order, less
    /// the agents the message does not reach.
    pub(crate) candidates: Vec<ChannelId>,
    /// The candidates that asked the person and wait for the answer (rule
    /// 10).
    #[serde(default)]
    pub(crate) asked: Vec<ChannelId>,
    /// The message's place among the front requests, kept for the agents
    /// picked: they come before those a later message asked for.
    pub(crate) seq: u64,
}

impl Pending {
    /// What the message asks with no policy (rule 8): the agents that asked
    /// the person, else every candidate.
    pub(crate) fn undecided(&self) -> Vec<ChannelId> {
        if self.asked.is_empty() {
            self.candidates.clone()
        } else {
            self.asked.clone()
        }
    }
}

/// The next turn, if any, and what was set aside on the way: the turns the
/// depth limit stopped (each recorded once, RFC §8.3) and the turns of their
/// own past it, which do not wait (an instruction is dropped, a regenerated
/// answer recorded and dropped).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Pick {
    pub(crate) entry: Option<Entry>,
    pub(crate) stopped: Vec<Entry>,
    pub(crate) dropped: Vec<Entry>,
}

/// The stored speak queue of one room.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct SpeakQueueState {
    pub(crate) entries: Vec<Entry>,
    /// Messages waiting for a decision, in the order they were committed.
    pub(crate) dispatching: Vec<Pending>,
    pub(crate) listening: Vec<ChannelId>,
    pub(crate) asked: Vec<(ChannelId, String)>,
    pub(crate) over: bool,
    pub(crate) turns_given: u32,
    pub(crate) last_speaker: Option<ChannelId>,
    pub(crate) seq: u64,
    pub(crate) waiting: bool,
    /// A person's message reached the room: it has someone to wait for.
    pub(crate) people_spoke: bool,
    /// The running turn of the process holding the lease (rule 16).
    pub(crate) speaking: Option<ChannelId>,
    pub(crate) lease_holder: Option<String>,
    pub(crate) lease_expires: f64,
    /// Counts the changes of the queue, for the lease holder to notice
    /// another process's.
    pub(crate) version: u64,
}

impl SpeakQueueState {
    // -- Asking --

    /// Queue `agent` for a turn `ask` asked for, or merge it into the turn it
    /// is owed: a front request (a person's) moves it to the front, at `seq`
    /// when the request kept its place, else after the requests before it.
    pub(crate) fn ask(&mut self, agent: &ChannelId, ask: Ask, front: bool, seq: Option<u64>) {
        let found = self
            .entries
            .iter()
            .position(|entry| entry.agent == *agent && !entry.own_turn());
        let at = found.unwrap_or_else(|| {
            self.entries.push(Entry::new(agent.clone()));
            self.entries.len() - 1
        });
        let promoted = front && self.entries.get(at).is_some_and(|entry| !entry.front);
        let place = if promoted {
            Some(seq.unwrap_or_else(|| self.next_seq()))
        } else {
            None
        };
        let Some(entry) = self.entries.get_mut(at) else {
            return;
        };
        entry.add(ask);
        entry.depth_recorded = false;
        if let Some(place) = place {
            entry.front = true;
            entry.seq = place;
        }
    }

    /// Leave `pending` to the dispatch policy; the oldest message waiting,
    /// once more than [`KEPT_PENDING`] wait, is returned for its candidates
    /// to take.
    pub(crate) fn wait_for_decision(&mut self, pending: Pending) -> Option<Pending> {
        self.dispatching.push(pending);
        (self.dispatching.len() > KEPT_PENDING).then(|| self.dispatching.remove(0))
    }

    /// The agents a decision picked for `pending`: at the front, at the
    /// message's place, as a person's message puts the agents it asks for.
    pub(crate) fn queue_picked(&mut self, pending: &Pending, agents: &[ChannelId]) {
        for agent in agents {
            let ask = Ask {
                event_id: pending.event_id.clone(),
                depth: pending.depth,
                asker: pending.asker.clone(),
                index: pending.index,
                person: true,
                named: false,
            };
            self.ask(agent, ask, true, Some(pending.seq));
        }
    }

    /// An instruction addressed to `agent`: a turn of its own, at the front,
    /// taking the instruction as its input, whose text `holder` keeps; with
    /// `post`, a task's result the turn posts as the agent's message.
    pub(crate) fn queue_instruction(
        &mut self,
        agent: &ChannelId,
        instruction_id: &str,
        depth: u32,
        holder: Option<String>,
        post: bool,
    ) {
        let mut entry = Entry::new(agent.clone());
        entry.front = true;
        entry.seq = self.next_seq();
        entry.instruction = Some(instruction_id.to_owned());
        entry.holder = holder;
        entry.post = post;
        entry.add(Ask {
            event_id: instruction_id.to_owned(),
            depth,
            asker: String::new(),
            index: 0,
            person: false,
            named: false,
        });
        self.entries.push(entry);
    }

    /// A regenerated answer to the event `ask` names: a turn of its own, at
    /// the front, once per agent and event; whether it was queued.
    pub(crate) fn queue_regeneration(&mut self, agent: &ChannelId, ask: Ask) -> bool {
        let queued = self.entries.iter().any(|entry| {
            entry.regenerate
                && entry.agent == *agent
                && entry
                    .asks
                    .first()
                    .is_some_and(|first| first.event_id == ask.event_id)
        });
        if queued {
            return false;
        }
        let mut entry = Entry::new(agent.clone());
        entry.front = true;
        entry.seq = self.next_seq();
        entry.regenerate = true;
        entry.add(ask);
        self.entries.push(entry);
        true
    }

    /// Records that `agent` asked `person`, once.
    pub(crate) fn record_asked(&mut self, agent: &ChannelId, person: &str) {
        let known = self
            .asked
            .iter()
            .any(|(asker, asked)| asker == agent && same_name(asked, person));
        if !known {
            self.asked.push((agent.clone(), person.to_owned()));
        }
    }

    /// The agents that asked `person` (every person when `None`), in order.
    pub(crate) fn asking(&self, person: Option<&str>) -> Vec<ChannelId> {
        let mut agents: Vec<ChannelId> = Vec::new();
        for (agent, asked) in &self.asked {
            let reads = person.is_none_or(|person| same_name(asked, person));
            if reads && !agents.contains(agent) {
                agents.push(agent.clone());
            }
        }
        agents
    }

    /// Clear what was asked of `person` (every person when `None`), only of
    /// `agents` when given.
    pub(crate) fn clear_asked(&mut self, person: Option<&str>, agents: Option<&[ChannelId]>) {
        self.asked.retain(|(agent, asked)| {
            !(person.is_none_or(|person| same_name(asked, person))
                && agents.is_none_or(|agents| agents.contains(agent)))
        });
    }

    // -- Giving turns --

    /// The entries in the order they get a turn: the front requests by their
    /// place, then the others as they came.
    pub(crate) fn ordered(&self) -> Vec<&Entry> {
        let mut fronts: Vec<&Entry> = self.entries.iter().filter(|entry| entry.front).collect();
        fronts.sort_by_key(|entry| entry.seq);
        fronts.extend(self.entries.iter().filter(|entry| !entry.front));
        fronts
    }

    /// The turn to give next, if any: the first entry that can take it, the
    /// agent that just spoke only when no other can (RFC §19.7.5 rule 7) or
    /// for a turn of its own. While the discussion waits for a person, only a
    /// turn of its own.
    pub(crate) fn next_turn(&mut self, max_depth: u32) -> Pick {
        let mut pick = Pick::default();
        let mut runnable: Vec<Entry> = Vec::new();
        let ordered: Vec<Entry> = self.ordered().into_iter().cloned().collect();
        for entry in ordered {
            if !self.may_take(&entry) || (self.waiting && !entry.own_turn()) {
                continue;
            }
            let past = entry
                .answered()
                .is_some_and(|answered| answered.depth.saturating_add(1) >= max_depth);
            if past {
                self.set_aside(entry, &mut pick);
            } else {
                runnable.push(entry);
            }
        }
        let first = runnable
            .iter()
            .position(|entry| entry.own_turn() || Some(&entry.agent) != self.last_speaker.as_ref())
            .unwrap_or(0);
        if first < runnable.len() {
            pick.entry = Some(runnable.swap_remove(first));
        }
        pick
    }

    /// Sets a turn past the depth limit aside: one of its own leaves the
    /// queue, another stays, recorded once.
    fn set_aside(&mut self, entry: Entry, pick: &mut Pick) {
        if entry.own_turn() {
            self.remove(&entry);
            pick.dropped.push(entry);
        } else if !entry.depth_recorded {
            let mut recorded = entry;
            if let Some(held) = self.entries.iter_mut().find(|held| **held == recorded) {
                held.depth_recorded = true;
            }
            recorded.depth_recorded = true;
            pick.stopped.push(recorded);
        }
    }

    /// Whether `entry` may take a turn. An agent that only listens answers a
    /// person who names it (§6.4).
    fn may_take(&self, entry: &Entry) -> bool {
        if entry.own_turn() || !self.listening.contains(&entry.agent) {
            return true;
        }
        entry.asks.iter().any(|ask| ask.person && ask.named)
    }

    /// Removes the first entry equal to `entry`.
    fn remove(&mut self, entry: &Entry) {
        if let Some(at) = self.entries.iter().position(|held| held == entry) {
            self.entries.remove(at);
        }
    }

    /// Gives `entry` its turn.
    pub(crate) fn take(&mut self, entry: &Entry) {
        self.remove(entry);
        self.speaking = Some(entry.agent.clone());
        self.turns_given += 1;
    }

    /// `agent`'s turn ended.
    pub(crate) fn ended(&mut self, agent: &ChannelId) {
        if self.speaking.as_ref() == Some(agent) {
            self.speaking = None;
        }
        self.last_speaker = Some(agent.clone());
    }

    /// A person's message: the discussion no longer waits (rule 10); whether
    /// it was waiting.
    pub(crate) fn person_wrote(&mut self) -> bool {
        self.people_spoke = true;
        std::mem::replace(&mut self.waiting, false)
    }

    /// Whether only a person can move the room on: the room has someone to
    /// wait for, and an agent asked one, an agent of the queue only listens,
    /// or the depth limit stopped a turn.
    pub(crate) fn owes_a_person(&self, has_people: bool) -> bool {
        if !(has_people || self.people_spoke) {
            return false;
        }
        if !self.asked.is_empty() {
            return true;
        }
        self.entries.iter().any(|entry| {
            (self.listening.contains(&entry.agent) && !entry.own_turn()) || entry.depth_recorded
        })
    }

    /// End the wait when no person is owed anything any more (rule 10);
    /// whether it ended.
    pub(crate) fn end_wait_for_nobody(&mut self) -> bool {
        let ended = self.waiting && !self.owes_a_person(true);
        if ended {
            self.waiting = false;
        }
        ended
    }

    /// Remove the instruction turns, whose text a stopped process held; the
    /// turns removed.
    pub(crate) fn drop_instructions(&mut self) -> Vec<Entry> {
        let (dropped, kept) = std::mem::take(&mut self.entries)
            .into_iter()
            .partition(|entry| entry.instruction.is_some());
        self.entries = kept;
        dropped
    }

    /// Forget `agents`, which the discussion no longer holds: their turns,
    /// their listening, what they asked people and their place among a
    /// message's candidates; the instruction turns it dropped (rule 1).
    pub(crate) fn drop_agents(&mut self, agents: &[ChannelId]) -> Vec<Entry> {
        let dropped = self
            .entries
            .iter()
            .filter(|entry| agents.contains(&entry.agent) && entry.instruction.is_some())
            .cloned()
            .collect();
        self.entries.retain(|entry| !agents.contains(&entry.agent));
        self.listening.retain(|agent| !agents.contains(agent));
        self.asked.retain(|(agent, _)| !agents.contains(agent));
        for pending in &mut self.dispatching {
            pending.candidates.retain(|agent| !agents.contains(agent));
            pending.asked.retain(|agent| !agents.contains(agent));
        }
        dropped
    }

    /// Empty the queue, the messages waiting for a decision with it; the
    /// instruction turns it dropped. Python's `drop`.
    pub(crate) fn drop_all(&mut self) -> Vec<Entry> {
        self.dispatching.clear();
        std::mem::take(&mut self.entries)
            .into_iter()
            .filter(|entry| entry.instruction.is_some())
            .collect()
    }

    /// The queue as a host reads it.
    pub(crate) fn view(&self) -> SpeakQueue {
        let mut queue: Vec<ChannelId> = Vec::new();
        for entry in self.ordered() {
            if !queue.contains(&entry.agent) {
                queue.push(entry.agent.clone());
            }
        }
        SpeakQueue {
            speaking: self.speaking.clone(),
            queue,
            listening: self.listening.iter().cloned().collect::<BTreeSet<_>>(),
            asked: self.asked.clone(),
            waiting: self.waiting,
            over: self.over,
        }
    }

    /// A place among the front requests, for a request served later.
    pub(crate) fn reserve_seq(&mut self) -> u64 {
        self.next_seq()
    }

    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    const MAX_DEPTH: u32 = 5;

    fn id(agent: &str) -> ChannelId {
        ChannelId::new(agent).unwrap()
    }

    fn agent_ask(event: &str, depth: u32, asker: &str) -> Ask {
        Ask {
            event_id: event.to_owned(),
            depth,
            asker: asker.to_owned(),
            index: 0,
            person: false,
            named: false,
        }
    }

    fn person_ask(event: &str) -> Ask {
        person_ask_by(event, "ops", false)
    }

    fn person_ask_by(event: &str, person: &str, named: bool) -> Ask {
        Ask {
            event_id: event.to_owned(),
            depth: 0,
            asker: person.to_owned(),
            index: 0,
            person: true,
            named,
        }
    }

    fn ask(state: &mut SpeakQueueState, agent: &str, asked: Ask) {
        state.ask(&id(agent), asked, false, None);
    }

    fn ask_front(state: &mut SpeakQueueState, agent: &str, asked: Ask) {
        state.ask(&id(agent), asked, true, None);
    }

    fn next(state: &mut SpeakQueueState) -> Option<String> {
        state
            .next_turn(MAX_DEPTH)
            .entry
            .map(|entry| entry.agent.to_string())
    }

    fn give(state: &mut SpeakQueueState) -> Option<String> {
        let entry = state.next_turn(MAX_DEPTH).entry?;
        state.take(&entry);
        state.ended(&entry.agent);
        Some(entry.agent.to_string())
    }

    fn queue(state: &SpeakQueueState) -> Vec<String> {
        state.view().queue.iter().map(ToString::to_string).collect()
    }

    fn owned(pairs: &[(&str, bool)]) -> Vec<(String, bool)> {
        pairs
            .iter()
            .map(|(asker, person)| ((*asker).to_owned(), *person))
            .collect()
    }

    // Python: tests/test_discussion_queue.py::test_an_agent_named_twice_is_owed_one_turn_answering_the_latest
    #[test]
    fn an_agent_named_twice_is_owed_one_turn_answering_the_latest() {
        let mut state = SpeakQueueState::default();
        ask(&mut state, "sre", agent_ask("e1", 1, "investigator"));
        ask(&mut state, "sre", agent_ask("e2", 2, "dev"));

        let entry = state.next_turn(MAX_DEPTH).entry.unwrap();
        assert_eq!(entry.agent, id("sre"));
        assert_eq!(entry.answered(), Some(&agent_ask("e2", 2, "dev")));
        assert_eq!(
            entry.askers(),
            owned(&[("investigator", false), ("dev", false)])
        );
        assert_eq!(queue(&state), ["sre"]);
    }

    // Python: tests/test_discussion_queue.py::test_a_merged_turn_answers_the_persons_message_over_a_later_agents
    #[test]
    fn a_merged_turn_answers_the_persons_message_over_a_later_agents() {
        let mut state = SpeakQueueState::default();
        ask_front(&mut state, "sre", person_ask("p1"));
        ask(&mut state, "sre", agent_ask("e2", 3, "dev"));

        let entry = state.next_turn(MAX_DEPTH).entry.unwrap();
        assert_eq!(entry.answered(), Some(&person_ask("p1")));
    }

    // Python: tests/test_discussion_queue.py::test_people_go_first_first_come_first_served_agents_at_the_back
    #[test]
    fn people_go_first_first_come_first_served_agents_at_the_back() {
        let mut state = SpeakQueueState::default();
        ask(&mut state, "dev", agent_ask("e1", 1, "investigator"));
        ask_front(&mut state, "comms", person_ask("p1"));
        ask_front(&mut state, "sre", person_ask("p2"));
        ask_front(&mut state, "dev", person_ask("p3"));

        assert_eq!(queue(&state), ["comms", "sre", "dev"]);
    }

    // Python: tests/test_discussion_queue.py::test_the_agent_that_just_spoke_waits_while_another_can_take_the_turn
    #[test]
    fn the_agent_that_just_spoke_waits_while_another_can_take_the_turn() {
        let mut state = SpeakQueueState::default();
        ask_front(&mut state, "investigator", person_ask("p1"));
        ask(&mut state, "sre", agent_ask("e1", 1, "investigator"));
        assert_eq!(give(&mut state).as_deref(), Some("investigator"));

        // A person's answer puts investigator first again; sre, owed a turn
        // since before, is served first (the POC's starvation run).
        ask_front(&mut state, "investigator", person_ask("p2"));
        assert_eq!(next(&mut state).as_deref(), Some("sre"));
        assert_eq!(give(&mut state).as_deref(), Some("sre"));
        assert_eq!(give(&mut state).as_deref(), Some("investigator"));
    }

    // Python: tests/test_discussion_queue.py::test_an_agent_alone_in_the_queue_may_speak_twice_in_a_row
    #[test]
    fn an_agent_alone_in_the_queue_may_speak_twice_in_a_row() {
        let mut state = SpeakQueueState::default();
        ask_front(&mut state, "sre", person_ask("p1"));
        assert_eq!(give(&mut state).as_deref(), Some("sre"));
        ask_front(&mut state, "sre", person_ask("p2"));
        assert_eq!(give(&mut state).as_deref(), Some("sre"));
    }

    // Python: tests/test_discussion_queue.py::test_an_agent_that_only_listens_answers_only_a_person_who_names_it
    #[test]
    fn an_agent_that_only_listens_answers_only_a_person_who_names_it() {
        let mut state = SpeakQueueState {
            listening: vec![id("sre")],
            ..SpeakQueueState::default()
        };
        ask(&mut state, "sre", agent_ask("e1", 1, "dev"));
        assert_eq!(next(&mut state), None);
        assert!(state.owes_a_person(true));

        // A person's message that does not name it (everyone, an answer to
        // what it asked) gives it no turn either.
        ask_front(&mut state, "sre", person_ask("p1"));
        assert_eq!(next(&mut state), None);

        ask_front(&mut state, "sre", person_ask_by("p2", "ops", true));
        assert_eq!(give(&mut state).as_deref(), Some("sre"));
        assert_eq!(state.listening, [id("sre")]);
    }

    // Python: tests/test_discussion_queue.py::test_an_instruction_turn_is_given_even_to_an_agent_that_only_listens
    #[test]
    fn an_instruction_turn_is_given_even_to_an_agent_that_only_listens() {
        let mut state = SpeakQueueState {
            listening: vec![id("sre")],
            ..SpeakQueueState::default()
        };
        ask(&mut state, "dev", agent_ask("e1", 1, "investigator"));
        state.queue_instruction(&id("sre"), "i1", 0, None, false);

        let entry = state.next_turn(MAX_DEPTH).entry.unwrap();
        assert_eq!(entry.agent, id("sre"));
        assert_eq!(entry.instruction.as_deref(), Some("i1"));
    }

    // Python: tests/test_discussion_queue.py::test_the_depth_limit_stops_a_turn_once_and_a_shallower_ask_resumes_it
    #[test]
    fn the_depth_limit_stops_a_turn_once_and_a_shallower_ask_resumes_it() {
        let mut state = SpeakQueueState::default();
        ask(&mut state, "sre", agent_ask("e4", 4, "dev"));

        let pick = state.next_turn(MAX_DEPTH);
        assert_eq!(pick.entry, None);
        let stopped: Vec<&ChannelId> = pick.stopped.iter().map(|entry| &entry.agent).collect();
        assert_eq!(stopped, [&id("sre")]);
        assert_eq!(state.next_turn(MAX_DEPTH).stopped, []); // recorded once
        assert!(state.owes_a_person(true));
        // With nobody to wait for, the room is idle, not waiting.
        assert!(!state.owes_a_person(false));

        ask_front(&mut state, "sre", person_ask("p1"));
        let entry = state.next_turn(MAX_DEPTH).entry.unwrap();
        assert_eq!(entry.answered(), Some(&person_ask("p1")));
    }

    // Python: tests/test_discussion_queue.py::test_asked_agents_are_found_and_cleared_per_person
    #[test]
    fn asked_agents_are_found_and_cleared_per_person() {
        let mut state = SpeakQueueState::default();
        state.record_asked(&id("dev"), "ops");
        state.record_asked(&id("sre"), "alice");
        assert_eq!(state.asking(Some("ops")), [id("dev")]);
        assert_eq!(state.asking(None), [id("dev"), id("sre")]);

        state.clear_asked(Some("ops"), None);
        assert_eq!(state.asked, [(id("sre"), "alice".to_owned())]);
        state.clear_asked(None, Some(&[id("sre")]));
        assert_eq!(state.asked, []);
    }

    // Python: tests/test_discussion_queue.py::test_dropping_the_queue_reports_the_instruction_turns_it_held
    #[test]
    fn dropping_the_queue_reports_the_instruction_turns_it_held() {
        let mut state = SpeakQueueState::default();
        ask(&mut state, "dev", agent_ask("e1", 1, "sre"));
        state.queue_instruction(&id("sre"), "i1", 0, None, false);

        let dropped = state.drop_all();
        let instructions: Vec<_> = dropped
            .iter()
            .map(|entry| entry.instruction.as_deref())
            .collect();
        assert_eq!(instructions, [Some("i1")]);
        assert_eq!(queue(&state), Vec::<String>::new());
    }

    // Python: tests/test_discussion_queue.py::test_what_is_stored_keeps_who_speaks_with_the_lease
    #[test]
    fn what_is_stored_keeps_who_speaks_with_the_lease() {
        let mut state = SpeakQueueState {
            lease_holder: Some("worker-a".to_owned()),
            lease_expires: 1.0,
            ..SpeakQueueState::default()
        };
        ask_front(&mut state, "sre", person_ask("p1"));
        let entry = state.next_turn(MAX_DEPTH).entry.unwrap();
        state.take(&entry);
        state.waiting = true;

        let restored: SpeakQueueState =
            serde_json::from_value(serde_json::to_value(&state).unwrap()).unwrap();
        assert_eq!(restored.speaking, Some(id("sre")));
        assert_eq!(restored.lease_holder.as_deref(), Some("worker-a"));
        assert_eq!(restored.turns_given, 1);
        assert!(restored.waiting);
    }

    // Python: tests/test_discussion_queue.py::test_while_it_waits_for_a_person_only_a_turn_of_its_own_is_given
    #[test]
    fn while_it_waits_for_a_person_only_a_turn_of_its_own_is_given() {
        let mut state = SpeakQueueState {
            waiting: true,
            ..SpeakQueueState::default()
        };
        ask(&mut state, "dev", agent_ask("e1", 1, "sre"));
        assert_eq!(next(&mut state), None);

        state.queue_regeneration(&id("sre"), person_ask("p0"));
        let entry = state.next_turn(MAX_DEPTH).entry.unwrap();
        assert_eq!(entry.agent, id("sre"));
        assert!(entry.regenerate);
        state.take(&entry);
        assert!(state.waiting);

        state.person_wrote();
        assert_eq!(next(&mut state).as_deref(), Some("dev"));
    }

    // Python: tests/test_discussion_queue.py::test_a_regenerated_answer_is_not_merged_into_a_pending_turn
    #[test]
    fn a_regenerated_answer_is_not_merged_into_a_pending_turn() {
        let mut state = SpeakQueueState::default();
        ask(&mut state, "sre", agent_ask("e1", 1, "dev"));
        state.queue_regeneration(&id("sre"), person_ask("p0"));
        ask(&mut state, "sre", agent_ask("e2", 1, "dev"));

        let read: Vec<(bool, Vec<&str>)> = state
            .ordered()
            .into_iter()
            .map(|entry| {
                (
                    entry.regenerate,
                    entry.asks.iter().map(|ask| ask.event_id.as_str()).collect(),
                )
            })
            .collect();
        assert_eq!(read, [(true, vec!["p0"]), (false, vec!["e1", "e2"])]);
    }

    // Python: tests/test_discussion_queue.py::test_a_restart_drops_the_instruction_turns_and_keeps_the_rest
    #[test]
    fn a_restart_drops_the_instruction_turns_and_keeps_the_rest() {
        let mut state = SpeakQueueState::default();
        ask(&mut state, "dev", agent_ask("e1", 1, "sre"));
        state.queue_instruction(&id("sre"), "i1", 0, None, false);

        let dropped = state.drop_instructions();
        let instructions: Vec<_> = dropped
            .iter()
            .map(|entry| entry.instruction.as_deref())
            .collect();
        assert_eq!(instructions, [Some("i1")]);
        assert_eq!(queue(&state), ["dev"]);
    }

    // Python: tests/test_discussion_queue.py::test_a_turn_of_its_own_past_the_depth_limit_is_set_aside_not_kept
    #[test]
    fn a_turn_of_its_own_past_the_depth_limit_is_set_aside_not_kept() {
        let mut state = SpeakQueueState::default();
        state.queue_instruction(&id("sre"), "i1", MAX_DEPTH, None, false);
        state.queue_regeneration(&id("dev"), agent_ask("e9", MAX_DEPTH, ""));

        let pick = state.next_turn(MAX_DEPTH);
        assert_eq!(pick.entry, None);
        let dropped: Vec<(&ChannelId, bool)> = pick
            .dropped
            .iter()
            .map(|entry| (&entry.agent, entry.own_turn()))
            .collect();
        assert_eq!(dropped, [(&id("sre"), true), (&id("dev"), true)]);
        assert_eq!(state.entries, []);
    }

    // Python: tests/test_discussion_queue.py::test_a_turn_of_its_own_is_not_held_back_by_the_last_speaker_rule
    #[test]
    fn a_turn_of_its_own_is_not_held_back_by_the_last_speaker_rule() {
        let mut state = SpeakQueueState {
            last_speaker: Some(id("sre")),
            ..SpeakQueueState::default()
        };
        state.queue_instruction(&id("sre"), "i1", 0, None, false);
        ask_front(&mut state, "dev", person_ask("p1"));

        let entry = state.next_turn(MAX_DEPTH).entry.unwrap();
        assert_eq!(entry.agent, id("sre"));
        assert_eq!(entry.instruction.as_deref(), Some("i1"));
    }

    // Python: tests/test_discussion_queue.py::test_a_regenerated_answer_is_queued_once_per_agent_and_event
    #[test]
    fn a_regenerated_answer_is_queued_once_per_agent_and_event() {
        let mut state = SpeakQueueState::default();
        assert!(state.queue_regeneration(&id("sre"), person_ask("p0")));
        assert!(!state.queue_regeneration(&id("sre"), person_ask("p0")));
        assert!(state.queue_regeneration(&id("sre"), person_ask("p1")));
        assert_eq!(state.entries.len(), 2);
    }

    // Python: tests/test_discussion_queue.py::test_a_flood_of_mentions_keeps_the_entry_small_and_the_persons_ask
    #[test]
    fn a_flood_of_mentions_keeps_the_entry_small_and_the_persons_ask() {
        let mut state = SpeakQueueState::default();
        ask_front(&mut state, "sre", person_ask_by("p0", "alice", false));
        for i in 0..100 {
            let asker = format!("agent{}", i % 20);
            ask(&mut state, "sre", agent_ask(&format!("e{i}"), 1, &asker));
        }

        let entry = state.entries.first().unwrap();
        assert_eq!(state.entries.len(), 1);
        assert_eq!(entry.asks.len(), KEPT_ASKS);
        assert_eq!(entry.answered(), Some(&person_ask_by("p0", "alice", false)));
        assert_eq!(entry.askers().len(), KEPT_ASKERS);
        assert_eq!(entry.askers().last(), Some(&("agent19".to_owned(), false)));
    }

    // Python: tests/test_discussion_queue.py::test_people_are_told_apart_ignoring_case_only
    #[test]
    fn people_are_told_apart_ignoring_case_only() {
        let mut state = SpeakQueueState::default();
        state.record_asked(&id("dev"), "Alice");
        state.record_asked(&id("dev"), "alice");
        assert_eq!(state.asked, [(id("dev"), "Alice".to_owned())]);
        assert_eq!(state.asking(Some("ALICE")), [id("dev")]);
        assert_eq!(state.asking(Some("Alice (2)")), Vec::<ChannelId>::new());
    }

    /// What Python's `model_dump(mode="json")` writes of a queue (roomkit
    /// origin/main, the same calls), which a process of either implementation
    /// reads.
    #[test]
    fn the_queue_is_stored_as_python_stores_it() {
        let mut state = SpeakQueueState::default();
        state.ask(
            &id("sre"),
            Ask {
                event_id: "p1".to_owned(),
                depth: 0,
                asker: "ops".to_owned(),
                index: 3,
                person: true,
                named: true,
            },
            true,
            None,
        );
        state.queue_instruction(&id("dev"), "i1", 2, Some("worker-a".to_owned()), true);
        state.record_asked(&id("dev"), "ops");
        state.listening.push(id("comms"));
        let written = serde_json::to_value(&state).unwrap();
        let python: serde_json::Value = serde_json::from_str(PYTHON_DUMP).unwrap();
        assert_eq!(written, python);
        let read: SpeakQueueState = serde_json::from_str(PYTHON_DUMP).unwrap();
        assert_eq!(read, state);
    }

    /// `pending.undecided()`, and a message pushed past the bound.
    #[test]
    fn a_message_waits_for_a_decision_within_a_bound() {
        let pending = |seq: u64| Pending {
            event_id: format!("p{seq}"),
            depth: 0,
            asker: "ops".to_owned(),
            index: seq,
            candidates: vec![id("a"), id("b")],
            asked: Vec::new(),
            seq,
        };
        let mut state = SpeakQueueState::default();
        for seq in 0..16 {
            assert_eq!(state.wait_for_decision(pending(seq)), None);
        }
        let oldest = state.wait_for_decision(pending(16)).unwrap();
        assert_eq!(oldest.event_id, "p0");
        assert_eq!(oldest.undecided(), [id("a"), id("b")]);
        let mut asked = pending(17);
        asked.asked = vec![id("b")];
        assert_eq!(asked.undecided(), [id("b")]);

        // The agents picked keep the message's place among the front requests.
        let place = state.reserve_seq();
        ask_front(&mut state, "c", person_ask("p18"));
        let mut kept = pending(19);
        kept.seq = place;
        state.queue_picked(&kept, &[id("a")]);
        assert_eq!(queue(&state), ["a", "c"]);
    }

    #[test]
    fn forgetting_agents_takes_their_turns_their_listening_and_their_place() {
        let mut state = SpeakQueueState {
            listening: vec![id("a"), id("b")],
            ..SpeakQueueState::default()
        };
        ask(&mut state, "a", agent_ask("e1", 1, "b"));
        ask(&mut state, "b", agent_ask("e2", 1, "a"));
        state.queue_instruction(&id("a"), "i1", 0, None, false);
        state.record_asked(&id("a"), "ops");
        state.record_asked(&id("b"), "ops");
        state.wait_for_decision(Pending {
            event_id: "p1".to_owned(),
            depth: 0,
            asker: "ops".to_owned(),
            index: 1,
            candidates: vec![id("a"), id("b")],
            asked: vec![id("a")],
            seq: 1,
        });

        let dropped = state.drop_agents(&[id("a")]);

        let instructions: Vec<_> = dropped
            .iter()
            .map(|entry| entry.instruction.as_deref())
            .collect();
        assert_eq!(instructions, [Some("i1")]);
        assert_eq!(queue(&state), ["b"]);
        assert_eq!(state.listening, [id("b")]);
        assert_eq!(state.asked, [(id("b"), "ops".to_owned())]);
        let pending = state.dispatching.first().unwrap();
        assert_eq!(pending.candidates, [id("b")]);
        assert_eq!(pending.asked, []);
    }

    #[test]
    fn the_wait_ends_once_no_person_is_owed_anything() {
        let mut state = SpeakQueueState {
            waiting: true,
            ..SpeakQueueState::default()
        };
        state.record_asked(&id("a"), "ops");
        assert!(!state.end_wait_for_nobody());
        assert!(state.waiting);
        state.clear_asked(None, None);
        assert!(state.end_wait_for_nobody());
        assert!(!state.waiting);
        assert!(!state.end_wait_for_nobody());
    }

    const PYTHON_DUMP: &str = r#"{
            "entries": [
                {
                    "agent": "sre",
                    "asks": [
                        {
                            "event_id": "p1",
                            "depth": 0,
                            "asker": "ops",
                            "index": 3,
                            "person": true,
                            "named": true
                        }
                    ],
                    "asked_by": [
                        [
                            "ops",
                            true
                        ]
                    ],
                    "front": true,
                    "seq": 1,
                    "instruction": null,
                    "holder": null,
                    "handed": false,
                    "regenerate": false,
                    "post": false,
                    "depth_recorded": false
                },
                {
                    "agent": "dev",
                    "asks": [
                        {
                            "event_id": "i1",
                            "depth": 2,
                            "asker": "",
                            "index": 0,
                            "person": false,
                            "named": false
                        }
                    ],
                    "asked_by": [],
                    "front": true,
                    "seq": 2,
                    "instruction": "i1",
                    "holder": "worker-a",
                    "handed": false,
                    "regenerate": false,
                    "post": true,
                    "depth_recorded": false
                }
            ],
            "dispatching": [],
            "listening": [
                "comms"
            ],
            "asked": [
                [
                    "dev",
                    "ops"
                ]
            ],
            "over": false,
            "turns_given": 0,
            "last_speaker": null,
            "seq": 2,
            "waiting": false,
            "people_spoke": false,
            "speaking": null,
            "lease_holder": null,
            "lease_expires": 0.0,
            "version": 0
        }"#;
}
