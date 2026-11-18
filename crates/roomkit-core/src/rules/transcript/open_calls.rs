//! Pair starts and ends by turn, round and provider call id (RFC §6.4).
use std::collections::{BTreeMap, VecDeque};

type Key<'a> = (Option<&'a str>, &'a str);

pub(super) struct OpenCalls<'a, T> {
    calls: BTreeMap<Key<'a>, VecDeque<(u64, T)>>,
    rounds: BTreeMap<Option<&'a str>, u64>,
}

impl<'a, T> OpenCalls<'a, T> {
    pub(super) fn new() -> Self {
        Self {
            calls: BTreeMap::new(),
            rounds: BTreeMap::new(),
        }
    }

    pub(super) fn hold(&mut self, key: Key<'a>, value: T) {
        let round = self.rounds.get(&key.0).copied().unwrap_or_default();
        let waiting = self.calls.entry(key).or_default();
        // A later round reusing an id cannot complete a previous round's
        // abandoned call. Duplicate ids within this round finish in FIFO order.
        if waiting
            .back()
            .is_some_and(|(previous, _)| *previous != round)
        {
            waiting.clear();
        }
        waiting.push_back((round, value));
    }

    pub(super) fn round_over(&mut self, turn: Option<&'a str>) {
        let number = self.rounds.entry(turn).or_default();
        *number = number.saturating_add(1);
    }

    pub(super) fn take(&mut self, key: Key<'a>) -> Option<T> {
        self.calls
            .get_mut(&key)?
            .pop_front()
            .map(|(_, value)| value)
    }
}
