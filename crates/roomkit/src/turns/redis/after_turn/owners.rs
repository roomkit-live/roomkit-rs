//! The most recent distinct turn IDs in admission order, like Python's `OrderedDict`.
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

#[derive(Default)]
pub(in crate::turns::redis) struct TurnOwners {
    by_turn: HashMap<Arc<str>, Arc<str>>,
    order: VecDeque<Arc<str>>,
}

impl TurnOwners {
    pub(super) fn remember(&mut self, turn: &str, peer: &str) {
        if let Some(previous) = self.by_turn.get_mut(turn) {
            *previous = Arc::from(peer);
            return;
        }
        let turn: Arc<str> = Arc::from(turn);
        self.order.push_back(Arc::clone(&turn));
        self.by_turn.insert(turn, Arc::from(peer));
        if self.order.len() > 10_000
            && let Some(oldest) = self.order.pop_front()
        {
            self.by_turn.remove(&oldest);
        }
    }

    pub(super) fn peer(&self, turn: &str) -> Option<Arc<str>> {
        self.by_turn.get(turn).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::TurnOwners;

    #[test]
    fn a_duplicate_updates_the_owner_without_aging_a_different_turn() {
        let mut owners = TurnOwners::default();
        owners.remember("first", "a");
        owners.remember("second", "b");
        owners.remember("first", "c");
        assert_eq!(owners.peer("first").as_deref(), Some("c"));
        for turn in 2..=10_000 {
            owners.remember(&turn.to_string(), "remaining");
        }
        assert!(owners.peer("first").is_none());
        assert_eq!(owners.peer("second").as_deref(), Some("b"));
    }
}
