//! The owner remembers retryable publications longer than answered questions.
//! A duplicate keeps its arrival time. Each new reference prunes its own history.
use std::collections::{HashMap, VecDeque};
use std::time::Duration;
use tokio::time::Instant;

use super::Message;

type Key = (String, String);

pub(super) struct Histories {
    questions: History<Option<String>>,
    published: History<()>,
}

impl Default for Histories {
    fn default() -> Self {
        Self {
            questions: History::new(10_000, Duration::from_secs(60)),
            // Work may be retried by the sender for five minutes.
            published: History::new(100_000, Duration::from_secs(600)),
        }
    }
}

impl super::Routes {
    pub(super) fn first(&mut self, message: &Message) -> Result<(), Option<String>> {
        let key = (message.sender.clone(), message.reference.clone());
        if message.kind == "work" {
            self.history.published.first(key).map_err(|()| None)
        } else {
            self.history.questions.first(key)
        }
    }

    pub(super) fn answered(&mut self, question: &Message, body: &str) {
        let key = (question.sender.clone(), question.reference.clone());
        if let Some(previous) = self.history.questions.seen.get_mut(&key) {
            *previous = Some(body.to_owned());
        }
    }
}

struct History<V> {
    limit: usize,
    lifetime: Duration,
    seen: HashMap<Key, V>,
    arrivals: VecDeque<(Instant, Key)>,
}

impl<V: Default + Clone> History<V> {
    fn new(limit: usize, lifetime: Duration) -> Self {
        Self {
            limit,
            lifetime,
            seen: HashMap::new(),
            arrivals: VecDeque::new(),
        }
    }

    fn first(&mut self, key: Key) -> Result<(), V> {
        if let Some(original) = self.seen.get(&key) {
            return Err(original.clone());
        }
        let now = Instant::now();
        self.arrivals.push_back((now, key.clone()));
        self.seen.insert(key, V::default());
        while self.arrivals.front().is_some_and(|(arrival, _)| {
            self.arrivals.len() > self.limit || now.duration_since(*arrival) > self.lifetime
        }) {
            if let Some((_, expired)) = self.arrivals.pop_front() {
                self.seen.remove(&expired);
            }
        }
        Ok(())
    }
}
