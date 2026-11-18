//! The optional bound on executor-owned channel builds (RFC §10.6.2).
//! A committed event alone leaves its build at the cold end. A turn or
//! channel call promotes its build. Active work pins that exact generation.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, PoisonError, Weak};

use roomkit_core::model::ChannelId;

use super::channels::Generations;
use crate::{InMemoryStore, RoomKit};

pub(super) struct Pool {
    bound: usize,
    state: Mutex<State>,
    kit: RoomKit<InMemoryStore>,
    generations: Arc<Generations>,
}

#[derive(Default)]
struct State {
    order: VecDeque<ChannelId>,
    using: HashMap<ChannelId, Weak<()>>,
}

pub(super) struct InUse {
    pool: Arc<Pool>,
    id: ChannelId,
    generation: Option<Arc<()>>,
}

impl Pool {
    pub(super) fn new(bound: usize, kit: RoomKit, generations: Arc<Generations>) -> Arc<Self> {
        Arc::new(Self {
            bound,
            state: Mutex::default(),
            kit,
            generations,
        })
    }

    pub(super) fn hold(self: &Arc<Self>, id: &ChannelId) -> InUse {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let slot = state.using.entry(id.clone()).or_default();
        let generation = slot.upgrade().unwrap_or_else(|| {
            let generation = Arc::new(());
            *slot = Arc::downgrade(&generation);
            generation
        });
        InUse {
            pool: Arc::clone(self),
            id: id.clone(),
            generation: Some(generation),
        }
    }

    pub(super) fn forget(&self, id: &ChannelId) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.order.retain(|other| other != id);
        state.using.remove(id);
    }

    fn shrink(&self, state: &mut State) {
        while state.order.len() > self.bound {
            let Some(index) = state.order.iter().position(|id| {
                state
                    .using
                    .get(id)
                    .is_none_or(|active| active.strong_count() == 0)
            }) else {
                return;
            };
            let Some(id) = state.order.remove(index) else {
                return;
            };
            state.using.remove(&id);
            // A new user must acquire this same mutex before reading the registry.
            match self.kit.unregister_channel(&id) {
                Ok(Some(channel)) => self.generations.remember(tokio::spawn(async move {
                    channel.close().await;
                })),
                Ok(None) | Err(crate::RoomKitError::KitClosed) => {}
                Err(error) => {
                    tracing::warn!(channel_id = %id, %error, "could not evict a channel build");
                }
            }
        }
    }
}

impl InUse {
    pub(super) fn record(&self, recent: bool) {
        let mut state = self
            .pool
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if recent {
            state.order.retain(|id| id != &self.id);
            state.order.push_back(self.id.clone());
            self.pool.shrink(&mut state);
        } else if !state.order.contains(&self.id) {
            state.order.push_front(self.id.clone());
        }
    }
}

impl Drop for InUse {
    fn drop(&mut self) {
        let mut state = self
            .pool
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        drop(self.generation.take());
        if state
            .using
            .get(&self.id)
            .is_some_and(|active| active.strong_count() == 0)
        {
            state.using.remove(&self.id);
        }
        self.pool.shrink(&mut state);
    }
}
