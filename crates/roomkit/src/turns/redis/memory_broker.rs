//! The backend's service kept in this process: the nodes that share one
//! reach each other as across a server. Nothing leaves the process, and
//! nothing expires but what a view forgets.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;

use super::broker::{Beater, Beats, Entry, Inbox, TurnBroker, TurnRegistry, View};
use super::registry::Beat;
use super::stream::Message;
use crate::RoomKitError;
use crate::turns::{TurnFuture, lock};

#[derive(Default)]
struct State {
    /// Each node's inbox, its entries by the order they were posted in.
    inboxes: BTreeMap<String, BTreeMap<u64, Message>>,
    posted: u64,
    /// Each executor's last beat, and its declaration when it has one.
    alive: BTreeMap<String, i64>,
    declared: BTreeMap<String, String>,
    /// Each room owner's last beat.
    owners: BTreeMap<String, i64>,
    /// Each node's last generation and the turns it runs.
    generations: BTreeMap<String, u64>,
    running: BTreeMap<String, BTreeSet<String>>,
    holders: BTreeMap<(String, String), String>,
    presence: BTreeMap<(String, String), bool>,
}

struct Shared {
    state: Mutex<State>,
    posted: Notify,
    started: Instant,
}

impl Shared {
    /// The service's time, in milliseconds.
    fn now(&self) -> i64 {
        i64::try_from(self.started.elapsed().as_millis()).unwrap_or(i64::MAX)
    }

    fn beat(&self, beat: &Beat) {
        let now = self.now();
        let mut state = lock(&self.state);
        let last = state.generations.get(&beat.node).copied().unwrap_or(0);
        if beat.generation < last {
            return;
        }
        state.generations.insert(beat.node.clone(), beat.generation);
        for (member, declared) in &beat.declared {
            state.alive.insert(member.clone(), now);
            state.declared.insert(member.clone(), declared.clone());
        }
        for member in &beat.undeclared {
            state.alive.insert(member.clone(), now);
            state.declared.remove(member);
        }
        for member in &beat.gone {
            state.alive.remove(member);
            state.declared.remove(member);
        }
        if beat.owner {
            state.owners.insert(beat.node.clone(), now);
        }
        state
            .running
            .insert(beat.node.clone(), beat.running.iter().cloned().collect());
    }
}

/// A service of this process alone. Clones share it.
#[derive(Clone)]
pub(super) struct MemoryBroker(Arc<Shared>);

impl MemoryBroker {
    pub(super) fn new() -> Self {
        Self(Arc::new(Shared {
            state: Mutex::default(),
            posted: Notify::new(),
            started: Instant::now(),
        }))
    }
}

impl TurnBroker for MemoryBroker {
    fn post<'a>(
        &'a self,
        node: &'a str,
        message: &'a Message,
    ) -> TurnFuture<'a, Result<(), RoomKitError>> {
        Box::pin(async move {
            {
                let mut state = lock(&self.0.state);
                state.posted += 1;
                let id = state.posted;
                state
                    .inboxes
                    .entry(node.to_owned())
                    .or_default()
                    .insert(id, message.clone());
            }
            self.0.posted.notify_waiters();
            Ok(())
        })
    }

    fn inbox<'a>(
        &'a self,
        node: &'a str,
        block: Duration,
    ) -> TurnFuture<'a, Result<Box<dyn Inbox>, RoomKitError>> {
        Box::pin(async move {
            let inbox: Box<dyn Inbox> = Box::new(MemoryInbox {
                shared: Arc::clone(&self.0),
                node: node.to_owned(),
                block,
            });
            Ok(inbox)
        })
    }

    fn acknowledge<'a>(
        &'a self,
        node: &'a str,
        ids: &'a [String],
    ) -> TurnFuture<'a, Result<(), RoomKitError>> {
        Box::pin(async move {
            let mut state = lock(&self.0.state);
            if let Some(inbox) = state.inboxes.get_mut(node) {
                for id in ids.iter().filter_map(|id| id.parse::<u64>().ok()) {
                    inbox.remove(&id);
                }
            }
            Ok(())
        })
    }
}

impl TurnRegistry for MemoryBroker {
    fn connect(&self) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async { Ok(()) })
    }

    fn connected(&self) -> bool {
        true
    }

    fn beat<'a>(&'a self, beat: &'a Beat) -> TurnFuture<'a, Result<(), RoomKitError>> {
        Box::pin(async move {
            self.0.beat(beat);
            Ok(())
        })
    }

    fn beater(&self) -> TurnFuture<'_, Result<Box<dyn Beater>, RoomKitError>> {
        Box::pin(async move {
            let beater: Box<dyn Beater> = Box::new(MemoryBeater(Arc::clone(&self.0)));
            Ok(beater)
        })
    }

    fn view(&self, forget_after_ms: i64) -> TurnFuture<'_, Result<View, RoomKitError>> {
        Box::pin(async move {
            let now = self.0.now();
            let oldest = now.saturating_sub(forget_after_ms);
            let mut state = lock(&self.0.state);
            let State {
                alive,
                declared,
                owners,
                ..
            } = &mut *state;
            alive.retain(|member, beat| {
                let kept = *beat > oldest;
                if !kept {
                    declared.remove(member);
                }
                kept
            });
            owners.retain(|_, beat| *beat > oldest);
            let executors = alive
                .iter()
                .map(|(member, beat)| (member.clone(), (*beat, declared.get(member).cloned())))
                .collect();
            Ok(View {
                now,
                executors,
                owners: owners.clone(),
            })
        })
    }

    fn forget_owner<'a>(&'a self, node: &'a str) -> TurnFuture<'a, Result<(), RoomKitError>> {
        Box::pin(async move {
            lock(&self.0.state).owners.remove(node);
            Ok(())
        })
    }

    fn beats<'a>(
        &'a self,
        members: &'a [&'a str],
        running: &'a [(&'a str, Vec<&'a str>)],
    ) -> TurnFuture<'a, Result<Beats, RoomKitError>> {
        Box::pin(async move {
            let state = lock(&self.0.state);
            Ok(Beats {
                last: members
                    .iter()
                    .map(|member| state.alive.get(*member).copied())
                    .collect(),
                running: running
                    .iter()
                    .map(|(node, turns)| {
                        let runs = state.running.get(*node);
                        turns
                            .iter()
                            .map(|turn| runs.is_some_and(|runs| runs.contains(*turn)))
                            .collect()
                    })
                    .collect(),
            })
        })
    }

    fn holder<'a>(
        &'a self,
        channel: &'a str,
        room: &'a str,
    ) -> TurnFuture<'a, Result<Option<String>, RoomKitError>> {
        Box::pin(async move {
            let key = (channel.to_owned(), room.to_owned());
            Ok(lock(&self.0.state).holders.get(&key).cloned())
        })
    }

    fn hold<'a>(
        &'a self,
        channel: &'a str,
        room: &'a str,
        member: &'a str,
    ) -> TurnFuture<'a, Result<(), RoomKitError>> {
        Box::pin(async move {
            let key = (channel.to_owned(), room.to_owned());
            lock(&self.0.state).holders.insert(key, member.to_owned());
            Ok(())
        })
    }

    fn forget_holder<'a>(
        &'a self,
        channel: &'a str,
        room: &'a str,
    ) -> TurnFuture<'a, Result<(), RoomKitError>> {
        Box::pin(async move {
            let key = (channel.to_owned(), room.to_owned());
            lock(&self.0.state).holders.remove(&key);
            Ok(())
        })
    }

    fn claim_presence<'a>(
        &'a self,
        channel_id: &'a str,
        room_id: &'a str,
        online: Option<bool>,
    ) -> TurnFuture<'a, Result<bool, RoomKitError>> {
        Box::pin(async move {
            let key = (room_id.to_owned(), channel_id.to_owned());
            let mut state = lock(&self.0.state);
            let Some(online) = online else {
                state.presence.remove(&key);
                return Ok(false);
            };
            Ok(state.presence.insert(key, online) != Some(online))
        })
    }
}

struct MemoryInbox {
    shared: Arc<Shared>,
    node: String,
    block: Duration,
}

impl MemoryInbox {
    fn after(&self, last: u64) -> Vec<Entry> {
        lock(&self.shared.state)
            .inboxes
            .get(&self.node)
            .into_iter()
            .flat_map(|inbox| inbox.range(last.saturating_add(1)..))
            .map(|(id, message)| Entry {
                id: id.to_string(),
                message: Some(message.clone()),
            })
            .collect()
    }
}

impl Inbox for MemoryInbox {
    fn read<'a>(&'a mut self, last: &'a str) -> TurnFuture<'a, Result<Vec<Entry>, RoomKitError>> {
        Box::pin(async move {
            let last = last.parse::<u64>().unwrap_or(0);
            let posted = self.shared.posted.notified();
            tokio::pin!(posted);
            // Listening before the first look: a message posted between
            // the look and the wait still wakes this read.
            posted.as_mut().enable();
            let entries = self.after(last);
            if !entries.is_empty() {
                return Ok(entries);
            }
            let _ = tokio::time::timeout(self.block, posted).await;
            Ok(self.after(last))
        })
    }
}

struct MemoryBeater(Arc<Shared>);

impl Beater for MemoryBeater {
    fn beat<'a>(&'a mut self, beat: &'a Beat) -> TurnFuture<'a, Result<(), RoomKitError>> {
        Box::pin(async move {
            self.0.beat(beat);
            Ok(())
        })
    }
}
