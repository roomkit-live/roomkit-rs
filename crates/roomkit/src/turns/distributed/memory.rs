//! A distributed turn backend's services kept in this process: the nodes
//! that share a broker and a registry reach each other as across a server.
//! Nothing leaves the process, and nothing expires but what a view forgets.
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;

use super::broker::{
    Beat, BeatWriter, Beats, BrokerMessage, InboxEntry, RegistryView, TurnBroker, TurnInbox,
    TurnRegistry,
};
use crate::RoomKitError;
use crate::turns::{TurnFuture, lock};

#[derive(Default)]
struct Inboxes {
    /// Each node's inbox, its entries by the order they were posted in.
    of: BTreeMap<String, BTreeMap<u64, BrokerMessage>>,
    posted: u64,
}

struct Posts {
    inboxes: Mutex<Inboxes>,
    posted: Notify,
}

/// The messages of a distributed turn backend, carried within this
/// process: for tests, and for nodes that share one process. Clones share
/// the inboxes.
#[derive(Clone)]
pub struct InMemoryTurnBroker(Arc<Posts>);

impl InMemoryTurnBroker {
    /// A broker with no inbox yet.
    pub fn new() -> Self {
        Self(Arc::new(Posts {
            inboxes: Mutex::default(),
            posted: Notify::new(),
        }))
    }
}

impl Default for InMemoryTurnBroker {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for InMemoryTurnBroker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryTurnBroker").finish_non_exhaustive()
    }
}

impl TurnBroker for InMemoryTurnBroker {
    fn post<'a>(
        &'a self,
        node: &'a str,
        message: &'a BrokerMessage,
    ) -> TurnFuture<'a, Result<(), RoomKitError>> {
        Box::pin(async move {
            {
                let mut inboxes = lock(&self.0.inboxes);
                inboxes.posted += 1;
                let id = inboxes.posted;
                inboxes
                    .of
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
    ) -> TurnFuture<'a, Result<Box<dyn TurnInbox>, RoomKitError>> {
        Box::pin(async move {
            let inbox: Box<dyn TurnInbox> = Box::new(Reader {
                posts: Arc::clone(&self.0),
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
            let mut inboxes = lock(&self.0.inboxes);
            if let Some(inbox) = inboxes.of.get_mut(node) {
                for id in ids.iter().filter_map(|id| id.parse::<u64>().ok()) {
                    inbox.remove(&id);
                }
            }
            Ok(())
        })
    }
}

struct Reader {
    posts: Arc<Posts>,
    node: String,
    block: Duration,
}

impl Reader {
    fn after(&self, last: u64) -> Vec<InboxEntry> {
        lock(&self.posts.inboxes)
            .of
            .get(&self.node)
            .into_iter()
            .flat_map(|inbox| inbox.range(last.saturating_add(1)..))
            .map(|(id, message)| InboxEntry {
                id: id.to_string(),
                message: Some(message.clone()),
            })
            .collect()
    }
}

impl TurnInbox for Reader {
    fn read<'a>(
        &'a mut self,
        last: &'a str,
    ) -> TurnFuture<'a, Result<Vec<InboxEntry>, RoomKitError>> {
        Box::pin(async move {
            let last = last.parse::<u64>().unwrap_or(0);
            let posted = self.posts.posted.notified();
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

#[derive(Default)]
struct Records {
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

struct Kept {
    records: Mutex<Records>,
    started: Instant,
}

impl Kept {
    /// The registry's time, in milliseconds.
    fn now(&self) -> i64 {
        i64::try_from(self.started.elapsed().as_millis()).unwrap_or(i64::MAX)
    }

    fn beat(&self, beat: &Beat) {
        let now = self.now();
        let mut records = lock(&self.records);
        let last = records.generations.get(&beat.node).copied().unwrap_or(0);
        if beat.generation < last {
            return;
        }
        records
            .generations
            .insert(beat.node.clone(), beat.generation);
        for (member, declared) in &beat.declared {
            records.alive.insert(member.clone(), now);
            records.declared.insert(member.clone(), declared.clone());
        }
        for member in &beat.undeclared {
            records.alive.insert(member.clone(), now);
            records.declared.remove(member);
        }
        for member in &beat.gone {
            records.alive.remove(member);
            records.declared.remove(member);
        }
        if beat.owner {
            records.owners.insert(beat.node.clone(), now);
        }
        records
            .running
            .insert(beat.node.clone(), beat.running.iter().cloned().collect());
    }
}

/// What the processes of a distributed turn backend share, kept within
/// this process: for tests, and for nodes that share one process. Clones
/// share what is kept.
#[derive(Clone)]
pub struct InMemoryTurnRegistry(Arc<Kept>);

impl InMemoryTurnRegistry {
    /// A registry that knows no node yet.
    pub fn new() -> Self {
        Self(Arc::new(Kept {
            records: Mutex::default(),
            started: Instant::now(),
        }))
    }
}

impl Default for InMemoryTurnRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for InMemoryTurnRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryTurnRegistry")
            .finish_non_exhaustive()
    }
}

impl TurnRegistry for InMemoryTurnRegistry {
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

    fn beater(&self) -> TurnFuture<'_, Result<Box<dyn BeatWriter>, RoomKitError>> {
        Box::pin(async move {
            let beater: Box<dyn BeatWriter> = Box::new(Writer(Arc::clone(&self.0)));
            Ok(beater)
        })
    }

    fn view(&self, forget_after_ms: i64) -> TurnFuture<'_, Result<RegistryView, RoomKitError>> {
        Box::pin(async move {
            let now = self.0.now();
            let oldest = now.saturating_sub(forget_after_ms);
            let mut records = lock(&self.0.records);
            let Records {
                alive,
                declared,
                owners,
                ..
            } = &mut *records;
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
            Ok(RegistryView {
                now,
                executors,
                owners: owners.clone(),
            })
        })
    }

    fn forget_owner<'a>(&'a self, node: &'a str) -> TurnFuture<'a, Result<(), RoomKitError>> {
        Box::pin(async move {
            lock(&self.0.records).owners.remove(node);
            Ok(())
        })
    }

    fn beats<'a>(
        &'a self,
        members: &'a [&'a str],
        running: &'a [(&'a str, Vec<&'a str>)],
    ) -> TurnFuture<'a, Result<Beats, RoomKitError>> {
        Box::pin(async move {
            let records = lock(&self.0.records);
            Ok(Beats {
                last: members
                    .iter()
                    .map(|member| records.alive.get(*member).copied())
                    .collect(),
                running: running
                    .iter()
                    .map(|(node, turns)| {
                        let runs = records.running.get(*node);
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
            Ok(lock(&self.0.records).holders.get(&key).cloned())
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
            lock(&self.0.records).holders.insert(key, member.to_owned());
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
            lock(&self.0.records).holders.remove(&key);
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
            let mut records = lock(&self.0.records);
            let Some(online) = online else {
                records.presence.remove(&key);
                return Ok(false);
            };
            Ok(records.presence.insert(key, online) != Some(online))
        })
    }
}

struct Writer(Arc<Kept>);

impl BeatWriter for Writer {
    fn beat<'a>(&'a mut self, beat: &'a Beat) -> TurnFuture<'a, Result<(), RoomKitError>> {
        Box::pin(async move {
            self.0.beat(beat);
            Ok(())
        })
    }
}
