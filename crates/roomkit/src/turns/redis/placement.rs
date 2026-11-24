//! The current holder keeps the room. Fresh rooms prefer the first eligible
//! worker served here; only rooms whose candidates are all remote are spread.
use std::collections::{BTreeMap, BTreeSet};

use roomkit_core::model::ChannelId;

use super::Worker;
use super::registry::{Target, quoted};
use super::{RedisTurnBackend, redis_error};
use crate::RoomKitError;
use crate::turns::{ExecutorInfo, TakesOptions, lock, names, no_executor, not_taken};

pub(super) struct Reserved {
    lifecycle: std::sync::Weak<crate::turns::executor::Lifecycle>,
    pub(super) look_up: super::WorkerLookup,
}

pub(super) enum Placed {
    Here(Worker),
    Elsewhere(Target),
}

impl Placed {
    pub(super) fn member(&self, node: &str) -> String {
        match self {
            Self::Here(worker) => format!("{node}:{}", worker.executor_id()),
            Self::Elsewhere(target) => target.member(),
        }
    }
}

struct Candidates {
    here: Vec<(String, Worker)>,
    elsewhere: BTreeMap<String, Target>,
}

impl Candidates {
    /// The one executor `only` names among those that take the turn; none
    /// when none does, or when several processes serve its id and it names
    /// no node.
    fn named(self, only: &ExecutorInfo, backend: &RedisTurnBackend) -> Option<Placed> {
        let node = backend.node_id();
        let mut here = self
            .here
            .into_iter()
            .map(|(_, worker)| worker)
            .filter(|worker| names(only, worker.executor_id(), Some(node)));
        let mut elsewhere = self
            .elsewhere
            .into_values()
            .filter(|target| names(only, &target.executor_id, Some(&target.node)));
        let chosen = match (here.next(), elsewhere.next()) {
            (Some(worker), None) => backend.local_target(worker),
            (None, Some(target)) => Placed::Elsewhere(target),
            _ => return None,
        };
        (here.next().is_none() && elsewhere.next().is_none()).then_some(chosen)
    }

    fn choose(
        mut self,
        held: Option<&str>,
        backend: &RedisTurnBackend,
        spread: bool,
    ) -> Option<Placed> {
        if backend.0.loop_back && !self.here.is_empty() {
            let index = self
                .here
                .iter()
                .position(|(member, _)| Some(member.as_str()) == held)
                .unwrap_or(0);
            return Some(backend.local_target(self.here.swap_remove(index).1));
        }
        if let Some(held) = held {
            if let Some(index) = self.here.iter().position(|(member, _)| member == held) {
                return Some(Placed::Here(self.here.swap_remove(index).1));
            }
            if let Some(target) = self.elsewhere.remove(held) {
                return Some(Placed::Elsewhere(target));
            }
        }
        if let Some((_, first)) = self.here.into_iter().next() {
            return Some(Placed::Here(first));
        }
        let count = std::num::NonZeroUsize::new(self.elsewhere.len())?;
        let mut next = lock(&backend.0.choice);
        let following = next.wrapping_add(1) % count;
        if spread {
            *next = following;
        }
        self.elsewhere
            .into_values()
            .nth(following)
            .map(Placed::Elsewhere)
    }
}

impl RedisTurnBackend {
    /// The room owner's live channels remain exclusively its own even after its
    /// executor closes. Keeping the kit weak avoids a cycle through its backend.
    pub(crate) fn reserve_owner(&self, worker: &Worker) {
        self.0.owner_worker.get_or_init(|| Reserved {
            lifecycle: worker.native().lifecycle(),
            look_up: worker.native().look_up(),
        });
        lock(&self.0.serving)
            .workers
            .retain(|served| !self.is_own(served) || served.hosted().is_some());
    }

    pub(super) fn is_own(&self, worker: &Worker) -> bool {
        self.owns_lifecycle(&worker.native().lifecycle())
    }

    fn owns_lifecycle(&self, worker: &std::sync::Weak<crate::turns::executor::Lifecycle>) -> bool {
        self.0
            .owner_worker
            .get()
            .is_some_and(|own| own.lifecycle.ptr_eq(worker))
    }

    fn local_target(&self, worker: Worker) -> Placed {
        if self.0.loop_back {
            Placed::Elsewhere(Target {
                node: self.node_id().to_owned(),
                executor_id: worker.executor_id().to_owned(),
            })
        } else {
            Placed::Here(worker)
        }
    }

    pub(super) fn owning(&self, channel: &ChannelId) -> Option<Worker> {
        let reserved = self.0.owner_worker.get()?;
        (reserved.look_up)().filter(|worker| worker.hosts_channel(channel))
    }

    pub(super) async fn place(
        &self,
        channel: &ChannelId,
        room: &str,
        hooks: &[String],
        organization: Option<&str>,
        refused: &BTreeSet<String>,
        spread: bool,
    ) -> Result<Placed, RoomKitError> {
        let options = TakesOptions {
            room_hooks: hooks.to_vec(),
            organization_id: organization.map(str::to_owned),
        };
        let holder = self.key(&format!(
            "holder:{}:{}",
            quoted(channel.as_str()),
            quoted(room)
        ));
        let chosen = if let Some(own) = self.owning(channel) {
            let member = format!("{}:{}", self.node_id(), own.executor_id());
            if refused.contains(&member) || !own.takes(channel, options) {
                return Err(no_executor(channel.as_str(), hooks, organization));
            }
            self.local_target(own)
        } else {
            let candidates = self.candidates(channel, &options, refused).await?;
            let mut conn = self.connection().await?;
            let held: Option<String> = redis::cmd("GET")
                .arg(&holder)
                .query_async(&mut conn)
                .await
                .map_err(|error| redis_error(&error))?;
            candidates
                .choose(held.as_deref(), self, spread)
                .ok_or_else(|| no_executor(channel.as_str(), hooks, organization))?
        };
        if spread {
            self.held_by(channel, room, &chosen.member(self.node_id()))
                .await?;
        }
        Ok(chosen)
    }

    /// The executor `only` names, when it takes the room's turn of `channel`
    /// as one placed would: the owner's own alone for a channel it holds
    /// live. The room's holder is left as it was, until that executor took
    /// the turn (`held_by`).
    pub(super) async fn named(
        &self,
        channel: &ChannelId,
        hooks: &[String],
        organization: Option<&str>,
        only: &ExecutorInfo,
    ) -> Result<Placed, RoomKitError> {
        let options = TakesOptions {
            room_hooks: hooks.to_vec(),
            organization_id: organization.map(str::to_owned),
        };
        let chosen = if let Some(own) = self.owning(channel) {
            (names(only, own.executor_id(), Some(self.node_id())) && own.takes(channel, options))
                .then(|| self.local_target(own))
        } else {
            self.candidates(channel, &options, &BTreeSet::new())
                .await?
                .named(only, self)
        };
        chosen.ok_or_else(|| not_taken(only, channel.as_str(), hooks, organization))
    }

    /// Record `member` as the executor holding the room's state of `channel`
    /// from now.
    pub(super) async fn held_by(
        &self,
        channel: &ChannelId,
        room: &str,
        member: &str,
    ) -> Result<(), RoomKitError> {
        let holder = self.key(&format!(
            "holder:{}:{}",
            quoted(channel.as_str()),
            quoted(room)
        ));
        let mut conn = self.connection().await?;
        redis::cmd("SET")
            .arg(holder)
            .arg(member)
            .arg("EX")
            .arg(7 * 24 * 3600)
            .exec_async(&mut conn)
            .await
            .map_err(|error| redis_error(&error))
    }

    async fn candidates(
        &self,
        channel: &ChannelId,
        options: &TakesOptions,
        refused: &BTreeSet<String>,
    ) -> Result<Candidates, RoomKitError> {
        // Evaluate application predicates outside the backend lock. Keep the selected
        // instance: a concurrent serve of the same id must not retarget the turn.
        let workers = lock(&self.0.serving).workers.clone();
        let here = workers
            .into_iter()
            .filter(|worker| worker.takes(channel, options.clone()))
            .map(|worker| {
                (
                    format!("{}:{}", self.node_id(), worker.executor_id()),
                    worker,
                )
            })
            .filter(|(member, _)| !refused.contains(member))
            .collect();
        let mut elsewhere = BTreeMap::new();
        for info in super::registry::peers(self).await? {
            if !info.covers(
                channel.as_str(),
                &options.room_hooks,
                options.organization_id.as_deref(),
            )? {
                continue;
            }
            if let Some(node) = info.node {
                let target = Target {
                    node,
                    executor_id: info.executor_id,
                };
                let member = target.member();
                if !refused.contains(&member) {
                    elsewhere.insert(member, target);
                }
            }
        }
        Ok(Candidates { here, elsewhere })
    }
}
