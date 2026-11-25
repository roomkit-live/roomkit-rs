//! The process's Redis heartbeat can stand in for messages only for a turn it still runs.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};

use tokio::sync::watch;
use tokio::time::MissedTickBehavior;

use super::registry::Target;
use super::{Node, RedisTurnBackend};
use crate::RoomKitError;
use crate::turns::{MemoryTurn, lock};

pub(super) struct Observation {
    pub(super) member: String,
    pub(super) turn: Arc<MemoryTurn>,
}

pub(super) type Observed = BTreeMap<String, Vec<Observation>>;

impl RedisTurnBackend {
    pub(super) fn watch_process(&self, target: &Target, turn: &Arc<MemoryTurn>) {
        if lock(&self.0.routes).watch(turn, target.member()) {
            tokio::spawn(observe(
                Arc::downgrade(&self.0),
                self.0.stop.subscribe(),
                self.timeouts().interval(),
            ));
        }
    }

    /// One score per watched executor and one membership query per node, regardless of its
    /// other concurrent turns. The registry applies the worker's beat and reads our snapshot
    /// of that worker as one.
    async fn read_process_beats(
        &self,
        nodes: Observed,
        previous: &mut BTreeMap<String, i64>,
    ) -> Result<(), RoomKitError> {
        let members: BTreeSet<_> = nodes
            .values()
            .flatten()
            .map(|watch| watch.member.as_str())
            .collect();
        let members: Vec<_> = members.into_iter().collect();
        let running: Vec<(&str, Vec<&str>)> = nodes
            .iter()
            .map(|(node, turns)| {
                let ids = turns.iter().map(|watch| &*watch.turn.turn_id).collect();
                (node.as_str(), ids)
            })
            .collect();
        let beats = self.registry().beats(&members, &running).await?;
        let scores = members
            .into_iter()
            .zip(beats.last)
            .filter_map(|(name, score)| score.map(|score| (name.to_owned(), score)))
            .collect::<BTreeMap<_, _>>();
        for (turns, present) in nodes.into_values().zip(beats.running) {
            for (watch, active) in turns.into_iter().zip(present) {
                if active
                    && let Some((old, new)) =
                        previous.get(&watch.member).zip(scores.get(&watch.member))
                    && new > old
                {
                    watch.turn.process_beat.store(true, Ordering::Release);
                }
            }
        }
        *previous = scores;
        Ok(())
    }
}

async fn observe(node: Weak<Node>, mut stop: watch::Receiver<bool>, interval: std::time::Duration) {
    let mut clock = tokio::time::interval(interval);
    clock.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut scores = BTreeMap::new();
    while !*stop.borrow() {
        tokio::select! {
            _ = stop.changed() => return,
            _ = clock.tick() => {}
        }
        let Some(node) = node.upgrade() else { return };
        let watching = lock(&node.routes).observed();
        if watching.is_empty() {
            return;
        }
        let backend = RedisTurnBackend(node);
        if let Err(error) = backend.read_process_beats(watching, &mut scores).await {
            tracing::debug!(%error, "Could not read the remote turn heartbeats");
        }
    }
}
