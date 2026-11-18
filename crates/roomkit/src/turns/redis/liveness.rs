//! The process's Redis heartbeat can stand in for messages only for a turn it still runs.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};

use tokio::sync::watch;
use tokio::time::MissedTickBehavior;

use super::registry::Target;
use super::{Node, RedisTurnBackend, redis_error};
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
    /// other concurrent turns. Redis applies the worker's beat and our snapshot atomically.
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
        let mut pipe = redis::pipe();
        pipe.atomic()
            .cmd("ZMSCORE")
            .arg(self.key("alive"))
            .arg(&members);
        for (node, turns) in &nodes {
            let ids: Vec<_> = turns.iter().map(|watch| &*watch.turn.turn_id).collect();
            pipe.cmd("SMISMEMBER")
                .arg(self.key(&format!("running:{node}")))
                .arg(&ids);
        }
        let mut client = self.connection().await?;
        let rows: Vec<Vec<Option<i64>>> =
            pipe.query_async(&mut client).await.map_err(|error| redis_error(&error))?;
        let mut rows = rows.into_iter();
        let scores = members
            .into_iter()
            .zip(rows.next().unwrap_or_default())
            .filter_map(|(name, score)| score.map(|score| (name.to_owned(), score)))
            .collect::<BTreeMap<_, _>>();
        for (turns, present) in nodes.into_values().zip(rows) {
            for (watch, active) in turns.into_iter().zip(present) {
                if active == Some(1)
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
