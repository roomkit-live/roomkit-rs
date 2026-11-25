//! Executor declarations and heartbeats use Redis server time in milliseconds.
use std::collections::BTreeSet;

use roomkit_core::model::Timestamp;
use serde::{Deserialize, Serialize};

use super::DistributedTurnBackend;
pub(super) use super::broker::Beat;
use super::broker::RegistryView as View;
use crate::RoomKitError;
use crate::turns::{ExecutorInfo, ExecutorInfoOptions};

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Declaration {
    pub(super) node: String,
    pub(super) executor_id: String,
    pub(super) channels: BTreeSet<String>,
    pub(super) room_hooks: BTreeSet<String>,
    pub(super) organizations: Option<BTreeSet<String>>,
}

/// The executors `view` holds a declaration of, online while they beat
/// within `timeout_ms`.
fn declared(view: View, timeout_ms: i64) -> Vec<ExecutorInfo> {
    let now = view.now;
    view.executors
        .into_iter()
        .filter_map(|(member, (beat, declared))| {
            let raw = declared?;
            let Ok(declaration) = serde_json::from_str::<Declaration>(&raw) else {
                tracing::warn!(%member, "The executor declaration is malformed");
                return None;
            };
            Some(ExecutorInfo::new(
                declaration.executor_id,
                ExecutorInfoOptions {
                    node: Some(declaration.node),
                    hosts: Some(declaration.channels),
                    organizations: declaration.organizations,
                    room_hooks: declaration.room_hooks,
                    last_beat: Timestamp::from_unix_millis(beat).ok(),
                    online: now.saturating_sub(beat) < timeout_ms,
                },
            ))
        })
        .collect()
}

pub(super) async fn executors(
    backend: &DistributedTurnBackend,
) -> Result<Vec<ExecutorInfo>, RoomKitError> {
    let elsewhere = peers(backend).await?;
    let mut rows: Vec<_> = crate::turns::lock(&backend.0.serving)
        .workers
        .iter()
        .filter(|worker| !worker.closed() && !backend.is_own(worker))
        // A worker served here is listed with its live declaration, one
        // whose host selection cannot be announced to other nodes included.
        .map(|worker| {
            let node = Some(backend.node_id().to_owned());
            ExecutorInfo::of(&**worker, crate::turns::ExecutorInfoOfOptions { node })
        })
        .collect();
    rows.extend(elsewhere);
    Ok(rows)
}

/// Other nodes' declarations. Local workers are read from the backend in serving order.
pub(super) async fn peers(
    backend: &DistributedTurnBackend,
) -> Result<Vec<ExecutorInfo>, RoomKitError> {
    let (view, timeout) = read_view(backend).await?;
    Ok(declared(view, timeout)
        .into_iter()
        .filter(|info| info.node.as_deref() != Some(backend.node_id()))
        .collect())
}

/// A draining executor still answers work it accepted after its declaration was withdrawn.
pub(super) async fn member_alive(
    backend: &DistributedTurnBackend,
    member: &str,
) -> Result<bool, RoomKitError> {
    let (view, timeout) = read_view(backend).await?;
    Ok(view
        .executors
        .get(member)
        .is_some_and(|(last, _)| view.now.saturating_sub(*last) < timeout))
}

async fn read_view(backend: &DistributedTurnBackend) -> Result<(View, i64), RoomKitError> {
    let timeout =
        i64::try_from(backend.timeouts().heartbeat_timeout.as_millis()).unwrap_or(i64::MAX);
    Ok((view(backend, timeout).await?, timeout))
}

/// Check against Redis server time, the same clock that wrote the owner heartbeat.
pub(super) async fn owner_alive(
    backend: &DistributedTurnBackend,
    peer: &str,
) -> Result<bool, RoomKitError> {
    let timeout =
        i64::try_from(backend.timeouts().heartbeat_timeout.as_millis()).unwrap_or(i64::MAX);
    let current = view(backend, timeout).await?;
    Ok(current
        .owners
        .get(peer)
        .is_some_and(|beat| current.now.saturating_sub(*beat) < timeout))
}

async fn view(backend: &DistributedTurnBackend, timeout: i64) -> Result<View, RoomKitError> {
    backend.registry().view(timeout.saturating_mul(10)).await
}

pub(super) async fn write_beat(
    backend: &DistributedTurnBackend,
    beat: &Beat,
) -> Result<(), RoomKitError> {
    backend.registry().beat(beat).await?;
    backend.beat_written(beat.generation);
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Target {
    pub(super) node: String,
    pub(super) executor_id: String,
}

impl Target {
    pub(super) fn member(&self) -> String {
        format!("{}:{}", self.node, self.executor_id)
    }
}

impl DistributedTurnBackend {
    /// Record a room's agent presence (§10.6.7). Exactly one owner claims each change.
    /// Passing `None` forgets the state and returns `false`, so the next attach publishes again.
    ///
    /// # Errors
    /// The backend is closed, or the Redis registry cannot be reached.
    pub async fn claim_presence(
        &self,
        channel_id: &str,
        room_id: &str,
        online: Option<bool>,
    ) -> Result<bool, RoomKitError> {
        self.ensure_open()?;
        self.listen().await;
        self.registry()
            .claim_presence(channel_id, room_id, online)
            .await
    }

    pub(super) async fn forget_holder(
        &self,
        channel: &str,
        room: &str,
    ) -> Result<(), RoomKitError> {
        self.registry().forget_holder(channel, room).await
    }
}
