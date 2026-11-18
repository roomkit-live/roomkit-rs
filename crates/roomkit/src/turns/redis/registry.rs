//! Executor declarations and heartbeats use Redis server time in milliseconds.
use std::collections::{BTreeMap, BTreeSet};

use roomkit_core::model::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{RedisTurnBackend, redis_error};
use crate::turns::{ExecutorInfo, ExecutorInfoOptions};
use crate::{RoomKitError, TurnExecutor};

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Declaration {
    pub(super) node: String,
    pub(super) executor_id: String,
    pub(super) channels: BTreeSet<String>,
    pub(super) room_hooks: BTreeSet<String>,
    pub(super) organizations: Option<BTreeSet<String>>,
}

#[derive(Deserialize)]
struct View {
    now: i64,
    owners: Value,
    executors: Value,
}

impl View {
    fn declared(self, timeout_ms: i64) -> Vec<ExecutorInfo> {
        // Redis Lua encodes an empty table as either [] or {}.
        let Ok(entries) = serde_json::from_value::<BTreeMap<String, (i64, Value)>>(self.executors)
        else {
            return Vec::new();
        };
        entries
            .into_iter()
            .filter_map(|(member, (beat, body))| {
                let raw = body.as_str()?;
                let Ok(declaration) = serde_json::from_str::<Declaration>(raw) else {
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
                        online: self.now.saturating_sub(beat) < timeout_ms,
                    },
                ))
            })
            .collect()
    }
}

pub(super) async fn executors(
    backend: &RedisTurnBackend,
) -> Result<Vec<ExecutorInfo>, RoomKitError> {
    let (view, timeout) = read_view(backend).await?;
    let node = backend.node_id();
    let mut rows: Vec<_> = crate::turns::lock(&backend.0.serving)
        .workers
        .values()
        .filter(|worker| !worker.closed())
        .map(|worker| local_info(worker, node))
        .collect();
    rows.extend(
        view.declared(timeout)
            .into_iter()
            .filter(|row| row.node.as_deref() != Some(node)),
    );
    Ok(rows)
}

/// Locally served workers have live declarations, including workers whose
/// dynamic host selection cannot be announced to other Redis nodes.
fn local_info(worker: &TurnExecutor, node: &str) -> ExecutorInfo {
    ExecutorInfo::new(
        worker.executor_id(),
        ExecutorInfoOptions {
            node: Some(node.to_owned()),
            hosts: worker.hosted(),
            organizations: worker.advertised_organizations(),
            room_hooks: worker.hooks().defined_room_hooks(),
            ..ExecutorInfoOptions::default()
        },
    )
}

/// A draining executor still answers work it accepted after its declaration was withdrawn.
pub(super) async fn member_alive(
    backend: &RedisTurnBackend,
    member: &str,
) -> Result<bool, RoomKitError> {
    let (view, timeout) = read_view(backend).await?;
    Ok(view
        .executors
        .get(member)
        .and_then(|row| row.get(0))
        .and_then(Value::as_i64)
        .is_some_and(|last| view.now.saturating_sub(last) < timeout))
}

async fn read_view(backend: &RedisTurnBackend) -> Result<(View, i64), RoomKitError> {
    let timeout =
        i64::try_from(backend.timeouts().heartbeat_timeout.as_millis()).unwrap_or(i64::MAX);
    Ok((view(backend, timeout).await?, timeout))
}

/// Check against Redis server time, the same clock that wrote the owner heartbeat.
pub(super) async fn owner_alive(
    backend: &RedisTurnBackend,
    peer: &str,
) -> Result<bool, RoomKitError> {
    let timeout =
        i64::try_from(backend.timeouts().heartbeat_timeout.as_millis()).unwrap_or(i64::MAX);
    let current = view(backend, timeout).await?;
    Ok(current
        .owners
        .get(peer)
        .and_then(Value::as_i64)
        .is_some_and(|beat| current.now.saturating_sub(beat) < timeout))
}

async fn view(backend: &RedisTurnBackend, timeout: i64) -> Result<View, RoomKitError> {
    let mut connection = backend.connection().await?;
    let body: String = redis::cmd("EVAL")
        .arg(include_str!("view.lua"))
        .arg(3)
        .arg(backend.key("alive"))
        .arg(backend.key("executors"))
        .arg(backend.key("owners"))
        .arg(timeout.saturating_mul(10))
        .query_async(&mut connection)
        .await
        .map_err(|error| redis_error(&error))?;
    serde_json::from_str(&body).map_err(|e| RoomKitError::Runtime(e.to_string()))
}

/// The Python and Rust workers publish the same heartbeat, including the node's
/// active requests, so the owner can distinguish a busy worker from a lost turn.
#[derive(Default, Serialize)]
pub(super) struct Beat {
    pub(super) node: String,
    pub(super) generation: u64,
    pub(super) declared: BTreeMap<String, String>,
    pub(super) undeclared: Vec<String>,
    pub(super) gone: Vec<String>,
    pub(super) running: Vec<String>,
    pub(super) owner: bool,
}

pub(super) async fn write_beat(
    backend: &RedisTurnBackend,
    connection: &mut impl redis::aio::ConnectionLike,
    beat: &Beat,
) -> Result<(), RoomKitError> {
    let suffix = |key| backend.key(&format!("{key}:{}", beat.node));
    let payload = serde_json::to_string(beat).map_err(|e| RoomKitError::Runtime(e.to_string()))?;
    redis::cmd("EVAL")
        .arg(include_str!("beat.lua"))
        .arg(6)
        .arg(backend.key("alive"))
        .arg(backend.key("executors"))
        .arg(backend.key("owners"))
        .arg(suffix("inbox"))
        .arg(suffix("running"))
        .arg(suffix("generation"))
        .arg(backend.ttl_ms())
        .arg(payload)
        .exec_async(connection)
        .await
        .map_err(|error| redis_error(&error))
}

/// Python's urllib quote(safe="") applied to one key component.
fn quoted(input: &str) -> String {
    use std::fmt::Write;
    let mut encoded = String::new();
    for byte in input.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.~".contains(&byte) {
            encoded.push(char::from(byte));
        } else {
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
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

impl RedisTurnBackend {
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
        let key = self.key(&format!(
            "presence:{}:{}",
            quoted(room_id),
            quoted(channel_id)
        ));
        let state = online.map_or("", |value| if value { "online" } else { "offline" });
        let mut connection = self.connection().await?;
        redis::cmd("EVAL")
            .arg(include_str!("claim_presence.lua"))
            .arg(1)
            .arg(key)
            .arg(state)
            .arg(7 * 24 * 60 * 60 * 1000)
            .query_async(&mut connection)
            .await
            .map_err(|error| redis_error(&error))
    }

    pub(super) async fn place(
        &self,
        channel: &str,
        room: &str,
        hooks: &[String],
        organization: Option<&str>,
        refused: &BTreeSet<String>,
        spread: bool,
    ) -> Result<Target, RoomKitError> {
        let mut takers = BTreeMap::new();
        for info in self.executors().await? {
            if info.covers(channel, hooks, organization)?
                && let Some(node) = info.node
            {
                let target = Target {
                    node,
                    executor_id: info.executor_id,
                };
                let member = target.member();
                if !refused.contains(&member) {
                    takers.insert(member, target);
                }
            }
        }
        let holder = self.key(&format!("holder:{}:{}", quoted(channel), quoted(room)));
        let mut conn = self.connection().await?;
        let held: Option<String> = redis::cmd("GET")
            .arg(&holder)
            .query_async(&mut conn)
            .await
            .map_err(|error| redis_error(&error))?;
        let target = held
            .and_then(|member| takers.remove(&member))
            .or_else(|| {
                let count = std::num::NonZeroUsize::new(takers.len())?;
                let mut next = crate::turns::lock(&self.0.choice);
                let following = next.wrapping_add(1) % count;
                if spread {
                    *next = following;
                }
                takers.into_values().nth(following)
            })
            .ok_or_else(|| no_executor(channel, hooks, organization))?;
        if spread {
            redis::cmd("SET")
                .arg(holder)
                .arg(target.member())
                .arg("EX")
                .arg(7 * 24 * 3600)
                .exec_async(&mut conn)
                .await
                .map_err(|error| redis_error(&error))?;
        }
        Ok(target)
    }
}

impl RedisTurnBackend {
    pub(super) async fn forget_holder(
        &self,
        channel: &str,
        room: &str,
    ) -> Result<(), RoomKitError> {
        let holder = self.key(&format!("holder:{}:{}", quoted(channel), quoted(room)));
        let mut conn = self.connection().await?;
        redis::cmd("DEL")
            .arg(holder)
            .exec_async(&mut conn)
            .await
            .map_err(|error| redis_error(&error))
    }
}

fn no_executor(channel: &str, hooks: &[String], organization: Option<&str>) -> RoomKitError {
    let hooks = if hooks.is_empty() {
        String::new()
    } else {
        let names = hooks
            .iter()
            .map(|s| crate::error::repr(s))
            .collect::<Vec<_>>()
            .join(", ");
        format!(" with room hooks [{names}]")
    };
    let org = organization.map_or_else(String::new, |o| {
        format!(" in organization {}", crate::error::repr(o))
    });
    RoomKitError::TurnLink {
        reason: "no_executor".into(),
        message: Some(format!(
            "No executor serves channel {}{hooks}{org}",
            crate::error::repr(channel)
        )),
    }
}
