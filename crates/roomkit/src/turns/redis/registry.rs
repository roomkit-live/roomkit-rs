//! Executor declarations and heartbeats use Redis server time in milliseconds.
use std::collections::{BTreeMap, BTreeSet};

use roomkit_core::model::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{RedisTurnBackend, redis_error};
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

#[derive(Deserialize)]
struct View {
    now: i64,
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
    let timeout =
        i64::try_from(backend.timeouts().heartbeat_timeout.as_millis()).unwrap_or(i64::MAX);
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
        .map_err(redis_error)?;
    let view: View =
        serde_json::from_str(&body).map_err(|e| RoomKitError::Runtime(e.to_string()))?;
    Ok(view.declared(timeout))
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
        .map_err(redis_error)
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
    pub(super) async fn place(
        &self,
        channel: &str,
        room: &str,
        hooks: &[String],
        organization: Option<&str>,
        refused: &BTreeSet<String>,
        spread: bool,
    ) -> Result<Target, RoomKitError> {
        use std::sync::atomic::Ordering;
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
            .map_err(redis_error)?;
        let target = if let Some(target) = held.and_then(|member| takers.remove(&member)) {
            target
        } else if !takers.is_empty() {
            let previous = if spread {
                self.0.choice.fetch_add(1, Ordering::Relaxed)
            } else {
                self.0.choice.load(Ordering::Relaxed)
            };
            let offset = previous.wrapping_add(1) % takers.len();
            takers
                .into_values()
                .nth(offset)
                .expect("choice modulo a nonempty collection")
        } else {
            return Err(RoomKitError::TurnLink {
                reason: "no_executor".into(),
                message: Some({
                    let hooks = if hooks.is_empty() {
                        String::new()
                    } else {
                        format!(
                            " with room hooks [{}]",
                            hooks
                                .iter()
                                .map(|s| crate::error::repr(s))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    };
                    let org = organization.map_or_else(String::new, |o| {
                        format!(" in organization {}", crate::error::repr(o))
                    });
                    format!(
                        "No executor serves channel {}{hooks}{org}",
                        crate::error::repr(channel)
                    )
                }),
            });
        };
        if spread {
            redis::cmd("SET")
                .arg(holder)
                .arg(target.member())
                .arg("EX")
                .arg(7 * 24 * 3600)
                .exec_async(&mut conn)
                .await
                .map_err(redis_error)?;
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
            .map_err(redis_error)
    }
}
