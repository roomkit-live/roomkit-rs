//! The process heartbeat continues while a turn occupies its runtime.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::registry::{Beat, Declaration, write_beat};
use super::{RedisTurnBackend, redis_error};
use crate::turns::lock;
use crate::{RoomKitError, TurnExecutor};

#[derive(Default)]
pub(super) struct Serving {
    pub(super) workers: BTreeMap<String, TurnExecutor>,
    pub(super) owner: bool,
    generation: u64,
    retiring: Vec<String>,
}

impl RedisTurnBackend {
    /// Advertise and serve this executor. A later registration can replace a closed executor.
    ///
    /// # Errors
    /// A registration on a closed backend fails, as does publishing its first Redis heartbeat.
    pub async fn serve(&self, executor: TurnExecutor) -> Result<(), RoomKitError> {
        if *self.0.stop.borrow() {
            return Err(super::routes::closed());
        }
        {
            let mut serving = lock(&self.0.serving);
            let retired: Vec<_> = serving
                .workers
                .values()
                .filter(|w| w.closed())
                .map(|w| w.executor_id().to_owned())
                .collect();
            for id in retired {
                serving.workers.remove(&id);
                serving.retiring.push(id);
            }
            serving.retiring.retain(|id| id != executor.executor_id());
            serving
                .workers
                .insert(executor.executor_id().into(), executor);
            serving.generation = serving.generation.saturating_add(1);
        }
        self.start().await
    }

    pub(super) async fn attend(&self) -> Result<(), RoomKitError> {
        if *self.0.stop.borrow() {
            return Err(super::routes::closed());
        }
        {
            let mut serving = lock(&self.0.serving);
            if !serving.owner {
                serving.owner = true;
                serving.generation = serving.generation.saturating_add(1);
            }
        }
        self.start().await
    }

    async fn start(&self) -> Result<(), RoomKitError> {
        self.listen().await;
        let mut conn = self.connection().await?;
        write_beat(self, &mut conn, &self.snapshot()).await?;
        self.0
            .heartbeat
            .get_or_try_init(|| async {
                let node = Arc::downgrade(&self.0);
                let quitting = Arc::clone(&self.0.quitting);
                let interval = self
                    .timeouts()
                    .heartbeat_interval
                    .max(Duration::from_millis(1));
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|err| RoomKitError::Runtime(err.to_string()))?;
                std::thread::Builder::new()
                    .name("roomkit-redis-heartbeat".into())
                    .spawn(move || {
                        runtime.block_on(async {
                            let mut conn = None;
                            while !quitting.load(Ordering::Acquire) {
                                tokio::time::sleep(interval).await;
                                if quitting.load(Ordering::Acquire) {
                                    break;
                                }
                                let Some(owner) = node.upgrade() else {
                                    break;
                                };
                                let backend = Self(owner);
                                let result = async {
                                    let connection = match &mut conn {
                                        Some(connection) => connection,
                                        slot @ None => {
                                            slot.insert(backend.beat_connection().await?)
                                        }
                                    };
                                    write_beat(&backend, connection, &backend.snapshot()).await
                                }
                                .await;
                                if let Err(error) = result {
                                    conn = None;
                                    tracing::warn!(%error, "Turn backend heartbeat failed");
                                }
                            }
                        });
                    })
                    .map_err(|e| RoomKitError::Runtime(e.to_string()))?;
                Ok::<(), RoomKitError>(())
            })
            .await?;
        Ok(())
    }

    async fn beat_connection(&self) -> Result<redis::aio::MultiplexedConnection, RoomKitError> {
        let bound = (self.timeouts().heartbeat_timeout / 2).max(Duration::from_millis(1));
        let cfg = redis::AsyncConnectionConfig::new()
            .set_connection_timeout(Some(bound))
            .set_response_timeout(Some(bound));
        self.0
            .address
            .get_multiplexed_async_connection_with_config(&cfg)
            .await
            .map_err(|error| redis_error(&error))
    }

    fn snapshot(&self) -> Beat {
        let scope = lock(&self.0.serving);
        let node = self.node_id();
        let mut beat = Beat {
            node: node.into(),
            generation: scope.generation,
            owner: scope.owner,
            running: lock(&self.0.routes).running(),
            ..Beat::default()
        };
        for (id, worker) in &scope.workers {
            let member = format!("{node}:{id}");
            if worker.closed() {
                beat.gone.push(member);
                continue;
            }
            if let Some(channels) = worker.hosted() {
                let decl = Declaration {
                    node: node.into(),
                    executor_id: id.clone(),
                    channels,
                    room_hooks: worker.hooks().defined_room_hooks(),
                    organizations: worker.advertised_organizations(),
                };
                if let Ok(raw) = serde_json::to_string(&decl) {
                    beat.declared.insert(member, raw);
                }
            }
        }
        beat.gone
            .extend(scope.retiring.iter().map(|id| format!("{node}:{id}")));
        beat
    }

    /// Withdraw this node and close the links it owns.
    ///
    /// # Errors
    /// Redis may be unavailable when withdrawing. Expired heartbeats are forgotten by readers.
    pub async fn close(&self) -> Result<(), RoomKitError> {
        let _closed = self.0.closing.lock().await;
        if *self.0.stop.borrow() {
            return Ok(());
        }
        self.0.quitting.store(true, Ordering::Release);
        self.drain_outbox(self.timeouts().bounded().cancel).await;
        self.0.stop.send_replace(true);
        lock(&self.0.routes).close();
        if self.0.data.get().is_none() {
            return Ok(());
        }
        let mut conn = self.connection().await?;
        let mut snapshot = self.snapshot();
        snapshot.generation = snapshot.generation.saturating_add(1);
        snapshot
            .gone
            .extend(std::mem::take(&mut snapshot.declared).into_keys());
        snapshot.owner = false;
        snapshot.running.clear();
        write_beat(self, &mut conn, &snapshot).await?;
        redis::cmd("ZREM")
            .arg(self.key("owners"))
            .arg(self.node_id())
            .exec_async(&mut conn)
            .await
            .map_err(|error| redis_error(&error))
    }
}

impl Drop for super::Node {
    fn drop(&mut self) {
        self.quitting.store(true, Ordering::Release);
    }
}
