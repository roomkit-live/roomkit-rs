//! The process heartbeat continues while a turn occupies its runtime.
use std::sync::Arc;
use std::time::Duration;

use super::Worker;
use super::registry::{Beat, Declaration, write_beat};
use super::{RedisTurnBackend, redis_error};
use crate::RoomKitError;
use crate::turns::lock;

#[derive(Default)]
pub(super) struct Serving {
    /// Registration order resolves the first eligible worker in this process.
    pub(super) workers: Vec<Worker>,
    pub(super) owner: bool,
    generation: u64,
    retiring: Vec<String>,
    closing: bool,
}

impl RedisTurnBackend {
    pub(super) fn worker(&self, id: &str) -> Option<Worker> {
        lock(&self.0.serving)
            .workers
            .iter()
            .find(|worker| worker.executor_id() == id)
            .cloned()
    }

    /// Advertise and serve this executor. A later registration can replace a closed executor.
    ///
    /// # Errors
    /// A registration on a closed backend fails, as does publishing its first Redis heartbeat.
    pub async fn serve<S: crate::ConversationStore>(
        &self,
        executor: crate::TurnExecutor<S>,
    ) -> Result<(), RoomKitError> {
        self.serve_erased(Arc::new(executor)).await
    }

    pub(crate) async fn serve_erased(&self, executor: Worker) -> Result<(), RoomKitError> {
        let owned = {
            let mut serving = lock(&self.0.serving);
            if serving.closing {
                return Err(super::routes::closed());
            }
            executor.reach_owner(Arc::new(self.local_owner()));
            let owned = self.0.owner_worker.get().is_some() && self.is_own(&executor);
            if !owned {
                executor.native().served_by(self);
            }
            let id = executor.executor_id();
            let Serving {
                workers, retiring, ..
            } = &mut *serving;
            retiring.retain(|old| old != id);
            workers.retain(|previous| {
                if previous.executor_id() == id {
                    return false;
                }
                if previous.closed() {
                    retiring.push(previous.executor_id().to_owned());
                    return false;
                }
                true
            });
            // A kit's own worker can be recovered from the owner's weak handle.
            // It advertises no remote hosts and must not keep its kit alive.
            if !owned || executor.hosted().is_some() {
                workers.push(executor);
            }
            serving.generation = serving.generation.saturating_add(1);
            owned
        };
        self.start().await?;
        if !owned {
            self.notify_coverage(super::CoverageNotice::ExecutorsChanged)
                .await;
        }
        Ok(())
    }

    pub(super) async fn attend(&self) -> Result<(), RoomKitError> {
        let first_use = {
            let mut serving = lock(&self.0.serving);
            if serving.closing {
                return Err(super::routes::closed());
            }
            let first = !serving.owner;
            if first {
                serving.owner = true;
                serving.generation = serving.generation.saturating_add(1);
            }
            first
        };
        self.start().await?;
        if first_use {
            self.notify_coverage(super::CoverageNotice::OwnerAttended)
                .await;
        }
        Ok(())
    }

    pub(super) async fn worker_closed(&self) -> Result<(), RoomKitError> {
        {
            let mut serving = lock(&self.0.serving);
            if serving.closing {
                return Ok(());
            }
            serving.generation = serving.generation.saturating_add(1);
        }
        let mut conn = self.connection().await?;
        write_beat(self, &mut conn, &self.snapshot()).await?;
        self.notify_coverage(super::CoverageNotice::ExecutorsChanged)
            .await;
        Ok(())
    }

    async fn start(&self) -> Result<(), RoomKitError> {
        let mut draining = self.0.draining.subscribe();
        tokio::select! {
            biased;
            _ = draining.wait_for(|closing| *closing) => Err(super::routes::closed()),
            started = self.announce() => started,
        }
    }

    async fn announce(&self) -> Result<(), RoomKitError> {
        self.listen().await;
        let mut conn = self.connection().await?;
        write_beat(self, &mut conn, &self.snapshot()).await?;
        self.0
            .heartbeat
            .get_or_try_init(|| async {
                let node = Arc::downgrade(&self.0);
                let mut stop = self.0.stop.subscribe();
                let (exited, done) = tokio::sync::watch::channel(());
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
                        let _exited = exited;
                        runtime.block_on(async {
                            let mut conn = None;
                            while !*stop.borrow() {
                                tokio::select! {
                                    _ = stop.changed() => break,
                                    () = tokio::time::sleep(interval) => {},
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
                                };
                                let result = tokio::select! {
                                    _ = stop.changed() => break,
                                    result = result => result,
                                };
                                if let Err(error) = result {
                                    conn = None;
                                    tracing::warn!(%error, "Turn backend heartbeat failed");
                                }
                            }
                        });
                    })
                    .map_err(|e| RoomKitError::Runtime(e.to_string()))?;
                Ok::<_, RoomKitError>(done)
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
        for worker in &scope.workers {
            let id = worker.executor_id();
            let member = format!("{node}:{id}");
            if scope.closing && !worker.closed() {
                beat.undeclared.push(member);
                continue;
            }
            if worker.closed() {
                beat.gone.push(member);
                continue;
            }
            if let Some(channels) = worker.hosted() {
                let decl = Declaration {
                    node: node.into(),
                    executor_id: id.to_owned(),
                    channels,
                    room_hooks: worker.room_hooks(),
                    organizations: worker.organizations(),
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
        let mut closed = self.0.closing.lock().await;
        if *closed {
            return Ok(());
        }
        let served_workers = {
            let mut serving = lock(&self.0.serving);
            serving.closing = true;
            serving.generation = serving.generation.saturating_add(1);
            lock(&self.0.routes).begin_close();
            self.0.draining.send_replace(true);
            !serving.workers.is_empty()
        };
        let bound = self.timeouts().bounded().cancel;
        // An owner can still be placing from its previous heartbeat view. Give the
        // request an interval to arrive and be refused, so it can be placed again.
        let grace = async {
            if served_workers {
                tokio::time::sleep(self.timeouts().interval()).await;
            }
        };
        let undeclared = self.publish_shutdown(false).await;
        tokio::join!(self.drain_jobs(), grace);
        self.drain_outbox(bound).await;
        self.0.stop.send_replace(true);
        if let Some(reader) = self.0.reader.get() {
            reader.abort();
        }
        if let Some(done) = self.0.heartbeat.get() {
            let mut done = done.clone();
            let _ = tokio::time::timeout(bound, done.changed()).await;
        }
        let withdrawn = self.publish_shutdown(true).await;
        *closed = withdrawn.is_ok();
        undeclared.and(withdrawn)
    }

    async fn publish_shutdown(&self, finished: bool) -> Result<(), RoomKitError> {
        let Some(conn) = self.0.data.get() else {
            return Ok(());
        };
        let mut conn = conn.clone();
        let mut beat = self.snapshot();
        if finished {
            beat.generation = beat.generation.saturating_add(1);
            beat.gone.append(&mut beat.undeclared);
            beat.owner = false;
            beat.running.clear();
        }
        let publish = async {
            write_beat(self, &mut conn, &beat).await?;
            if finished {
                redis::cmd("ZREM")
                    .arg(self.key("owners"))
                    .arg(self.node_id())
                    .exec_async(&mut conn)
                    .await
                    .map_err(|error| redis_error(&error))?;
            }
            Ok(())
        };
        tokio::time::timeout(self.timeouts().bounded().cancel, publish)
            .await
            .map_err(|_| {
                RoomKitError::Runtime("Timed out withdrawing the Redis turn backend".into())
            })?
    }
}

#[cfg(test)]
mod injected_client_tests;
