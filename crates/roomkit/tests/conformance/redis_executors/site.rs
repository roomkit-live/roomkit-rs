//! A worker the scenario kills has its own Tokio runtime. Dropping that runtime
//! stops its Redis stream and active work without sending a graceful close.
use roomkit::{RedisTurnBackend, RoomKitError, TurnExecutor};
use tokio::sync::oneshot;

use super::topology::Topology;

pub(super) struct Site {
    pub(super) hosting: RedisTurnBackend,
    pub(super) executor: TurnExecutor,
    isolated: Option<Runtime>,
}

impl Site {
    pub(super) async fn start(
        topology: &Topology,
        executor: TurnExecutor,
        isolated: bool,
    ) -> Result<Self, RoomKitError> {
        let hosting = topology.worker()?;
        let runtime = if isolated {
            Some(Runtime::start(hosting.clone(), executor.clone()).await?)
        } else {
            hosting.serve(executor.clone()).await?;
            None
        };
        Ok(Self {
            hosting,
            executor,
            isolated: runtime,
        })
    }

    pub(super) async fn advertise(&self, executor: TurnExecutor) -> Result<(), RoomKitError> {
        let hosting = self.hosting.clone();
        let served = async move { hosting.serve(executor).await };
        if let Some(runtime) = &self.isolated {
            runtime.handle.spawn(served).await.map_err(runtime_error)?
        } else {
            served.await
        }
    }

    pub(super) async fn close(self, topology: &Topology) -> Result<(), RoomKitError> {
        let result = topology.close_worker(&self.hosting, &self.executor).await;
        if let Some(runtime) = self.isolated {
            runtime.stop().await?;
        }
        result
    }

    pub(super) async fn kill(self, topology: &Topology) -> Result<(), RoomKitError> {
        let id = self.executor.executor_id().to_owned();
        drop(self.executor);
        drop(self.hosting);
        let runtime = self
            .isolated
            .ok_or_else(|| RoomKitError::Value(format!("executor {id} has no isolated runtime")))?;
        runtime.stop().await?;
        // A new turn may follow the kill in the next step. Let the public registry
        // discover this loss as it would after the process disappeared.
        let observe = async {
            loop {
                if !topology
                    .owner()
                    .executors()
                    .await?
                    .iter()
                    .any(|info| info.executor_id == id && info.online)
                {
                    return Ok(());
                }
                tokio::time::sleep(topology.owner().timeouts().heartbeat_interval).await;
            }
        };
        tokio::time::timeout(crate::scripted::GATE_BOUND, observe)
            .await
            .map_err(|_| {
                RoomKitError::Runtime(format!(
                    "executor {id} kept beating after its runtime stopped"
                ))
            })?
    }
}

struct Runtime {
    handle: tokio::runtime::Handle,
    stop: oneshot::Sender<()>,
    stopped: oneshot::Receiver<()>,
}

impl Runtime {
    async fn start(
        hosting: RedisTurnBackend,
        executor: TurnExecutor,
    ) -> Result<Self, RoomKitError> {
        let (ready, serving) = oneshot::channel();
        let (stop, stopping) = oneshot::channel();
        let (ended, stopped) = oneshot::channel();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(runtime_error)?;
        let handle = runtime.handle().clone();
        std::thread::Builder::new()
            .name("roomkit-scenario-executor".into())
            .spawn(move || {
                runtime.block_on(async move {
                    let result = hosting.serve(executor).await;
                    let served = result.is_ok();
                    if ready.send(result).is_ok() && served {
                        let _ = stopping.await;
                    }
                });
                drop(runtime);
                let _ = ended.send(());
            })
            .map_err(runtime_error)?;
        serving.await.map_err(runtime_error)??;
        Ok(Self {
            handle,
            stop,
            stopped,
        })
    }

    async fn stop(self) -> Result<(), RoomKitError> {
        let _ = self.stop.send(());
        self.stopped.await.map_err(runtime_error)
    }
}

fn runtime_error(error: impl std::fmt::Display) -> RoomKitError {
    RoomKitError::Runtime(error.to_string())
}
