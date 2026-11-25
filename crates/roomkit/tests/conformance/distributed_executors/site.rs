//! A worker the scenario kills has its own Tokio runtime. Dropping that runtime
//! stops its Redis stream and active work without sending a graceful close.
use roomkit::turns::DistributedTurnBackend;
use roomkit::{RoomKitError, TurnExecutor};
use tokio::sync::oneshot;

use super::topology::Topology;

pub(super) struct Site {
    pub(super) hosting: DistributedTurnBackend,
    pub(super) executor: TurnExecutor,
    mode: Mode,
}

enum Mode {
    Ordinary,
    Isolated(Runtime),
    Silent(super::silent::Silent),
}

impl Site {
    pub(super) async fn start(
        topology: &Topology,
        executor: TurnExecutor,
        isolated: bool,
        kit: Option<&str>,
    ) -> Result<Self, RoomKitError> {
        let hosting = topology.worker(kit)?;
        let mode = if isolated {
            Mode::Isolated(Runtime::start(hosting.clone(), executor.clone()).await?)
        } else {
            hosting.serve(executor.clone()).await?;
            Mode::Ordinary
        };
        Ok(Self {
            hosting,
            executor,
            mode,
        })
    }

    pub(super) async fn silent(
        topology: &Topology,
        executor: TurnExecutor,
        worker: &super::Worker,
        run: std::sync::Arc<crate::run::Run>,
    ) -> Result<Self, RoomKitError> {
        let hosting = topology.worker(worker.kit.as_deref())?;
        let silent = super::silent::Silent::start(
            &topology.fresh(),
            hosting.node_id(),
            executor.clone(),
            worker,
            run,
        )
        .await?;
        Ok(Self {
            hosting,
            executor,
            mode: Mode::Silent(silent),
        })
    }

    pub(super) async fn advertise(&self, executor: TurnExecutor) -> Result<(), RoomKitError> {
        if matches!(self.mode, Mode::Silent(_)) {
            return Err(RoomKitError::Value(format!(
                "silent executor {} does not support pause or resume",
                self.executor.executor_id()
            )));
        }
        let hosting = self.hosting.clone();
        let served = async move { hosting.serve(executor).await };
        if let Mode::Isolated(runtime) = &self.mode {
            runtime.handle.spawn(served).await.map_err(runtime_error)?
        } else {
            served.await
        }
    }

    pub(super) async fn close(self, topology: &Topology) -> Result<(), RoomKitError> {
        if let Mode::Silent(silent) = self.mode {
            let stopped = silent.close().await;
            self.executor.close().await;
            return stopped;
        }
        let result = topology.close_worker(&self.hosting, &self.executor).await;
        if let Mode::Isolated(runtime) = self.mode {
            runtime.stop().await?;
        }
        result
    }

    /// Stops the worker as a killed process stops. With `observe`, waits until
    /// the registry reads it offline; without, returns at once, for a restart
    /// under its id that a supervisor makes as soon as the process is gone.
    pub(super) async fn kill(self, topology: &Topology, observe: bool) -> Result<(), RoomKitError> {
        let id = self.executor.executor_id().to_owned();
        drop(self.executor);
        drop(self.hosting);
        let Mode::Isolated(runtime) = self.mode else {
            return Err(RoomKitError::Value(format!(
                "executor {id} has no isolated runtime"
            )));
        };
        runtime.stop().await?;
        if !observe {
            return Ok(());
        }
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
        hosting: DistributedTurnBackend,
        executor: TurnExecutor,
    ) -> Result<Self, RoomKitError> {
        let (ready, serving) = oneshot::channel();
        let (stop, stopping) = oneshot::channel();
        let (ended, stopped) = oneshot::channel();
        std::thread::Builder::new()
            .name("roomkit-scenario-executor".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(err) => {
                        drop(ready.send(Err(runtime_error(err))));
                        return;
                    }
                };
                let handle = runtime.handle().clone();
                runtime.block_on(async move {
                    let result = hosting.serve(executor).await.map(|()| handle);
                    let served = result.is_ok();
                    if ready.send(result).is_ok() && served {
                        let _ = stopping.await;
                    }
                });
                drop(runtime);
                let _ = ended.send(());
            })
            .map_err(runtime_error)?;
        let handle = serving.await.map_err(runtime_error)??;
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
