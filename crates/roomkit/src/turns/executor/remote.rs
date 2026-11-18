//! A backend hands an executor one received turn. Transport readers and writers
//! forward the numbered envelopes; the ordinary executor owns its entire lifetime.

use std::sync::Arc;

use super::{Build, Executor, FrameSender, Incoming};
use crate::error::RoomKitError;
use crate::turn::TurnKit;
use crate::turns::context::TurnContext;
use crate::turns::observe::Carried;
use crate::turns::pipe::{PipeReceiver, pipe};
use crate::turns::wire::{Frame, TurnRequest};
use crate::turns::{MemoryTurn, Turns};

/// The transport that supplied the request and the context it lends to the worker.
#[derive(Default)]
pub(crate) struct WorkerOrigin {
    pub(crate) context: TurnContext,
    pub(crate) waits_for_writes: bool,
    #[cfg(feature = "redis")]
    pub(crate) owner: Option<crate::turns::redis::PeerOwner>,
}

/// The transport side of a worker's running turn.
pub(crate) struct WorkerTurn {
    memory: Arc<MemoryTurn>,
    frames: PipeReceiver<Frame>,
    _writes: Option<tokio::sync::oneshot::Sender<()>>,
    #[cfg(feature = "redis")]
    cut: tokio::sync::watch::Sender<Option<&'static str>>,
}

impl WorkerTurn {
    /// Admit one request and run it on the same engine that serves a local owner.
    pub(crate) fn start(
        turns: &Arc<Turns>,
        kit: &Arc<dyn TurnKit>,
        executor_id: String,
        request: TurnRequest,
        build: Build,
        origin: WorkerOrigin,
    ) -> Result<Self, RoomKitError> {
        let id: Arc<str> = Arc::from(request.turn_id.as_str());
        let running = turns
            .executor_lifecycle
            .admit(Arc::clone(&id))
            .ok_or_else(|| RoomKitError::TurnLink {
                reason: "no_executor".to_owned(),
                message: Some("the executor is closed or this turn is already running".to_owned()),
            })?;
        let observers = turns.observers();
        let (frames, received) = pipe(Arc::clone(&id), Carried::Frame, observers.clone());
        let (controls, incoming) = pipe(Arc::clone(&id), Carried::Control, observers);
        let memory =
            MemoryTurn::with_context(id, frames, controls, turns.timeouts(), origin.context);
        #[cfg(feature = "redis")]
        let memory = MemoryTurn {
            owner: origin.owner,
            ..memory
        };
        let memory = Arc::new(memory);
        #[cfg(feature = "redis")]
        let cut = running.cutter();
        let executor = Executor {
            running,
            stale: false,
            id: executor_id,
            turns: Arc::clone(turns),
            turn: Arc::clone(&memory),
            sender: Arc::new(FrameSender::new(Arc::clone(&memory))),
            kit: Arc::clone(kit),
        };
        let end = Release(Arc::clone(&memory));
        let (writes, wait) = origin
            .waits_for_writes
            .then(tokio::sync::oneshot::channel)
            .unzip();
        let work = async move {
            executor
                .run(incoming, Incoming::Worker(Box::new(request), build))
                .await;
            drop(end);
            if let Some(wait) = wait {
                drop(wait.await);
            }
            drop(executor);
        };
        if !kit.spawn(Box::pin(TurnContext::default().scope(work))) {
            return Err(RoomKitError::KitClosed);
        }
        Ok(Self {
            memory,
            frames: received,
            _writes: writes,
            #[cfg(feature = "redis")]
            cut,
        })
    }

    /// Accept the owner's numbered control envelope.
    #[cfg(any(test, feature = "redis"))]
    pub(crate) fn control(&self, envelope: String) {
        self.memory.controls.deliver_raw(envelope);
    }

    /// A backend close cuts local work without consuming a sequence number of the owner.
    #[cfg(feature = "redis")]
    pub(crate) fn cut_for_shutdown(&self) {
        self.cut.send_replace(Some("backend_closed"));
    }

    /// Forward a local control with the first stream reader's context, once its
    /// pull has been issued. Remote Redis turns receive no process-local context.
    #[cfg(feature = "redis")]
    pub(crate) fn local_control(&self, envelope: String, owner: &MemoryTurn) {
        if let Some(context) = crate::turns::lock(&owner.contexts).stream.clone() {
            self.memory.first_stream_context(context);
        }
        self.control(envelope);
    }

    /// The next numbered frame for the owner, through the terminal frame.
    #[cfg(any(test, feature = "redis"))]
    pub(crate) async fn next_frame(&mut self) -> Option<String> {
        self.frames.next_raw().await
    }
}

impl Drop for WorkerTurn {
    fn drop(&mut self) {
        self.memory.shut();
    }
}

struct Release(Arc<MemoryTurn>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.shut();
    }
}

pub(crate) fn factory_error(error: &RoomKitError) -> Box<crate::turns::wire::WireError> {
    Box::new(crate::turns::error::turn_failure(error))
}

pub(crate) fn missing(
    id: &roomkit_core::model::ChannelId,
    executor: &str,
) -> Box<crate::turns::wire::WireError> {
    no_executor(format!("Executor {executor} builds no channel '{id}'"))
}

pub(crate) fn refused(
    id: &roomkit_core::model::ChannelId,
    executor: &str,
    organization: Option<&str>,
) -> Box<crate::turns::wire::WireError> {
    let room = organization.map_or_else(
        || "a room without an organization".to_owned(),
        |org| format!("organization '{org}'"),
    );
    no_executor(format!(
        "Executor {executor} does not serve {room} (channel '{id}')"
    ))
}

pub(crate) fn no_executor(message: String) -> Box<crate::turns::wire::WireError> {
    let mut error = crate::turns::error::one("roomkit.core.exceptions:TurnLinkError", message);
    error
        .bases
        .push("roomkit.core.exceptions:RoomKitError".into());
    error
        .attributes
        .insert("reason".into(), "no_executor".into());
    Box::new(error)
}

struct LinkEnd(Arc<dyn crate::turns::ExecutorLink>);

impl Drop for LinkEnd {
    fn drop(&mut self) {
        self.0.turn_ended();
    }
}

impl WorkerTurn {
    /// Drive both directions independently. Receiving a cancel never waits on an outgoing write.
    pub(crate) fn serve_link(
        mut self,
        application: Arc<dyn crate::turns::ExecutorLink>,
    ) -> impl Future<Output = ()> + Send + 'static {
        let end = LinkEnd(application);
        async move {
            {
                let link = &*end.0;
                let incoming = read_controls(link, &self.memory);
                let outgoing = write_frames(link, &mut self.frames);
                tokio::pin!(outgoing);
                tokio::select! {
                    () = incoming => outgoing.await,
                    () = &mut outgoing => {}
                }
            }
            drop(end);
            drop(self);
        }
    }
}

async fn read_controls(link: &dyn crate::turns::ExecutorLink, memory: &MemoryTurn) {
    loop {
        let message = match link.receive().await {
            Ok(message) => message,
            Err(RoomKitError::TurnLink { reason, .. }) if reason == "frame_lost" => {
                memory
                    .controls
                    .end(crate::turns::pipe::LinkFailure::FrameLost);
                return;
            }
            Err(_) => break,
        };
        if matches!(message, crate::turns::wire::Control::Pull { .. })
            && let Some(ctx) = link.stream_context()
        {
            memory.first_stream_context(ctx);
        }
        if let Err(error) = memory.controls.put(&message) {
            tracing::warn!(reason=%error.0, "The executor could not carry an incoming control");
            break;
        }
    }
    memory.controls.shut();
}

async fn write_frames(link: &dyn crate::turns::ExecutorLink, frames: &mut PipeReceiver<Frame>) {
    while let Ok((seq, frame)) = frames.get().await {
        match link.send(frame).await {
            Ok(written) if written == seq => {}
            Ok(written) => {
                tracing::warn!(
                    seq,
                    written,
                    "The executor link assigned a different frame number"
                );
                break;
            }
            Err(error) => {
                tracing::warn!(%error, "The executor link could not write a frame");
                break;
            }
        }
    }
}
