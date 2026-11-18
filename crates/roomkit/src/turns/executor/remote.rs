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

/// The transport side of a worker's running turn.
pub(crate) struct WorkerTurn {
    memory: Arc<MemoryTurn>,
    frames: PipeReceiver<Frame>,
}

impl WorkerTurn {
    /// Admit one request and run it on the same engine that serves a local owner.
    pub(crate) fn start(
        turns: &Arc<Turns>,
        kit: Arc<dyn TurnKit>,
        executor_id: String,
        request: TurnRequest,
        build: Build,
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
        let memory = Arc::new(MemoryTurn::with_context(
            id,
            frames,
            controls,
            turns.timeouts(),
            TurnContext::default(),
        ));
        let executor = Executor {
            running,
            stale: false,
            id: executor_id,
            turns: Arc::clone(turns),
            turn: Arc::clone(&memory),
            sender: Arc::new(FrameSender::new(Arc::clone(&memory))),
            kit: Arc::clone(&kit),
        };
        let end = Release(Arc::clone(&memory));
        let work = async move {
            let _end = end;
            executor
                .run(incoming, Incoming::Worker(Box::new(request), build))
                .await;
        };
        if !kit.spawn(Box::pin(TurnContext::default().scope(work))) {
            return Err(RoomKitError::KitClosed);
        }
        Ok(Self {
            memory,
            frames: received,
        })
    }

    /// Accept the owner's numbered control envelope.
    pub(crate) fn control(&self, envelope: String) {
        self.memory.controls.deliver_raw(envelope);
    }

    /// The next numbered frame for the owner, through the terminal frame.
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

pub(crate) fn factory_error(error: &RoomKitError) -> crate::turns::wire::WireError {
    crate::turns::error::turn_failure(error)
}

pub(crate) fn missing(
    id: &roomkit_core::model::ChannelId,
    executor: &str,
) -> crate::turns::wire::WireError {
    no_executor(format!("Executor {executor} builds no channel '{id}'"))
}

pub(crate) fn refused(
    id: &roomkit_core::model::ChannelId,
    executor: &str,
    organization: Option<&str>,
) -> crate::turns::wire::WireError {
    let room = organization.map_or_else(
        || "a room without an organization".to_owned(),
        |org| format!("organization '{org}'"),
    );
    no_executor(format!(
        "Executor {executor} does not serve {room} (channel '{id}')"
    ))
}

pub(crate) fn no_executor(message: String) -> crate::turns::wire::WireError {
    let mut error = crate::turns::error::one("roomkit.core.exceptions:TurnLinkError", message);
    error
        .bases
        .push("roomkit.core.exceptions:RoomKitError".into());
    error
        .attributes
        .insert("reason".into(), "no_executor".into());
    error
}
