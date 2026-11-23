//! The two ends of a registered in-process turn share bounded, numbered pipes.
use serde::de::DeserializeOwned;
use std::sync::{Arc, Mutex, Weak};

use super::{InMemoryTurnBackend, State, Worker, closed};
use crate::RoomKitError;
use crate::turns::pipe::{LinkFailure, PipeReceiver, pipe};
use crate::turns::wire::{Control, Frame};
use crate::turns::{
    Carried, CarriedRequest, ExecutorLink, MemoryTurn, Observers, TurnContext, TurnError,
    TurnFuture, TurnLink, TurnRequest, TurnTimeouts, lock,
};

struct Reader<M> {
    incoming: PipeReceiver<M>,
    failure: Option<LinkFailure>,
}

impl<M: DeserializeOwned> Reader<M> {
    fn new(incoming: PipeReceiver<M>) -> tokio::sync::Mutex<Self> {
        tokio::sync::Mutex::new(Self {
            incoming,
            failure: None,
        })
    }
    async fn get(&mut self) -> Result<(u64, M), RoomKitError> {
        if let Some(reason) = &self.failure {
            return Err(TurnError::link(reason).into_kit_error());
        }
        match self.incoming.get().await {
            Ok(row) => Ok(row),
            Err(reason) => {
                let error = TurnError::link(&reason).into_kit_error();
                self.failure = Some(reason);
                Err(error)
            }
        }
    }
}

struct OwnerLink {
    request: TurnRequest,
    turn: Arc<MemoryTurn>,
    frames: tokio::sync::Mutex<Reader<Frame>>,
    state: Weak<Mutex<State>>,
    token: u64,
}

impl TurnLink for OwnerLink {
    fn request(&self) -> &TurnRequest {
        &self.request
    }
    fn taken(&self) -> bool {
        true
    }
    fn receive(&self) -> TurnFuture<'_, Result<(u64, Frame), RoomKitError>> {
        Box::pin(async { self.frames.lock().await.get().await })
    }
    fn send(&self, message: Control) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async move {
            if matches!(message, Control::Pull { .. }) {
                self.turn.lend_stream_context();
            }
            self.turn
                .controls
                .put(&message)
                .map(|_| ())
                .map_err(|error| RoomKitError::Value(error.0))
        })
    }
    fn release(&self) {
        self.turn.shut();
        if let Some(state) = self.state.upgrade() {
            lock(&state).turns.remove(&self.token);
        }
    }
}
impl Drop for OwnerLink {
    fn drop(&mut self) {
        self.release();
    }
}

struct ExecutorEnd {
    request: TurnRequest,
    turn: Arc<MemoryTurn>,
    controls: tokio::sync::Mutex<Reader<Control>>,
}

impl ExecutorLink for ExecutorEnd {
    fn request(&self) -> &TurnRequest {
        &self.request
    }
    fn receive(&self) -> TurnFuture<'_, Result<Control, RoomKitError>> {
        Box::pin(async {
            self.controls
                .lock()
                .await
                .get()
                .await
                .map(|(_, message)| message)
        })
    }
    fn send(&self, frame: Frame) -> TurnFuture<'_, Result<u64, RoomKitError>> {
        Box::pin(async move {
            self.turn
                .frames
                .put(&frame)
                .map_err(|error| RoomKitError::Value(error.0))
        })
    }
    fn turn_context(&self) -> TurnContext {
        self.turn.turn_context()
    }
    fn stream_context(&self) -> Option<TurnContext> {
        lock(&self.turn.contexts).stream.clone()
    }
}

/// The executor's end of a turn whose owner reads `turn`'s frames and writes
/// the controls `controls` receives: also what another backend hands an
/// executor of this process it placed a turn on.
pub(in crate::turns) fn executor_end(
    request: TurnRequest,
    turn: Arc<MemoryTurn>,
    controls: PipeReceiver<Control>,
) -> Arc<dyn ExecutorLink> {
    Arc::new(ExecutorEnd {
        request,
        turn,
        controls: Reader::new(controls),
    })
}

pub(super) fn submit(
    backend: &InMemoryTurnBackend,
    worker: &Worker,
    request: TurnRequest,
) -> Result<Arc<dyn TurnLink>, RoomKitError> {
    let delivered = CarriedRequest::from_public(&request)
        .and_then(crate::turns::executor::parse)
        .map_err(RoomKitError::Value)?;
    let id = Arc::<str>::from(request.turn_id.as_str());
    let (frames, reading) = pipe(Arc::clone(&id), Carried::Frame, Observers::default());
    let (controls, receiving) = pipe(Arc::clone(&id), Carried::Control, Observers::default());
    let turn = Arc::new(MemoryTurn::new(
        id,
        frames,
        controls,
        TurnTimeouts::default(),
    ));
    let link = executor_end(delivered, Arc::clone(&turn), receiving);
    let token = {
        let mut state = lock(&backend.state);
        if state.closed {
            return Err(closed());
        }
        request.hand_over();
        worker.take(link)?;
        let token = state.next;
        state.next = state.next.wrapping_add(1);
        state.turns.insert(token, Arc::downgrade(&turn));
        token
    };
    Ok(Arc::new(OwnerLink {
        request,
        turn,
        frames: Reader::new(reading),
        state: Arc::downgrade(&backend.state),
        token,
    }))
}
