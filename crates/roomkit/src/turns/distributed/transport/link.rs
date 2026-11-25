//! A public owner reads the same numbered pipes the built-in driver receives.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex;

use crate::RoomKitError;
use crate::turns::observe::{Carried, Observers};
use crate::turns::pipe::{LinkFailure, PipeReceiver, pipe};
use crate::turns::wire::{Control, Frame};
use crate::turns::{MemoryTurn, TurnError, TurnFuture, TurnLink, TurnRequest, TurnTimeouts};

pub(in crate::turns::distributed) struct RedisLink {
    request: TurnRequest,
    pub(in crate::turns::distributed) memory: Arc<MemoryTurn>,
    reader: Mutex<Reader>,
    pub(in crate::turns::distributed) read_end: Arc<AtomicBool>,
}

struct Reader {
    frames: PipeReceiver<Frame>,
    ended: Option<LinkFailure>,
}

impl RedisLink {
    pub(in crate::turns::distributed) fn new(
        request: TurnRequest,
        timeouts: TurnTimeouts,
    ) -> (Self, PipeReceiver<Control>) {
        let id = Arc::<str>::from(request.turn_id.as_str());
        let (frames, read) = pipe(Arc::clone(&id), Carried::Frame, Observers::default());
        let (controls, outgoing) = pipe(Arc::clone(&id), Carried::Control, Observers::default());
        let memory = Arc::new(MemoryTurn::new(id, frames, controls, timeouts));
        (
            Self {
                request,
                memory,
                read_end: Arc::default(),
                reader: Mutex::new(Reader {
                    frames: read,
                    ended: None,
                }),
            },
            outgoing,
        )
    }
}

impl TurnLink for RedisLink {
    fn request(&self) -> &TurnRequest {
        &self.request
    }

    fn taken(&self) -> bool {
        true
    }

    fn receive(&self) -> TurnFuture<'_, Result<(u64, Frame), RoomKitError>> {
        Box::pin(async move {
            let mut read = self.reader.lock().await;
            if let Some(error) = &read.ended {
                return Err(TurnError::link(error).into_kit_error());
            }
            match read.frames.get().await {
                Ok((seq, frame)) => {
                    if matches!(frame, Frame::End) {
                        self.read_end.store(true, Ordering::Release);
                    }
                    Ok((seq, frame))
                }
                Err(error) => {
                    let failure = TurnError::link(&error).into_kit_error();
                    read.ended = Some(error);
                    Err(failure)
                }
            }
        })
    }

    fn send(&self, message: Control) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async move {
            if matches!(message, Control::Pull { .. }) {
                self.memory.lend_stream_context();
            }
            self.memory
                .controls
                .put(&message)
                .map(|_| ())
                .map_err(|e| RoomKitError::Value(e.0))
        })
    }

    fn release(&self) {
        self.memory.shut();
    }

    fn beat_heard(&self) -> bool {
        self.memory.process_beat.swap(false, Ordering::AcqRel)
    }
}

impl Drop for RedisLink {
    fn drop(&mut self) {
        self.release();
    }
}
