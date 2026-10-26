//! The reader's side of one turn on its owner: its output, then its stream,
//! each read granting the executor its window again once half of it was
//! read (RFC §10.6.4).

use std::sync::Arc;
use std::sync::atomic::Ordering;

use roomkit_core::model::{ChannelOutput, ErrorRecord, Metadata};
use tokio::sync::{mpsc, watch};

use super::{Canceller, Main, OwnerState, cancel};
use crate::channel::ChannelError;
use crate::context::ExecutionContext;
use crate::stream::ResponseItem;
use crate::turns::codec::{decode_end, decode_loop_end, decode_output, decode_start};
use crate::turns::error::{RemoteError, TurnError, to_record};
use crate::turns::lock;
use crate::turns::pulls::PullWindow;
use crate::turns::wire::{Control, Item};

/// Where the reader stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    /// It awaits the output: dropped, it cancels the turn.
    Output,
    /// It reads the stream: dropped, it closes it.
    Stream,
    /// It read all it will.
    Done,
}

/// An item of a turn's stream, as the owner reads it.
#[derive(Debug)]
pub(crate) enum RemoteItem {
    /// An item a channel's response yields.
    Item(ResponseItem),
    /// The loop went on after a round that ended without a call: the text
    /// segment ends there (RFC §6.4).
    SegmentBreak,
}

/// The owner's side of one turn, as its reader holds it.
#[derive(Debug)]
pub(crate) struct OwnerTurn {
    state: Arc<OwnerState>,
    main: mpsc::UnboundedReceiver<Main>,
    stage: Stage,
    window: PullWindow,
    /// The calls whose start was read and whose end was not: the start's
    /// frame, and the call's id.
    open: Vec<(u64, String)>,
}

impl OwnerTurn {
    pub(super) fn new(state: Arc<OwnerState>, main: mpsc::UnboundedReceiver<Main>) -> Self {
        Self {
            state,
            main,
            stage: Stage::Output,
            window: PullWindow::default(),
            open: Vec::new(),
        }
    }

    /// The turn's output, and whether its stream follows; at the kit's
    /// shutdown (`cut`), the turn is cancelled and its end awaited.
    pub(crate) async fn output(
        &mut self,
        mut cut: watch::Receiver<bool>,
    ) -> Result<(ChannelOutput, bool), ErrorRecord> {
        self.wait_here();
        let mut cutting = false;
        loop {
            let next = tokio::select! {
                biased;
                next = self.main.recv() => next,
                Ok(_) = cut.wait_for(|cut| *cut), if !cutting => {
                    cutting = true;
                    self.cancel("kit_closed");
                    continue;
                }
            };
            self.stage = Stage::Done;
            return match next {
                Some(Main::Output(output, streams)) => {
                    if streams {
                        self.stage = Stage::Stream;
                    }
                    Ok((decode_output(output), streams))
                }
                Some(Main::Error(error)) => Err(to_record(&error)),
                Some(Main::Failed(error)) => Err(error.to_record()),
                Some(Main::End) if cutting => Err(TurnError::new(
                    "cancelled",
                    "the turn was cut before its output",
                )
                .to_record()),
                Some(Main::End) => Err(TurnError::new(
                    "executor_lost",
                    "the turn ended before its output",
                )
                .to_record()),
                Some(Main::Item(..)) => Err(TurnError::new(
                    "frame_invalid",
                    "an item of the turn came before its output",
                )
                .to_record()),
                None => {
                    Err(TurnError::new("backend_closed", "the turn backend closed").to_record())
                }
            };
        }
    }

    /// The stream's next item, pulling the executor further when the
    /// reader read enough of its window (RFC §10.6.4); `None` at its end.
    pub(crate) async fn next_item(&mut self) -> Option<Result<RemoteItem, ChannelError>> {
        loop {
            if self.stage != Stage::Stream {
                return None;
            }
            self.wait_here();
            if let Some(pull) = self.window.pull() {
                self.grant(&pull);
            }
            let next = self.main.recv().await;
            let read = match next {
                Some(Main::Item(seq, item)) => self.read(seq, item),
                Some(Main::Error(error)) => Read::Failed(Box::new(RemoteError(error))),
                Some(Main::Failed(error)) => Read::Failed(Box::new(error)),
                Some(Main::End) => Read::Failed(Box::new(TurnError::new(
                    "executor_lost",
                    "the turn ended in the middle of its stream",
                ))),
                Some(Main::Output(..)) => Read::Failed(Box::new(TurnError::new(
                    "frame_invalid",
                    "a second output of the turn came",
                ))),
                None => Read::Failed(Box::new(TurnError::new(
                    "backend_closed",
                    "the turn backend closed",
                ))),
            };
            match read {
                Read::Item(item) => return Some(Ok(item)),
                Read::Skipped => {}
                Read::Ended => {
                    self.stage = Stage::Done;
                    return None;
                }
                Read::Failed(error) => {
                    self.stage = Stage::Done;
                    return Some(Err(error));
                }
            }
        }
    }

    /// Grants the executor the window again, the stream advancing in the
    /// context of the call reading it.
    fn grant(&self, pull: &Control) {
        let Control::Pull { count, .. } = pull else {
            return;
        };
        self.state.turn.lend_stream_context();
        self.state
            .granted
            .fetch_add(u64::from(*count), Ordering::AcqRel);
        self.put(pull);
    }

    /// What the reader reads of `item`, the frame `seq`.
    fn read(&mut self, seq: u64, item: Item) -> Read {
        match item {
            Item::Text { text, deltas } => {
                let deltas = deltas.unwrap_or(1);
                if deltas == 0 {
                    return Read::Failed(Box::new(TurnError::new(
                        "frame_invalid",
                        "a burst of no delta",
                    )));
                }
                self.window.read_deltas(deltas);
                Read::Item(RemoteItem::Item(ResponseItem::Text(text)))
            }
            Item::Thinking { .. } => {
                self.window.read_deltas(1);
                Read::Skipped
            }
            Item::ToolStart {
                tool_name,
                tool_id,
                arguments,
                ..
            } => {
                self.window.read_other();
                self.open.push((seq, tool_id.clone()));
                let start = decode_start(tool_name, tool_id, arguments);
                Read::Item(RemoteItem::Item(ResponseItem::ToolStart(start)))
            }
            Item::ToolEnd {
                start,
                tool_name,
                tool_id,
                arguments,
                result,
                status,
                duration_ms,
                error,
                structured_content,
                outcome,
                refused_but_ran,
            } => {
                self.window.read_other();
                self.close_start(start, &tool_id);
                let end = decode_end(crate::turns::wire::ToolEnd {
                    tool_name,
                    tool_id,
                    arguments,
                    result,
                    status,
                    duration_ms,
                    error,
                    structured_content,
                    outcome,
                    refused_but_ran,
                });
                Read::Item(RemoteItem::Item(ResponseItem::ToolEnd(end)))
            }
            Item::SegmentBreak => {
                self.window.read_other();
                Read::Item(RemoteItem::SegmentBreak)
            }
            Item::LoopEnd { reason, usage, .. } => {
                self.window.read_other();
                Read::Item(RemoteItem::Item(decode_loop_end(reason, usage)))
            }
            Item::StreamEnd => Read::Ended,
        }
    }

    /// The call the end of `tool_id` closes: the start it names, else the
    /// earliest open under its id (two calls under one id are two calls,
    /// RFC §6.4).
    fn close_start(&mut self, start: Option<u64>, tool_id: &str) {
        let position = self
            .open
            .iter()
            .position(|(seq, id)| start == Some(*seq) || (start.is_none() && id == tool_id));
        if let Some(position) = position {
            self.open.remove(position);
        }
    }

    /// The arguments the open call `tool_id` runs with once its gate fixed
    /// them, when the executor said so (`tool_args`).
    pub(crate) fn ran_with(&self, tool_id: &str) -> Option<Metadata> {
        let (seq, _) = self.open.iter().find(|(_, id)| id == tool_id)?;
        lock(&self.state.ran_with).get(seq).cloned()
    }

    /// Cuts the turn (RFC §10.6.8): `cancel` crosses with `reason`, and the
    /// owner waits for its end within the cancel bound. The reader reads
    /// nothing more of it.
    pub(crate) fn cancel(&mut self, reason: &str) {
        self.stage = Stage::Done;
        cancel(&self.state, reason);
    }

    /// Returns once the turn is over for the owner: its end came, its
    /// executor was lost, or its cancel bound passed.
    pub(crate) async fn ended(&self) {
        self.canceller().ended().await;
    }

    /// What cuts the turn from outside its reader.
    pub(crate) fn canceller(&self) -> Canceller {
        Canceller {
            state: Arc::clone(&self.state),
        }
    }

    /// The context of the current call is the one waiting on the turn.
    fn wait_here(&self) {
        *lock(&self.state.waiting) = ExecutionContext::current();
    }

    fn put(&self, control: &Control) {
        if let Err(uncarried) = self.state.turn.controls.put(control) {
            tracing::warn!(target: "roomkit::turns", why = %uncarried.0, "a control message could not be sent");
        }
    }
}

impl Drop for OwnerTurn {
    fn drop(&mut self) {
        match self.stage {
            // A call cut before the turn answered (RFC §10.6.8).
            Stage::Output => cancel(&self.state, "cancelled"),
            // A stream nobody reads further: the reader's close (§10.6.4).
            Stage::Stream if !*self.state.over.borrow() => self.put(&Control::Close),
            Stage::Stream | Stage::Done => {}
        }
    }
}

/// What one read gave the reader.
enum Read {
    Item(RemoteItem),
    Skipped,
    Ended,
    Failed(ChannelError),
}
