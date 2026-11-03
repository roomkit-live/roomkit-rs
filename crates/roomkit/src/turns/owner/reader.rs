//! The reader's side of one turn on its owner: its output, then its stream,
//! each read granting the executor its window again once half of it was
//! read (RFC §10.6.4).

use std::sync::Arc;
use std::sync::atomic::Ordering;

use roomkit_core::model::{
    ChannelOutput, Metadata, ToolCallContent, ToolCallOutcome, ToolCallStatus,
};
use tokio::sync::{mpsc, watch};

use super::{Canceller, Main, OwnerState, Steerable, cancel};
use crate::channel::ChannelError;
use crate::context::ExecutionContext;
use crate::stream::ResponseItem;
use crate::turns::codec::{WireLoopEnd, decode_end, decode_loop_end, decode_output, decode_start};
use crate::turns::error::{RemoteError, TurnError, TurnFailure};
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
    /// A steering cut ended it: the calls it left open end, cancelled.
    Cut,
    /// It read all it will.
    Done,
}

/// An item of a turn's stream, as the owner reads it.
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "an item is moved once, from its frame to the reader; a segment break is rare"
)]
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
    /// frame, and the start.
    open: Vec<(u64, ToolCallContent)>,
    /// The turn's record as the last `record` frame gave it: the record a
    /// loop's end ends the turn on.
    record: Option<Metadata>,
}

impl OwnerTurn {
    pub(super) fn new(state: Arc<OwnerState>, main: mpsc::UnboundedReceiver<Main>) -> Self {
        Self {
            state,
            main,
            stage: Stage::Output,
            window: PullWindow::default(),
            open: Vec::new(),
            record: None,
        }
    }

    /// The turn's output, and whether its stream follows; as the kit's
    /// close starts (`cut`), the turn is cancelled and its end awaited.
    pub(crate) async fn output(
        &mut self,
        mut cut: watch::Receiver<bool>,
    ) -> Result<(ChannelOutput, bool), TurnFailure> {
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
            if let Some(Main::Record(record)) = next {
                self.record = Some(record);
                continue;
            }
            self.stage = Stage::Done;
            let failed = match next {
                Some(Main::Output(output, streams)) => {
                    if streams {
                        self.stage = Stage::Stream;
                    }
                    return Ok((decode_output(output), streams));
                }
                Some(Main::Error(error)) => return Err(TurnFailure::crossed(&error)),
                Some(Main::Failed(error)) => Err(error.to_record()),
                Some(Main::End) if cutting => Err(TurnError::link_reason("cancelled").to_record()),
                // Cut by a steering `Cancel` before it answered (RFC §10.6.8).
                Some(Main::End) if self.state.steer_cut.load(Ordering::Acquire) => {
                    return Ok((ChannelOutput::empty(), false));
                }
                // An end, or a frame that is not the output, as Python's
                // owner reads either.
                Some(Main::End | Main::Item(..) | Main::Record(_)) => Err(TurnError::new(
                    "executor_lost",
                    "The turn ended before its output",
                )
                .to_record()),
                None => Err(TurnError::link_reason("backend_closed").to_record()),
            };
            return failed.map_err(TurnFailure::from);
        }
    }

    /// The stream's next item, pulling the executor further when the
    /// reader read enough of its window (RFC §10.6.4); `None` at its end.
    pub(crate) async fn next_item(&mut self) -> Option<Result<RemoteItem, ChannelError>> {
        loop {
            if self.stage == Stage::Cut {
                return self.cut_end();
            }
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
                Some(Main::Record(record)) => {
                    self.record = Some(record);
                    Read::Skipped
                }
                Some(Main::Error(error)) => Read::Failed(Box::new(RemoteError(error))),
                Some(Main::Failed(error)) => Read::Failed(Box::new(error)),
                // Cut by a steering `Cancel`: the calls it left open end
                // cancelled, one per read, then the stream (RFC §10.6.4).
                Some(Main::End) if self.state.steer_cut.load(Ordering::Acquire) => {
                    self.stage = Stage::Cut;
                    return self.cut_end();
                }
                // An end, or a second output, as Python's owner reads either.
                Some(Main::End | Main::Output(..)) => Read::Failed(Box::new(TurnError::new(
                    "executor_lost",
                    "The turn ended in the middle of its stream",
                ))),
                None => Read::Failed(Box::new(TurnError::link_reason("backend_closed"))),
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

    /// The next end of a call a steering cut left open, cancelled; `None`
    /// once none is left, the stream over.
    fn cut_end(&mut self) -> Option<Result<RemoteItem, ChannelError>> {
        if self.open.is_empty() {
            self.stage = Stage::Done;
            return None;
        }
        let (_, start) = self.open.remove(0);
        let end = ToolCallContent {
            status: ToolCallStatus::Failed,
            outcome: Some(ToolCallOutcome::Cancelled),
            error: Some("cancelled".to_owned()),
            ..start
        };
        Some(Ok(RemoteItem::Item(ResponseItem::ToolCallEnd(end))))
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
            Item::Thinking { thinking } => {
                self.window.read_deltas(1);
                Read::Item(RemoteItem::Item(ResponseItem::Thinking(thinking)))
            }
            Item::ToolStart {
                tool_name,
                tool_id,
                arguments,
                intent,
                ..
            } => {
                self.window.read_other();
                let start = decode_start(tool_name, tool_id, arguments, intent);
                self.open.push((seq, start.clone()));
                Read::Item(RemoteItem::Item(ResponseItem::ToolCallStart(start)))
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
                intent,
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
                    intent,
                });
                Read::Item(RemoteItem::Item(ResponseItem::ToolCallEnd(end)))
            }
            Item::SegmentBreak => {
                self.window.read_other();
                Read::Item(RemoteItem::SegmentBreak)
            }
            Item::LoopEnd {
                reason,
                rounds,
                usage,
                max_rounds,
                timeout_seconds,
                budget_tokens,
                budget_usd,
            } => {
                self.window.read_other();
                let end = WireLoopEnd {
                    reason,
                    rounds,
                    usage,
                    max_rounds,
                    timeout_seconds,
                    budget_tokens,
                    budget_usd,
                };
                let record = self.record.take();
                Read::Item(RemoteItem::Item(decode_loop_end(end, record.as_ref())))
            }
            Item::StreamEnd => Read::Ended,
        }
    }

    /// The call the end of `tool_id` closes: the start it names, else the
    /// earliest open under its id (two calls under one id are two calls,
    /// RFC §6.4).
    fn close_start(&mut self, start: Option<u64>, tool_id: &str) {
        let position = self.open.iter().position(|(seq, open)| {
            start == Some(*seq) || (start.is_none() && open.tool_id == tool_id)
        });
        if let Some(position) = position {
            self.open.remove(position);
        }
    }

    /// The arguments the open call `tool_id` runs with once its gate fixed
    /// them, when the executor said so (`tool_args`).
    pub(crate) fn ran_with(&self, tool_id: &str) -> Option<Metadata> {
        let (seq, _) = self.open.iter().find(|(_, open)| open.tool_id == tool_id)?;
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

    /// What steers the turn from outside its reader (RFC §10.6.8).
    pub(crate) fn steerable(&self) -> Steerable {
        Steerable::new(&self.state)
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
            Stage::Stream | Stage::Cut | Stage::Done => {}
        }
    }
}

/// What one read gave the reader.
#[expect(
    clippy::large_enum_variant,
    reason = "a read is moved once, to the reader: an item carries its event whole"
)]
enum Read {
    Item(RemoteItem),
    Skipped,
    Ended,
    Failed(ChannelError),
}
