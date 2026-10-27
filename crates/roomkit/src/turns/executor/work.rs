//! A turn's work on the executor: the channel's `on_event()`, its output,
//! then its stream, advanced as the owner's pulls allow (RFC §10.6.4):
//! deltas ahead of the owner's reader within what it granted, never past
//! any other item without a later pull, each item sent after the arguments
//! its calls' gates fixed.

use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use super::link::FrameSender;
use crate::channel::DynChannel;
use crate::stream::{ResponseItem, ResponseStream};
use crate::turn::TurnHost;
use crate::turns::codec::{encode_loop_end, encode_output};
use crate::turns::error::{CHANNEL_ERROR, channel_error, one};
use crate::turns::pipe::Uncarried;
use crate::turns::pulls::PullGate;
use crate::turns::wire::{Frame, Item, TurnRequest};
use crate::turns::{MemoryTurn, lock};

/// The pulls a turn's stream reads on, shared by its work and its
/// controls.
#[derive(Debug, Default)]
pub(super) struct Flow {
    pub(super) gate: Mutex<PullGate>,
    pub(super) wake: Notify,
}

impl Flow {
    /// Returns once the stream may read an item, or its reader closed it.
    async fn granted(&self) {
        loop {
            let mut woken = std::pin::pin!(self.wake.notified());
            woken.as_mut().enable();
            {
                let gate = lock(&self.gate);
                if gate.granted() || gate.closing {
                    return;
                }
            }
            woken.await;
        }
    }

    fn closing(&self) -> bool {
        lock(&self.gate).closing
    }
}

/// The turn's work: the channel's `on_event()`, its output, then its
/// stream as the owner pulls it.
pub(super) async fn work(
    turn: Arc<MemoryTurn>,
    sender: Arc<FrameSender>,
    host: TurnHost,
    flow: Arc<Flow>,
    request: TurnRequest,
    channel: Arc<dyn DynChannel>,
) {
    let TurnRequest {
        event,
        binding,
        context,
        ..
    } = request;
    // `on_event()` is called within the turn and its context, so the work
    // it does before its future, as much as its future's, is the turn's.
    let reaction = host
        .clone()
        .scope(async move { channel.on_event(event, binding, context).await });
    let answered = turn.turn_context().scope(reaction).await;
    let output = match answered {
        Ok(output) => output,
        Err(error) => {
            let error = channel_error(&*error);
            return refuse_or_send(&sender, &Frame::Error { error });
        }
    };
    let stream = host.take_stream();
    let frame = Frame::Output {
        output: encode_output(output),
        streams: stream.is_some(),
    };
    if let Err(why) = sender.send(&frame) {
        return refuse(&sender, "output", &why);
    }
    if let Some(stream) = stream {
        let mut pump = Pump {
            turn: &turn,
            sender: &sender,
            host: &host,
            flow: &flow,
            rounds: 0,
            starting: false,
        };
        Box::pin(pump.serve(stream)).await;
    }
}

/// What a read of the stream gave.
type Read = Option<Result<ResponseItem, crate::channel::ChannelError>>;

/// The advance of one turn's stream.
struct Pump<'a> {
    turn: &'a MemoryTurn,
    sender: &'a FrameSender,
    host: &'a TurnHost,
    flow: &'a Flow,
    /// The runs of call starts read: the loop's end counts them as its
    /// rounds.
    rounds: u64,
    starting: bool,
}

impl Pump<'_> {
    /// Advances `stream` until its end, a failure or a close.
    async fn serve(&mut self, mut stream: ResponseStream) {
        loop {
            self.flow.granted().await;
            if self.flow.closing() {
                return;
            }
            let (read, closing) = self.read(&mut stream).await;
            let reported = self
                .host
                .with_ran_with(|ran_with| self.sender.report_args(ran_with));
            if let Err(why) = reported {
                return refuse(self.sender, "tool arguments", &why);
            }
            // An item read once the reader closed is the reader's no more;
            // the stream's end or its failure still crosses.
            if closing && matches!(read, Some(Ok(_))) {
                return self.closed_on(read);
            }
            if !self.send(read) {
                return;
            }
        }
    }

    /// Reads one item, in the context of the stream's first reader and
    /// within the turn's host; whether the reader closed meanwhile.
    async fn read(&self, stream: &mut ResponseStream) -> (Read, bool) {
        lock(&self.flow.gate).start_read();
        let next = crate::stream::next_item(stream);
        let read = self
            .turn
            .stream_context()
            .scope(self.host.clone().scope(next))
            .await;
        let mut gate = lock(&self.flow.gate);
        gate.end_read();
        match &read {
            Some(Ok(ResponseItem::Text(_))) => gate.read_deltas(1),
            Some(Ok(_)) | None => gate.read_other(),
            Some(Err(_)) => {}
        }
        (read, gate.closing)
    }

    /// An item read once the reader closed the stream: a call's end still
    /// reaches the owner, the call closed with its own outcome; any other
    /// is dropped.
    fn closed_on(&self, read: Read) {
        if let Some(Ok(ResponseItem::ToolEnd(end))) = read {
            self.sender.send_ended(end);
        }
    }

    /// Sends what `read` gave; whether the stream goes on.
    fn send(&mut self, read: Read) -> bool {
        let sent = match read {
            Some(Ok(ResponseItem::Text(text))) => {
                self.starting = false;
                let item = Item::Text { text, deltas: None };
                self.sender.send(&Frame::Item { item }).map(drop)
            }
            Some(Ok(ResponseItem::ToolStart(start))) => {
                if !self.starting {
                    self.rounds = self.rounds.saturating_add(1);
                    self.starting = true;
                }
                self.sender.send_start(start)
            }
            Some(Ok(ResponseItem::ToolEnd(end))) => {
                self.starting = false;
                self.host.forget_ran_with(&end.tool_id);
                self.sender.send_end(end)
            }
            Some(Ok(ResponseItem::End(record))) => {
                self.starting = false;
                let (whole, item) = encode_loop_end(&record, self.rounds);
                let recorded = match whole {
                    Some(record) => self.sender.send(&Frame::Record { record }).map(drop),
                    None => Ok(()),
                };
                recorded.and_then(|()| self.sender.send(&Frame::Item { item }).map(drop))
            }
            Some(Err(error)) => {
                let error = channel_error(&*error);
                refuse_or_send(self.sender, &Frame::Error { error });
                return false;
            }
            None => {
                let item = Item::StreamEnd;
                refuse_or_send(self.sender, &Frame::Item { item });
                return false;
            }
        };
        match sent {
            Ok(()) => true,
            Err(why) => {
                refuse(self.sender, "stream item", &why);
                false
            }
        }
    }
}

/// Sends `frame`; one that cannot cross is refused to the owner.
fn refuse_or_send(sender: &FrameSender, frame: &Frame) {
    if let Err(why) = sender.send(frame) {
        refuse(sender, "error", &why);
    }
}

/// Tells the owner the turn's `what` cannot cross the backend: the turn
/// fails with that error.
fn refuse(sender: &FrameSender, what: &str, why: &Uncarried) {
    tracing::warn!(target: "roomkit::turns", what, why = %why.0, "a turn's frame cannot cross the turn backend");
    let error = one(
        CHANNEL_ERROR,
        format!("the turn's {what} cannot cross the turn backend: {}", why.0),
    );
    drop(sender.send(&Frame::Error { error }));
}
