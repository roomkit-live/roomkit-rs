//! The standalone driver's output and its caller-paced response stream.
use crate::ChannelError;
use crate::stream::{ResponseItem, ResponseStream};
use crate::turns::{Answer, ErrorClasses, OwnerTurn, RemoteItem, TurnFuture};
use futures_core::Stream;
use roomkit_core::model::ChannelOutput;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

/// The completed turn output and the optional stream to read from its executor.
pub struct TurnClientOutput {
    /// The channel's output and live response metadata.
    pub output: ChannelOutput,
    /// Pull this stream to advance the executor. Dropping it closes the turn's stream.
    pub response_stream: Option<ResponseStream>,
}
impl fmt::Debug for TurnClientOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TurnClientOutput")
            .field("output", &self.output)
            .field("streaming", &self.response_stream.is_some())
            .finish()
    }
}
type Step = TurnFuture<'static, (OwnerTurn, Option<Result<RemoteItem, ChannelError>>)>;
struct Reader {
    next: Option<Step>,
    errors: ErrorClasses,
}
fn read(mut turn: OwnerTurn) -> Step {
    Box::pin(async move {
        let item = turn.next_item().await;
        (turn, item)
    })
}
impl Stream for Reader {
    type Item = Result<ResponseItem, ChannelError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(next) = &mut self.next else {
            return Poll::Ready(None);
        };
        let (turn, item) = ready!(next.as_mut().poll(cx));
        self.next = item.is_some().then(|| read(turn));
        Poll::Ready(item.map(|v| {
            v.map(|remote| match remote {
                RemoteItem::Item(item) => item,
                RemoteItem::SegmentBreak => ResponseItem::SegmentBreak,
            })
            .map_err(|error| {
                match error.downcast_ref::<crate::turns::error::RemoteError>() {
                    Some(remote) => self.errors.rebuild(&remote.0).into(),
                    None => match error.downcast_ref::<crate::turns::TurnError>() {
                        Some(link) => link.clone().into_kit_error().into(),
                        None => error,
                    },
                }
            })
        }))
    }
}
impl TurnClientOutput {
    pub(super) fn from_answer(answer: Answer, errors: &ErrorClasses) -> Self {
        match answer {
            Answer::Output(output) => Self {
                output: *output,
                response_stream: None,
            },
            Answer::Streamed(output, turn) => Self {
                output: *output,
                response_stream: Some(Box::pin(Reader {
                    next: Some(read(turn)),
                    errors: errors.clone(),
                })),
            },
        }
    }
}
