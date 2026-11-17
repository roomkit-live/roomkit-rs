//! One JSON body, handed to Hyper only after the headers have been flushed.

use hyper::body::{Body as HttpBody, Bytes, Frame, SizeHint};
use std::{
    convert::Infallible,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use super::io::Operations;

pub(super) struct Body {
    bytes: Option<Bytes>,
    operations: Arc<Operations>,
}

impl Body {
    pub(super) fn new(bytes: Option<Bytes>, operations: Arc<Operations>) -> Self {
        Self { bytes, operations }
    }
}

impl HttpBody for Body {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        let this = self.get_mut();
        if this.bytes.is_some() && !this.operations.body_ready(cx) {
            return Poll::Pending;
        }
        Poll::Ready(this.bytes.take().map(|bytes| Ok(Frame::data(bytes))))
    }

    fn is_end_stream(&self) -> bool {
        self.bytes.is_none()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.bytes.as_ref().map_or(0, |bytes| bytes.len() as u64))
    }
}
