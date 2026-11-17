//! Deadlines at socket operations, not at an entire HTTP request.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures_util::task::AtomicWaker;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::Sleep;

#[cfg(test)]
#[path = "io_tests.rs"]
mod tests;

const HEAD: u8 = 0;
const BODY: u8 = 1;
const READ: u8 = 2;
const IDLE: u8 = 3;

/// A request has two logical writes: its headers, then its JSON body. Hyper's
/// body waits for the first flush so those operations cannot be coalesced.
pub(super) struct Operations {
    phase: AtomicU8,
    has_body: AtomicBool,
    yielded: AtomicBool,
    body: AtomicWaker,
    reader: AtomicWaker,
}

impl Operations {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            phase: AtomicU8::new(IDLE),
            has_body: AtomicBool::new(false),
            yielded: AtomicBool::new(false),
            body: AtomicWaker::new(),
            reader: AtomicWaker::new(),
        })
    }

    pub(super) fn begin(&self, body: bool) {
        self.has_body.store(body, Ordering::Release);
        self.yielded.store(false, Ordering::Release);
        self.phase.store(HEAD, Ordering::Release);
        self.reader.wake();
    }

    pub(super) fn idle(&self) {
        self.phase.store(IDLE, Ordering::Release);
    }

    pub(super) fn body_ready(&self, cx: &Context<'_>) -> bool {
        self.body.register(cx.waker());
        if self.phase.load(Ordering::Acquire) == HEAD {
            return false;
        }
        self.yielded.store(true, Ordering::Release);
        true
    }

    fn flushed(&self) {
        match self.phase.load(Ordering::Acquire) {
            HEAD => {
                let next = if self.has_body.load(Ordering::Acquire) {
                    BODY
                } else {
                    READ
                };
                self.phase.store(next, Ordering::Release);
                self.body.wake();
                self.reader.wake();
            }
            BODY if self.yielded.load(Ordering::Acquire) => {
                self.phase.store(READ, Ordering::Release);
                self.reader.wake();
            }
            _ => {}
        }
    }
}

pub(super) struct Timed<S> {
    stream: S,
    operations: Arc<Operations>,
    read: Option<Pin<Box<Sleep>>>,
    write: Option<Pin<Box<Sleep>>>,
}

impl<S> Timed<S> {
    pub(super) fn new(stream: S, operations: Arc<Operations>) -> Self {
        Self {
            stream,
            operations,
            read: None,
            write: None,
        }
    }
}

fn deadline(
    timer: &mut Option<Pin<Box<Sleep>>>,
    seconds: u64,
    cx: &mut Context<'_>,
) -> io::Result<()> {
    let timer =
        timer.get_or_insert_with(|| Box::pin(tokio::time::sleep(Duration::from_secs(seconds))));
    if timer.as_mut().poll(cx).is_ready() {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "MCP network operation timed out",
        ));
    }
    Ok(())
}

impl<S: AsyncRead + Unpin> AsyncRead for Timed<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.operations.reader.register(cx.waker());
        let phase = this.operations.phase.load(Ordering::Acquire);
        if phase == READ {
            deadline(&mut this.read, 300, cx)?;
        } else {
            this.read = None;
            // httpcore finishes the request's writes before reading its
            // response. An early HTTP reply must not release a blocked body
            // or return that still-writing connection to the idle pool.
            if matches!(phase, HEAD | BODY) {
                return Poll::Pending;
            }
        }
        let result = Pin::new(&mut this.stream).poll_read(cx, buf);
        if result.is_ready() {
            this.read = None;
        }
        result
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Timed<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        // Partial progress does not restart a write: httpcore bounds the whole
        // logical write, including the final buffered TLS bytes.
        deadline(&mut this.write, 5, cx)?;
        Pin::new(&mut this.stream).poll_write(cx, bytes)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.write.is_some() {
            deadline(&mut this.write, 5, cx)?;
        }
        ready!(Pin::new(&mut this.stream).poll_flush(cx))?;
        if this.write.take().is_some() {
            this.operations.flushed();
        }
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}
