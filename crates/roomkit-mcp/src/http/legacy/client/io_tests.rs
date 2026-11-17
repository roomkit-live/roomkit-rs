//! The write deadline includes partial progress and buffered flushes.

use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn an_idle_flush_cannot_release_the_request_body_before_headers() {
    let (stream, _peer) = tokio::io::duplex(64);
    let operations = Operations::new();
    let mut stream = Timed::new(stream, Arc::clone(&operations));
    operations.begin(true);
    stream.flush().await.unwrap();
    let cx = Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(!operations.body_ready(&cx));
    stream.write_all(b"headers").await.unwrap();
    stream.flush().await.unwrap();
    assert!(operations.body_ready(&cx));
}

#[tokio::test(start_paused = true)]
async fn partial_write_progress_does_not_restart_the_whole_write_budget() {
    let (stream, mut peer) = tokio::io::duplex(1);
    let mut stream = Timed::new(stream, Operations::new());
    let task = tokio::spawn(async move { stream.write_all(b"abcd").await });
    assert_eq!(peer.read_u8().await.unwrap(), b'a');
    tokio::time::advance(Duration::from_secs(4)).await;
    assert_eq!(peer.read_u8().await.unwrap(), b'b');
    // The second byte progressed, but the same logical write still began at 0.
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(
        task.await.unwrap().unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
}

#[tokio::test(start_paused = true)]
async fn a_flush_starts_a_fresh_budget_for_the_next_logical_write() {
    let (stream, mut peer) = tokio::io::duplex(1);
    let mut stream = Timed::new(stream, Operations::new());
    let (started, beginning) = tokio::sync::oneshot::channel();
    let (flushed, reached) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        stream.write_all(b"a").await?;
        let _sent = started.send(());
        stream.write_all(b"b").await?;
        stream.flush().await?;
        let _sent = flushed.send(());
        stream.write_all(b"cd").await
    });
    beginning.await.unwrap();
    tokio::time::advance(Duration::from_secs(4)).await;
    assert_eq!(peer.read_u8().await.unwrap(), b'a');
    reached.await.unwrap();
    tokio::time::advance(Duration::from_secs(4)).await;
    assert!(!task.is_finished());
    assert_eq!(peer.read_u8().await.unwrap(), b'b');
    assert_eq!(peer.read_u8().await.unwrap(), b'c');
    task.await.unwrap().unwrap();
}

struct Buffered;
impl AsyncWrite for Buffered {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Pending
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test(start_paused = true)]
async fn bytes_buffered_by_tls_still_have_to_flush_inside_the_write_budget() {
    let mut stream = Timed::new(Buffered, Operations::new());
    stream.write_all(b"buffered").await.unwrap();
    tokio::time::advance(Duration::from_secs(4)).await;
    let flushing = stream.flush();
    tokio::pin!(flushing);
    assert!(futures_util::poll!(&mut flushing).is_pending());
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(flushing.await.unwrap_err().kind(), io::ErrorKind::TimedOut);
}
