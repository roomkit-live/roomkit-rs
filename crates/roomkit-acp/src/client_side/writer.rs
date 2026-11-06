//! The connection's writer: one task writing each message whole, in the
//! order it is handed them, as the Python SDK's sender does.

use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

/// A message to write, one line, and who is told once it is written.
pub(super) struct Outgoing {
    pub(super) line: String,
    pub(super) written: oneshot::Sender<bool>,
}

/// Writes each message of `outbox` to `writer` whole, telling its sender
/// whether it was written, until the queue closes; then shuts the stream.
/// Once a write failed, nothing more is written and every later message
/// is told so.
pub(super) async fn write<W: AsyncWrite + Unpin>(
    mut writer: W,
    mut outbox: mpsc::UnboundedReceiver<Outgoing>,
) {
    let mut broken = false;
    while let Some(Outgoing { line, written }) = outbox.recv().await {
        if !broken {
            broken =
                writer.write_all(line.as_bytes()).await.is_err() || writer.flush().await.is_err();
        }
        // A sender that went has no one to tell.
        let _gone = written.send(!broken);
    }
    // A stream the agent closed already cannot be shut again.
    let _shut = writer.shutdown().await;
}
