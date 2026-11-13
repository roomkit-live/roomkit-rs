//! A returned RPC acknowledges its session independently of the response reader.

use std::sync::Arc;

use super::Hold;
use crate::channel::context::Seen;
use crate::transport::AcpTransport;

/// Capture the exact connection and session before prompting. The connector's
/// guard refuses an old answer after another room has reopened the connection.
pub(super) fn on_return<T: AcpTransport>(
    hold: &Hold<T>,
    connection: &Arc<T::Connection>,
    session: &str,
    seen: Seen,
) -> impl FnOnce() + Send + 'static {
    let shared = Arc::clone(&hold.shared);
    let room = hold.room_id.clone();
    let connection = Arc::clone(connection);
    let session = session.to_owned();
    move || {
        if let Some(index) = seen.index {
            shared.connector.acknowledged(
                &room,
                &connection,
                &session,
                index,
                seen.labelled,
                seen.instructed,
            );
        }
    }
}
