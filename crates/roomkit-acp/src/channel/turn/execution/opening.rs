//! Opening, configuring and recovering the session a turn owns.

use super::{Hold, interrupted, raised};
use crate::{AcpError, AcpTransport};
use roomkit::ChannelError;
use std::sync::Arc;

pub(super) async fn open<T: AcpTransport>(
    hold: &mut Hold<T>,
) -> Result<Option<(Arc<T::Connection>, String)>, ChannelError> {
    if hold.detached() {
        interrupted(&hold.record);
        return Ok(None);
    }
    let shared = Arc::clone(&hold.shared);
    let preparation = Arc::clone(&hold.preparation);
    let opened = tokio::select! {
        biased;
        () = shared.connector.closed() => return Err(Box::new(roomkit::RoomKitError::Cancelled)),
        result = preparation.run(open_session(hold)) => result,
    };
    let Some(opened) = opened else {
        interrupted(&hold.record);
        return Ok(None);
    };
    let opened = opened.map_err(raised)?;
    hold.session_generation = Some(hold.generation);
    if hold.detached() {
        interrupted(&hold.record);
        return Ok(None);
    }
    Ok(Some(opened))
}

/// Closing the transport cannot wake a host's local MCP supplier. The caller
/// selects the connector's close signal while this whole opening is pending.
async fn open_session<T: AcpTransport>(
    hold: &mut Hold<T>,
) -> Result<(Arc<T::Connection>, String), AcpError> {
    let shared = Arc::clone(&hold.shared);
    let connection = shared.connector.connection().await?;
    if hold.standalone {
        let session = shared
            .connector
            .open_turn(&hold.room_id, &connection, &shared.cwd)
            .await?;
        // Store ownership before configuration can yield: cancellation and
        // abandoned readers release it while the same room guard is held.
        let session = hold.temporary.insert(session);
        shared.connector.configure_turn(session).await?;
        return Ok((connection, session.id.clone()));
    }
    let session = shared
        .connector
        .session(&hold.room_id, &connection, &shared.cwd)
        .await?;
    Ok((connection, session))
}

pub(super) async fn rebuild<T: AcpTransport>(
    hold: &mut Hold<T>,
    connection: &Arc<T::Connection>,
) -> Result<Option<String>, ChannelError> {
    let shared = Arc::clone(&hold.shared);
    hold.recovering = true;
    let rebuilt = tokio::select! {
        biased;
        () = shared.connector.closed() => return Ok(None),
        result = hold.preparation.run(shared.connector.rebuild(&hold.room_id, connection, &shared.cwd)) => result,
    };
    if shared.connector.is_closed() {
        return Ok(None);
    }
    let Some(rebuilt) = rebuilt else {
        interrupted(&hold.record);
        return Ok(None);
    };
    let session = rebuilt.map_err(raised)?;
    hold.record.with_value("acp", |acp| {
        if let Some(acp) = acp.as_object_mut() {
            for name in ["interrupted", "stop_reason", "prompt_returned"] {
                acp.shift_remove(name);
            }
        }
    });
    Ok(Some(session))
}
