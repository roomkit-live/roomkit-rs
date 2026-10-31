//! A recording transport named by a string, as the examples build theirs.

use roomkit::model::{ChannelId, ChannelType, ModelError};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};

/// A recording transport `id` of `channel_type`, declaring a text channel's
/// capabilities.
///
/// # Errors
///
/// An `id` no channel id may be.
pub fn recording(id: &str, channel_type: ChannelType) -> Result<RecordingTransport, ModelError> {
    Ok(RecordingTransport::new(
        ChannelId::new(id)?,
        channel_type,
        RecordingTransportOptions::default(),
    ))
}
