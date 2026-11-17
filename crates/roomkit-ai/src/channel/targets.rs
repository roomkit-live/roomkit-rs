//! The provider reads the room and its outgoing transports (RFC §6.7, §5.8).

use roomkit::model::{ChannelCapabilities, ChannelCategory, RoomContext};

use super::{AiChannel, AiProvider};
use crate::tool_loop::Adopted;

impl<P: AiProvider> AiChannel<P> {
    pub(super) fn provider_room(&self, context: &RoomContext) -> Adopted {
        let mut targets = context.bindings.iter().filter(|binding| {
            binding.category == ChannelCategory::Transport
                && binding.sends()
                && binding.channel_id != self.channel_id
        });
        let capabilities = targets.next().map(|first| {
            targets.fold(first.capabilities.clone(), |shared, binding| {
                intersected(shared, &binding.capabilities)
            })
        });
        Adopted {
            room: Some(context.clone()),
            target_media_types: capabilities
                .as_ref()
                .map_or_else(Vec::new, |caps| caps.media_types.iter().copied().collect()),
            target_capabilities: capabilities,
            ..Adopted::default()
        }
    }
}

/// Python keeps the first target's MIME/formats/custom fields; only the
/// categories, flags and numeric limits are shared across all targets.
fn intersected(
    mut shared: ChannelCapabilities,
    other: &ChannelCapabilities,
) -> ChannelCapabilities {
    shared
        .media_types
        .retain(|kind| other.media_types.contains(kind));
    shared.supports_threading &= other.supports_threading;
    shared.supports_reactions &= other.supports_reactions;
    shared.supports_edit &= other.supports_edit;
    shared.supports_delete &= other.supports_delete;
    shared.supports_read_receipts &= other.supports_read_receipts;
    shared.supports_typing &= other.supports_typing;
    shared.supports_templates &= other.supports_templates;
    shared.supports_rich_text &= other.supports_rich_text;
    shared.supports_buttons &= other.supports_buttons;
    shared.supports_cards &= other.supports_cards;
    shared.supports_quick_replies &= other.supports_quick_replies;
    shared.supports_media &= other.supports_media;
    shared.supports_audio &= other.supports_audio;
    shared.supports_video &= other.supports_video;
    shared.max_length = smallest(shared.max_length, other.max_length);
    shared.max_buttons = smallest(shared.max_buttons, other.max_buttons);
    shared.max_media_size_bytes = smallest(shared.max_media_size_bytes, other.max_media_size_bytes);
    shared.max_audio_duration_seconds = smallest(
        shared.max_audio_duration_seconds,
        other.max_audio_duration_seconds,
    );
    shared.max_video_duration_seconds = smallest(
        shared.max_video_duration_seconds,
        other.max_video_duration_seconds,
    );
    shared
}

fn smallest<T: Ord>(a: Option<T>, b: Option<T>) -> Option<T> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, None) => a,
        (None, b) => b,
    }
}
