//! What an AI channel's turn reads of the kit's readers of an author's
//! media (RFC §6.4), as Python's kit wires them into its AI channels at
//! registration.

use super::TurnHost;
use crate::{AttachedMedia, MediaResolver};

impl TurnHost {
    /// The kit's media resolver
    /// ([`RoomKitOptions::media_resolver`](crate::RoomKitOptions::media_resolver)),
    /// which an AI channel's turn asks for a reference's bytes. Not a
    /// public verb.
    #[doc(hidden)]
    pub fn media_resolver(&self) -> Option<MediaResolver> {
        self.shared.kit.media().media_resolver
    }

    /// The kit's attached-media reader
    /// ([`RoomKitOptions::attached_media`](crate::RoomKitOptions::attached_media)),
    /// with which an AI channel's turn reads its events. Not a public verb.
    #[doc(hidden)]
    pub fn attached_media(&self) -> Option<AttachedMedia> {
        self.shared.kit.media().attached_media
    }
}
