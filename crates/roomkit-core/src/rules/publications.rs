//! The keys of proactive publications, apart from every other key (RFC
//! §22.1, §5.2), as the Python implementation's `_publication_keys` keeps
//! them.
//!
//! A keyed `deliver()` names one publication in its room, whatever transport
//! carries it, and is looked up within the room, whatever channel committed
//! it. An inbound message's key is its sender's: on a channel whose client
//! chooses it (a WebSocket, a webhook), a sender could key a message like a
//! coming publication, which a lookup within the room would then take for
//! that publication and never publish. So a publication is stored under its
//! key in a namespace of the framework's own (`roomkit:publication:<key>`),
//! which only the delivery path writes: the pipeline drops a key in that
//! namespace from any other message, and the doors that store a caller's
//! event as given refuse one.

use crate::model::ChannelId;

/// The namespace of the keys the framework stores its publications under,
/// Python's `PUBLICATION_KEY_PREFIX`.
pub const PUBLICATION_KEY_PREFIX: &str = "roomkit:publication:";

/// The key a publication the application keyed `key` is stored under.
pub fn publication_key(key: &str) -> String {
    format!("{PUBLICATION_KEY_PREFIX}{key}")
}

/// The application's key of a publication stored under `stored`; any other
/// key as it is.
pub fn application_key(stored: &str) -> &str {
    stored
        .strip_prefix(PUBLICATION_KEY_PREFIX)
        .unwrap_or(stored)
}

/// Whether `key` is in the framework's publication namespace.
pub fn is_reserved(key: &str) -> bool {
    key.starts_with(PUBLICATION_KEY_PREFIX)
}

/// `key` as the pipeline admits it on a message (RFC §22.1): a key in the
/// publication namespace only on the message the delivery path publishes
/// under it, `publishing`; none otherwise. Any other key as it is.
pub fn admitted(key: Option<String>, publishing: Option<&str>) -> Option<String> {
    match key {
        Some(key) if is_reserved(&key) && publishing != Some(key.as_str()) => None,
        key => key,
    }
}

/// The channel the pipeline looks `key` up from, the event's `channel_id`:
/// none for a publication's, looked up within the room whatever channel
/// committed it (RFC §22.1, §13.4).
pub fn lookup_channel(key: &str, channel_id: &ChannelId) -> Option<ChannelId> {
    (!is_reserved(key)).then(|| channel_id.clone())
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    // Python: src/roomkit/core/_publication_keys.py, the namespace's rules
    // test_proactive_delivery's TestAKeyNamesOnePublicationInItsRoom relies on.
    #[test]
    fn a_key_in_the_namespace_is_admitted_on_its_publication_alone() {
        let stored = publication_key("k");
        assert_eq!(stored, "roomkit:publication:k");
        assert_eq!(application_key(&stored), "k");
        assert_eq!(application_key("k"), "k");
        assert_eq!(admitted(Some(stored.clone()), None), None);
        assert_eq!(
            admitted(Some(stored.clone()), Some("roomkit:publication:j")),
            None
        );
        assert_eq!(
            admitted(Some(stored.clone()), Some(&stored)),
            Some(stored.clone())
        );
        assert_eq!(admitted(Some("k".to_owned()), None), Some("k".to_owned()));
        let web = ChannelId::new("web").unwrap();
        assert_eq!(lookup_channel(&stored, &web), None);
        assert_eq!(lookup_channel("k", &web), Some(web));
    }
}
