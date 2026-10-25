//! Idempotency keys (RFC §13.4, §14.3): a blank key is no key.
//!
//! A key, empty or whitespace only, names no message: an inbound message, a
//! direct injection and a record committed outside the pipeline that carry
//! one are processed and stored as events that carry none, and a store's
//! uniqueness of `(room, source channel, key)` leaves it out.

/// Whether `key` is a key: neither empty nor whitespace only (RFC §13.4).
pub fn is_key(key: &str) -> bool {
    !key.trim().is_empty()
}

/// The key `written` carries, `None` for none and for a blank one (RFC
/// §13.4): what the pipeline stores and looks up.
pub fn key(written: Option<String>) -> Option<String> {
    written.filter(|key| is_key(key))
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn a_blank_key_is_no_key() {
        let read = [None, Some(""), Some("   "), Some("\t\n"), Some("SM1")]
            .map(|written| key(written.map(str::to_owned)));
        assert_eq!(read, [None, None, None, None, Some("SM1".to_owned())]);
    }
}
