//! Identity verification's rules for an inbound message (RFC §10.1 step
//! 5b, §11.7.3, §11.7.4): whose verification labels it, whether its text
//! replies with a code, and how it is held from the room's agents; the
//! Python implementation's module functions of
//! `core/mixins/verification.py`.

mod digits;

use serde_json::json;

use crate::model::{
    ChannelCategory, EventContent, IdentificationStatus, Identity, IdentityId, RoomContext,
    RoomEvent, Visibility,
};
use crate::rules::{authors, permissions};

/// The event metadata key marking a message held for verification (RFC
/// §11.7.3), Python's `HELD_KEY`. Its value records the visibility the
/// message had, which its release restores, and whose it is: the identity
/// and its organization. Only the runtime writes it: the pipeline drops
/// one a sender's message carries.
#[doc(hidden)]
pub const HELD_KEY: &str = "roomkit:verification_held";

/// The identity of `event`'s sender as the room's IDENTIFIED participant its
/// id names on its channel (RFC §5.5), if one: Python's
/// `_participant_identity`. A sender just resolved is not one yet: the
/// pipeline stamps the identity's id as the event's `participant_id` and
/// records the participant at its commit.
#[doc(hidden)]
pub fn participant_identity(event: &RoomEvent, context: &RoomContext) -> Option<IdentityId> {
    authors::named_record(event, &context.participants)
        .filter(|record| record.identification == IdentificationStatus::Identified)
        .and_then(|record| record.identity_id.clone())
}

/// The identity and the organization whose verification labels `event`
/// (RFC §11.7.3, §11.7.7), `None` for a sender who is not identified:
/// Python's `_sender_identity`. The identity is the one step 5 resolved,
/// `resolved`, or the room's IDENTIFIED participant the event names when
/// resolution was skipped (RFC §11.6); the organization is always the
/// room's, so a verification proven in another organization never labels
/// this conversation.
pub(crate) fn sender_identity(
    event: &RoomEvent,
    context: &RoomContext,
    resolved: Option<&Identity>,
) -> Option<(IdentityId, Option<String>)> {
    let identity_id = match resolved {
        Some(identity) => identity.id.clone(),
        None => participant_identity(event, context)?,
    };
    Some((identity_id, context.room.organization_id.clone()))
}

/// The code `event`'s text replies with, when its whole text has a code's
/// form (RFC §11.7.4): `code_length` digits, the whitespace around them
/// left out, as Python's `str.strip()`, `str.isdigit()` and `len()` read
/// them. `None` for any other message.
pub(crate) fn code_reply(event: &RoomEvent, code_length: u32) -> Option<String> {
    let EventContent::Text(content) = &event.content else {
        return None;
    };
    let text = content.body.trim_matches(is_python_space);
    let length = usize::try_from(code_length).ok()?;
    let digits = !text.is_empty() && text.chars().all(is_python_digit);
    (digits && text.chars().count() == length).then(|| text.to_owned())
}

/// Holds `event` from the room's agents, the transports it reached still
/// seeing it (RFC §11.7.3), as Python's `_held`. Holding only narrows: the
/// scope the event is committed with, its own visibility within its source
/// binding's (RFC §7.3), keeps its transport channels and loses its
/// intelligence channels, never gaining a channel it did not reach. The
/// mark records that scope, which its release restores, and whose the
/// message is.
pub(crate) fn hold(
    event: &mut RoomEvent,
    context: &RoomContext,
    identity_id: &IdentityId,
    organization_id: Option<&str>,
) {
    let scope = match context.get_binding(&event.source.channel_id) {
        Some(source) => {
            permissions::effective_visibility(&event.visibility, source, &context.bindings)
        }
        None => event.visibility.clone(),
    };
    let mark = json!({
        "visibility": scope.to_string(),
        "identity_id": identity_id,
        "organization_id": organization_id,
    });
    event.visibility = transport_part(&scope, context);
    event.metadata.insert(HELD_KEY.to_owned(), mark);
}

/// The part of `visibility` that reaches transport channels, and no more:
/// Python's `_transport_part`. A list keeps the transports of the room it
/// names, in its normal form, `none` when it names none.
fn transport_part(visibility: &Visibility, context: &RoomContext) -> Visibility {
    match visibility {
        Visibility::All | Visibility::Transport => Visibility::Transport,
        Visibility::Intelligence | Visibility::None => Visibility::None,
        Visibility::Internal => Visibility::Internal,
        Visibility::Channels(_) => {
            let reached = context
                .bindings
                .iter()
                .filter(|binding| binding.category == ChannelCategory::Transport)
                .filter(|binding| permissions::visibility_allows(visibility, binding))
                .map(|binding| binding.channel_id.clone());
            Visibility::channels(reached).unwrap_or(Visibility::None)
        }
    }
}

/// Whether Python's `str.isspace()` reads `c` as whitespace: Unicode's
/// `White_Space`, and the information separators `\x1c` to `\x1f`.
fn is_python_space(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

/// Whether Python's `str.isdigit()` reads `c` as a digit.
fn is_python_digit(c: char) -> bool {
    let c = u32::from(c);
    digits::DIGITS
        .binary_search_by(|&(first, last)| {
            if last < c {
                std::cmp::Ordering::Less
            } else if first > c {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        })
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        ChannelBinding, ChannelId, ChannelType, EventSource, Room, RoomId, TextContent,
    };

    fn message(body: &str) -> RoomEvent {
        RoomEvent::new(
            RoomId::new("room-1").unwrap(),
            EventSource::new(ChannelId::new("sms").unwrap(), ChannelType::SMS),
            TextContent::new(body),
        )
    }

    // Python: tests/test_verification_inbound.py::test_a_code_reply_is_consumed_and_verifies,
    // its reply " {code} ": the whitespace around a code is no part of it.
    #[test]
    fn a_code_reply_is_read_without_the_whitespace_around_it() {
        assert_eq!(
            code_reply(&message(" 123456 "), 6).as_deref(),
            Some("123456")
        );
        assert_eq!(
            code_reply(&message("\u{1c}123456\u{3000}"), 6).as_deref(),
            Some("123456")
        );
    }

    // Python: core/mixins/verification.py::_consume_code, `text.isdigit()`
    // and `len(text) != code_length`.
    #[test]
    fn a_code_reply_is_its_length_of_digits_python_reads() {
        assert_eq!(code_reply(&message("12345"), 6), None);
        assert_eq!(code_reply(&message("1234567"), 6), None);
        assert_eq!(code_reply(&message("12 456"), 6), None);
        assert_eq!(code_reply(&message("12345a"), 6), None);
        assert_eq!(code_reply(&message(""), 6), None);
        // Python's isdigit reads every decimal digit, and the superscripts.
        assert_eq!(
            code_reply(&message("\u{661}\u{662}\u{663}\u{664}\u{665}\u{666}"), 6).as_deref(),
            Some("\u{661}\u{662}\u{663}\u{664}\u{665}\u{666}")
        );
        assert!(code_reply(&message("12345\u{b2}"), 6).is_some());
        // A fraction is numeric, no digit.
        assert_eq!(code_reply(&message("12345\u{bd}"), 6), None);
    }

    // Python: core/mixins/verification.py::_transport_part.
    #[test]
    fn a_held_scope_keeps_its_transports_only() {
        let room = Room::new(RoomId::new("room-1").unwrap());
        let mut context = RoomContext::new(room);
        let binding = |id: &str, category| {
            let mut binding = ChannelBinding::new(
                ChannelId::new(id).unwrap(),
                RoomId::new("room-1").unwrap(),
                ChannelType::WEBSOCKET,
            );
            binding.category = category;
            binding
        };
        context.bindings = vec![
            binding("advisor", ChannelCategory::Transport),
            binding("ai", ChannelCategory::Intelligence),
            binding("other", ChannelCategory::Transport),
        ];
        let listed: Visibility = "ai,advisor".parse().unwrap();
        let none: Visibility = "ai".parse().unwrap();
        assert_eq!(transport_part(&listed, &context).to_string(), "advisor");
        assert_eq!(transport_part(&none, &context), Visibility::None);
        assert_eq!(
            transport_part(&Visibility::All, &context),
            Visibility::Transport
        );
        assert_eq!(
            transport_part(&Visibility::Intelligence, &context),
            Visibility::None
        );
        assert_eq!(
            transport_part(&Visibility::Internal, &context),
            Visibility::Internal
        );
    }
}
