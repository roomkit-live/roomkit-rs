//! Addresses in their channel's form, and the recipient a binding delivers
//! to (RFC §5.7, §10.1 step 1, §10.2 step 3d, Appendix A).
//!
//! A channel addressed by phone number compares and stores numbers in
//! E.164; an email channel compares and stores an address lower-case,
//! without its display name; any other id is kept as written (RFC §5.7).
//! The sender of an inbound message is written in its channel's form before
//! anything reads it (RFC §10.1 step 1, gap G-052):
//!
//! | Written | Read as |
//! |---|---|
//! | `whatsapp:+15550000001`, `+1 (555) 000-0001`, `0015550000001` | `+15550000001` |
//! | `+33 (0)6 12 34 56 78` | `+33612345678` |
//! | `15550000001`, `5550000001`, channel country code `1` | `+15550000001` |
//! | `393 123 4567`, channel country code `39` | `+393931234567` |
//! | `06 12 34 56 78`, channel country code `33` | `+33612345678` |
//! | `15550000001`, `5550000001`, `07911 123456`, no country code | as written |
//! | `12345` (a short code), `alice`, `whatsapp:alice` | as written |
//! | `Alice <Alice@Example.com>` on an email channel | `alice@example.com` |
//!
//! A binding's correspondent and recipient, and a participant's id, are
//! stored as their writer wrote them; routing compares them in their
//! channel's form, an [`AddressForm`] (gap G-058).

use serde_json::{Number, Value};
use thiserror::Error;

use crate::model::{ChannelBinding, ChannelType};

/// The binding metadata key of a recipient's phone number (Appendix A.1,
/// A.3, A.4).
pub const PHONE_NUMBER: &str = "phone_number";

/// The binding metadata key of a recipient's email address (Appendix A.2).
pub const EMAIL_ADDRESS: &str = "email_address";

/// The binding metadata key of a Messenger recipient (Appendix A.5).
pub const FACEBOOK_USER_ID: &str = "facebook_user_id";

/// The binding metadata key of a Teams conversation (Appendix A.6).
pub const TEAMS_CONVERSATION_ID: &str = "teams_conversation_id";

/// The binding metadata key of a Telegram chat, which its provider sends to
/// (Appendix A.13, gap G-051).
pub const TELEGRAM_CHAT_ID: &str = "telegram_chat_id";

/// The metadata key under which a Telegram message carries the chat it was
/// posted in (Appendix A.13, gap G-057).
pub const TELEGRAM_CHAT: &str = "chat_id";

/// The metadata key under which a Teams message carries the conversation it
/// was posted in (Appendix A.6, gap G-057).
pub const TEAMS_CONVERSATION: &str = "conversation_id";

/// The metadata key under which a WhatsApp Personal group message carries
/// its group (Appendix A.4, gap G-057); a private message carries none.
pub const WHATSAPP_GROUP: &str = "group_jid";

/// The sender under which the framework writes into a room itself; it names
/// no correspondent (RFC §10.4).
pub const SYSTEM_SENDER: &str = "system";

/// The most digits a phone number holds (ITU-T E.164).
const MAX_DIGITS: usize = 15;

/// The most digits of a short code, which is kept as written; a number has
/// more (gap G-052).
const MAX_SHORT_CODE_DIGITS: usize = 6;

/// The most digits of a national number: digits that start with the
/// channel's country code are read as written with it, without its `+`,
/// only when there are more (gap G-052).
const MAX_NATIONAL_DIGITS: usize = 10;

/// How a channel's senders are addressed (RFC §5.7, gap G-051).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Addressing {
    /// By phone number, compared and stored in E.164 (`+15550000001`).
    PhoneNumber,
    /// By email address, compared and stored lower-case, without a display
    /// name.
    EmailAddress,
    /// By the chat a message was posted in: replies go to the chat, not to
    /// its sender, and the chat is a binding's correspondent (Telegram,
    /// Teams).
    Chat,
    /// By an id kept as written.
    AsWritten,
}

impl Addressing {
    /// How a channel of `channel_type` is addressed (gap G-051): SMS, MMS,
    /// RCS, WhatsApp, WhatsApp Personal and VOICE by phone number, EMAIL by
    /// email address, Telegram and Teams by chat, any other type by an id as
    /// written.
    pub fn of(channel_type: ChannelType) -> Self {
        match channel_type {
            ChannelType::Sms
            | ChannelType::Mms
            | ChannelType::Rcs
            | ChannelType::Whatsapp
            | ChannelType::WhatsappPersonal
            | ChannelType::Voice => Self::PhoneNumber,
            ChannelType::Email => Self::EmailAddress,
            ChannelType::Telegram | ChannelType::Teams => Self::Chat,
            ChannelType::Websocket
            | ChannelType::Messenger
            | ChannelType::Webhook
            | ChannelType::RealtimeVoice
            | ChannelType::AudioVideo
            | ChannelType::RealtimeAudioVideo
            | ChannelType::Video
            | ChannelType::Conference
            | ChannelType::Cli
            | ChannelType::Push
            | ChannelType::Ai
            | ChannelType::System => Self::AsWritten,
        }
    }

    /// Whether replies go to the address a sender writes from, so that the
    /// sender's address is a binding's recipient (RFC §5.7). On a chat
    /// channel they go to the chat.
    pub fn replies_to_sender(self) -> bool {
        match self {
            Self::PhoneNumber | Self::EmailAddress | Self::AsWritten => true,
            Self::Chat => false,
        }
    }
}

/// A country code a channel's numbers written without one take (RFC §5.7):
/// one to three digits, the first not `0` (ITU-T E.164).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CountryCode(String);

/// A value that is not a country code.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("{0:?} is not a country code: one to three digits, the first not 0")]
pub struct InvalidCountryCode(pub String);

impl CountryCode {
    /// The country code `code`, written with or without its `+` (`1`,
    /// `+33`).
    ///
    /// # Errors
    ///
    /// Returns [`InvalidCountryCode`] when `code` is not one to three digits,
    /// the first not `0`.
    pub fn new(code: &str) -> Result<Self, InvalidCountryCode> {
        let digits = code.strip_prefix('+').unwrap_or(code);
        let valid = (1..=3).contains(&digits.len())
            && digits.chars().all(|character| character.is_ascii_digit())
            && !digits.starts_with('0');
        if valid {
            Ok(Self(digits.to_owned()))
        } else {
            Err(InvalidCountryCode(code.to_owned()))
        }
    }

    /// The code's digits, without its `+`.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// How a channel writes the addresses its bindings and participants name
/// (RFC §5.7): the form routing compares them in (gap G-058).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AddressForm {
    /// How the channel's senders are addressed.
    pub addressing: Addressing,
    /// The country code a phone number written without one takes.
    pub country_code: Option<CountryCode>,
    /// The binding metadata key the channel delivers to, if any (RFC §10.2
    /// step 3d).
    pub recipient_key: Option<String>,
}

impl AddressForm {
    /// `address` in the channel's form.
    pub fn normalize(&self, address: &str) -> String {
        normalize(address, self.addressing, self.country_code.as_ref())
    }

    /// The recipient `binding` delivers to, in the channel's form: its
    /// value under the channel's recipient key, when it is one
    /// ([`has_recipient`]). A number, a Telegram chat id, is kept as
    /// written.
    pub fn recipient(&self, binding: &ChannelBinding) -> Option<String> {
        let key = self.recipient_key.as_deref()?;
        match written_recipient(binding, key)? {
            Recipient::Text(recipient) => Some(self.normalize(recipient)),
            Recipient::Number(recipient) => Some(recipient.to_string()),
        }
    }
}

/// A recipient as a binding's metadata holds it.
enum Recipient<'binding> {
    Text(&'binding str),
    Number(&'binding Number),
}

/// The recipient `binding` holds under `key`: a string that is not blank,
/// or a number (RFC §5.7, §10.2 step 3d, gap G-075). Null, a blank string,
/// a boolean, an array and an object are none.
fn written_recipient<'binding>(
    binding: &'binding ChannelBinding,
    key: &str,
) -> Option<Recipient<'binding>> {
    match binding.metadata.get(key)? {
        Value::String(recipient) if !recipient.trim().is_empty() => {
            Some(Recipient::Text(recipient))
        }
        Value::Number(recipient) => Some(Recipient::Number(recipient)),
        Value::String(_) | Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) => {
            None
        }
    }
}

/// `address` in the form of a channel addressed by `addressing` (RFC §5.7,
/// §10.1 step 1): a phone number in E.164 when it reads as one, an email
/// address lower-case without its display name, any other id as written.
pub fn normalize(
    address: &str,
    addressing: Addressing,
    country_code: Option<&CountryCode>,
) -> String {
    match addressing {
        Addressing::PhoneNumber => {
            phone_number(address, country_code).unwrap_or_else(|| address.to_owned())
        }
        Addressing::EmailAddress => email_address(address),
        Addressing::Chat | Addressing::AsWritten => address.to_owned(),
    }
}

/// The E.164 form of `address`, `None` when it does not read as a phone
/// number, or reads as one written without its country code on a channel
/// that configures none: such an address is kept as written (gap G-052).
///
/// A scheme (`whatsapp:`, `tel:`) and the separators `space - . ( )` are
/// dropped, and so is the trunk prefix written `(0)` after a country code.
/// A number written with `+` or the international prefix `00` is
/// international. Six digits or fewer are a short code. A number starting
/// with the trunk prefix `0` is national: it takes the channel's country code
/// in place of the `0`, or is kept as written. With a country code, any
/// other number of more than ten digits that start with the code is
/// written with it, without its `+` (`15550000001` on a channel declaring
/// `1`); any other is national and takes the code before its digits, so an
/// Italian mobile `3931234567` on a channel declaring `39` is
/// `+393931234567`. Without a country code, digits without `+` are kept as
/// written: whether they carry a code cannot be told (`13800138000` is a
/// national number in China, `+13800138000` one in North America), and
/// reading them as international would merge two people. No E.164 number
/// starts with `0`.
pub fn phone_number(address: &str, country_code: Option<&CountryCode>) -> Option<String> {
    let number = without_trunk_mark(without_scheme(address.trim()));
    let (international, digits) = dial(&number)?;
    if international {
        return e164(&digits);
    }
    if digits.len() <= MAX_SHORT_CODE_DIGITS {
        return None;
    }
    if let Some(national) = digits.strip_prefix('0') {
        return country_code.and_then(|code| e164(&format!("{}{national}", code.as_str())));
    }
    let code = country_code?;
    if digits.starts_with(code.as_str()) && digits.len() > MAX_NATIONAL_DIGITS {
        return e164(&digits);
    }
    e164(&format!("{}{digits}", code.as_str()))
}

/// `address` lower-case, without its display name (RFC §5.7): `Alice
/// <Alice@Example.com>` is `alice@example.com`.
pub fn email_address(address: &str) -> String {
    let address = address.trim();
    let bare = address
        .strip_suffix('>')
        .and_then(|named| named.rsplit_once('<'))
        .map_or(address, |(_, bare)| bare);
    bare.trim().to_lowercase()
}

/// The binding metadata key a channel of `channel_type` delivers to by
/// default (RFC §5.7, Appendix A, gap G-051): `phone_number` for SMS, MMS,
/// RCS, WhatsApp and WhatsApp Personal, `email_address` for EMAIL,
/// `facebook_user_id` for Messenger, `teams_conversation_id` for Teams and
/// `telegram_chat_id` for Telegram. `None` for a channel that delivers to a
/// destination of its own, a WebSocket's connections, a webhook's URL, a
/// voice session, which needs no recipient (RFC §10.2 step 3d).
pub fn recipient_key(channel_type: ChannelType) -> Option<&'static str> {
    match channel_type {
        ChannelType::Sms
        | ChannelType::Mms
        | ChannelType::Rcs
        | ChannelType::Whatsapp
        | ChannelType::WhatsappPersonal => Some(PHONE_NUMBER),
        ChannelType::Email => Some(EMAIL_ADDRESS),
        ChannelType::Messenger => Some(FACEBOOK_USER_ID),
        ChannelType::Teams => Some(TEAMS_CONVERSATION_ID),
        ChannelType::Telegram => Some(TELEGRAM_CHAT_ID),
        ChannelType::Websocket
        | ChannelType::Webhook
        | ChannelType::Voice
        | ChannelType::RealtimeVoice
        | ChannelType::AudioVideo
        | ChannelType::RealtimeAudioVideo
        | ChannelType::Video
        | ChannelType::Conference
        | ChannelType::Cli
        | ChannelType::Push
        | ChannelType::Ai
        | ChannelType::System => None,
    }
}

/// The metadata key under which a message on a channel of `channel_type`
/// carries the chat it was posted in, by default (RFC §5.7, gaps G-051 and
/// G-057): `chat_id` for Telegram, `conversation_id` for Teams, and
/// `group_jid` for WhatsApp Personal, whose group messages are their
/// group's and private messages their sender's. `None` for every other
/// type, whose messages are routed by their sender.
pub fn chat_key(channel_type: ChannelType) -> Option<&'static str> {
    match channel_type {
        ChannelType::Telegram => Some(TELEGRAM_CHAT),
        ChannelType::Teams => Some(TEAMS_CONVERSATION),
        ChannelType::WhatsappPersonal => Some(WHATSAPP_GROUP),
        ChannelType::Sms
        | ChannelType::Mms
        | ChannelType::Rcs
        | ChannelType::Email
        | ChannelType::Whatsapp
        | ChannelType::Websocket
        | ChannelType::Messenger
        | ChannelType::Webhook
        | ChannelType::Voice
        | ChannelType::RealtimeVoice
        | ChannelType::AudioVideo
        | ChannelType::RealtimeAudioVideo
        | ChannelType::Video
        | ChannelType::Conference
        | ChannelType::Cli
        | ChannelType::Push
        | ChannelType::Ai
        | ChannelType::System => None,
    }
}

/// Whether `binding` has a recipient under `key`, the one
/// [`AddressForm::recipient`] reads: a string that is not blank, or a
/// number (RFC §10.2 step 3d, gap G-075). A delivery to a binding with
/// none is refused `no_recipient`.
pub fn has_recipient(binding: &ChannelBinding, key: &str) -> bool {
    written_recipient(binding, key).is_some()
}

/// The text after a URI scheme (`whatsapp:`, `tel:`), or `address` when it
/// has none.
fn without_scheme(address: &str) -> &str {
    match address.split_once(':') {
        Some((scheme, rest))
            if !scheme.is_empty()
                && scheme
                    .chars()
                    .all(|character| character.is_ascii_alphabetic()) =>
        {
            rest.trim_start()
        }
        Some(_) | None => address,
    }
}

/// An international number without the trunk prefix written `(0)` after its
/// country code: `+33 (0)6 12 34 56 78` dials `+33 6 12 34 56 78`.
fn without_trunk_mark(number: &str) -> String {
    if number.starts_with('+') {
        number.replacen("(0)", "", 1)
    } else {
        number.to_owned()
    }
}

/// The digits of a dialled number, and whether it was written international
/// (a leading `+` or `00`); `None` when anything but digits and separators
/// is written.
fn dial(number: &str) -> Option<(bool, String)> {
    let mut plus = false;
    let mut digits = String::with_capacity(number.len());
    for character in number.chars() {
        match character {
            '0'..='9' => digits.push(character),
            '+' if digits.is_empty() && !plus => plus = true,
            ' ' | '-' | '.' | '(' | ')' => {}
            _ => return None,
        }
    }
    if plus {
        return Some((true, digits));
    }
    match digits.strip_prefix("00") {
        Some(international) => Some((true, international.to_owned())),
        None => Some((false, digits)),
    }
}

/// `+digits`, when they make a number: more than a short code's digits, at
/// most E.164's, and a country code first, which never starts with `0`.
fn e164(digits: &str) -> Option<String> {
    let length = digits.len();
    let number = length > MAX_SHORT_CODE_DIGITS && length <= MAX_DIGITS && !digits.starts_with('0');
    number.then(|| format!("+{digits}"))
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;
    use crate::model::{ChannelCategory, ChannelDirection, ChannelId, RoomId, Timestamp};

    fn code(code: &str) -> CountryCode {
        CountryCode::new(code).unwrap()
    }

    fn phone(address: &str) -> String {
        normalize(address, Addressing::PhoneNumber, None)
    }

    fn phone_in(address: &str, country_code: &str) -> String {
        normalize(address, Addressing::PhoneNumber, Some(&code(country_code)))
    }

    #[test]
    fn a_number_with_a_scheme_its_international_prefix_or_separators_is_one_number() {
        let written = [
            "whatsapp:+15550000001",
            "+1 (555) 000-0001",
            "tel:+1.555.000.0001",
            "0015550000001",
        ];
        assert_eq!(written.map(phone), ["+15550000001"; 4]);
    }

    /// Gap G-052: with no country code declared, whether digits carry one
    /// cannot be told.
    #[test]
    fn digits_without_a_plus_are_kept_as_written_where_the_channel_has_no_country_code() {
        assert_eq!(phone("15550000001"), "15550000001");
    }

    #[test]
    fn digits_starting_with_the_channel_s_country_code_and_longer_than_ten_carry_it() {
        assert_eq!(phone_in("15550000001", "1"), "+15550000001");
    }

    /// Gap G-052: an Italian mobile number starts with `3`, as Italy's
    /// country code `39` does.
    #[test]
    fn ten_digits_starting_with_the_channel_s_country_code_are_a_national_number() {
        assert_eq!(phone_in("393 123 4567", "39"), "+393931234567");
    }

    #[test]
    fn a_number_without_its_country_code_takes_the_channel_s() {
        assert_eq!(phone_in("555 000 0001", "1"), "+15550000001");
    }

    #[test]
    fn a_number_with_a_trunk_prefix_takes_the_country_code_in_its_place() {
        assert_eq!(phone_in("06 12 34 56 78", "+33"), "+33612345678");
    }

    #[test]
    fn a_number_starting_with_the_channel_s_country_code_is_written_with_it() {
        assert_eq!(phone_in("33612345678", "33"), "+33612345678");
    }

    #[test]
    fn a_number_without_its_country_code_is_kept_where_the_channel_has_none() {
        assert_eq!(phone("555 000 0001"), "555 000 0001");
    }

    #[test]
    fn a_long_national_number_with_a_trunk_prefix_is_kept_where_the_channel_has_none() {
        assert_eq!(phone("07911 123456"), "07911 123456");
    }

    #[test]
    fn a_long_national_number_with_a_trunk_prefix_takes_the_channel_s_code() {
        assert_eq!(phone_in("07911 123456", "44"), "+447911123456");
    }

    #[test]
    fn a_trunk_prefix_written_after_the_country_code_is_dropped() {
        assert_eq!(phone("+33 (0)6 12 34 56 78"), "+33612345678");
    }

    #[test]
    fn an_international_number_starting_with_zero_is_no_number() {
        assert_eq!(phone_number("+0123456789", None), None);
    }

    #[test]
    fn a_short_code_is_kept_as_written_even_with_a_country_code() {
        assert_eq!(phone_in("12345", "1"), "12345");
    }

    #[test]
    fn an_id_that_is_no_number_is_kept_as_written() {
        assert_eq!(
            ["alice", "whatsapp:alice", "alice@example.com"].map(phone),
            ["alice", "whatsapp:alice", "alice@example.com"]
        );
    }

    #[test]
    fn a_number_longer_than_e164_allows_is_kept_as_written() {
        assert_eq!(phone("+1234567890123456"), "+1234567890123456");
    }

    #[test]
    fn a_plus_after_a_digit_is_no_number() {
        assert_eq!(phone_number("1+5550000001", None), None);
    }

    #[test]
    fn an_email_address_loses_its_display_name_and_its_case() {
        assert_eq!(
            normalize("Alice <Alice@Example.com>", Addressing::EmailAddress, None),
            "alice@example.com"
        );
    }

    #[test]
    fn a_bare_email_address_is_lower_cased_and_trimmed() {
        assert_eq!(email_address(" Bob@Example.COM "), "bob@example.com");
    }

    #[test]
    fn a_chat_channel_keeps_its_sender_as_written() {
        assert_eq!(
            normalize("+1 (555) 000-0001", Addressing::Chat, None),
            "+1 (555) 000-0001"
        );
    }

    #[test]
    fn phone_channels_are_addressed_by_phone_number() {
        let types = [
            ChannelType::Sms,
            ChannelType::Mms,
            ChannelType::Rcs,
            ChannelType::Whatsapp,
            ChannelType::WhatsappPersonal,
            ChannelType::Voice,
        ];
        assert_eq!(types.map(Addressing::of), [Addressing::PhoneNumber; 6]);
    }

    #[test]
    fn a_websocket_is_addressed_by_ids_as_written() {
        assert_eq!(
            Addressing::of(ChannelType::Websocket),
            Addressing::AsWritten
        );
    }

    #[test]
    fn a_chat_channel_does_not_reply_to_its_sender() {
        assert!(!Addressing::of(ChannelType::Telegram).replies_to_sender());
    }

    #[test]
    fn a_country_code_is_one_to_three_digits_not_starting_with_zero() {
        let refused =
            ["", "0", "1234", "+", "4a", "033"].map(|code| CountryCode::new(code).is_err());
        assert_eq!(refused, [true; 6]);
    }

    #[test]
    fn a_country_code_is_kept_without_its_plus() {
        assert_eq!(code("+44").as_str(), "44");
    }

    /// Gap G-051: a Telegram binding delivers to `telegram_chat_id`.
    #[test]
    fn a_telegram_channel_delivers_to_its_telegram_chat_id() {
        assert_eq!(
            recipient_key(ChannelType::Telegram),
            Some("telegram_chat_id")
        );
    }

    #[test]
    fn the_appendix_a_channels_name_their_recipient_key() {
        let keys = [
            ChannelType::Sms,
            ChannelType::Email,
            ChannelType::Messenger,
            ChannelType::Teams,
            ChannelType::Rcs,
        ]
        .map(recipient_key);
        assert_eq!(
            keys,
            [
                Some("phone_number"),
                Some("email_address"),
                Some("facebook_user_id"),
                Some("teams_conversation_id"),
                Some("phone_number"),
            ]
        );
    }

    #[test]
    fn a_websocket_and_a_webhook_need_no_recipient() {
        assert_eq!(
            [ChannelType::Websocket, ChannelType::Webhook].map(recipient_key),
            [None, None]
        );
    }

    fn binding_with(metadata: Value) -> ChannelBinding {
        let mut binding = ChannelBinding::new(
            ChannelId::new("sms").unwrap(),
            RoomId::new("room_1").unwrap(),
            ChannelType::Sms,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
            Timestamp::UNIX_EPOCH,
        );
        if let Value::Object(metadata) = metadata {
            binding.metadata = metadata;
        }
        binding
    }

    #[test]
    fn a_binding_with_a_number_has_a_recipient() {
        let binding = binding_with(json!({ "phone_number": "+15550000001" }));
        assert!(has_recipient(&binding, PHONE_NUMBER));
    }

    #[test]
    fn a_null_or_empty_recipient_is_none() {
        let empty = binding_with(json!({ "phone_number": " " }));
        let null = binding_with(json!({ "phone_number": null }));
        assert!(!has_recipient(&empty, PHONE_NUMBER) && !has_recipient(&null, PHONE_NUMBER));
    }

    /// Gap G-075: a binding has the recipient a form reads, and only that
    /// one: a boolean, an array or an object is none, a number is one.
    #[test]
    fn a_binding_has_a_recipient_when_a_form_reads_one() {
        let form = sms_form();
        let values = [
            json!(true),
            json!(["+15550000001"]),
            json!({ "number": "+15550000001" }),
            json!(15_550_000_001_u64),
        ];
        let read: Vec<(bool, bool)> = values
            .into_iter()
            .map(|value| {
                let binding = binding_with(json!({ "phone_number": value }));
                (
                    has_recipient(&binding, PHONE_NUMBER),
                    form.recipient(&binding).is_some(),
                )
            })
            .collect();
        assert_eq!(
            read,
            [(false, false), (false, false), (false, false), (true, true)]
        );
    }

    fn sms_form() -> AddressForm {
        AddressForm {
            addressing: Addressing::PhoneNumber,
            country_code: Some(code("1")),
            recipient_key: Some(PHONE_NUMBER.to_owned()),
        }
    }

    #[test]
    fn a_form_reads_a_recipient_in_its_channel_s_form() {
        let binding = binding_with(json!({ "phone_number": "+1 (555) 000-0001" }));
        assert_eq!(
            sms_form().recipient(&binding),
            Some("+15550000001".to_owned())
        );
    }

    #[test]
    fn a_form_reads_a_numeric_recipient_as_written() {
        let form = AddressForm {
            addressing: Addressing::Chat,
            country_code: None,
            recipient_key: Some(TELEGRAM_CHAT_ID.to_owned()),
        };
        let binding = binding_with(json!({ "telegram_chat_id": -100_777 }));
        assert_eq!(form.recipient(&binding), Some("-100777".to_owned()));
    }

    /// Gap G-051: a WhatsApp Personal group's recipient is its JID, which
    /// no phone rule rewrites.
    #[test]
    fn a_form_reads_a_group_s_jid_as_written() {
        let binding = binding_with(json!({ "phone_number": "120363@g.us" }));
        assert_eq!(
            sms_form().recipient(&binding),
            Some("120363@g.us".to_owned())
        );
    }

    #[test]
    fn a_form_without_a_recipient_key_reads_no_recipient() {
        let form = AddressForm {
            recipient_key: None,
            ..sms_form()
        };
        let binding = binding_with(json!({ "phone_number": "+15550000001" }));
        assert_eq!(form.recipient(&binding), None);
    }

    /// Gap G-051: a chat channel's message carries its chat under a key of
    /// its own, not under the binding's recipient key.
    #[test]
    fn a_chat_channel_carries_its_chat_under_its_chat_key() {
        assert_eq!(
            [ChannelType::Telegram, ChannelType::Teams, ChannelType::Sms].map(chat_key),
            [Some("chat_id"), Some("conversation_id"), None]
        );
    }

    /// RFC §5.7, gap G-051: a WhatsApp Personal group message carries its
    /// group under `group_jid`.
    #[test]
    fn a_whatsapp_personal_group_message_carries_its_group_under_group_jid() {
        assert_eq!(chat_key(ChannelType::WhatsappPersonal), Some("group_jid"));
    }

    /// Properties of the form a channel writes its addresses in.
    mod properties {
        use proptest::prelude::*;

        use super::*;

        /// What a phone number is written with: digits, `+`, the
        /// separators, and a scheme.
        fn written_number() -> impl Strategy<Value = String> {
            let characters = proptest::collection::vec(
                proptest::sample::select("0123456789+ -.()".chars().collect::<Vec<_>>()),
                0..20,
            );
            let scheme = proptest::sample::select(vec!["", "tel:", "whatsapp:"]);
            (scheme, characters).prop_map(|(scheme, characters)| {
                format!("{scheme}{}", characters.into_iter().collect::<String>())
            })
        }

        fn country_code() -> impl Strategy<Value = Option<CountryCode>> {
            proptest::option::of(
                proptest::sample::select(vec!["1", "33", "39", "44", "86"]).prop_map(code),
            )
        }

        proptest! {
            /// RFC §5.7: a number in its channel's form stays in it, so
            /// comparing addresses compares their forms once.
            #[test]
            fn a_phone_number_s_form_is_its_own_form(
                written in written_number(),
                country_code in country_code(),
            ) {
                let once = normalize(&written, Addressing::PhoneNumber, country_code.as_ref());
                let twice = normalize(&once, Addressing::PhoneNumber, country_code.as_ref());
                prop_assert_eq!(twice, once);
            }
        }
    }
}
