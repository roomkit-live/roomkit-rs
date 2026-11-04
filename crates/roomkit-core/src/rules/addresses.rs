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

use crate::model::{ChannelBinding, ChannelType, Metadata};

/// The binding metadata key of a recipient's phone number (Appendix A.1,
/// A.3, A.4).
pub(crate) const PHONE_NUMBER: &str = "phone_number";

/// The binding metadata key of a recipient's email address (Appendix A.2).
pub(crate) const EMAIL_ADDRESS: &str = "email_address";

/// The binding metadata key of a Messenger recipient (Appendix A.5).
pub(crate) const FACEBOOK_USER_ID: &str = "facebook_user_id";

/// The binding metadata key of a Teams conversation (Appendix A.6).
pub(crate) const TEAMS_CONVERSATION_ID: &str = "teams_conversation_id";

/// The binding metadata key of a Telegram chat, which its provider sends to
/// (Appendix A.13, gap G-051).
pub(crate) const TELEGRAM_CHAT_ID: &str = "telegram_chat_id";

/// The metadata key under which a Telegram message carries the chat it was
/// posted in (Appendix A.13, gap G-057).
pub(crate) const TELEGRAM_CHAT: &str = "chat_id";

/// The metadata key under which a Teams message carries the conversation it
/// was posted in (Appendix A.6, gap G-057).
pub(crate) const TEAMS_CONVERSATION: &str = "conversation_id";

/// The metadata key under which a WhatsApp Personal group message carries
/// its group (Appendix A.4, gap G-057); a private message carries none.
pub(crate) const WHATSAPP_GROUP: &str = "group_jid";

/// The sender under which the framework writes into a room itself; it names
/// no correspondent (RFC §10.4).
pub(crate) const SYSTEM_SENDER: &str = "system";

/// The reason a message routed under [`SYSTEM_SENDER`] is refused (RFC §10.1
/// step 2): the framework names the room it writes to.
pub(crate) const RESERVED_SENDER: &str = "reserved_sender";

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
#[non_exhaustive]
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
    /// How a channel of `channel_type` is addressed by default (gap G-051):
    /// SMS, MMS, RCS, WhatsApp, WhatsApp Personal and VOICE by phone number,
    /// EMAIL by email address, Telegram and Teams by chat, any other name by
    /// an id as written. A channel's declaration takes it, and may set
    /// another (`ChannelConfig::addressing`).
    pub(crate) fn of(channel_type: &ChannelType) -> Self {
        match channel_type.as_str() {
            "sms" | "mms" | "rcs" | "whatsapp" | "whatsapp_personal" | "voice" => Self::PhoneNumber,
            "email" => Self::EmailAddress,
            "telegram" | "teams" => Self::Chat,
            _ => Self::AsWritten,
        }
    }

    /// Whether replies go to the address a sender writes from, so that the
    /// sender's address is a binding's recipient (RFC §5.7). On a chat
    /// channel they go to the chat.
    pub(crate) fn replies_to_sender(self) -> bool {
        match self {
            Self::PhoneNumber | Self::EmailAddress | Self::AsWritten => true,
            Self::Chat => false,
        }
    }
}

/// A channel's addressing declaration (RFC §5.7, Appendix A), Python's
/// `ChannelAddressing(addressing, recipient_key=None, chat_key=None,
/// replies_to_sender=False)`: how its addresses are written, the binding
/// metadata key its recipient is written under (`None` for a channel that
/// delivers to a destination of its own), the inbound metadata key a
/// message carries its chat under, on a channel that answers chats, and
/// whether replies go to the address the correspondent writes from, the
/// framework then recording an inbound sender as a binding's recipient.
///
/// ```
/// use roomkit_core::model::ChannelType;
/// use roomkit_core::rules::addresses::{Addressing, addressing_of};
///
/// let sms = addressing_of(&ChannelType::SMS);
/// assert_eq!(sms.addressing, Addressing::PhoneNumber);
/// assert_eq!(sms.recipient_key.as_deref(), Some("phone_number"));
/// assert!(sms.names_conversations());
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelAddressing {
    /// How its addresses are written.
    pub addressing: Addressing,
    /// The binding metadata key its recipient is written under.
    pub recipient_key: Option<String>,
    /// The inbound metadata key a message carries its chat under.
    pub chat_key: Option<String>,
    /// Whether replies go to the address the correspondent writes from.
    pub replies_to_sender: bool,
}

/// What [`ChannelAddressing::new`] takes beside the addressing: Python's
/// keywords, their defaults `None`, `None` and `false`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChannelAddressingOptions {
    /// The binding metadata key its recipient is written under.
    pub recipient_key: Option<String>,
    /// The inbound metadata key a message carries its chat under.
    pub chat_key: Option<String>,
    /// Whether replies go to the address the correspondent writes from.
    pub replies_to_sender: bool,
}

/// What [`ChannelAddressing::normalize`] takes beside the address:
/// Python's `country_code=None`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NormalizeOptions {
    /// The country code a phone number written without one takes.
    pub country_code: Option<CountryCode>,
}

impl ChannelAddressing {
    /// The declaration of a channel addressed by `addressing`.
    pub fn new(addressing: Addressing, options: ChannelAddressingOptions) -> Self {
        Self {
            addressing,
            recipient_key: options.recipient_key,
            chat_key: options.chat_key,
            replies_to_sender: options.replies_to_sender,
        }
    }

    /// Whether a binding's recipient is a conversation's address: the
    /// sender's, or a chat. A recipient a channel merely forwards (a
    /// webhook's) names no one.
    pub fn names_conversations(&self) -> bool {
        self.recipient_key.is_some() && (self.replies_to_sender || self.chat_key.is_some())
    }

    /// `address` in the form the channel compares and stores it in: a
    /// phone number written without its country code takes the one
    /// `options` names, or is kept as written without one.
    pub fn normalize(&self, address: &str, options: NormalizeOptions) -> String {
        let NormalizeOptions { country_code } = options;
        normalize(address, self.addressing, country_code.as_ref())
    }
}

/// The addressing a channel of `channel_type` takes by default (RFC §5.7,
/// Appendix A), as Python's `addressing_of` gives it: SMS, MMS, RCS,
/// WhatsApp and WhatsApp Personal by phone number under `phone_number`,
/// WhatsApp Personal's groups under `group_jid`; VOICE by phone number with
/// no recipient; EMAIL by email address under `email_address`; Messenger
/// as written under `facebook_user_id`; Telegram and Teams by chat, under
/// `telegram_chat_id` and `teams_conversation_id`, their chats under
/// `chat_id` and `conversation_id`; any other type as written, with no
/// recipient and no chat key.
pub fn addressing_of(channel_type: &ChannelType) -> ChannelAddressing {
    let replies_to_sender = matches!(
        channel_type.as_str(),
        "sms" | "mms" | "rcs" | "whatsapp" | "whatsapp_personal" | "email" | "messenger"
    );
    ChannelAddressing {
        addressing: Addressing::of(channel_type),
        recipient_key: recipient_key(channel_type).map(str::to_owned),
        chat_key: chat_key(channel_type).map(str::to_owned),
        replies_to_sender,
    }
}

/// A country code a channel's numbers written without one take (RFC §5.7):
/// one to three digits, the first not `0` (ITU-T E.164).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CountryCode(String);

/// A value that is not a country code: Python's `ValueError`, which says
/// what it refused in its message and holds no field.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("{0:?} is not a country code: one to three digits, the first not 0")]
pub struct InvalidCountryCode(String);

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
    /// The recipient the channel answers for the binding being read, in its
    /// own form, when it was asked (Python's `routed_recipient`, RFC §5.7):
    /// routing takes it over the value under the key, a value that is not a
    /// recipient apart. `None` reads the key.
    pub answered_recipient: Option<String>,
}

impl AddressForm {
    /// `address` in the channel's form.
    pub fn normalize(&self, address: &str) -> String {
        normalize(address, self.addressing, self.country_code.as_ref())
    }

    /// The recipient `binding` delivers to, in the channel's form: its
    /// value under the channel's recipient key, when it is one
    /// (`has_recipient`). An integer, a Telegram chat id, is written in
    /// its digits.
    pub fn recipient(&self, binding: &ChannelBinding) -> Option<String> {
        match self.held(binding) {
            Held::Recipient(recipient) => Some(recipient),
            Held::Nothing | Held::NotARecipient(_) => None,
        }
    }

    /// Who `binding` delivers to, as routing reads it (RFC §5.7): a value
    /// under the channel's recipient key that is not a recipient; else the
    /// channel's own answer; else its recipient under the key, in the
    /// channel's form, or no value. A channel with no recipient key holds no
    /// value there.
    pub(crate) fn held(&self, binding: &ChannelBinding) -> Held<String> {
        let under_key = self
            .recipient_key
            .as_deref()
            .map_or(Held::Nothing, |key| held(binding.metadata.get(key)));
        match (under_key, &self.answered_recipient) {
            (Held::NotARecipient(kind), _) => Held::NotARecipient(kind),
            (_, Some(answered)) => Held::Recipient(answered.clone()),
            (Held::Recipient(Written::Text(recipient)), None) => {
                Held::Recipient(self.normalize(recipient))
            }
            (Held::Recipient(Written::Integer(recipient)), None) => {
                Held::Recipient(recipient.to_string())
            }
            (Held::Nothing, None) => Held::Nothing,
        }
    }
}

/// What a binding holds under its channel's recipient key, or a message
/// under its chat key (RFC §5.7, gap G-078).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Held<T> {
    /// No value: the key absent, null, or a blank string.
    Nothing,
    /// A recipient.
    Recipient(T),
    /// A value that is not a recipient, and is no absence either: a value
    /// no one is reached at. Only its JSON type is kept (`boolean`,
    /// `float`, `array`, `object`), which a log may show; the value itself
    /// may be a person's address.
    NotARecipient(&'static str),
}

/// An address as a JSON value holds it (RFC §5.7, gap G-075).
pub(crate) enum Written<'value> {
    /// A string that is not blank, as written.
    Text(&'value str),
    /// An integer, which names the address its digits write: `12345` and
    /// `"12345"` are one address.
    Integer(&'value Number),
}

/// What `value`, read under a recipient key or a chat key, holds (RFC
/// §5.7, gaps G-075, G-078): an address when it is a string that is not
/// blank, or an integer, a chat id written as one; no value when it is
/// absent, null or a blank string; any other value (a boolean, a float, a
/// whole one included (`42.0`), an array, an object) is not a recipient. A
/// float has no single written form, so it cannot be compared as an
/// address.
pub(crate) fn held(value: Option<&Value>) -> Held<Written<'_>> {
    let Some(value) = value else {
        return Held::Nothing;
    };
    match value {
        Value::Null => Held::Nothing,
        Value::String(address) if address.trim().is_empty() => Held::Nothing,
        Value::String(address) => Held::Recipient(Written::Text(address)),
        Value::Number(address) if address.is_i64() || address.is_u64() => {
            Held::Recipient(Written::Integer(address))
        }
        Value::Number(_) => Held::NotARecipient("float"),
        Value::Bool(_) => Held::NotARecipient("boolean"),
        Value::Array(_) => Held::NotARecipient("array"),
        Value::Object(_) => Held::NotARecipient("object"),
    }
}

/// The address `value` holds ([`held`]): `None` for no value and for a
/// value that is not a recipient alike. A delivery reads a binding's
/// recipient so, and refuses both `no_recipient`; a message's chat is read
/// so, either being no chat.
pub(crate) fn written(value: &Value) -> Option<Written<'_>> {
    match held(Some(value)) {
        Held::Recipient(address) => Some(address),
        Held::Nothing | Held::NotARecipient(_) => None,
    }
}

/// `address` in the form of a channel addressed by `addressing` (RFC §5.7,
/// §10.1 step 1): a phone number in E.164 when it reads as one, an email
/// address lower-case without its display name, any other id as written.
pub(crate) fn normalize(
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
pub(crate) fn phone_number(address: &str, country_code: Option<&CountryCode>) -> Option<String> {
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
pub(crate) fn email_address(address: &str) -> String {
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
/// voice session, which needs no recipient (RFC §10.2 step 3d), and for any
/// name not listed: a channel's declaration sets its own.
pub(crate) fn recipient_key(channel_type: &ChannelType) -> Option<&'static str> {
    match channel_type.as_str() {
        "sms" | "mms" | "rcs" | "whatsapp" | "whatsapp_personal" => Some(PHONE_NUMBER),
        "email" => Some(EMAIL_ADDRESS),
        "messenger" => Some(FACEBOOK_USER_ID),
        "teams" => Some(TEAMS_CONVERSATION_ID),
        "telegram" => Some(TELEGRAM_CHAT_ID),
        _ => None,
    }
}

/// The metadata key under which a message on a channel of `channel_type`
/// carries the chat it was posted in, by default (RFC §5.7, gaps G-051 and
/// G-057): `chat_id` for Telegram, `conversation_id` for Teams, and
/// `group_jid` for WhatsApp Personal, whose group messages are their
/// group's and private messages their sender's. `None` for every other
/// name, whose messages are routed by their sender, unless a channel's
/// declaration sets a key.
pub(crate) fn chat_key(channel_type: &ChannelType) -> Option<&'static str> {
    match channel_type.as_str() {
        "telegram" => Some(TELEGRAM_CHAT),
        "teams" => Some(TEAMS_CONVERSATION),
        "whatsapp_personal" => Some(WHATSAPP_GROUP),
        _ => None,
    }
}

/// The recipient `metadata` holds under `key`, as written (RFC §5.7, §10.2
/// step 3d), as Python's `written_recipient` reads it: a string that is not
/// blank, as it is, or an integer, a chat id, in its digits; `None` for no
/// value and for a value that is not a recipient.
pub fn written_recipient(metadata: &Metadata, key: &str) -> Option<String> {
    match metadata.get(key).and_then(written)? {
        Written::Text(recipient) => Some(recipient.to_owned()),
        Written::Integer(recipient) => Some(recipient.to_string()),
    }
}

/// Whether `binding` has a recipient under `key`, the one
/// [`AddressForm::recipient`] reads: a string that is not blank, or an
/// integer (RFC §5.7, §10.2 step 3d, gaps G-075, G-078). A delivery to a
/// binding with none, no value or a value that is not a recipient alike, is
/// refused `no_recipient`.
pub(crate) fn has_recipient(binding: &ChannelBinding, key: &str) -> bool {
    binding.metadata.get(key).and_then(written).is_some()
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

    /// What Python's `addressing_of` gives each channel type (probe
    /// `tasks/probes/step5-lot4/py_addressing_of.out`, roomkit 38ef0625):
    /// addressing, recipient key, chat key, replies to the sender, names
    /// conversations.
    type Row = (
        &'static str,
        Addressing,
        Option<&'static str>,
        Option<&'static str>,
        bool,
        bool,
    );

    const PYTHON_ADDRESSING: [Row; 24] = [
        (
            "sms",
            Addressing::PhoneNumber,
            Some("phone_number"),
            None,
            true,
            true,
        ),
        (
            "mms",
            Addressing::PhoneNumber,
            Some("phone_number"),
            None,
            true,
            true,
        ),
        (
            "rcs",
            Addressing::PhoneNumber,
            Some("phone_number"),
            None,
            true,
            true,
        ),
        (
            "email",
            Addressing::EmailAddress,
            Some("email_address"),
            None,
            true,
            true,
        ),
        (
            "whatsapp",
            Addressing::PhoneNumber,
            Some("phone_number"),
            None,
            true,
            true,
        ),
        (
            "whatsapp_personal",
            Addressing::PhoneNumber,
            Some("phone_number"),
            Some("group_jid"),
            true,
            true,
        ),
        ("websocket", Addressing::AsWritten, None, None, false, false),
        ("ai", Addressing::AsWritten, None, None, false, false),
        ("voice", Addressing::PhoneNumber, None, None, false, false),
        (
            "realtime_voice",
            Addressing::AsWritten,
            None,
            None,
            false,
            false,
        ),
        (
            "realtime_audio_video",
            Addressing::AsWritten,
            None,
            None,
            false,
            false,
        ),
        ("push", Addressing::AsWritten, None, None, false, false),
        (
            "messenger",
            Addressing::AsWritten,
            Some("facebook_user_id"),
            None,
            true,
            true,
        ),
        (
            "telegram",
            Addressing::Chat,
            Some("telegram_chat_id"),
            Some("chat_id"),
            false,
            true,
        ),
        (
            "teams",
            Addressing::Chat,
            Some("teams_conversation_id"),
            Some("conversation_id"),
            false,
            true,
        ),
        ("discord", Addressing::AsWritten, None, None, false, false),
        ("buzz", Addressing::AsWritten, None, None, false, false),
        ("webhook", Addressing::AsWritten, None, None, false, false),
        ("video", Addressing::AsWritten, None, None, false, false),
        (
            "audio_video",
            Addressing::AsWritten,
            None,
            None,
            false,
            false,
        ),
        (
            "conference",
            Addressing::AsWritten,
            None,
            None,
            false,
            false,
        ),
        ("cli", Addressing::AsWritten, None, None, false, false),
        ("host", Addressing::AsWritten, None, None, false, false),
        ("system", Addressing::AsWritten, None, None, false, false),
    ];

    /// Python's `addressing_of`, for every channel type it names (probe
    /// `tasks/probes/step5-lot4/py_addressing_of.out`, roomkit 38ef0625).
    #[test]
    fn addressing_of_matches_python() {
        let python = PYTHON_ADDRESSING;
        for (name, addressing, recipient_key, chat_key, replies_to_sender, names_conversations) in
            python
        {
            let read = addressing_of(&ChannelType::new(name).unwrap());
            assert_eq!(
                (
                    read.addressing,
                    read.recipient_key.as_deref(),
                    read.chat_key.as_deref(),
                    read.replies_to_sender,
                    read.names_conversations()
                ),
                (
                    addressing,
                    recipient_key,
                    chat_key,
                    replies_to_sender,
                    names_conversations
                ),
                "{name}"
            );
        }
    }

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
            ChannelType::SMS,
            ChannelType::MMS,
            ChannelType::RCS,
            ChannelType::WHATSAPP,
            ChannelType::WHATSAPP_PERSONAL,
            ChannelType::VOICE,
        ];
        assert_eq!(
            types.each_ref().map(Addressing::of),
            [Addressing::PhoneNumber; 6]
        );
    }

    #[test]
    fn a_websocket_is_addressed_by_ids_as_written() {
        assert_eq!(
            Addressing::of(&ChannelType::WEBSOCKET),
            Addressing::AsWritten
        );
    }

    #[test]
    fn a_chat_channel_does_not_reply_to_its_sender() {
        assert!(!Addressing::of(&ChannelType::TELEGRAM).replies_to_sender());
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
            recipient_key(&ChannelType::TELEGRAM),
            Some("telegram_chat_id")
        );
    }

    #[test]
    fn the_appendix_a_channels_name_their_recipient_key() {
        let keys = [
            ChannelType::SMS,
            ChannelType::EMAIL,
            ChannelType::MESSENGER,
            ChannelType::TEAMS,
            ChannelType::RCS,
        ]
        .each_ref()
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

    /// RFC §10.5, Appendix A.14.1: the host channel is never delivered to
    /// and carries no chat: its ids are kept as written.
    #[test]
    fn a_host_channel_is_addressed_as_written_with_no_recipient_nor_chat() {
        assert_eq!(
            (
                Addressing::of(&ChannelType::HOST),
                recipient_key(&ChannelType::HOST),
                chat_key(&ChannelType::HOST)
            ),
            (Addressing::AsWritten, None, None)
        );
    }

    #[test]
    fn a_websocket_and_a_webhook_need_no_recipient() {
        assert_eq!(
            [ChannelType::WEBSOCKET, ChannelType::WEBHOOK]
                .each_ref()
                .map(recipient_key),
            [None, None]
        );
    }

    fn binding_with(metadata: Value) -> ChannelBinding {
        let mut binding = ChannelBinding::at(
            ChannelId::new("sms").unwrap(),
            RoomId::new("room_1").unwrap(),
            ChannelType::SMS,
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
            answered_recipient: None,
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

    /// RFC §5.7: a float has no single written form, so it is no
    /// address: a whole one, one a store reads back with an exponent and
    /// one with a fraction are none, an integer its digits.
    #[test]
    fn a_float_is_no_recipient_a_whole_one_included() {
        let form = AddressForm {
            addressing: Addressing::Chat,
            country_code: None,
            recipient_key: Some(TELEGRAM_CHAT_ID.to_owned()),
        };
        let read: Vec<(bool, Option<String>)> = [json!(42.0), json!(1e16), json!(4.5), json!(42)]
            .into_iter()
            .map(|value| {
                let binding = binding_with(json!({ "telegram_chat_id": value }));
                (
                    has_recipient(&binding, TELEGRAM_CHAT_ID),
                    form.recipient(&binding),
                )
            })
            .collect();
        assert_eq!(
            read,
            [
                (false, None),
                (false, None),
                (false, None),
                (true, Some("42".to_owned()))
            ]
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
            [ChannelType::TELEGRAM, ChannelType::TEAMS, ChannelType::SMS]
                .each_ref()
                .map(chat_key),
            [Some("chat_id"), Some("conversation_id"), None]
        );
    }

    /// RFC §5.7, gap G-051: a WhatsApp Personal group message carries its
    /// group under `group_jid`.
    #[test]
    fn a_whatsapp_personal_group_message_carries_its_group_under_group_jid() {
        assert_eq!(chat_key(&ChannelType::WHATSAPP_PERSONAL), Some("group_jid"));
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
            // A failing case is printed, never written to a file a later run
            // would replay.
            #![proptest_config(ProptestConfig {
                failure_persistence: None,
                ..ProptestConfig::default()
            })]

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
