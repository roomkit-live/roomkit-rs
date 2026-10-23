//! A business's SMS number, as a provider sends from it.

use std::future::{Future, ready};

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    RoomContext, RoomEvent,
};
use roomkit::{Channel, ChannelDeclaration, ChannelError, text_of};

use crate::Log;

/// A business's SMS number, as a provider sends from it: each event goes to
/// the number the room's binding names under `phone_number` (RFC Appendix
/// A.1), and the pipeline refuses a delivery to a binding naming none
/// (`no_recipient`). It prints and records what it sent where. Cloning it
/// shares the record.
#[derive(Clone, Debug)]
pub struct SmsNumber {
    declaration: ChannelDeclaration,
    sent: Log<(String, String)>,
}

impl SmsNumber {
    /// The SMS number registered as `channel_id`.
    pub fn new(channel_id: ChannelId) -> Self {
        Self {
            declaration: ChannelDeclaration::new(
                channel_id,
                ChannelType::Sms,
                ChannelCategory::Transport,
                ChannelDirection::Bidirectional,
            ),
            sent: Log::new(),
        }
    }

    /// The channel's id.
    pub fn id(&self) -> &ChannelId {
        &self.declaration.channel_id
    }

    /// What was sent to `number`, in order.
    pub fn sent_to(&self, number: &str) -> Vec<String> {
        self.sent
            .entries()
            .into_iter()
            .filter(|(to, _)| to == number)
            .map(|(_, text)| text)
            .collect()
    }
}

impl Channel for SmsNumber {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let to = binding
            .metadata
            .get("phone_number")
            .and_then(|number| number.as_str())
            .unwrap_or_default()
            .to_owned();
        let text = text_of(&event.content);
        println!("  SMS to {to}: {text:?}");
        self.sent.push((to, text));
        ready(Ok(ChannelOutput::default()))
    }
}
