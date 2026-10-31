//! A business's SMS number, as a provider sends from it.

use std::collections::BTreeMap;
use std::future::{Future, ready};

use roomkit::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, InboundMessage, RoomContext, RoomEvent,
};
use roomkit::{
    Channel, ChannelAddressing, ChannelError, CountryCode, TransportChannel,
    TransportChannelOptions,
};

use crate::Log;

/// A business's SMS number, as a provider sends from it: a transport
/// addressed as an SMS channel is (RFC §5.7, Appendix A.1), its numbers in
/// E.164, each event it delivers going to the number the room's binding
/// names under `phone_number`; the pipeline refuses a delivery to a
/// binding naming none (`no_recipient`). Like a provider sending an event's
/// text, it sends nothing for an event that has none, a tool call's row.
/// It prints and records what it sent where. Cloning it shares the record.
#[derive(Clone, Debug)]
pub struct SmsNumber {
    channel_id: ChannelId,
    transport: TransportChannel,
    sent: Log<(String, String)>,
}

impl SmsNumber {
    /// The SMS number registered as `channel_id`.
    pub fn new(channel_id: ChannelId) -> Self {
        Self {
            channel_id: channel_id.clone(),
            transport: TransportChannel::new(
                channel_id,
                ChannelType::SMS,
                TransportChannelOptions::default(),
            ),
            sent: Log::new(),
        }
    }

    /// The channel's id.
    pub fn id(&self) -> &ChannelId {
        &self.channel_id
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
    fn channel_id(&self) -> ChannelId {
        self.transport.channel_id()
    }

    fn channel_type(&self) -> ChannelType {
        self.transport.channel_type()
    }

    fn normalize_address(&self, address: &str) -> String {
        self.transport.normalize_address(address)
    }

    fn recipient_key(&self) -> Option<String> {
        self.transport.recipient_key()
    }

    fn recipient_address(&self, binding: &ChannelBinding) -> Option<String> {
        self.transport.recipient_address(binding)
    }

    fn recipient_metadata(&self, address: &str) -> BTreeMap<String, String> {
        self.transport.recipient_metadata(address)
    }

    fn conversation_address(&self, message: &InboundMessage) -> String {
        self.transport.conversation_address(message)
    }

    fn reply_metadata(&self, message: &InboundMessage) -> BTreeMap<String, String> {
        self.transport.reply_metadata(message)
    }

    fn address_config(&self) -> (ChannelAddressing, Option<CountryCode>) {
        self.transport.address_config()
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        self.transport.handle_inbound(message, context)
    }

    fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let text = Self::extract_text(&event);
        if !text.is_empty() {
            let to = self.recipient_address(&binding).unwrap_or_default();
            println!("  SMS to {to}: {text:?}");
            self.sent.push((to, text));
        }
        ready(Ok(ChannelOutput::empty()))
    }
}
