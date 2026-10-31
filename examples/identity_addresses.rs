//! Identities and their addresses (RFC §5.6, §14.1, §10.4 steps 1 and 2,
//! §17.2). A bank knows its clients as identities of its organization,
//! each with the addresses they use, and gives each one a room bound to its
//! one SMS number, so the router finds a room by who the sender is: an
//! address linked to an identity brings its person's messages to their
//! room, whatever way the number is written. An address belongs to one
//! identity of an organization; moving it to another is an unlink, then a
//! link. Identities are the organization's: another organization knows no
//! one by the same number.
//!
//! A binding not declared `group` is one correspondent's conversation, and
//! its answers go to that correspondent (RFC §10.4). Marie's room records
//! her as its member: her first text is taken in, and the binding then
//! names the number it came from, so a text from another of her numbers
//! gets a room of its own. To keep one room for all of a client's numbers,
//! the bank opens it naming the identity as the binding's correspondent,
//! as Jean's room does: every number linked to him reaches it, and its
//! answers go to the number the binding delivers to, the one he first
//! wrote from unless the bank names one.
//!
//! ```sh
//! cargo run --example identity_addresses
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;

use roomkit::model::{
    ChannelId, ChannelType, Identity, IdentityId, InboundMessage, Participant, ParticipantId,
    RoomId, TextContent,
};
use roomkit::store::{LinkAddressOptions, ResolveIdentityOptions, UnlinkAddressOptions};
use roomkit::testing::{EchoAgent, EchoAgentOptions};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, ProcessInboundOptions, RoomKit,
    StoreError,
};

use shared::{Checks, Clock, SmsNumber};

/// The organization the bank's rooms and identities belong to.
const BANK: &str = "bank";
const JEAN: &str = "+15551234567";
const JEAN_WORK: &str = "+15559998888";
const MARIE: &str = "+15557654321";
/// The bank's kit, its SMS number and the agent that answers on it.
struct Bank {
    kit: RoomKit,
    clock: Clock,
    sms: SmsNumber,
    agent: EchoAgent,
}

impl Bank {
    fn new() -> Result<Self, Box<dyn Error>> {
        let clock = Clock::new();
        let kit = clock.kit();
        let sms = SmsNumber::new(ChannelId::new("sms")?);
        let agent = EchoAgent::new(
            ChannelId::new("ai")?,
            "You said: ",
            EchoAgentOptions { quote: true },
        );
        kit.register_channel(sms.clone())?;
        kit.register_channel(agent.clone())?;
        Ok(Self {
            kit,
            clock,
            sms,
            agent,
        })
    }

    /// The identity `id` of the bank, named `name`, using `numbers` on SMS.
    async fn client(
        &self,
        id: &str,
        name: &str,
        numbers: &[&str],
    ) -> Result<IdentityId, Box<dyn Error>> {
        let identity_id = IdentityId::new(id)?;
        let identity = Identity {
            organization_id: Some(BANK.to_owned()),
            display_name: Some(name.to_owned()),
            ..Identity::new(identity_id.clone())
        };
        let store = self.kit.store();
        store.create_identity(identity).await?;
        for number in numbers {
            store
                .link_address(
                    &identity_id,
                    &ChannelType::SMS,
                    number,
                    LinkAddressOptions {
                        organization_id: Some(BANK.to_owned()),
                    },
                )
                .await?;
        }
        Ok(identity_id)
    }

    /// A room of the bank, bound to its agent, and to its number with
    /// `options`.
    async fn open(
        &self,
        room: &str,
        options: AttachChannelOptions,
    ) -> Result<RoomId, Box<dyn Error>> {
        let room_id = RoomId::new(room)?;
        let bank = || Some(BANK.to_owned());
        let room = CreateRoomOptions {
            room_id: Some(room_id.clone()),
            organization_id: bank(),
            ..Default::default()
        };
        self.kit.create_room(room).await?;
        let sms = AttachChannelOptions {
            organization_id: bank(),
            ..options
        };
        self.kit
            .attach_channel(&room_id, self.sms.id(), sms)
            .await?;
        let agent = AttachChannelOptions {
            organization_id: bank(),
            ..Default::default()
        };
        self.kit
            .attach_channel(&room_id, self.agent.channel_id(), agent)
            .await?;
        Ok(room_id)
    }

    /// A room of the bank with the client `identity_id` recorded as its
    /// member on SMS: its binding names no one until the client's first
    /// text, whose number it then names.
    async fn room_of(
        &self,
        identity_id: &IdentityId,
        room: &str,
    ) -> Result<RoomId, Box<dyn Error>> {
        let room_id = self.open(room, AttachChannelOptions::default()).await?;
        let member = Participant {
            identity_id: Some(identity_id.clone()),
            joined_at: self.clock.now(),
            ..Participant::new(
                ParticipantId::new(format!("member_{identity_id}"))?,
                room_id.clone(),
                self.sms.id().clone(),
            )
        };
        self.kit.store().add_participant(member).await?;
        Ok(room_id)
    }

    /// A room of the bank whose SMS binding names the client `identity_id`
    /// as its correspondent: every number linked to the identity is the
    /// room's conversation.
    async fn room_naming(
        &self,
        identity_id: &IdentityId,
        room: &str,
    ) -> Result<RoomId, Box<dyn Error>> {
        let correspondent = ParticipantId::new(identity_id.as_str())?;
        let naming = AttachChannelOptions {
            participant_id: Some(correspondent),
            ..Default::default()
        };
        self.open(room, naming).await
    }

    /// Moves `number` from the identity `from` to `to` (RFC §5.6): an
    /// unlink, then a link; the rooms that named it stay as they are.
    async fn move_number(
        &self,
        from: &IdentityId,
        to: &IdentityId,
        number: &str,
    ) -> Result<(), Box<dyn Error>> {
        let store = self.kit.store();
        let unlinked = UnlinkAddressOptions {
            organization_id: Some(BANK.to_owned()),
        };
        store
            .unlink_address(from, &ChannelType::SMS, number, unlinked)
            .await?;
        let linked = LinkAddressOptions {
            organization_id: Some(BANK.to_owned()),
        };
        store
            .link_address(to, &ChannelType::SMS, number, linked)
            .await?;
        Ok(())
    }

    /// A text from `number` to the SMS number of `organization`'s rooms;
    /// returns the room it was routed to.
    async fn text_from(&self, organization: &str, number: &str) -> Result<RoomId, Box<dyn Error>> {
        let message =
            InboundMessage::new(self.sms.id().clone(), number, TextContent::new("Bonjour"));
        let options = ProcessInboundOptions {
            organization_id: Some(organization.to_owned()),
            ..Default::default()
        };
        let result = self.kit.process_inbound(message, options).await?;
        let room = result.event.ok_or("the message was refused")?.room_id;
        println!("{number} texts {organization}: routed to {room}.");
        Ok(room)
    }
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let bank = Bank::new()?;
    let store = bank.kit.store();
    let mut checks = Checks::default();

    println!("The bank knows Jean, on his phone and his work phone, and Marie.");
    let jean = bank.client("id_jean", "Jean", &[JEAN, JEAN_WORK]).await?;
    let marie = bank.client("id_marie", "Marie", &[MARIE]).await?;
    let jeans_room = bank.room_naming(&jean, "jean_room").await?;
    let maries_room = bank.room_of(&marie, "marie_room").await?;
    let found = store
        .resolve_identity(
            &ChannelType::SMS,
            JEAN,
            ResolveIdentityOptions {
                organization_id: Some(BANK.to_owned()),
            },
        )
        .await?;
    let elsewhere = store
        .resolve_identity(
            &ChannelType::SMS,
            JEAN,
            ResolveIdentityOptions {
                organization_id: Some("clinic".to_owned()),
            },
        )
        .await?;
    checks.expect(
        "an address resolves to its identity within the organization, and to none outside it",
        found.is_some_and(|identity| identity.id == jean) && elsewhere.is_none(),
    );

    let landed = bank.text_from(BANK, "+1 (555) 123-4567").await?;
    checks.expect(
        "Jean's text, his number written his own way, reaches his room through his identity",
        landed == jeans_room && bank.sms.sent_to(JEAN) == ["You said: Bonjour"],
    );
    let from_work = bank.text_from(BANK, JEAN_WORK).await?;
    checks.expect(
        "his work phone reaches the same room, which names his identity, answered at his phone",
        from_work == jeans_room
            && bank.sms.sent_to(JEAN).len() == 2
            && bank.sms.sent_to(JEAN_WORK).is_empty(),
    );

    let taken = store
        .link_address(
            &jean,
            &ChannelType::SMS,
            MARIE,
            LinkAddressOptions {
                organization_id: Some(BANK.to_owned()),
            },
        )
        .await;
    checks.expect(
        "an address belongs to one identity of the organization: Marie's number cannot be Jean's, \
         and the refusal names neither the number nor Marie's identity",
        matches!(
            &taken,
            Err(StoreError::AddressLinked { channel_type }) if *channel_type == ChannelType::SMS
        ) && taken.as_ref().is_err_and(|refusal| {
            let told = refusal.to_string();
            !told.contains(MARIE) && !told.contains(marie.as_str())
        }),
    );

    println!("Jean gives his work phone to Marie: the bank unlinks it from him, links it to her.");
    bank.move_number(&jean, &marie, JEAN_WORK).await?;
    let landed = bank.text_from(BANK, JEAN_WORK).await?;
    checks.expect(
        "a text from the number moved reaches Marie's room, and is answered there",
        landed == maries_room && bank.sms.sent_to(JEAN_WORK) == ["You said: Bonjour"],
    );
    let from_her_own = bank.text_from(BANK, MARIE).await?;
    checks.expect(
        "Marie's room names the number she first wrote from: her own number gets a room of its own",
        from_her_own != maries_room && from_her_own != jeans_room,
    );

    let landed = bank.text_from("clinic", JEAN).await?;
    let clinic_room = store.get_room(&landed).await?;
    checks.expect(
        "the clinic knows no one by Jean's number: his text opens a room of the clinic's",
        landed != jeans_room
            && clinic_room.is_some_and(|room| room.organization_id.as_deref() == Some("clinic")),
    );
    Ok(checks.finish())
}
