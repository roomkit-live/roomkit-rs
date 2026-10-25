//! Names that read alike (RFC §6.4, §10.1 step 12): Alice writes on SMS,
//! then someone on the web chat signs `ALICE`, and a third sender `Аlice`,
//! its first letter Cyrillic. The room tells them apart: each message is
//! committed with the record of its author, its name, its rank among the
//! sources whose names read like it, and its source, a digest. Alice keeps
//! rank 1 when she writes again. The room's register of authors, which
//! fixes the ranks, is its private state, apart from the metadata clients
//! may read (RFC §14.1).
//!
//! ```sh
//! cargo run --example names_that_read_alike
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;

use roomkit::ConversationStore;
use roomkit::model::{ChannelId, ChannelType, EventContent, EventType, InboundMessage, RoomId};
use roomkit::testing::RecordingTransport;

use shared::Checks;

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let kit = shared::kit();
    let sms = RecordingTransport::new(ChannelId::new("sms")?, ChannelType::Sms);
    let web = RecordingTransport::new(ChannelId::new("web")?, ChannelType::Websocket);
    kit.register_channel(sms.clone())?;
    kit.register_channel(web.clone())?;
    let room = RoomId::new("room_1")?;
    kit.create_room(&room).await?;
    kit.attach(&room, sms.id()).await?;
    kit.attach(&room, web.id()).await?;

    for (channel, sender, name) in [
        (sms.id(), "+15551234567", "Alice"),
        (web.id(), "visitor-7", "ALICE"),
        (sms.id(), "+15559876543", "\u{410}lice"),
        (sms.id(), "+15551234567", "Alice"),
    ] {
        println!("{sender} writes, signed {name:?}.");
        let mut message = InboundMessage::new(channel.clone(), sender, EventContent::text("Hello"));
        message
            .metadata
            .insert("sender_name".to_owned(), name.into());
        kit.process_inbound_in(&room, message).await?;
    }

    let timeline = shared::timeline(&kit, &room).await?;
    let ranks: Vec<(String, u64)> = timeline
        .iter()
        .filter(|event| event.event_type == EventType::Message)
        .filter_map(|event| {
            let author = event.metadata.get("author")?;
            let name = author.get("name")?.as_str()?.to_owned();
            Some((name, author.get("rank")?.as_u64()?))
        })
        .collect();
    println!("Authors: {ranks:?}");
    let register = kit
        .store()
        .get_private_state(&room, "author_register")
        .await?;
    let stored = kit.store().get_room(&room).await?;

    let mut checks = Checks::default();
    checks.expect(
        "each name that reads like Alice's takes the next rank, and Alice keeps hers",
        ranks
            == [
                ("Alice".to_owned(), 1),
                ("ALICE".to_owned(), 2),
                ("\u{410}lice".to_owned(), 3),
                ("Alice".to_owned(), 1),
            ],
    );
    checks.expect(
        "the register is the room's private state, never in its metadata",
        register.is_some()
            && stored.is_some_and(|room| !room.metadata.contains_key("author_register")),
    );
    Ok(checks.finish())
}
