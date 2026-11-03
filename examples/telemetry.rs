//! A message's spans: the kit reports what it does to its telemetry
//! provider, here one that records it, and a test reads the tree. An
//! application hands its OpenTelemetry tracer provider to
//! `OpenTelemetryProvider` with the cargo feature `opentelemetry`.
//!
//! ```sh
//! cargo run --example telemetry
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;
use std::sync::Arc;

use roomkit::model::{ChannelId, ChannelType, InboundMessage, RoomId, TextContent};
use roomkit::telemetry::MockTelemetryProvider;
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, ProcessInboundOptions, RoomKit, RoomKitOptions,
};

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let telemetry = Arc::new(MockTelemetryProvider::default());
    let kit = RoomKit::new(RoomKitOptions {
        telemetry: Some(Arc::clone(&telemetry).into()),
        ..RoomKitOptions::default()
    });
    let room = RoomId::new("room-1")?;
    let create = CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await?;
    for channel in ["sms", "ws"] {
        let channel = ChannelId::new(channel)?;
        let transport = RecordingTransport::new(
            channel.clone(),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        );
        kit.register_channel(transport)?;
        kit.attach_channel(&room, &channel, AttachChannelOptions::default())
            .await?;
    }
    let message = InboundMessage::new(
        ChannelId::new("sms")?,
        "+15551234567",
        TextContent::new("Hello"),
    );
    let options = ProcessInboundOptions {
        room_id: Some(room),
        ..ProcessInboundOptions::default()
    };
    kit.process_inbound(message, options).await?;
    kit.close().await?;

    let spans = telemetry.completed_spans();
    for span in &spans {
        println!(
            "{} ({}) under {:?}: {}",
            span.name, span.kind, span.parent_id, span.status
        );
    }
    let inbound = spans.iter().find(|span| span.name == "framework.inbound");
    let broadcast = spans.iter().find(|span| span.name == "framework.broadcast");
    let (Some(inbound), Some(broadcast)) = (inbound, broadcast) else {
        println!("FAIL the message opened a framework.inbound and a framework.broadcast span");
        return Ok(ExitCode::FAILURE);
    };
    if broadcast.parent_id.as_ref() != Some(&inbound.id) {
        println!("FAIL the broadcast sits under the inbound");
        return Ok(ExitCode::FAILURE);
    }
    if inbound.attributes.contains_key("sender_id") {
        println!("FAIL a span holds no address by default");
        return Ok(ExitCode::FAILURE);
    }
    println!("All checks passed.");
    Ok(ExitCode::SUCCESS)
}
