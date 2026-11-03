//! An AI agent whose model is overloaded (RFC §6.4): Alice asks a bakery by
//! SMS, and the agent's provider answers 503, once and again. The agent's
//! retry policy tries the generation once more, after 10 ms; once that
//! retry is spent, the agent asks its fallback provider, another model,
//! whose answer is the agent's. Nothing of a refused generation reached
//! the room, so nothing is said twice.
//!
//! Both models are local fakes of the OpenAI wire, so it needs no network
//! and no key. With `OPENAI_BASE_URL`, `OPENAI_API_KEY` and `OPENAI_MODEL`
//! set, the fallback is that endpoint instead.
//!
//! ```sh
//! cargo run --example agent_fallback
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;
use std::sync::Arc;

use roomkit::model::{ChannelId, InboundMessage, RetryPolicy, RoomId, TextContent};
use roomkit::{AttachChannelOptions, Channel, CreateRoomOptions, ProcessInboundOptions};
use roomkit_ai::{AiChannel, AiChannelOptions};

use shared::text_of;
use shared::{Checks, FakeOpenAi, OpenAiEndpoint, Reply, SmsNumber};

const SYSTEM_PROMPT: &str = "You answer the customers of a bakery, in one short sentence.";

/// What the fallback model answers.
const BACKUP: &str = "Yes, we have sourdough until noon.";

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let overloaded = Reply::refusing(503, "The server is overloaded.");
    let primary = OpenAiEndpoint::Fake(FakeOpenAi::start(&[overloaded]).await?);
    let backup = OpenAiEndpoint::from_env(&[BACKUP]).await?;
    println!(
        "The agent calls {}, then {}.",
        primary.shown_base_url(),
        backup.shown_base_url()
    );
    let options = AiChannelOptions {
        system_prompt: Some(SYSTEM_PROMPT.to_owned()),
        retry_policy: Some(RetryPolicy {
            max_retries: 1,
            base_delay_seconds: 0.01,
            ..RetryPolicy::default()
        }),
        fallback_provider: Some(Arc::new(backup.provider()?)),
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(ChannelId::new("assistant")?, primary.provider()?, options)?;

    let kit = shared::kit();
    let sms = SmsNumber::new(ChannelId::new("sms")?);
    let agent_id = agent.channel_id();
    kit.register_channel(sms.clone())?;
    kit.register_channel(agent)?;
    let room = RoomId::new("room_1")?;
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await?;
    for channel in [sms.id(), &agent_id] {
        kit.attach_channel(&room, channel, AttachChannelOptions::default())
            .await?;
    }

    let text = "Hi! Do you have sourdough today?";
    println!("Alice writes by SMS: {text:?}");
    let message = InboundMessage::new(sms.id().clone(), "+15551234567", TextContent::new(text));
    let options = ProcessInboundOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    };
    let result = kit.process_inbound(message, options).await?;

    let timeline = shared::timeline(&kit, &room).await?;
    shared::print_timeline(&timeline);
    let answers: Vec<String> = timeline
        .iter()
        .filter(|event| event.source.channel_id == agent_id)
        .map(|event| text_of(&event.content))
        .collect();
    let asked = primary.fake().map_or(0, |fake| fake.requests().len());
    println!("The overloaded model was asked {asked} times.");

    let mut checks = Checks::default();
    checks.expect("the turn did not fail", result.error.is_none());
    checks.expect(
        "the overloaded model is asked once, then once more",
        asked == 2,
    );
    checks.expect("the agent answers once", answers.len() == 1);
    if let Some(fake) = backup.fake() {
        checks.expect("the fallback is asked once", fake.requests().len() == 1);
        checks.expect("the answer is the fallback's", answers == [BACKUP]);
    }
    kit.close().await?;
    Ok(checks.finish())
}
