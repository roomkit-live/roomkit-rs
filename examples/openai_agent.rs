//! An AI agent answering through an OpenAI-compatible Chat Completions
//! endpoint (RFC §6.4): Alice asks a bakery by SMS and the agent answers;
//! then Bob writes on the web chat, and the agent's turn reads the room's
//! conversation, each turn in its author's role and, now that two people
//! speak, each line labelled by its speaker.
//!
//! It runs against a local fake of the OpenAI wire, which answers from a
//! script and keeps what the agent sent, so it needs no network and no key.
//! With `OPENAI_BASE_URL`, `OPENAI_API_KEY` and `OPENAI_MODEL` set, it runs
//! against that endpoint instead.
//!
//! ```sh
//! cargo run --example openai_agent
//! OPENAI_BASE_URL=https://api.openai.com/v1 OPENAI_API_KEY=sk-... \
//!     OPENAI_MODEL=gpt-6-luna cargo run --example openai_agent
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;

use roomkit::model::{ChannelId, ChannelType, EventContent, InboundMessage, RoomId};
use roomkit::testing::RecordingTransport;
use roomkit::text_of;
use roomkit_openai::{OpenAiAgent, OpenAiConfig};
use serde_json::Value;

use shared::{Checks, OpenAiEndpoint};

const SYSTEM_PROMPT: &str = "You answer the customers of a bakery, in one short sentence.";

/// What the fake answers, turn after turn.
const REPLIES: &[&str] = &[
    "Yes, we have sourdough until noon.",
    "Yes Bob, two loaves will be ready at 5.",
];

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let endpoint = OpenAiEndpoint::from_env(REPLIES).await?;
    println!("The agent calls {}.", endpoint.base_url());
    let config = OpenAiConfig::new(endpoint.base_url(), endpoint.api_key(), endpoint.model())
        .with_system_prompt(SYSTEM_PROMPT)
        .with_history(20);
    let agent = OpenAiAgent::new(ChannelId::new("assistant")?, config)?;

    let kit = shared::kit();
    let sms = RecordingTransport::new(ChannelId::new("sms")?, ChannelType::Sms);
    let web = RecordingTransport::new(ChannelId::new("web")?, ChannelType::Websocket);
    let agent_id = agent.id().clone();
    kit.register_channel(sms.clone())?;
    kit.register_channel(web.clone())?;
    kit.register_channel(agent)?;
    let room = RoomId::new("room_1")?;
    kit.create_room(&room).await?;
    for channel in [sms.id(), web.id(), &agent_id] {
        kit.attach(&room, channel).await?;
    }

    let mut errors = Vec::new();
    for (channel, sender, name, text) in [
        (
            &sms,
            "+15551234567",
            "Alice",
            "Hi! Do you have sourdough today?",
        ),
        (&web, "visitor-7", "Bob", "Can I pick up two loaves at 5?"),
    ] {
        println!("{name} writes on {}: {text:?}", channel.id());
        let mut message =
            InboundMessage::new(channel.id().clone(), sender, EventContent::text(text));
        message
            .metadata
            .insert("sender_name".to_owned(), name.into());
        let result = kit.process_inbound_in(&room, message).await?;
        errors.extend(result.error);
    }

    let timeline = shared::timeline(&kit, &room).await?;
    shared::print_timeline(&timeline);
    let answers: Vec<String> = timeline
        .iter()
        .filter(|event| event.source.channel_id == agent_id)
        .map(|event| text_of(&event.content))
        .collect();

    let mut checks = Checks::default();
    checks.expect("no turn failed", errors.is_empty());
    checks.expect(
        "the agent answers each message, and its answers reach Alice on SMS",
        answers.len() == 2
            && answers.iter().all(|answer| !answer.is_empty())
            && answers.iter().all(|answer| sms.texts().contains(answer)),
    );
    if let Some(fake) = endpoint.fake() {
        let requests = fake.requests();
        println!("The agent's second turn sent:");
        for message in requests.get(1).map(messages).unwrap_or_default() {
            println!("  {message}");
        }
        checks.expect("the answers are the model's", answers == REPLIES);
        checks.expect(
            "the second turn reads Alice's message, the agent's answer and Bob's, \
             each speaker labelled",
            requests.get(1).is_some_and(second_turn_is_labelled),
        );
    }
    Ok(checks.finish())
}

/// The messages of a Chat Completions request.
fn messages(request: &Value) -> Vec<Value> {
    request["messages"].as_array().cloned().unwrap_or_default()
}

/// Whether the second turn sent the system prompt, then Alice's message
/// labelled, the agent's answer as the assistant's, and Bob's message
/// labelled, followed by the note that says how speakers are named.
fn second_turn_is_labelled(request: &Value) -> bool {
    let sent: Vec<(String, String)> = messages(request)
        .iter()
        .map(|message| {
            let field = |key: &str| message[key].as_str().unwrap_or_default().to_owned();
            (field("role"), field("content"))
        })
        .collect();
    let [system, alice, answer, bob] = sent.as_slice() else {
        return false;
    };
    system == &("system".to_owned(), SYSTEM_PROMPT.to_owned())
        && alice
            == &(
                "user".to_owned(),
                r#"Alice: "Hi! Do you have sourdough today?""#.to_owned(),
            )
        && answer.0 == "assistant"
        && bob.0 == "user"
        && bob
            .1
            .starts_with(r#"Bob: "Can I pick up two loaves at 5?""#)
        && bob.1.contains("[Speaker labels from the runtime")
}
