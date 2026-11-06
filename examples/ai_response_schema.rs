//! An agent that answers in a JSON shape it declares (RFC §6.7): a support
//! message is triaged into a department, an urgency flag and a one-line
//! summary. The agent's `response_schema` is every turn's; a room asking
//! for another shape sets its own in its binding's metadata. The provider
//! sends the schema as a strict `json_schema` response format and checks
//! the document the model wrote before the room receives it: an answer that
//! is no such document fails the turn, and nothing of it is stored or sent.
//!
//! The model is a local fake of the OpenAI wire, so it needs no network and
//! no key. With `OPENAI_BASE_URL`, `OPENAI_API_KEY` and `OPENAI_MODEL` set,
//! it is that endpoint instead, and the checks, written for the fake's
//! answers, are skipped.
//!
//! ```sh
//! cargo run --example ai_response_schema
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;

use roomkit::model::{ChannelId, InboundMessage, Metadata, RoomId, TextContent};
use roomkit::{AttachChannelOptions, Channel, CreateRoomOptions, ProcessInboundOptions, RoomKit};
use roomkit_ai::{AiChannel, AiChannelOptions};
use serde_json::{Map, Value, json};

use shared::text_of;
use shared::{Checks, OpenAiEndpoint, SmsNumber};

const SYSTEM_PROMPT: &str = "You triage customer support messages.";

const MESSAGE: &str = "I was charged twice for March. This is the third time I'm calling. \
                       If this isn't fixed today I'm cancelling.";

/// What the fake answers the support room, the escalations room, then the
/// support room again: prose where a document is due.
const TRIAGED: &str =
    r#"{"department": "billing", "urgent": true, "summary": "Charged twice, third call."}"#;
const URGENCY: &str = r#"{"urgent": true, "reason": "Threatens to cancel today."}"#;
const PROSE: &str = "Sure! This one is for billing.";

/// The shape every turn answers in, unless its room asks for another.
fn triage_schema() -> Map<String, Value> {
    object(json!({
        "type": "object",
        "properties": {
            "department": {
                "type": "string",
                "enum": ["billing", "technical", "sales", "other"],
                "description": "The team that should take the message.",
            },
            "urgent": { "type": "boolean", "description": "Whether the customer threatens to leave." },
            "summary": { "type": "string", "description": "One short sentence." },
        },
        "required": ["department", "urgent", "summary"],
        "additionalProperties": false,
    }))
}

/// What the escalations room asks of the same agent instead.
fn urgency_schema() -> Map<String, Value> {
    object(json!({
        "type": "object",
        "properties": {
            "urgent": { "type": "boolean" },
            "reason": { "type": "string", "description": "Why, in a few words." },
        },
        "required": ["urgent", "reason"],
        "additionalProperties": false,
    }))
}

fn object(value: Value) -> Map<String, Value> {
    if let Value::Object(members) = value {
        return members;
    }
    Map::new()
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let endpoint = OpenAiEndpoint::from_env(&[TRIAGED, URGENCY, PROSE]).await?;
    println!("The agent calls {}.", endpoint.shown_base_url());
    let options = AiChannelOptions {
        system_prompt: Some(SYSTEM_PROMPT.to_owned()),
        response_schema: Some(triage_schema()),
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(ChannelId::new("triage")?, endpoint.provider()?, options)?;
    let agent_id = agent.channel_id();
    let kit = shared::kit();
    let sms = SmsNumber::new(ChannelId::new("sms")?);
    kit.register_channel(sms.clone())?;
    kit.register_channel(agent)?;
    let support = RoomId::new("support")?;
    let escalations = RoomId::new("escalations")?;
    let mut urgency = Metadata::new();
    urgency.insert("response_schema".into(), Value::Object(urgency_schema()));
    for (room, metadata) in [(&support, Metadata::new()), (&escalations, urgency)] {
        open(&kit, room, (sms.id(), &agent_id), metadata).await?;
    }

    let mut checks = Checks::default();
    let triaged = answer(&kit, &sms, &support, &agent_id).await?;
    println!("The support room stores: {triaged:?}");
    let urgent = answer(&kit, &sms, &escalations, &agent_id).await?;
    println!("The escalations room stores: {urgent:?}");
    let refused = kit
        .process_inbound(said(&sms, MESSAGE), into(&support))
        .await?;
    let failed = refused.error.map(|error| error.message);
    println!("A prose answer fails the turn: {failed:?}");

    if let Some(fake) = endpoint.fake() {
        let formats: Vec<Option<Value>> = fake
            .requests()
            .iter()
            .map(|request| {
                request
                    .pointer("/response_format/json_schema/schema")
                    .cloned()
            })
            .collect();
        checks.expect(
            "each request carries its room's schema",
            formats
                == [
                    Some(Value::Object(triage_schema())),
                    Some(Value::Object(urgency_schema())),
                    Some(Value::Object(triage_schema())),
                ],
        );
        checks.expect("the support room stores the triage", triaged == [TRIAGED]);
        checks.expect(
            "the escalations room stores the urgency",
            urgent == [URGENCY],
        );
        checks.expect(
            "the prose fails the turn as no JSON document",
            failed.is_some_and(|message| message.contains("the answer is not a JSON document")),
        );
        let stored = stored_answers(&kit, &support, &agent_id).await?;
        checks.expect("nothing of the prose is stored", stored == [TRIAGED]);
    }
    kit.close().await?;
    Ok(checks.finish())
}

/// Creates `room` and attaches the SMS number and the agent, the agent's
/// binding carrying `metadata`.
async fn open(
    kit: &RoomKit,
    room: &RoomId,
    (sms, agent): (&ChannelId, &ChannelId),
    metadata: Metadata,
) -> Result<(), Box<dyn Error>> {
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await?;
    kit.attach_channel(room, sms, AttachChannelOptions::default())
        .await?;
    let options = AttachChannelOptions {
        metadata,
        ..AttachChannelOptions::default()
    };
    kit.attach_channel(room, agent, options).await?;
    Ok(())
}

/// The customer's message by SMS.
fn said(sms: &SmsNumber, text: &str) -> InboundMessage {
    InboundMessage::new(sms.id().clone(), "+15551234567", TextContent::new(text))
}

fn into(room: &RoomId) -> ProcessInboundOptions {
    ProcessInboundOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    }
}

/// What the agent stores in `room` once the customer wrote there.
async fn answer(
    kit: &RoomKit,
    sms: &SmsNumber,
    room: &RoomId,
    agent: &ChannelId,
) -> Result<Vec<String>, Box<dyn Error>> {
    kit.process_inbound(said(sms, MESSAGE), into(room)).await?;
    stored_answers(kit, room, agent).await
}

/// The agent's messages `room` stores.
async fn stored_answers(
    kit: &RoomKit,
    room: &RoomId,
    agent: &ChannelId,
) -> Result<Vec<String>, Box<dyn Error>> {
    let timeline = shared::timeline(kit, room).await?;
    Ok(timeline
        .iter()
        .filter(|event| &event.source.channel_id == agent)
        .map(|event| text_of(&event.content))
        .collect())
}
