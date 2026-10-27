//! An AI agent's skills (RFC §6.4, §21.1, §24): a shop's agent serves the
//! skills of `examples/skills/`, each a directory holding a `SKILL.md` and
//! its references, and Alice asks it about a return by SMS.
//!
//! - Discovery is strict and atomic (RFC §24.5): a malformed skill in
//!   `examples/skills-broken/` fails it, the error naming the skill, and
//!   the registry is left empty, although `examples/skills/` was fine.
//! - The agent's system prompt lists the skills after its own (RFC §24.3),
//!   and it serves `activate_skill` and `read_skill_reference`, which its
//!   tool policy does not govern (RFC §21.1).
//! - The model activates `order-returns`: the activation answers the
//!   skill's instructions and its references; the model reads the return
//!   policy. `../SKILL.md`, an absolute path and `supplier-prices.md`, a
//!   link out of the skill, are refused (RFC §24.6): the supplier's
//!   prices never reach the model.
//! - On Alice's next message, the system prompt carries the skill's
//!   instructions, and an activation answers that they are in force (RFC
//!   §24.4).
//! - The shop restarts on the same SQLite file: the new agent reads the
//!   room's activation from its stored rows, and the instructions ride the
//!   system prompt again.
//!
//! It runs against a local fake of the OpenAI wire, which scripts the
//! model's calls, so it needs no network and no key.
//!
//! ```sh
//! cargo run --example agent_skills
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use roomkit::model::{ChannelId, ChannelType, EventContent, InboundMessage, RoomId};
use roomkit::testing::RecordingTransport;
use roomkit::{ConversationStore, RoomKit};
use roomkit_ai::{SkillRegistry, ToolPolicy};
use roomkit_openai::{OpenAiAgent, OpenAiConfig};
use roomkit_sqlite::SqliteStore;
use serde_json::Value;

use shared::{Checks, FakeOpenAi, OpenAiEndpoint, Reply, TempDatabase};

const SYSTEM_PROMPT: &str = "You help the customers of an online shop.";

/// What the private file holds, which the link out of the skill points
/// at.
const PRIVATE: &str = "PRIVATE";

/// The heading of the active skills' instructions in the system prompt.
const ACTIVE: &str = "# Active skill instructions (binding rules)\n\n## Skill: order-returns\n";

/// The model's script: the activation, the reads, the answer; the
/// activation again on the next message, and its answer; after the
/// restart, an answer.
fn script() -> Vec<Reply> {
    let read = |file: &str| format!(r#"{{"skill_name": "order-returns", "filename": "{file}"}}"#);
    let reads = [
        read("return-policy.md"),
        read("../SKILL.md"),
        read("/etc/passwd"),
        read("supplier-prices.md"),
    ];
    let activate = r#"{"name": "order-returns"}"#;
    vec![
        Reply::calling("", [("call_1", "activate_skill", activate)]),
        Reply::calling(
            "",
            [
                ("call_2", "read_skill_reference", reads[0].as_str()),
                ("call_3", "read_skill_reference", reads[1].as_str()),
                ("call_4", "read_skill_reference", reads[2].as_str()),
                ("call_5", "read_skill_reference", reads[3].as_str()),
            ],
        ),
        Reply::from("Yes: order 42 can be returned within 30 days of delivery."),
        Reply::calling("", [("call_6", "activate_skill", activate)]),
        Reply::from("No: shipping it back is free with the prepaid label."),
        Reply::from("Within 5 business days of the parcel's receipt."),
    ]
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let mut checks = Checks::default();
    a_malformed_skill_fails_discovery(&mut checks);

    let endpoint = OpenAiEndpoint::Fake(FakeOpenAi::start(&script()).await?);
    let database = TempDatabase::new("agent_skills");
    let shop = Shop::open(database.path(), &endpoint, &mut checks).await?;
    shop.alice_says("Can I return order 42? It came 20 days ago.")
        .await?;
    shop.alice_says("Do I pay for shipping it back?").await?;
    let mut answers = shop.close().await;
    println!("The shop restarts on the same file.");
    let shop = Shop::open(database.path(), &endpoint, &mut checks).await?;
    shop.alice_says("When will I see the refund?").await?;
    answers.extend(shop.close().await);

    let requests = endpoint
        .fake()
        .map(FakeOpenAi::requests)
        .unwrap_or_default();
    print_the_turns(&requests);
    check_the_prompt(&mut checks, &requests);
    check_the_activation_and_reads(&mut checks, &requests);
    check_the_next_turns(&mut checks, &requests);
    checks.expect(
        "Alice receives the three answers by SMS",
        answers.len() == 3 && answers.first().is_some_and(|text| text.starts_with("Yes")),
    );
    Ok(checks.finish())
}

/// A directory of the examples' package.
fn examples(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(name)
}

/// Discovery over `examples/skills/` and `examples/skills-broken/` fails
/// on the malformed skill, naming it, and registers nothing.
fn a_malformed_skill_fails_discovery(checks: &mut Checks) {
    let mut skills = SkillRegistry::new();
    let failed = skills.discover([examples("skills"), examples("skills-broken")]);
    let error = failed
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    println!("Discovery with the broken skill: {error}");
    checks.expect(
        "a malformed skill fails discovery, names itself, and leaves the registry empty",
        error.starts_with("Validation failed for order-tracking: Invalid name 'order_tracking'")
            && skills.is_empty(),
    );
}

/// The shop: a kit on the SQLite file, Alice's SMS, and the agent serving
/// the skills of `examples/skills/`.
struct Shop {
    kit: RoomKit<SqliteStore>,
    sms: RecordingTransport,
    room: RoomId,
}

impl Shop {
    /// The shop on the file `database`, its agent calling `endpoint`.
    async fn open(
        database: &Path,
        endpoint: &OpenAiEndpoint,
        checks: &mut Checks,
    ) -> Result<Self, Box<dyn Error>> {
        let mut skills = SkillRegistry::new();
        let found = skills.discover([examples("skills")])?;
        checks.expect(
            "discovery registers the two skills of examples/skills/",
            found == 2 && skills.get("order-returns").is_some(),
        );
        let config = OpenAiConfig::new(endpoint.base_url(), endpoint.api_key(), endpoint.model())
            .with_system_prompt(SYSTEM_PROMPT);
        // The policy denies every tool of the host: the skills' two tools
        // stay declared (RFC §21.1).
        let agent = OpenAiAgent::new(ChannelId::new("assistant")?, config)?
            .with_skills(skills)?
            .with_tool_policy(ToolPolicy::new().deny("*"));
        let kit = RoomKit::with_store(SqliteStore::open(database).await?);
        let sms = RecordingTransport::new(ChannelId::new("sms")?, ChannelType::SMS);
        kit.register_channel(sms.clone())?;
        kit.register_channel(agent)?;
        let room = RoomId::new("returns")?;
        if kit.store().get_room(&room).await?.is_none() {
            kit.create_room(&room).await?;
            for channel in ["sms", "assistant"] {
                kit.attach(&room, &ChannelId::new(channel)?).await?;
            }
        }
        Ok(Self { kit, sms, room })
    }

    async fn alice_says(&self, text: &str) -> Result<(), Box<dyn Error>> {
        println!("Alice: {text}");
        let message = InboundMessage::new(
            self.sms.id().clone(),
            "+15551234567",
            EventContent::text(text),
        );
        self.kit.process_inbound_in(&self.room, message).await?;
        self.kit.idle().await;
        Ok(())
    }

    /// Closes the shop; returns the answers Alice received.
    async fn close(self) -> Vec<String> {
        self.kit.shutdown().await;
        self.sms.texts()
    }
}

/// The system prompt a request opens with.
fn system(request: &Value) -> &str {
    request
        .pointer("/messages/0")
        .filter(|message| message["role"] == "system")
        .and_then(|message| message["content"].as_str())
        .unwrap_or_default()
}

/// What a request carried as the result of the call `id`.
fn result<'a>(requests: &'a [Value], id: &str) -> &'a str {
    requests
        .iter()
        .filter_map(|request| request["messages"].as_array())
        .flatten()
        .find(|message| message["role"] == "tool" && message["tool_call_id"] == id)
        .and_then(|message| message["content"].as_str())
        .unwrap_or_default()
}

/// Prints, turn by turn, what the model read of the skills.
fn print_the_turns(requests: &[Value]) {
    println!("The model read:");
    for (id, what) in [
        ("call_1", "the activation"),
        ("call_2", "the return policy"),
        ("call_3", "../SKILL.md"),
        ("call_4", "/etc/passwd"),
        ("call_5", "supplier-prices.md"),
        ("call_6", "the activation again"),
    ] {
        println!("  {what}: {}", result(requests, id));
    }
    for (turn, request) in [(1, 0), (2, 3), (3, 5)] {
        let active = requests
            .get(request)
            .is_some_and(|request| system(request).contains(ACTIVE));
        println!("  turn {turn}'s system prompt carries the skill's instructions: {active}");
    }
}

/// The checks of the first request: the system prompt, then the manifest,
/// and the two tools.
fn check_the_prompt(checks: &mut Checks, requests: &[Value]) {
    let first = requests.first().cloned().unwrap_or_default();
    let prompt = system(&first);
    let tools: Vec<&str> = first
        .get("tools")
        .and_then(Value::as_array)
        .map(|tools| {
            tools
                .iter()
                .filter_map(|tool| tool.pointer("/function/name").and_then(Value::as_str))
                .collect()
        })
        .unwrap_or_default();
    checks.expect(
        "the system prompt lists the skills after the shop's own, and the two tools are declared",
        prompt.starts_with(&format!(
            "{SYSTEM_PROMPT}\n\nYou have access to Agent Skills"
        )) && prompt.contains("<available_skills>\n  <skill name=\"gift-wrapping\">")
            && prompt.contains("  <skill name=\"order-returns\">\n    <description>Handles")
            && !prompt.contains(ACTIVE)
            && tools == ["activate_skill", "read_skill_reference"],
    );
}

/// The checks of the activation and of the reads.
fn check_the_activation_and_reads(checks: &mut Checks, requests: &[Value]) {
    let activation: Value = serde_json::from_str(result(requests, "call_1")).unwrap_or_default();
    checks.expect(
        "the activation answers the skill's instructions and the reference a read takes",
        activation
            .get("instructions")
            .and_then(Value::as_str)
            .is_some_and(|body| body.contains("Read references/return-policy.md"))
            && activation.get("references") == Some(&serde_json::json!(["return-policy.md"])),
    );
    checks.expect(
        "the return policy is read",
        result(requests, "call_2").contains("shipping back is free"),
    );
    checks.expect(
        "../SKILL.md, an absolute path and a link out of the skill are refused",
        result(requests, "call_3")
            == r#"{"error": "Invalid reference filename: '../SKILL.md' must not contain a path"}"#
            && result(requests, "call_4")
                == r#"{"error": "Invalid reference filename: '/etc/passwd' must be relative"}"#
            && result(requests, "call_5")
                == r#"{"error": "Invalid reference filename: 'supplier-prices.md' escapes references/"}"#,
    );
    checks.expect(
        "the supplier's prices never reach the model",
        !requests
            .iter()
            .any(|request| request.to_string().contains(PRIVATE)),
    );
}

/// The checks of the turns after the activation, before and after the
/// restart.
fn check_the_next_turns(checks: &mut Checks, requests: &[Value]) {
    let repeat: Value = serde_json::from_str(result(requests, "call_6")).unwrap_or_default();
    checks.expect(
        "the next turn's system prompt carries the instructions, and a repeat answers that \
         the skill is active",
        requests
            .get(3)
            .is_some_and(|request| system(request).contains(ACTIVE))
            && repeat.get("already_active") == Some(&Value::Bool(true)),
    );
    checks.expect(
        "after the restart, the system prompt carries the instructions again",
        requests.len() == 6
            && requests
                .get(5)
                .is_some_and(|request| system(request).contains(ACTIVE)),
    );
}
