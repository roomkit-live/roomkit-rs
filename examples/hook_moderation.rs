//! Content moderation with `BEFORE_BROADCAST` hooks (RFC §9.4, §9.5). Two
//! SYNC hooks run in priority order, the lower first: a filter blocks a
//! message with a prohibited word, and a redactor modifies a message with a
//! phone number, replacing the number before anyone reads it. A blocked
//! message is stored `BLOCKED` by the filter and reaches no one, and the
//! hooks after the one that blocked do not run.
//!
//! ```sh
//! cargo run --example hook_moderation
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;

use roomkit::model::{
    ChannelId, ChannelType, EventContent, EventStatus, HookResult, InboundMessage, RoomEvent,
    RoomId,
};
use roomkit::testing::RecordingTransport;
use roomkit::{HookExecution, HookSpec, HookTrigger, InboundResult, RoomKit, text_of};

use shared::{Checks, Log};

/// The words the filter refuses.
const PROHIBITED: [&str; 3] = ["badword", "spam", "scam"];

/// The first prohibited word of `text`, whatever its case and the
/// punctuation around it.
fn prohibited_word(text: &str) -> Option<String> {
    text.split_whitespace()
        .map(|word| {
            word.trim_matches(|c: char| !c.is_alphanumeric())
                .to_lowercase()
        })
        .find(|word| PROHIBITED.contains(&word.as_str()))
}

/// Whether `word` is a North American phone number: ten digits, grouped
/// 3-3-4 with `-` or `.`, or not at all.
fn is_phone_number(word: &str) -> bool {
    let digits: String = word.chars().filter(char::is_ascii_digit).collect();
    let separators = word.chars().filter(|c| matches!(c, '-' | '.')).count();
    digits.len() == 10 && digits.len() + separators == word.len() && separators <= 2
}

/// `text` with every phone number replaced by `[REDACTED]`.
fn redacted(text: &str) -> String {
    text.split(' ')
        .map(|word| {
            if is_phone_number(word) {
                "[REDACTED]"
            } else {
                word
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// A SYNC `BEFORE_BROADCAST` hook named `name` at `priority`.
fn before_broadcast(name: &str, priority: i32) -> HookSpec {
    HookSpec {
        priority,
        ..HookSpec::new(name, HookTrigger::BeforeBroadcast, HookExecution::Sync)
    }
}

/// Registers the filter, at priority 0, and the redactor, at priority 1;
/// each notes in `ran` its name and the text it read.
fn moderate(kit: &RoomKit, ran: &Log<String>) -> Result<(), Box<dyn Error>> {
    let noted = ran.clone();
    kit.hook(before_broadcast("profanity_filter", 0), move |event, _| {
        let text = text_of(&event.content);
        noted.push(format!("profanity_filter read {text:?}"));
        let found = prohibited_word(&text);
        async move {
            Ok(match found {
                Some(word) => HookResult::block(format!("prohibited word {word:?}")),
                None => HookResult::allow(),
            })
        }
    })?;
    let noted = ran.clone();
    kit.hook(before_broadcast("pii_redactor", 1), move |event, _| {
        let text = text_of(&event.content);
        noted.push(format!("pii_redactor read {text:?}"));
        let clean = redacted(&text);
        async move {
            if clean == text {
                return Ok(HookResult::allow());
            }
            // The replacement is the event every channel receives, and the
            // one the room stores.
            let modified = RoomEvent {
                content: EventContent::text(clean),
                ..event
            };
            Ok(HookResult::modify(modified).with_reason("phone number redacted"))
        }
    })?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let kit = shared::kit();
    let user = RecordingTransport::new(ChannelId::new("ws_user")?, ChannelType::Websocket);
    let agent = RecordingTransport::new(ChannelId::new("ws_agent")?, ChannelType::Websocket);
    kit.register_channel(user.clone())?;
    kit.register_channel(agent.clone())?;
    let ran = Log::new();
    moderate(&kit, &ran)?;

    let room = RoomId::new("moderated")?;
    kit.create_room(&room).await?;
    kit.attach(&room, user.id()).await?;
    kit.attach(&room, agent.id()).await?;

    let says = async |text: &str| -> Result<InboundResult, Box<dyn Error>> {
        let message = InboundMessage::new(user.id().clone(), "user", EventContent::text(text));
        let result = kit.process_inbound(message, Some(&room)).await?;
        println!("The user writes {text:?}: blocked {}.", result.blocked);
        Ok(result)
    };
    let normal = says("Hello, how are you?").await?;
    let profane = says("This is badword content").await?;
    let phone = says("Call me at 555-123-4567 please").await?;
    let both = says("No spam, call 555.987.6543").await?;

    let timeline = shared::timeline(&kit, &room).await?;
    shared::print_timeline(&timeline);
    println!("Delivered to the agent: {:?}", agent.texts());
    println!("Hooks run: {:#?}", ran.entries());

    let mut checks = Checks::default();
    checks.expect(
        "a message no hook objects to is delivered as written",
        !normal.blocked && agent.texts().first().map(String::as_str) == Some("Hello, how are you?"),
    );
    checks.expect(
        "a prohibited word blocks the message, with the filter's reason",
        profane.blocked && profane.reason.as_deref() == Some("prohibited word \"badword\""),
    );
    checks.expect(
        "the blocked messages are stored BLOCKED by the filter, and reach no one",
        timeline
            .iter()
            .filter(|event| event.status == EventStatus::Blocked)
            .all(|event| event.blocked_by.as_deref() == Some("profanity_filter"))
            && !agent
                .texts()
                .iter()
                .any(|text| text.contains("badword") || text.contains("spam")),
    );
    checks.expect(
        "a phone number is redacted before the agent reads it, and stored redacted",
        !phone.blocked
            && agent.texts().get(1).map(String::as_str) == Some("Call me at [REDACTED] please")
            && phone
                .event
                .is_some_and(|event| text_of(&event.content) == "Call me at [REDACTED] please"),
    );
    checks.expect(
        "the filter runs first; once it blocks, the redactor never reads the message",
        both.blocked
            && ran.entries().last().map(String::as_str)
                == Some("profanity_filter read \"No spam, call 555.987.6543\""),
    );
    Ok(checks.finish())
}
