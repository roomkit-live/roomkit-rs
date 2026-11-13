//! Bounded, visibility-filtered room conversation the session did not receive.

use std::collections::BTreeMap;

use roomkit::model::{ChannelId, EventId, RoomContext, RoomEvent};
use roomkit_core::rules::transcript::{identifier, quoted};
use roomkit_core::rules::visible_events;

use crate::text::acp_event_text;

const CLOSING: &str = " Context only; the request follows.]";
const ENTRY_LIMIT: usize = 4000;

pub(super) fn block(
    context: &RoomContext,
    trigger: &RoomEvent,
    channel: &ChannelId,
    after: Option<u64>,
    limit: usize,
    labels: &BTreeMap<EventId, Option<String>>,
) -> String {
    if limit == 0 {
        return String::new();
    }
    let missed: Vec<RoomEvent> = visible_events(context, channel)
        .into_iter()
        .filter(|event| {
            after.is_none_or(|after| event.index > after)
                && event.id != trigger.id
                && (event.source.channel_id != *channel
                    || event
                        .metadata
                        .get("acp")
                        .and_then(|meta| meta.get("standalone"))
                        .and_then(serde_json::Value::as_bool)
                        == Some(true))
                && super::readable(event)
        })
        .collect();
    let start = after.map_or(0, |after| after.saturating_add(1));
    let oldest = context.history_start_index.unwrap_or_else(|| {
        context
            .recent_events
            .iter()
            .map(|event| event.index)
            .min()
            .unwrap_or(start)
    });
    let unloaded = oldest.saturating_sub(start);
    if missed.is_empty() {
        return if unloaded == 0 {
            String::new()
        } else {
            format!(
                "[Room context — none of the loaded messages are new to you; {}.{CLOSING}",
                not_loaded(unloaded)
            )
        };
    }
    let shown = &missed[missed.len().saturating_sub(limit)..];
    let mut lines = vec![header(shown.len(), missed.len(), unloaded)];
    lines.extend(shown.iter().enumerate().map(|(index, event)| {
        let label = if event.source.channel_id == *channel {
            "you (in a separate session)".to_owned()
        } else {
            labels.get(&event.id).cloned().flatten().unwrap_or_else(|| {
                format!(
                    "@{}",
                    identifier(event.source.channel_id.as_str(), "channel")
                )
            })
        };
        format!(
            "[{}] {label}: {}",
            index + 1,
            quoted(&acp_event_text(event), ENTRY_LIMIT)
        )
    }));
    lines.push("[End of room context]".to_owned());
    lines.join("\n")
}

fn count(number: u64, noun: &str) -> String {
    format!("{number} {noun}{}", if number == 1 { "" } else { "s" })
}

fn not_loaded(unloaded: u64) -> String {
    format!(
        "up to {} {} not loaded",
        count(unloaded, "earlier room event"),
        if unloaded == 1 { "was" } else { "were" }
    )
}

fn header(shown: usize, total: usize, unloaded: u64) -> String {
    if unloaded > 0 {
        let lead = if shown < total {
            format!("the {shown} most recent of {total} loaded messages")
        } else {
            format!(
                "the {shown} most recent message{}",
                if shown == 1 { "" } else { "s" }
            )
        };
        return format!(
            "[Room context — {lead} you did not receive; {}.{CLOSING}",
            not_loaded(unloaded)
        );
    }
    if shown < total {
        return format!(
            "[Room context — the {shown} most recent of {total} messages you did not receive; the earlier ones are not shown.{CLOSING}"
        );
    }
    format!(
        "[Room context — {shown} message{} you did not receive.{CLOSING}",
        if shown == 1 { "" } else { "s" }
    )
}
