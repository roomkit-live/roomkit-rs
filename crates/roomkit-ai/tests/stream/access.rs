//! What a streaming channel reads once the room no longer lets it read
//! the answer, or once a row's visibility keeps the row from it (RFC §6.1,
//! §7.2, §7.5, §12.2 step 14s; review of step 4 milestone 6).

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use pretty_assertions::assert_eq;
use roomkit::model::{EventStatus, EventType, HookResult, Visibility};
use roomkit::{
    DetachChannelOptions, HookExecution, HookSpec, HookTrigger, MuteOptions, RoomKit,
    SetVisibilityOptions,
};
use roomkit_ai::testing::ScriptedRound;
use roomkit_ai::{ToolDefinition, ToolOutput, Tools};
use serde_json::json;

use super::{Read, Room, Stop, Streaming, channel, lookup, room_id};

/// A row a streaming channel may not read is never handed to it (review of
/// step 4 milestone 6, security): a `BEFORE_BROADCAST` hook that narrows a
/// segment's visibility away from it leaves the row to the channels it
/// names, and the channel is handed nothing more of the response in the
/// stream, its later rows delivered as their plans say; a row a hook
/// blocks is handed to nobody, and the stream goes on. The deltas before
/// the hook were shown, as RFC §6.4 accepts. (roomkit hands the narrowed
/// row to the streaming channel:
/// tasks/probes/step4-m6/withheld/py_withheld_probe.out.)
#[tokio::test]
async fn a_row_the_streaming_target_may_not_read_is_never_handed_to_it() {
    for narrowed in [true, false] {
        let rounds = vec![
            ScriptedRound::calling("", [("call_1", "lookup_order", "{}")])
                .in_pieces(["Secret ", "part."]),
            ScriptedRound::writing(["Public ", "part."]),
        ];
        let runs = Arc::new(AtomicUsize::new(0));
        let (room, _model) =
            Room::new(rounds, lookup(runs, None), Streaming::new(Stop::Never)).await;
        let spec = HookSpec::new("judge", HookTrigger::BeforeBroadcast, HookExecution::Sync);
        room.kit
            .hook(spec, move |mut event, _context| async move {
                if roomkit::text_of(&event.content) != "Secret part." {
                    return Ok(HookResult::allow());
                }
                if !narrowed {
                    return Ok(HookResult::block("secret"));
                }
                event.visibility = Visibility::channels([channel("web")]).unwrap();
                Ok(HookResult::modify(event))
            })
            .unwrap();
        room.says("Tell me.").await;
        let mut read = vec![
            Read::Text("Secret ".to_owned()),
            Read::Text("part.".to_owned()),
        ];
        // Past a narrowed row, the rest reaches the channel as its plan
        // says, through deliver(); past a blocked one, the stream goes on.
        let delivered: Vec<u64> = if narrowed {
            vec![3, 5, 6, 7]
        } else {
            read.extend([
                Read::Row(
                    EventType::ToolCallStart,
                    "[Tool call: lookup_order]".to_owned(),
                    5,
                ),
                Read::Row(
                    EventType::ToolCallEnd,
                    "[Tool call: lookup_order]".to_owned(),
                    6,
                ),
                Read::Text("Public ".to_owned()),
                Read::Text("part.".to_owned()),
                Read::Row(EventType::Message, "Public part.".to_owned(), 7),
            ]);
            vec![3]
        };
        assert_eq!(room.ws.read(), read, "narrowed: {narrowed}");
        assert_eq!(room.ws.delivered(), delivered, "narrowed: {narrowed}");
        room.kit.close().await.unwrap();
    }
}

/// How a tool changes the room while the agent's answer streams.
#[derive(Clone, Copy, Debug)]
enum Change {
    /// It detaches the streaming channel.
    Detach,
    /// It narrows the agent's visibility to the webhook.
    Narrow,
    /// It mutes the agent.
    Mute,
}

/// `change`, a tool that makes `change` to the room through `kit`.
fn changing(kit: &RoomKit, change: Change) -> Tools {
    let weak = kit.downgrade();
    let definition = ToolDefinition::new("change", "Changes the room", json!({ "type": "object" }));
    Tools::new().tool(definition, move |_call, _context| {
        let kit = weak.upgrade().unwrap();
        async move {
            match change {
                Change::Detach => {
                    kit.detach_channel(&room_id(), &channel("ws"), DetachChannelOptions::default())
                        .await
                        .unwrap();
                }
                Change::Narrow => {
                    let web = Visibility::channels([channel("web")]).unwrap();
                    kit.set_visibility(
                        &room_id(),
                        &channel("agent"),
                        web,
                        SetVisibilityOptions::default(),
                    )
                    .await
                    .unwrap();
                }
                Change::Mute => {
                    kit.mute(&room_id(), &channel("agent"), MuteOptions::default())
                        .await
                        .unwrap();
                }
            }
            Ok(ToolOutput::Text("done".to_owned()))
        }
    })
}

/// A tool makes `change` while the answer streams: the streaming channel,
/// whether it takes the tool-call rows or not, reads what it read before,
/// and nothing of the round after, neither its text nor its rows, which
/// are stored as the room now says, `stored_as`.
async fn reads_nothing_once_withdrawn(change: Change, stored_as: EventStatus) {
    for tool_rows in [true, false] {
        let rounds = vec![
            ScriptedRound::calling("", [("call_1", "change", "{}")]).in_pieces(["Public."]),
            ScriptedRound::writing(["After ", "the ", "change."]),
        ];
        let ws = Streaming::new(Stop::Never);
        let ws = if tool_rows {
            ws
        } else {
            ws.taking_no_tool_row()
        };
        let (room, _model) = Room::with(rounds, |kit| changing(kit, change), ws).await;
        room.says("Go.").await;
        let mut read = vec![
            Read::Text("Public.".to_owned()),
            Read::Row(EventType::Message, "Public.".to_owned(), 4),
        ];
        if tool_rows {
            read.push(Read::Row(
                EventType::ToolCallStart,
                "[Tool call: change]".to_owned(),
                5,
            ));
        }
        let shape = format!("{change:?}, tool rows {tool_rows}");
        assert_eq!(room.ws.read(), read, "{shape}");
        assert_eq!(room.ws.delivered(), [3], "{shape}");
        let last = room.timeline().await.pop().unwrap();
        assert_eq!(
            (roomkit::text_of(&last.content), last.status),
            ("After the change.".to_owned(), stored_as),
            "{shape}"
        );
        room.kit.close().await.unwrap();
    }
}

/// RFC §7.5: a streaming channel a tool detaches mid-answer reads none of
/// the answer's text after it.
#[tokio::test]
async fn a_streaming_channel_detached_mid_answer_reads_no_more_of_it() {
    reads_nothing_once_withdrawn(Change::Detach, EventStatus::Delivered).await;
}

/// RFC §7.2: a streaming channel the agent's visibility no longer reaches
/// reads none of the answer's text after the change.
#[tokio::test]
async fn a_streaming_channel_the_agent_no_longer_reaches_reads_no_more() {
    reads_nothing_once_withdrawn(Change::Narrow, EventStatus::Delivered).await;
}

/// RFC §7.2: a muted agent writes nothing: once muted mid-answer, its text
/// streams to no channel, its rows stored `BLOCKED`.
#[tokio::test]
async fn a_muted_agent_s_answer_streams_no_more() {
    reads_nothing_once_withdrawn(Change::Mute, EventStatus::Blocked).await;
}

/// A hook that hides the tool-call rows from the streaming channel keeps
/// them from it, nothing else: the next round's text streams on, its row
/// handed in the stream (review of milestone 6, P8).
#[tokio::test]
async fn a_hook_hiding_the_tool_rows_leaves_the_text_streaming() {
    let rounds = vec![
        ScriptedRound::calling("Let me check.", [("call_1", "lookup_order", "{}")]),
        ScriptedRound::writing(["Order ", "shipped."]),
    ];
    let runs = Arc::new(AtomicUsize::new(0));
    let (room, _model) = Room::new(rounds, lookup(runs, None), Streaming::new(Stop::Never)).await;
    let spec = HookSpec::new(
        "hide_tools",
        HookTrigger::BeforeBroadcast,
        HookExecution::Sync,
    );
    room.kit
        .hook(spec, |mut event, _context| async move {
            if event.event_type == EventType::Message {
                return Ok(HookResult::allow());
            }
            event.visibility = Visibility::channels([channel("web")]).unwrap();
            Ok(HookResult::modify(event))
        })
        .unwrap();
    room.says("Where is my order?").await;
    assert_eq!(
        room.ws.read(),
        [
            Read::Text("Let me check.".to_owned()),
            Read::Row(EventType::Message, "Let me check.".to_owned(), 4),
            Read::Text("Order ".to_owned()),
            Read::Text("shipped.".to_owned()),
            Read::Row(EventType::Message, "Order shipped.".to_owned(), 7),
        ]
    );
    assert_eq!(room.ws.delivered(), [3]);
    assert_eq!(
        room.web_rows(),
        [
            (EventType::Message, 4),
            (EventType::ToolCallStart, 5),
            (EventType::ToolCallEnd, 6),
            (EventType::Message, 7)
        ]
    );
    room.kit.close().await.unwrap();
}
