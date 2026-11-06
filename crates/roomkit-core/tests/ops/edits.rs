//! Edits and deletes, effect by effect (RFC §10.1 steps 8a and 12, §10.3,
//! §6.6, §9.4): step 8a on every pass, the target changed in the commit of
//! the edit or the delete and never before, the mutation triggers after the
//! release, native and fallback delivery, and the host's direct calls.
//!
//! The room holds a WhatsApp-like sender `wa`, a WebSocket `ws` that edits
//! and deletes natively, and an SMS-like `sms` that does neither. Its first
//! event, `evt_1` at index 0, is `+15551234567`'s "I need 5000$".

use std::cell::RefCell;
use std::rc::Rc;

use roomkit_core::model::hook::{AllowOptions, BlockOptions, ModifyOptions};
use roomkit_core::model::{
    ChannelOutput, ChannelSet, CompositeContent, DeleteContent, DeleteType, DirectEvent,
    EditContent, EditSource, EventContent, EventId, EventStatus, EventType, EventUpdates,
    HookResult, InboundMessage, InjectedEvent, Participant, ParticipantId, ParticipantRole, Room,
    RoomEvent, RoomId, RoomStatus, RoomTimers, TextContent, Timestamp, Visibility,
};
use roomkit_core::ops::{Entry, InboundResult};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::HookTrigger;
use roomkit_core::rules::transcoding;
use serde_json::json;

use crate::support::{Answer, Reentry, Sim, calling, channel, notice_event, response, room_id};

const SENDER: &str = "+15551234567";
const OTHER: &str = "+15550000009";

/// The room, with `evt_1` committed and the trace cleared.
fn room() -> Sim {
    let mut sim = Sim::new();
    sim.transport("wa");
    let ws = sim.transport("ws");
    ws.capabilities.supports_edit = true;
    ws.capabilities.supports_delete = true;
    sim.transport("sms");
    sim.send(Sim::message("wa", "I need 5000$")).unwrap();
    forget(&mut sim);
    sim
}

/// Forgets what the driver did so far.
fn forget(sim: &mut Sim) {
    sim.trace.clear();
    sim.journal.clear();
    sim.notices.clear();
}

fn id(event_id: &str) -> EventId {
    EventId::new(event_id).unwrap()
}

/// `sender`'s edit of `target` on `wa`, to `text`.
fn edit_from(sender: &str, target: &str, text: &str) -> InboundMessage {
    let mut message = InboundMessage::new(
        channel("wa"),
        sender,
        EventContent::Edit(EditContent {
            edit_source: Some(EditSource::Sender),
            ..EditContent::new(id(target), TextContent::new(text))
        }),
    );
    message.event_type = EventType::Edit;
    message
}

/// The sender's edit of `evt_1`, to "I need 50000$" (RFC Appendix B.6).
fn edit() -> InboundMessage {
    edit_from(SENDER, "evt_1", "I need 50000$")
}

/// `sender`'s delete of `target` on `wa`, claiming `delete_type`.
fn delete_from(sender: &str, target: &str, delete_type: DeleteType) -> InboundMessage {
    let mut message = InboundMessage::new(
        channel("wa"),
        sender,
        EventContent::Delete(DeleteContent {
            delete_type,
            ..DeleteContent::new(id(target))
        }),
    );
    message.event_type = EventType::Delete;
    message
}

/// The sender's delete of `evt_1` (RFC Appendix B.7).
fn delete() -> InboundMessage {
    delete_from(SENDER, "evt_1", DeleteType::Sender)
}

/// The texts of `events`, as a reader takes them.
fn texts(events: &[RoomEvent]) -> Vec<String> {
    events
        .iter()
        .map(|event| transcoding::text_of(&event.content))
        .collect()
}

/// The stored event `event_id`.
fn stored<'sim>(sim: &'sim Sim, event_id: &str) -> &'sim RoomEvent {
    sim.timeline
        .iter()
        .find(|event| event.id.as_str() == event_id)
        .unwrap()
}

/// The events `channel_id` was delivered from now on.
fn deliveries(sim: &mut Sim, channel_id: &str) -> Rc<RefCell<Vec<RoomEvent>>> {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let kept = Rc::clone(&seen);
    sim.deliver_with(
        channel_id,
        Box::new(move |event| {
            kept.borrow_mut().push(event.clone());
            Ok(ChannelOutput::default())
        }),
    );
    seen
}

/// The journal from the step 6 lock to the first delivery.
fn locked_section(sim: &Sim) -> Vec<&str> {
    sim.journal
        .iter()
        .map(String::as_str)
        .skip_while(|line| !line.starts_with("lock"))
        .take_while(|line| !line.starts_with("on_event"))
        .collect()
}

/// What a refused message left: its result, the timeline, the room's count.
fn refusal(result: &InboundResult, sim: &Sim) -> (bool, Option<String>, bool, usize, u64) {
    (
        result.blocked,
        result.reason.clone(),
        result.event.is_none(),
        sim.timeline.len(),
        sim.room.event_count,
    )
}

/// A refusal of step 8a with `reason`: blocked, nothing committed, the
/// room's count unchanged.
fn refused_with(reason: &str) -> (bool, Option<String>, bool, usize, u64) {
    (true, Some(reason.to_owned()), true, 1, 1)
}

/// RFC §10.3, Appendix B.6: an edit the room accepts.
mod an_accepted_edit {
    use super::*;
    use pretty_assertions::assert_eq;

    /// RFC §10.1 steps 8a, 12 and 13: the target is read under the lock,
    /// updated in the edit's own commit, and `ON_EVENT_UPDATED` fires once
    /// the lock is released.
    #[test]
    fn commits_with_its_target_and_fires_on_event_updated_after_the_release() {
        let mut sim = room();
        sim.observer("projection", HookTrigger::OnEventUpdated);
        sim.send(edit()).unwrap();
        assert_eq!(
            locked_section(&sim),
            [
                "lock room_1",
                "load_context room_1",
                "get_event evt_1",
                "commit #1 wa delivered \"Correction: I need 50000$\" updating evt_1",
                "enter lane room_1 evt_2",
                "release room_1",
                "fire on_event_updated",
                "await lane room_1 evt_2",
            ]
        );
    }

    #[test]
    fn leaves_its_target_edited() {
        let mut sim = room();
        sim.send(edit()).unwrap();
        let target = stored(&sim, "evt_1");
        assert_eq!(
            (
                target.content.clone(),
                serde_json::Value::Object(target.metadata.clone())
            ),
            (
                TextContent::new("I need 50000$").into(),
                json!({ "edited": true })
            )
        );
    }

    #[test]
    fn is_itself_stored_in_the_timeline_at_its_own_index() {
        let mut sim = room();
        let result = sim.send(edit()).unwrap();
        let committed = result.event.unwrap();
        assert_eq!(
            (committed.index, committed.r#type, committed.status),
            (1, EventType::Edit, EventStatus::Delivered)
        );
    }

    #[test]
    fn fires_on_event_updated_with_the_edited_target() {
        let mut sim = room();
        sim.observer("projection", HookTrigger::OnEventUpdated);
        sim.send(edit()).unwrap();
        let payloads = sim.async_payloads();
        assert_eq!((payloads.len(), payloads[0]), (1, stored(&sim, "evt_1")));
    }

    /// RFC §10.1 step 8a: the hooks run on an accepted edit, after its
    /// check.
    #[test]
    fn is_checked_before_its_hooks_run() {
        let mut sim = room();
        sim.before_broadcast(
            "moderation",
            Box::new(|_| HookOutcome::Returned(HookResult::allow(AllowOptions::default()))),
        );
        sim.send(edit()).unwrap();
        assert_eq!(
            locked_section(&sim)[2..4].to_vec(),
            ["get_event evt_1", "hook moderation"]
        );
    }

    /// The delivery reads the target as the commit left it, and the edit
    /// itself as the newest event, as Python's broadcast context holds it.
    #[test]
    fn hands_its_readers_the_target_as_edited() {
        let mut sim = room();
        sim.echo_agent("ai", "Noted");
        sim.send(edit()).unwrap();
        assert_eq!(
            sim.history_seen_by("ai"),
            ["I need 50000$", "Correction: I need 50000$"]
        );
    }

    /// A later turn reads the corrected message and the correction, then
    /// its own message.
    #[test]
    fn leaves_a_later_turn_its_correction() {
        let mut sim = room();
        sim.agent("ai", Box::new(|_, _| Ok(ChannelOutput::default())));
        sim.send(edit()).unwrap();
        sim.send(Sim::message("wa", "Is it fine?")).unwrap();
        assert_eq!(
            sim.history_seen_by("ai"),
            ["I need 50000$", "Correction: I need 50000$", "Is it fine?"]
        );
    }

    /// An edit of an edited message is accepted: the target holds the
    /// latest correction.
    #[test]
    fn of_an_edited_message_corrects_it_again() {
        let mut sim = room();
        sim.send(edit()).unwrap();
        sim.send(edit_from(SENDER, "evt_1", "I need 500000$"))
            .unwrap();
        assert_eq!(
            stored(&sim, "evt_1").content,
            TextContent::new("I need 500000$").into()
        );
    }
}

/// RFC §10.3, Appendix B.7: a delete the room accepts.
mod an_accepted_delete {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn marks_its_target_deleted_keeping_its_content() {
        let mut sim = room();
        sim.send(delete()).unwrap();
        let target = stored(&sim, "evt_1");
        assert_eq!(
            (
                target.content.clone(),
                serde_json::Value::Object(target.metadata.clone())
            ),
            (
                TextContent::new("I need 5000$").into(),
                json!({ "deleted": true })
            )
        );
    }

    #[test]
    fn fires_on_event_deleted_after_the_release_with_the_deleted_target() {
        let mut sim = room();
        sim.observer("projection", HookTrigger::OnEventDeleted);
        sim.send(delete()).unwrap();
        assert_eq!(
            (
                locked_section(&sim)[3..].to_vec(),
                sim.async_payloads()[0].metadata.get("deleted").cloned()
            ),
            (
                vec![
                    "commit #1 wa delivered \"[Message deleted]\" updating evt_1",
                    "enter lane room_1 evt_2",
                    "release room_1",
                    "fire on_event_deleted",
                    "await lane room_1 evt_2",
                ],
                Some(json!(true))
            )
        );
    }

    /// Gap G-062: the delete's readers hold its target, its text withheld.
    #[test]
    fn hands_its_readers_the_target_without_its_text() {
        let mut sim = room();
        sim.agent("ai", Box::new(|_, _| Ok(ChannelOutput::default())));
        sim.send(delete()).unwrap();
        assert_eq!(sim.history_seen_by("ai"), ["", "[Message deleted]"]);
    }

    /// Gap G-062: a later turn reads the delete, never the deleted text.
    #[test]
    fn leaves_a_later_turn_the_delete_without_the_deleted_text() {
        let mut sim = room();
        sim.agent("ai", Box::new(|_, _| Ok(ChannelOutput::default())));
        sim.send(delete()).unwrap();
        sim.send(Sim::message("wa", "Is it gone?")).unwrap();
        assert_eq!(
            sim.history_seen_by("ai"),
            ["", "[Message deleted]", "Is it gone?"]
        );
    }

    /// Gap G-062: the correction of a deleted message is withheld too.
    #[test]
    fn of_an_edited_message_leaves_a_later_turn_neither_text() {
        let mut sim = room();
        sim.agent("ai", Box::new(|_, _| Ok(ChannelOutput::default())));
        sim.send(edit()).unwrap();
        sim.send(delete()).unwrap();
        sim.send(Sim::message("wa", "Is it gone?")).unwrap();
        assert_eq!(
            sim.history_seen_by("ai"),
            ["", "Correction: ", "[Message deleted]", "Is it gone?"]
        );
    }

    /// Gap G-062: the channel the next message arrives on parses it without
    /// the deleted text, in a room an agent reads the history of.
    #[test]
    fn leaves_the_next_message_s_parser_without_the_deleted_text() {
        let mut sim = room();
        sim.agent("ai", Box::new(|_, _| Ok(ChannelOutput::default())));
        sim.send(delete()).unwrap();
        sim.send(Sim::message("wa", "Is it gone?")).unwrap();
        let parsed = sim.parse_contexts.last().unwrap();
        assert_eq!(texts(&parsed.recent_events), ["", "[Message deleted]"]);
    }

    /// Gap G-062: hooks, which are the host's code, read the room whole.
    #[test]
    fn leaves_the_hooks_the_deleted_text() {
        let mut sim = room();
        sim.observer("audit", HookTrigger::AfterBroadcast);
        sim.send(delete()).unwrap();
        sim.send(Sim::message("wa", "Is it gone?")).unwrap();
        let context = sim.async_contexts().last().copied().unwrap();
        assert_eq!(
            texts(&context.recent_events),
            ["I need 5000$", "[Message deleted]", "Is it gone?"]
        );
    }

    /// Gap G-062: what a reader is handed changes nothing stored.
    #[test]
    fn keeps_its_target_s_text_in_the_store_once_an_agent_read_it() {
        let mut sim = room();
        sim.agent("ai", Box::new(|_, _| Ok(ChannelOutput::default())));
        sim.send(delete()).unwrap();
        sim.send(Sim::message("wa", "Is it gone?")).unwrap();
        assert_eq!(
            stored(&sim, "evt_1").content,
            TextContent::new("I need 5000$").into()
        );
    }

    #[test]
    fn makes_a_later_edit_of_its_target_refused() {
        let mut sim = room();
        sim.send(delete()).unwrap();
        let result = sim.send(edit()).unwrap();
        assert_eq!(result.reason.as_deref(), Some("target_event_not_found"));
    }

    /// RFC §10.3 validation rule 3, gap G-062: an owner on their channel
    /// deletes anyone's message.
    #[test]
    fn by_an_owner_removes_another_sender_s_message() {
        let mut sim = room();
        let mut owner = Participant::at(
            ParticipantId::new(OTHER).unwrap(),
            room_id(),
            channel("wa"),
            Timestamp::UNIX_EPOCH,
        );
        owner.role = ParticipantRole::Owner;
        sim.participants.push(owner);
        let result = sim
            .send(delete_from(OTHER, "evt_1", DeleteType::Admin))
            .unwrap();
        assert_eq!(
            (
                result.blocked,
                stored(&sim, "evt_1").metadata.get("deleted")
            ),
            (false, Some(&json!(true)))
        );
    }
}

/// RFC §6.6, §10.3 broadcast behavior: each target receives the change
/// natively when it supports it, as text otherwise.
mod native_and_fallback_delivery {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn an_edit_reaches_a_channel_that_edits_as_the_edit_itself() {
        let mut sim = room();
        let ws = deliveries(&mut sim, "ws");
        sim.send(edit()).unwrap();
        let delivered = ws.borrow()[0].content.clone();
        assert!(
            matches!(delivered, EventContent::Edit(edit) if edit.target_event_id == id("evt_1"))
        );
    }

    #[test]
    fn an_edit_reaches_a_channel_that_does_not_edit_as_a_correction() {
        let mut sim = room();
        let sms = deliveries(&mut sim, "sms");
        sim.send(edit()).unwrap();
        assert_eq!(
            sms.borrow()[0].content,
            TextContent::new("Correction: I need 50000$").into()
        );
    }

    #[test]
    fn a_delete_reaches_a_channel_that_deletes_as_the_delete_itself() {
        let mut sim = room();
        let ws = deliveries(&mut sim, "ws");
        sim.send(delete()).unwrap();
        let delivered = ws.borrow()[0].content.clone();
        assert!(matches!(delivered, EventContent::Delete(_)));
    }

    #[test]
    fn a_delete_reaches_a_channel_that_does_not_delete_as_a_notice() {
        let mut sim = room();
        let sms = deliveries(&mut sim, "sms");
        sim.send(delete()).unwrap();
        assert_eq!(
            sms.borrow()[0].content,
            TextContent::new("[Message deleted]").into()
        );
    }

    /// Gap G-062: a correction of a message only `ws` saw reaches `ws`
    /// only, whatever visibility the edit asked for.
    #[test]
    fn an_edit_of_a_message_one_channel_saw_reaches_that_channel_only() {
        let mut sim = Sim::new();
        sim.transport("wa");
        sim.transport("ws");
        sim.transport("sms");
        let mut restricted = Sim::message("wa", "My card is 4242");
        restricted.visibility = ChannelSet::new([channel("ws")])
            .map(Visibility::Channels)
            .unwrap();
        sim.send(restricted).unwrap();
        forget(&mut sim);
        let result = sim
            .send(edit_from(SENDER, "evt_1", "My card is 4343"))
            .unwrap();
        assert_eq!(
            (
                result.event.map(|event| event.visibility.to_string()),
                sim.traced("deliver")
            ),
            (Some("ws".to_owned()), vec!["deliver ws <- evt_2"])
        );
    }
}

/// RFC §10.3: a blocked edit or delete leaves its target unmutated and fires
/// neither trigger.
mod a_blocked_change {
    use super::*;
    use pretty_assertions::assert_eq;

    fn blocking(sim: &mut Sim) {
        sim.before_broadcast(
            "moderation",
            Box::new(|_| {
                HookOutcome::Returned(HookResult::block("no edits", BlockOptions::default()))
            }),
        );
        sim.observer("updated", HookTrigger::OnEventUpdated);
        sim.observer("deleted", HookTrigger::OnEventDeleted);
    }

    #[test]
    fn edit_leaves_its_target_unchanged_and_fires_nothing() {
        let mut sim = room();
        blocking(&mut sim);
        sim.send(edit()).unwrap();
        let target = stored(&sim, "evt_1");
        assert_eq!(
            (
                target.content.clone(),
                target.metadata.is_empty(),
                sim.async_triggers(),
                sim.timeline_lines()[1].clone()
            ),
            (
                TextContent::new("I need 5000$").into(),
                true,
                Vec::new(),
                "#1 wa blocked:moderation \"Correction: I need 50000$\"".to_owned()
            )
        );
    }

    #[test]
    fn delete_leaves_its_target_unmarked_and_fires_nothing() {
        let mut sim = room();
        blocking(&mut sim);
        sim.send(delete()).unwrap();
        assert_eq!(
            (
                stored(&sim, "evt_1").metadata.is_empty(),
                sim.async_triggers()
            ),
            (true, Vec::new())
        );
    }

    /// RFC §7.5 rule 2: a source that may not write blocks the edit at step
    /// 11, after its check; its target stays as it was.
    #[test]
    fn edit_from_a_muted_source_leaves_its_target_unchanged() {
        let mut sim = room();
        sim.bindings[0].muted = true;
        let result = sim.send(edit()).unwrap();
        assert_eq!(
            (
                result.reason,
                stored(&sim, "evt_1").content.clone(),
                sim.traced("commit")
            ),
            (
                Some("source_muted".to_owned()),
                TextContent::new("I need 5000$").into(),
                vec!["commit #1 wa blocked:source_muted \"Correction: I need 50000$\""]
            )
        );
    }
}

/// RFC §10.1 step 8a: a change that fails its check is refused with one of
/// the four reasons, and nothing of it is stored.
mod a_refused_change {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn whose_target_does_not_exist_is_refused_target_event_not_found() {
        let mut sim = room();
        let result = sim
            .send(edit_from(SENDER, "evt_404", "I need 50000$"))
            .unwrap();
        assert_eq!(
            refusal(&result, &sim),
            refused_with("target_event_not_found")
        );
    }

    #[test]
    fn whose_target_is_another_room_s_is_refused_target_event_not_found() {
        let mut sim = room();
        let mut elsewhere = stored(&sim, "evt_1").clone();
        elsewhere.id = id("evt_9");
        elsewhere.room_id = RoomId::new("room_2").unwrap();
        sim.seed_room(Room::at(
            RoomId::new("room_2").unwrap(),
            Timestamp::UNIX_EPOCH,
        ));
        sim.seed_other("room_2", elsewhere);
        let result = sim
            .send(edit_from(SENDER, "evt_9", "I need 50000$"))
            .unwrap();
        assert_eq!(
            refusal(&result, &sim),
            refused_with("target_event_not_found")
        );
    }

    /// Gap G-062: a refused record was never received.
    #[test]
    fn whose_target_was_refused_is_refused_target_event_not_found() {
        let mut sim = room();
        sim.timeline[0].status = EventStatus::Blocked;
        let result = sim.send(edit()).unwrap();
        assert_eq!(
            refusal(&result, &sim),
            refused_with("target_event_not_found")
        );
    }

    /// Gap G-062: an edit or a delete is told by its type and its content
    /// together.
    #[test]
    fn whose_type_and_content_disagree_is_refused_target_event_not_found() {
        let mut sim = room();
        let mut bare = Sim::message("wa", "Oops");
        bare.event_type = EventType::Edit;
        let result = sim.send(bare).unwrap();
        assert_eq!(
            refusal(&result, &sim),
            refused_with("target_event_not_found")
        );
    }

    /// Gap G-062: an edit whose new content is itself a change would write
    /// a delete of another event on its target, unchecked.
    #[test]
    fn whose_new_content_is_a_delete_is_refused_target_event_not_found() {
        let mut sim = room();
        let mut nested = edit();
        nested.content = EventContent::Edit(EditContent {
            edit_source: Some(EditSource::Sender),
            ..EditContent::new(
                id("evt_1"),
                EventContent::Delete(DeleteContent::new(id("evt_9"))),
            )
        });
        let result = sim.send(nested).unwrap();
        assert_eq!(
            refusal(&result, &sim),
            refused_with("target_event_not_found")
        );
    }

    /// Gap G-062: a message hiding an edit in a composite would reach a
    /// channel that edits natively as an edit, unchecked.
    #[test]
    fn hiding_an_edit_in_a_composite_is_refused_target_event_not_found() {
        let mut sim = room();
        let parts = vec![TextContent::new("See below").into(), edit().content.clone()];
        let smuggled = InboundMessage::new(
            channel("wa"),
            OTHER,
            EventContent::Composite(CompositeContent::new(parts).unwrap()),
        );
        let result = sim.send(smuggled).unwrap();
        assert_eq!(
            refusal(&result, &sim),
            refused_with("target_event_not_found")
        );
    }

    #[test]
    fn from_a_sender_naming_no_one_is_refused_identity_required_for_edit() {
        let mut sim = room();
        let result = sim.send(edit_from("", "evt_1", "I need 50000$")).unwrap();
        assert_eq!(
            refusal(&result, &sim),
            refused_with("identity_required_for_edit")
        );
    }

    #[test]
    fn from_another_sender_is_refused_not_original_author() {
        let mut sim = room();
        let result = sim.send(edit_from(OTHER, "evt_1", "I need 0$")).unwrap();
        assert_eq!(refusal(&result, &sim), refused_with("not_original_author"));
    }

    #[test]
    fn an_admin_delete_from_a_member_is_refused_not_authorized() {
        let mut sim = room();
        let result = sim
            .send(delete_from(OTHER, "evt_1", DeleteType::Admin))
            .unwrap();
        assert_eq!(refusal(&result, &sim), refused_with("not_authorized"));
    }

    #[test]
    fn a_system_delete_from_a_transport_is_refused_not_authorized() {
        let mut sim = room();
        let result = sim
            .send(delete_from(SENDER, "evt_1", DeleteType::System))
            .unwrap();
        assert_eq!(refusal(&result, &sim), refused_with("not_authorized"));
    }

    /// Nothing runs after the check: no hook, no commit, no delivery, and
    /// the lock is released (the simulated driver checks it).
    #[test]
    fn runs_no_hook_and_reaches_no_channel() {
        let mut sim = room();
        sim.before_broadcast(
            "moderation",
            Box::new(|_| HookOutcome::Returned(HookResult::allow(AllowOptions::default()))),
        );
        sim.send(edit_from(OTHER, "evt_1", "I need 0$")).unwrap();
        assert_eq!(
            sim.trace,
            [
                "load_context room_1",
                "handle_inbound wa",
                "lock room_1",
                "load_context room_1",
                "get_event evt_1",
                "release room_1",
            ]
        );
    }

    #[test]
    fn leaves_its_target_unchanged() {
        let mut sim = room();
        sim.send(edit_from(OTHER, "evt_1", "I need 0$")).unwrap();
        assert_eq!(
            stored(&sim, "evt_1").content,
            TextContent::new("I need 5000$").into()
        );
    }

    /// A store that cannot read the target fails the message, with nothing
    /// written and the lock released.
    #[test]
    fn whose_target_cannot_be_read_fails_the_message() {
        let mut sim = room();
        sim.failing_effect = Some("get_event");
        let sent = sim.send(edit());
        assert_eq!(
            (
                sent.map(|result| result.blocked).is_err(),
                sim.timeline.len()
            ),
            (true, 1)
        );
    }
}

/// RFC §10.1 step 14, §19.3.1: an agent's response goes through step 8a too.
mod an_agent_s_response {
    use super::*;
    use pretty_assertions::assert_eq;

    /// An agent that answers "Helo" to a greeting, then corrects its last
    /// answer when told of the typo, targeting what `target` picks from the
    /// history it was handed.
    fn correcting_agent(sim: &mut Sim, target: fn(&[RoomEvent]) -> Option<EventId>) {
        sim.agent(
            "ai",
            Box::new(move |event, context| {
                let typo =
                    matches!(&event.content, EventContent::Text(text) if text.body == "typo?");
                if !typo {
                    if matches!(event.content, EventContent::Text(_)) {
                        return Ok(ChannelOutput {
                            responded: true,
                            response_events: vec![response("ai", event, "Helo")],
                            ..ChannelOutput::empty()
                        });
                    }
                    return Ok(ChannelOutput::default());
                }
                let Some(target_event_id) = target(&context.recent_events) else {
                    return Ok(ChannelOutput::default());
                };
                let mut correction = response("ai", event, "");
                correction.r#type = EventType::Edit;
                correction.content = EventContent::Edit(EditContent {
                    edit_source: Some(EditSource::Sender),
                    ..EditContent::new(target_event_id, TextContent::new("Hello"))
                });
                Ok(ChannelOutput {
                    responded: true,
                    response_events: vec![correction],
                    ..ChannelOutput::empty()
                })
            }),
        );
    }

    fn last_of(channel_id: &'static str) -> impl Fn(&[RoomEvent]) -> Option<EventId> {
        move |history| {
            history
                .iter()
                .rev()
                .find(|event| event.source.channel_id.as_str() == channel_id)
                .map(|event| event.id.clone())
        }
    }

    #[test]
    fn edits_the_agent_s_own_response() {
        let mut sim = room();
        correcting_agent(&mut sim, |history| last_of("ai")(history));
        sim.observer("projection", HookTrigger::OnEventUpdated);
        sim.send(Sim::message("wa", "Bonjour")).unwrap();
        sim.send(Sim::message("wa", "typo?")).unwrap();
        let answer = stored(&sim, "evt_3");
        assert_eq!(
            (
                answer.content.clone(),
                answer.metadata.get("edited").cloned(),
                sim.async_triggers()
            ),
            (
                TextContent::new("Hello").into(),
                Some(json!(true)),
                vec![HookTrigger::OnEventUpdated]
            )
        );
    }

    #[test]
    fn may_not_edit_a_participant_s_message() {
        let mut sim = room();
        correcting_agent(&mut sim, |history| last_of("wa")(history));
        sim.send(Sim::message("wa", "typo?")).unwrap();
        assert_eq!(
            (stored(&sim, "evt_1").content.clone(), sim.timeline_lines()),
            (
                TextContent::new("I need 5000$").into(),
                vec![
                    "#0 wa delivered \"I need 5000$\"".to_owned(),
                    "#1 wa delivered \"typo?\"".to_owned(),
                ]
            )
        );
    }
}

/// RFC §9.4, gap G-031: an event a hook injects is the hook's.
mod a_hook {
    use super::*;
    use pretty_assertions::assert_eq;

    /// The framework's delete of `evt_1`, claiming `delete_type`.
    fn moderation_delete(delete_type: DeleteType) -> RoomEvent {
        let mut delete = notice_event("placeholder", "");
        delete.r#type = EventType::Delete;
        let mut content = DeleteContent {
            delete_type,
            ..DeleteContent::new(id("evt_1"))
        };
        content.reason = Some("spam".to_owned());
        delete.content = EventContent::Delete(content);
        delete
    }

    /// A hook that injects `injected` when the room hears "report it".
    fn injecting(sim: &mut Sim, injected: RoomEvent) {
        sim.before_broadcast(
            "moderation",
            Box::new(move |event| {
                let reported =
                    matches!(&event.content, EventContent::Text(text) if text.body == "report it");
                let result = if reported {
                    HookResult::allow(AllowOptions {
                        injected: Some(vec![InjectedEvent {
                            target_channel_ids: Some(vec![channel("ws"), channel("sms")]),
                            ..InjectedEvent::new(injected.clone())
                        }]),
                    })
                } else {
                    HookResult::allow(AllowOptions::default())
                };
                HookOutcome::Returned(result)
            }),
        );
    }

    #[test]
    fn injects_a_system_delete_that_marks_its_target_deleted() {
        let mut sim = room();
        injecting(&mut sim, moderation_delete(DeleteType::System));
        sim.observer("projection", HookTrigger::OnEventDeleted);
        sim.send(Sim::message("wa", "report it")).unwrap();
        assert_eq!(
            (
                stored(&sim, "evt_1").metadata.get("deleted").cloned(),
                sim.traced("commit"),
                sim.async_triggers()
            ),
            (
                Some(json!(true)),
                vec![
                    "commit #1 wa delivered \"report it\"",
                    "commit #2 system delivered \"[Message deleted]\" updating evt_1",
                ],
                vec![HookTrigger::OnEventDeleted]
            )
        );
    }

    #[test]
    fn may_not_inject_a_delete_in_the_author_s_name() {
        let mut sim = room();
        injecting(&mut sim, moderation_delete(DeleteType::Sender));
        sim.send(Sim::message("wa", "report it")).unwrap();
        assert_eq!(
            (
                stored(&sim, "evt_1").metadata.is_empty(),
                sim.timeline.len()
            ),
            (true, 2)
        );
    }

    /// RFC §9.4, gap G-020: a modify keeps the event an edit targets; the
    /// new content it writes stands.
    #[test]
    fn s_modify_cannot_retarget_an_edit() {
        let mut sim = room();
        sim.send(Sim::message("wa", "Bonjour")).unwrap();
        forget(&mut sim);
        sim.before_broadcast(
            "redactor",
            Box::new(|event| {
                let mut redacted = event.clone();
                if let EventContent::Edit(edit) = &mut redacted.content {
                    edit.target_event_id = id("evt_2");
                    *edit.new_content = TextContent::new("I need [redacted]").into();
                }
                HookOutcome::Returned(HookResult::modify(redacted, ModifyOptions::default()))
            }),
        );
        sim.send(edit()).unwrap();
        assert_eq!(
            (
                stored(&sim, "evt_1").content.clone(),
                stored(&sim, "evt_2").content.clone()
            ),
            (
                TextContent::new("I need [redacted]").into(),
                TextContent::new("Bonjour").into()
            )
        );
    }

    /// RFC §9.4, §10.3, gap G-062: a hook that redacts an event as text
    /// redacts an edit too: its text is the edit's new content, of the same
    /// target and `edit_source`, and the target takes it.
    #[test]
    fn s_redaction_as_text_is_an_edit_s_new_content() {
        let mut sim = room();
        sim.before_broadcast(
            "redactor",
            Box::new(|event| {
                let mut redacted = event.clone();
                redacted.content = TextContent::new("I need [redacted]").into();
                HookOutcome::Returned(HookResult::modify(redacted, ModifyOptions::default()))
            }),
        );
        sim.send(edit()).unwrap();
        let redacted = EventContent::Edit(EditContent {
            edit_source: Some(EditSource::Sender),
            ..EditContent::new(id("evt_1"), TextContent::new("I need [redacted]"))
        });
        assert_eq!(
            (
                stored(&sim, "evt_2").content.clone(),
                stored(&sim, "evt_1").content.clone()
            ),
            (redacted, TextContent::new("I need [redacted]").into())
        );
    }

    /// Gap G-062: a modify cannot slip a change into an edit's new content;
    /// the target takes the new content step 8a saw.
    #[test]
    fn s_modify_cannot_nest_a_delete_in_an_edit() {
        let mut sim = room();
        sim.before_broadcast(
            "nester",
            Box::new(|event| {
                let mut nested = event.clone();
                if let EventContent::Edit(edit) = &mut nested.content {
                    *edit.new_content = EventContent::Delete(DeleteContent::new(id("evt_9")));
                }
                HookOutcome::Returned(HookResult::modify(nested, ModifyOptions::default()))
            }),
        );
        sim.send(edit()).unwrap();
        assert_eq!(
            stored(&sim, "evt_1").content,
            TextContent::new("I need 50000$").into()
        );
    }

    /// Gap G-062: an injected change reaches those of its targets that
    /// could see what it changes.
    #[test]
    fn s_system_delete_of_a_message_one_channel_saw_reaches_that_channel_only() {
        let mut sim = Sim::new();
        sim.transport("wa");
        sim.transport("ws");
        sim.transport("sms");
        let mut restricted = Sim::message("wa", "My card is 4242");
        restricted.visibility = ChannelSet::new([channel("ws")])
            .map(Visibility::Channels)
            .unwrap();
        sim.send(restricted).unwrap();
        forget(&mut sim);
        injecting(&mut sim, moderation_delete(DeleteType::System));
        sim.send(Sim::message("wa", "report it")).unwrap();
        let delete = stored(&sim, "evt_3");
        assert_eq!(
            (
                delete.r#type,
                delete.visibility.to_string(),
                sim.traced("deliver")
            ),
            (
                EventType::Delete,
                "ws".to_owned(),
                vec![
                    "deliver sms <- evt_2",
                    "deliver ws <- evt_2",
                    "deliver ws <- evt_3"
                ]
            )
        );
    }

    /// RFC §10.3: a hook that blocks a message and injects its delete
    /// targets a refused record; the delete is refused, and logged.
    #[test]
    fn s_delete_of_the_message_it_blocks_is_refused() {
        let mut sim = room();
        sim.before_broadcast(
            "moderation",
            Box::new(|event| {
                let reported =
                    matches!(&event.content, EventContent::Text(text) if text.body == "spam");
                if !reported {
                    return HookOutcome::Returned(HookResult::allow(AllowOptions::default()));
                }
                let mut delete = moderation_delete(DeleteType::System);
                if let EventContent::Delete(content) = &mut delete.content {
                    content.target_event_id = event.id.clone();
                }
                HookOutcome::Returned(HookResult::block(
                    "spam",
                    BlockOptions {
                        injected: Some(vec![InjectedEvent::new(delete)]),
                        ..Default::default()
                    },
                ))
            }),
        );
        sim.send(Sim::message("wa", "spam")).unwrap();
        assert_eq!(
            sim.timeline_lines(),
            [
                "#0 wa delivered \"I need 5000$\"",
                "#1 wa blocked:moderation \"spam\""
            ]
        );
    }

    /// RFC §9.4: a modify cannot turn a message into an edit past step 8a.
    #[test]
    fn s_modify_cannot_turn_a_message_into_an_edit() {
        let mut sim = room();
        sim.before_broadcast(
            "forger",
            Box::new(|event| {
                let mut forged = event.clone();
                forged.content = EventContent::Edit(EditContent {
                    edit_source: Some(EditSource::Sender),
                    ..EditContent::new(id("evt_1"), TextContent::new("forged"))
                });
                HookOutcome::Returned(HookResult::modify(forged, ModifyOptions::default()))
            }),
        );
        sim.send(Sim::message("wa", "Bonjour")).unwrap();
        assert_eq!(
            (
                stored(&sim, "evt_1").content.clone(),
                sim.timeline_lines()[1].clone()
            ),
            (
                TextContent::new("I need 5000$").into(),
                "#1 wa delivered \"Bonjour\"".to_owned()
            )
        );
    }
}

/// RFC §10.3 "Direct mutation (host-owned)".
mod a_direct_update {
    use super::*;
    use pretty_assertions::assert_eq;

    fn rewrite(text: &str) -> EventUpdates {
        EventUpdates {
            content: Some(TextContent::new(text).into()),
            ..EventUpdates::default()
        }
    }

    /// RFC §10.3: the trigger fires once the lock is released, with the
    /// room's context read then.
    #[test]
    fn replaces_the_fields_it_names_under_the_lock_and_fires_after_the_release() {
        let mut sim = room();
        sim.observer("projection", HookTrigger::OnEventUpdated);
        sim.update_event("evt_1", rewrite("I need 6000$")).unwrap();
        assert_eq!(
            sim.journal,
            [
                "lock room_1",
                "load_context room_1",
                "get_event evt_1",
                "update event evt_1",
                "release room_1",
                "load_context room_1",
                "fire on_event_updated",
            ]
        );
    }

    #[test]
    fn adds_no_edited_marker() {
        let mut sim = room();
        let updated = sim
            .update_event("evt_1", rewrite("I need 6000$"))
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                &updated == stored(&sim, "evt_1"),
                updated.metadata.is_empty(),
                updated.content
            ),
            (true, true, TextContent::new("I need 6000$").into())
        );
    }

    #[test]
    fn replaces_the_metadata_whole() {
        let mut sim = room();
        sim.timeline[0]
            .metadata
            .insert("crm".to_owned(), json!("c_1"));
        let update = EventUpdates {
            metadata: Some([("tag".to_owned(), json!("vip"))].into_iter().collect()),
            ..EventUpdates::default()
        };
        sim.update_event("evt_1", update).unwrap();
        assert_eq!(
            serde_json::Value::Object(stored(&sim, "evt_1").metadata.clone()),
            json!({ "tag": "vip" })
        );
    }

    #[test]
    fn of_a_missing_event_is_a_no_op_firing_nothing() {
        let mut sim = room();
        sim.observer("projection", HookTrigger::OnEventUpdated);
        let updated = sim.update_event("evt_404", rewrite("x")).unwrap();
        assert_eq!(
            (updated, sim.traced("update event"), sim.async_triggers()),
            (None, Vec::<&str>::new(), Vec::new())
        );
    }

    #[test]
    fn of_another_room_s_event_is_a_no_op() {
        let mut sim = room();
        let mut elsewhere = stored(&sim, "evt_1").clone();
        elsewhere.id = id("evt_9");
        elsewhere.room_id = RoomId::new("room_2").unwrap();
        sim.seed_room(Room::at(
            RoomId::new("room_2").unwrap(),
            Timestamp::UNIX_EPOCH,
        ));
        sim.seed_other("room_2", elsewhere);
        let updated = sim.update_event("evt_9", rewrite("x")).unwrap();
        assert_eq!(
            (updated, sim.traced("update event")),
            (None, Vec::<&str>::new())
        );
    }

    /// Gap G-063: an update that changes nothing writes nothing and fires
    /// nothing.
    #[test]
    fn changing_nothing_writes_nothing_and_fires_nothing() {
        let mut sim = room();
        sim.observer("projection", HookTrigger::OnEventUpdated);
        let updated = sim.update_event("evt_1", rewrite("I need 5000$")).unwrap();
        assert_eq!(
            (
                updated.map(|event| event.content),
                sim.traced("update event"),
                sim.async_triggers()
            ),
            (
                Some(TextContent::new("I need 5000$").into()),
                Vec::<&str>::new(),
                Vec::new()
            )
        );
    }

    /// Gap G-063: a missing room is a no-op, as a missing target is.
    #[test]
    fn in_a_missing_room_changes_and_fires_nothing() {
        let mut sim = room();
        sim.observer("projection", HookTrigger::OnEventUpdated);
        sim.room_exists = false;
        assert_eq!(
            (
                sim.update_event("evt_1", rewrite("x")),
                sim.traced("update event"),
                sim.async_triggers()
            ),
            (Ok(None), vec![], vec![])
        );
    }

    /// Gap G-063: a change to what the room stored is no new event.
    #[test]
    fn in_a_closed_room_is_made() {
        let mut sim = room();
        sim.room.status = RoomStatus::Closed;
        let updated = sim.update_event("evt_1", rewrite("I need 6000$")).unwrap();
        assert_eq!(
            updated.map(|event| event.content),
            Some(TextContent::new("I need 6000$").into())
        );
    }

    /// Gap G-045: the room's due timers are applied first, and reported.
    #[test]
    fn in_a_room_whose_close_fell_due_applies_the_close_first() {
        let mut sim = room();
        sim.room.timers = RoomTimers {
            inactive_after_seconds: Some(60),
            closed_after_seconds: Some(60),
            last_activity_at: None,
        };
        sim.update_event("evt_1", rewrite("I need 6000$")).unwrap();
        assert_eq!(
            (sim.room.status, sim.traced("update")),
            (
                RoomStatus::Closed,
                vec!["update room room_1 Closed", "update event evt_1"]
            )
        );
    }

    /// RFC §10.3: the trigger's context is the room read once the lock is
    /// released: an event another call committed meanwhile is in it.
    #[test]
    fn fires_with_the_room_read_after_the_release() {
        let mut sim = room();
        sim.observer("projection", HookTrigger::OnEventUpdated);
        sim.committed_before_unlocked_read = Some(notice_event("evt_9", "Meanwhile"));
        sim.update_event("evt_1", rewrite("I need 6000$")).unwrap();
        let history = sim
            .async_contexts()
            .first()
            .map(|context| texts(&context.recent_events));
        assert_eq!(
            history,
            Some(vec!["I need 6000$".to_owned(), "Meanwhile".to_owned()])
        );
    }

    /// Gap G-063: a read of the room that fails once the lock is released
    /// changes nothing the update did; its trigger fires with the room as
    /// the update left it.
    #[test]
    fn whose_room_cannot_be_read_after_the_release_still_fires() {
        let mut sim = room();
        sim.observer("projection", HookTrigger::OnEventUpdated);
        sim.failing_effect = Some("unlocked load_context room_1");
        let updated = sim.update_event("evt_1", rewrite("I need 6000$"));
        let history = sim
            .async_contexts()
            .first()
            .map(|context| texts(&context.recent_events));
        assert_eq!(
            (
                updated.map(|event| event.map(|event| event.content)),
                sim.async_triggers(),
                history
            ),
            (
                Ok(Some(TextContent::new("I need 6000$").into())),
                vec![HookTrigger::OnEventUpdated],
                Some(vec!["I need 6000$".to_owned()])
            )
        );
    }

    /// The lock is released when the write fails (the simulated driver
    /// checks it), and nothing fires.
    #[test]
    fn that_the_store_fails_fails_with_nothing_fired() {
        let mut sim = room();
        sim.observer("projection", HookTrigger::OnEventUpdated);
        sim.failing_effect = Some("update event");
        let updated = sim.update_event("evt_1", rewrite("x"));
        assert_eq!((updated.is_err(), sim.async_triggers()), (true, Vec::new()));
    }
}

/// RFC §10.3 "Direct mutation (host-owned)": a hard delete.
mod a_direct_delete {
    use super::*;
    use pretty_assertions::assert_eq;

    /// The room with a reply `evt_2` in `evt_1`'s thread and an unrelated
    /// `evt_3`.
    fn threaded() -> Sim {
        let mut sim = room();
        let mut reply = Sim::message("wa", "Is that right?");
        reply.parent_event_id = Some(id("evt_1"));
        sim.send(reply).unwrap();
        sim.send(Sim::message("wa", "Unrelated")).unwrap();
        forget(&mut sim);
        sim
    }

    /// Gap G-063: a missing room is a no-op, as a missing target is.
    #[test]
    fn in_a_missing_room_removes_and_fires_nothing() {
        let mut sim = threaded();
        sim.observer("projection", HookTrigger::OnEventDeleted);
        sim.room_exists = false;
        assert_eq!(
            (
                sim.delete_event("evt_1", true),
                sim.traced("delete event"),
                sim.async_triggers()
            ),
            (Ok(vec![]), vec![], vec![])
        );
    }

    #[test]
    fn removes_the_event_and_its_replies_and_returns_their_ids() {
        let mut sim = threaded();
        let removed = sim.delete_event("evt_1", true).unwrap();
        assert_eq!(
            (removed, sim.timeline_lines()),
            (
                vec![id("evt_1"), id("evt_2")],
                vec!["#2 wa delivered \"Unrelated\"".to_owned()]
            )
        );
    }

    /// Gap G-063: an edit holds its target's content, and goes with it.
    #[test]
    fn takes_the_edits_of_what_it_removes() {
        let mut sim = threaded();
        sim.send(edit()).unwrap();
        let removed = sim.delete_event("evt_1", false).unwrap();
        assert_eq!(
            (removed, sim.timeline_lines()),
            (
                vec![id("evt_1"), id("evt_4")],
                vec![
                    "#1 wa delivered \"Is that right?\"".to_owned(),
                    "#2 wa delivered \"Unrelated\"".to_owned()
                ]
            )
        );
    }

    #[test]
    fn of_a_closed_room_is_made() {
        let mut sim = threaded();
        sim.room.status = RoomStatus::Closed;
        let removed = sim.delete_event("evt_3", true).unwrap();
        assert_eq!(removed, vec![id("evt_3")]);
    }

    #[test]
    fn without_cascade_keeps_the_replies() {
        let mut sim = threaded();
        let removed = sim.delete_event("evt_1", false).unwrap();
        assert_eq!((removed, sim.timeline.len()), (vec![id("evt_1")], 2));
    }

    /// RFC §10.3: once the lock is released, with the room's context read
    /// then, in the order of the ids returned.
    #[test]
    fn fires_on_event_deleted_for_each_removed_event_after_the_release() {
        let mut sim = threaded();
        sim.observer("projection", HookTrigger::OnEventDeleted);
        sim.delete_event("evt_1", true).unwrap();
        let payloads: Vec<&str> = sim
            .async_payloads()
            .iter()
            .map(|event| event.id.as_str())
            .collect();
        assert_eq!(
            (sim.journal[3..].to_vec(), payloads),
            (
                vec![
                    "release room_1".to_owned(),
                    "load_context room_1".to_owned(),
                    "fire on_event_deleted".to_owned(),
                    "fire on_event_deleted".to_owned()
                ],
                vec!["evt_1", "evt_2"]
            )
        );
    }

    /// RFC §10.3: each trigger's context is the room read once the lock is
    /// released, without the events removed: an event another call
    /// committed meanwhile is in it.
    #[test]
    fn fires_with_the_room_read_after_the_release() {
        let mut sim = threaded();
        sim.observer("projection", HookTrigger::OnEventDeleted);
        sim.committed_before_unlocked_read = Some(notice_event("evt_9", "Meanwhile"));
        sim.delete_event("evt_1", true).unwrap();
        let histories: Vec<Vec<String>> = sim
            .async_contexts()
            .iter()
            .map(|context| texts(&context.recent_events))
            .collect();
        let read = vec!["Unrelated".to_owned(), "Meanwhile".to_owned()];
        assert_eq!(histories, [read.clone(), read]);
    }

    #[test]
    fn of_a_missing_event_is_a_no_op_firing_nothing() {
        let mut sim = threaded();
        sim.observer("projection", HookTrigger::OnEventDeleted);
        let removed = sim.delete_event("evt_404", true).unwrap();
        assert_eq!(
            (removed, sim.timeline.len(), sim.async_triggers()),
            (Vec::new(), 3, Vec::new())
        );
    }

    /// Gap G-064: the counters stay, and an index is never given again.
    #[test]
    fn leaves_the_counters_and_the_next_event_takes_a_new_index() {
        let mut sim = threaded();
        sim.delete_event("evt_1", true).unwrap();
        let counted = sim.room.event_count;
        let result = sim.send(Sim::message("wa", "Hello?")).unwrap();
        assert_eq!(
            (counted, result.event.map(|event| event.index)),
            (3, Some(3))
        );
    }

    #[test]
    fn makes_a_later_edit_of_the_event_refused() {
        let mut sim = threaded();
        sim.delete_event("evt_1", true).unwrap();
        let result = sim.send(edit()).unwrap();
        assert_eq!(result.reason.as_deref(), Some("target_event_not_found"));
    }

    #[test]
    fn of_another_room_s_event_removes_nothing() {
        let mut sim = threaded();
        let mut elsewhere = stored(&sim, "evt_3").clone();
        elsewhere.id = id("evt_9");
        elsewhere.room_id = RoomId::new("room_2").unwrap();
        sim.seed_room(Room::at(
            RoomId::new("room_2").unwrap(),
            Timestamp::UNIX_EPOCH,
        ));
        sim.seed_other("room_2", elsewhere);
        let removed = sim.delete_event("evt_9", true).unwrap();
        assert_eq!(
            (removed, sim.other_timeline_lines("room_2").len()),
            (Vec::new(), 1)
        );
    }
}

/// An event that is neither an edit nor a delete reads no target.
mod another_event {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn reads_no_target() {
        let mut sim = room();
        sim.send(Sim::message("wa", "Bonjour")).unwrap();
        assert_eq!(sim.traced("get_event"), Vec::<&str>::new());
    }
}

/// RFC §10.1 step 8a, §13.5, gap G-070: an edit's target is checked again
/// once the hooks that ran under the lock are over, so a change a hook's code
/// committed ahead of the edit is not undone by it.
mod a_target_changed_by_a_hook {
    use super::*;
    use pretty_assertions::assert_eq;

    /// A moderation hook deletes the target the edit was accepted for: the
    /// edit is refused, and the target stays deleted.
    #[test]
    fn refuses_the_edit_the_target_no_longer_takes() {
        let mut sim = room();
        sim.reentrant_hook(
            "moderator",
            true,
            Reentry {
                entries: calling(|event| {
                    (event.r#type == EventType::Edit).then(|| {
                        let mut delete = DirectEvent::new(
                            channel("wa"),
                            EventContent::Delete(DeleteContent::new(id("evt_1"))),
                        );
                        delete.event_type = EventType::Delete;
                        delete.participant_id = Some(ParticipantId::new(SENDER).unwrap());
                        Entry::Direct {
                            room_id: room_id(),
                            organization_id: None,
                            event: Box::new(delete),
                        }
                    })
                }),
                answer: Answer::Awaits(Box::new(|_| {
                    HookOutcome::Returned(HookResult::allow(AllowOptions::default()))
                })),
            },
        );
        let result = sim.send(edit()).unwrap();
        let target = stored(&sim, "evt_1");
        assert_eq!(
            (
                result.reason.as_deref(),
                transcoding::text_of(&target.content),
                target.metadata.get("deleted").cloned()
            ),
            (
                Some("target_event_not_found"),
                "I need 5000$".to_owned(),
                Some(json!(true))
            )
        );
    }
}

/// RFC §10.3 "Mutation triggers", §9.2, gap G-062: what `ON_EVENT_UPDATED`
/// and `ON_EVENT_DELETED` carry when the pipeline commits a change: the
/// target as the payload, which the hooks' filters read, and the room as
/// the change's commit left it.
mod a_mutation_trigger {
    use super::*;
    use pretty_assertions::assert_eq;
    use roomkit_core::model::{ChannelId, RoomContext};
    use roomkit_core::ops::Notice;
    use roomkit_core::rules::hook_registry::{HookExecution, HookSpec};

    /// The context `trigger`'s observers were handed.
    fn context_of(sim: &Sim, trigger: HookTrigger) -> RoomContext {
        sim.notices
            .iter()
            .find_map(|notice| match notice {
                Notice::AsyncHooks {
                    trigger: fired,
                    context,
                    ..
                } if *fired == trigger => Some((**context).clone()),
                Notice::AsyncHooks { .. }
                | Notice::FrameworkEvent(_)
                | Notice::StopReading { .. }
                | Notice::CutReadOver { .. }
                | Notice::Span(_)
                | Notice::Answered(_) => None,
            })
            .unwrap()
    }

    /// The history of `context`, each event's index, id and text, and the
    /// room's counters.
    fn history(context: &RoomContext) -> (Vec<(u64, String, String)>, u64, Option<u64>) {
        let events = context
            .recent_events
            .iter()
            .map(|event| {
                (
                    event.index,
                    event.id.to_string(),
                    transcoding::text_of(&event.content),
                )
            })
            .collect();
        (events, context.room.event_count, context.room.latest_index)
    }

    fn line(index: u64, id: &str, text: &str) -> (u64, String, String) {
        (index, id.to_owned(), text.to_owned())
    }

    /// The target rewritten, and the change itself in the history, the
    /// newest event at its committed index, the counters past it.
    #[test]
    fn of_an_edit_holds_the_room_as_its_commit_left_it() {
        let mut sim = room();
        sim.observer("projection", HookTrigger::OnEventUpdated);
        sim.send(edit()).unwrap();
        assert_eq!(
            history(&context_of(&sim, HookTrigger::OnEventUpdated)),
            (
                vec![
                    line(0, "evt_1", "I need 50000$"),
                    line(1, "evt_2", "Correction: I need 50000$"),
                ],
                2,
                Some(1)
            )
        );
    }

    #[test]
    fn of_a_delete_holds_the_room_as_its_commit_left_it() {
        let mut sim = room();
        sim.observer("projection", HookTrigger::OnEventDeleted);
        sim.send(delete()).unwrap();
        let context = context_of(&sim, HookTrigger::OnEventDeleted);
        assert_eq!(
            (
                history(&context),
                context.recent_events[0].metadata.get("deleted").cloned()
            ),
            (
                (
                    vec![
                        line(0, "evt_1", "I need 5000$"),
                        line(1, "evt_2", "[Message deleted]"),
                    ],
                    2,
                    Some(1)
                ),
                Some(json!(true))
            )
        );
    }

    /// An event a hook's code committed ahead of the change, during its
    /// hooks, is in the history before it.
    #[test]
    fn holds_what_was_committed_ahead_of_the_change_during_its_hooks() {
        let mut sim = room();
        sim.observer("projection", HookTrigger::OnEventUpdated);
        sim.reentrant_hook(
            "notes",
            true,
            Reentry {
                entries: calling(|event| {
                    (event.r#type == EventType::Edit).then(|| Entry::Direct {
                        room_id: room_id(),
                        organization_id: None,
                        event: Box::new(DirectEvent::new(
                            channel("ws"),
                            TextContent::new("Heads up"),
                        )),
                    })
                }),
                answer: Answer::Awaits(Box::new(|_| {
                    HookOutcome::Returned(HookResult::allow(AllowOptions::default()))
                })),
            },
        );
        sim.send(edit()).unwrap();
        let (events, event_count, latest_index) =
            history(&context_of(&sim, HookTrigger::OnEventUpdated));
        let texts: Vec<(u64, String)> = events
            .into_iter()
            .map(|(index, _, text)| (index, text))
            .collect();
        assert_eq!(
            (texts, event_count, latest_index),
            (
                vec![
                    (0, "I need 50000$".to_owned()),
                    (1, "Heads up".to_owned()),
                    (2, "Correction: I need 50000$".to_owned()),
                ],
                3,
                Some(2)
            )
        );
    }

    /// The filters read the target's source: an observer of the target's
    /// channel sees a correction the framework's hook made, one of the
    /// change's channel does not.
    #[test]
    fn reaches_the_observers_filtered_on_its_target_s_channel() {
        let mut sim = room();
        let filtered = |sim: &mut Sim, name: &str, channel_id: &str| {
            let mut spec = HookSpec::new(name, HookTrigger::OnEventUpdated, HookExecution::Async);
            spec.filter.channel_ids = Some([ChannelId::new(channel_id).unwrap()].into());
            sim.config.hooks.register(spec).unwrap()
        };
        let of_the_target = filtered(&mut sim, "of_wa", "wa");
        filtered(&mut sim, "of_system", "system");
        let mut correction = notice_event("placeholder", "");
        correction.r#type = EventType::Edit;
        correction.content = EventContent::Edit(EditContent {
            edit_source: Some(EditSource::System),
            ..EditContent::new(id("evt_1"), TextContent::new("I need [redacted]"))
        });
        sim.before_broadcast(
            "redactor",
            Box::new(move |event| {
                let reported =
                    matches!(&event.content, EventContent::Text(text) if text.body == "report it");
                if !reported {
                    return HookOutcome::Returned(HookResult::allow(AllowOptions::default()));
                }
                HookOutcome::Returned(HookResult::allow(AllowOptions {
                    injected: Some(vec![InjectedEvent::new(correction.clone())]),
                }))
            }),
        );
        sim.send(Sim::message("wa", "report it")).unwrap();
        let fired: Vec<_> = sim
            .notices
            .iter()
            .filter_map(|notice| match notice {
                Notice::AsyncHooks { trigger, hooks, .. }
                    if *trigger == HookTrigger::OnEventUpdated =>
                {
                    Some(hooks.clone())
                }
                Notice::AsyncHooks { .. }
                | Notice::FrameworkEvent(_)
                | Notice::StopReading { .. }
                | Notice::CutReadOver { .. }
                | Notice::Span(_)
                | Notice::Answered(_) => None,
            })
            .collect();
        assert_eq!(fired, [vec![of_the_target]]);
    }
}
