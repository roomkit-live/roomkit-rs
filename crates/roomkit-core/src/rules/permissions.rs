//! Who reads, who writes, and whom a broadcast reaches (RFC §7.5, §10.2,
//! §19.3).

use std::collections::BTreeSet;

use crate::model::{
    AgentResponsePolicy, ChannelBinding, ChannelCategory, ChannelId, EventContent, EventId,
    EventStatus, Metadata, RoomContext, RoomEvent, Visibility,
};
use crate::rules::chain_depth;
use crate::rules::edits::{self, Mutation};

/// Why a source's event may not be broadcast (RFC §7.5 rule 2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WriteRefusal {
    /// The binding's access does not include writing.
    ReadOnly,
    /// The binding is muted.
    Muted,
    /// The source has no binding in the room: a channel the integrator
    /// detached, which step 2 leaves unbound (RFC §7.5 rule 7, §10.1 step 2,
    /// gap G-015).
    Unbound,
}

impl WriteRefusal {
    /// The `blocked_by` value the refused event is stored with.
    pub fn blocked_by(self) -> &'static str {
        match self {
            Self::ReadOnly => "source_read_only",
            Self::Muted => "source_muted",
            Self::Unbound => "source_unbound",
        }
    }
}

/// Why `source` may not have its events broadcast, `None` when it may (RFC
/// §7.5 rule 2).
///
/// An access without writing, `none` included, is reported before a mute:
/// it is the lasting cause (gap G-015).
pub fn write_refusal(source: &ChannelBinding) -> Option<WriteRefusal> {
    if !source.access.writes() {
        Some(WriteRefusal::ReadOnly)
    } else if source.muted {
        Some(WriteRefusal::Muted)
    } else {
        None
    }
}

/// Whether `reader` receives `event` through `on_event()`: it reads, the
/// event is not its own, and the event's visibility admits it (RFC §7.5
/// rules 1 and 5).
pub fn receives(reader: &ChannelBinding, event: &RoomEvent) -> bool {
    reader.access.reads()
        && reader.channel_id != event.source.channel_id
        && event.visibility.admits(&reader.channel_id, reader.category)
}

/// The events of `timeline` that may be rebuilt as context for `reader`
/// (RFC §7.5 rule 8), in timeline order.
///
/// A refused event reaches no reader, its own source included. A reader
/// always has its own accepted events; it has another source's when it
/// reads and the event's visibility admits it. The event's visibility is the
/// one stamped at its commit (gap G-009), so the answer does not change when
/// a binding changes later.
pub fn history_for<'timeline>(
    reader: &ChannelBinding,
    timeline: &'timeline [RoomEvent],
) -> Vec<&'timeline RoomEvent> {
    timeline
        .iter()
        .filter(|event| event.status != EventStatus::Blocked)
        .filter(|event| {
            event.source.channel_id == reader.channel_id
                || (reader.access.reads()
                    && event.visibility.admits(&reader.channel_id, reader.category))
        })
        .collect()
}

/// The context a channel receives: the room as it is, with only the events
/// it may see (RFC §7.5 rule 8), and nothing of what was deleted: a
/// soft-deleted event, and an edit of one, reach it with their content
/// withheld (RFC §10.3, gap G-062).
///
/// Every context the pipeline hands a channel goes through here: the one it
/// parses a message with, and the one each target of a broadcast is told
/// and delivered with. Hooks, which are host code, receive the room whole,
/// as the store keeps it.
pub fn context_for(reader: &ChannelBinding, whole: &RoomContext) -> RoomContext {
    let deleted = deleted_events(&whole.recent_events);
    RoomContext {
        room: whole.room.clone(),
        bindings: whole.bindings.clone(),
        participants: whole.participants.clone(),
        recent_events: history_for(reader, &whole.recent_events)
            .into_iter()
            .map(|event| withheld(event, &deleted))
            .collect(),
    }
}

/// The events `timeline` shows deleted (RFC §10.3): an event it holds is
/// deleted when it carries the mark an accepted delete leaves, which a host's
/// direct update may have removed since; an event before the window, when
/// the window holds an accepted delete of it.
fn deleted_events(timeline: &[RoomEvent]) -> BTreeSet<&EventId> {
    let held: BTreeSet<&EventId> = timeline.iter().map(|event| &event.id).collect();
    let marked = timeline
        .iter()
        .filter(|event| edits::is_deleted(event))
        .map(|event| &event.id);
    let before_the_window = timeline
        .iter()
        .filter(|event| event.status != EventStatus::Blocked)
        .filter_map(|event| match edits::mutation(event) {
            Ok(Some(Mutation::Delete(delete))) => Some(&delete.target_event_id),
            Ok(Some(Mutation::Edit(_)) | None) | Err(_) => None,
        })
        .filter(|target| !held.contains(target));
    marked.chain(before_the_window).collect()
}

/// `event` as a reader is handed it: whole, unless it was deleted or is an
/// edit of an event that was (gap G-062). What was said is then withheld: a
/// deleted event's content, or an edit's new content, becomes an empty
/// text, and the provider's original payload, which holds the text, is left
/// out (`source.raw_payload`). The rest stays: the id, index, type, source
/// with the provider's message id, thread, metadata with `metadata.deleted`,
/// and the provider's structured data (`channel_data`), so the reader tells
/// a deleted event from an empty one, the delete that follows names an
/// event the reader holds, and a channel deletes it natively.
fn withheld(event: &RoomEvent, deleted: &BTreeSet<&EventId>) -> RoomEvent {
    let mut handed = event.clone();
    if deleted.contains(&event.id) {
        handed.content = EventContent::text("");
    } else if let EventContent::Edit(edit) = &mut handed.content
        && deleted.contains(&edit.target_event_id)
    {
        *edit.new_content = EventContent::text("");
    } else {
        return handed;
    }
    handed.source.raw_payload = Metadata::new();
    handed
}

/// The visibility an event is committed with: what its sender asked,
/// narrowed to what its source binding allows (gap G-009).
pub fn effective_visibility(
    requested: &Visibility,
    source: &ChannelBinding,
    bindings: &[ChannelBinding],
) -> Visibility {
    requested.intersect(&source.visibility, |channel| {
        bindings
            .iter()
            .find(|binding| &binding.channel_id == channel)
            .map(|binding| binding.category)
    })
}

/// Whom the broadcast of one event reaches (RFC §10.2 steps 2 and 3c).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeliveryPlan {
    /// The transport channels that receive the event, in channel id order.
    pub transports: Vec<TransportTarget>,
    /// The intelligence channels asked to act on the event, a turn each.
    pub agents: Vec<ChannelBinding>,
    /// The intelligence channels the event solicits but that are not asked,
    /// because their response would reach the chain-depth limit (RFC §8.3).
    pub not_asked: Vec<ChannelBinding>,
}

/// A transport channel a broadcast reaches.
#[derive(Clone, Debug, PartialEq)]
pub struct TransportTarget {
    /// The channel's binding, as planned under the room lock.
    pub binding: ChannelBinding,
    /// Whether `deliver()` is called after `on_event()`: only for a channel
    /// that sends to the outside (gap G-014).
    pub delivers: bool,
}

/// Plans the broadcast of `event` among the room's `bindings` (RFC §10.2).
///
/// A channel receives the event when [`receives`] says so. Among
/// intelligence channels, only the solicited ones are part of the plan
/// (RFC §19.3.2: an unsolicited channel is neither asked nor told):
///
/// - an addressed event solicits the channels it names and no other;
/// - an unaddressed event solicits every intelligence channel that receives
///   it, except an agent's output in a room whose policy is
///   [`AddressedOnly`](AgentResponsePolicy::AddressedOnly), which solicits
///   none.
///
/// A solicited channel whose response would reach `max_chain_depth` is not
/// asked (RFC §8.3).
pub fn plan_delivery(
    event: &RoomEvent,
    bindings: &[ChannelBinding],
    policy: AgentResponsePolicy,
    max_chain_depth: u32,
) -> DeliveryPlan {
    let at_limit = chain_depth::reaches_limit(event.chain_depth, max_chain_depth);
    let from_agent = bindings
        .iter()
        .find(|binding| binding.channel_id == event.source.channel_id)
        .is_some_and(|source| source.category == ChannelCategory::Intelligence);

    let mut readers: Vec<&ChannelBinding> = bindings
        .iter()
        .filter(|binding| receives(binding, event))
        .collect();
    readers.sort_by(|first, second| first.channel_id.cmp(&second.channel_id));

    let mut plan = DeliveryPlan::default();
    for reader in readers {
        match reader.category {
            ChannelCategory::Transport => plan.transports.push(TransportTarget {
                binding: reader.clone(),
                delivers: reader.direction.sends(),
            }),
            ChannelCategory::Intelligence => {
                if !solicits(event, reader, from_agent, policy) {
                    continue;
                }
                if at_limit {
                    plan.not_asked.push(reader.clone());
                } else {
                    plan.agents.push(reader.clone());
                }
            }
        }
    }
    plan
}

/// The plan of `event` delivered to named channels only, such as an event a
/// hook injects (RFC §9.4): each named transport channel bound to the room
/// that reads, other than the event's own source (RFC §7.5 rule 5), in
/// channel id order; `None` names no channel.
///
/// A named intelligence channel is not asked: an injected event asks no
/// agent (gap G-021). Its visibility names the agent, which reads it in its
/// history at its next turn.
pub fn plan_targets(
    event: &RoomEvent,
    targets: Option<&[ChannelId]>,
    bindings: &[ChannelBinding],
) -> DeliveryPlan {
    let mut plan = DeliveryPlan::default();
    let Some(targets) = targets else {
        return plan;
    };
    let mut readers: Vec<&ChannelBinding> = bindings
        .iter()
        .filter(|binding| {
            targets.contains(&binding.channel_id)
                && binding.category == ChannelCategory::Transport
                && binding.access.reads()
                && binding.channel_id != event.source.channel_id
        })
        .collect();
    readers.sort_by(|first, second| first.channel_id.cmp(&second.channel_id));
    plan.transports = readers
        .into_iter()
        .map(|reader| TransportTarget {
            binding: reader.clone(),
            delivers: reader.direction.sends(),
        })
        .collect();
    plan
}

fn solicits(
    event: &RoomEvent,
    agent: &ChannelBinding,
    from_agent: bool,
    policy: AgentResponsePolicy,
) -> bool {
    match &event.addressed_to {
        Some(addressed) => addressed.contains(&agent.channel_id),
        None => match policy {
            AgentResponsePolicy::AgentChain => true,
            AgentResponsePolicy::AddressedOnly => !from_agent,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        Access, ChannelDirection, ChannelId, ChannelType, EventContent, EventDirection, EventId,
        EventSource, EventType, RoomId, Timestamp,
    };

    fn channel(id: &str) -> ChannelId {
        ChannelId::new(id).unwrap()
    }

    fn binding(id: &str, category: ChannelCategory) -> ChannelBinding {
        let channel_type = match category {
            ChannelCategory::Transport => ChannelType::Websocket,
            ChannelCategory::Intelligence => ChannelType::Ai,
        };
        ChannelBinding::new(
            channel(id),
            RoomId::new("room_1").unwrap(),
            channel_type,
            category,
            ChannelDirection::Bidirectional,
            Timestamp::UNIX_EPOCH,
        )
    }

    fn transport(id: &str) -> ChannelBinding {
        binding(id, ChannelCategory::Transport)
    }

    fn agent(id: &str) -> ChannelBinding {
        binding(id, ChannelCategory::Intelligence)
    }

    fn event_from(source: &ChannelBinding) -> RoomEvent {
        RoomEvent::new(
            EventId::new("evt_1").unwrap(),
            RoomId::new("room_1").unwrap(),
            EventType::Message,
            EventSource::new(
                source.channel_id.clone(),
                source.channel_type,
                EventDirection::Inbound,
            ),
            EventContent::text("hello"),
            Timestamp::UNIX_EPOCH,
        )
    }

    fn ids(bindings: &[ChannelBinding]) -> Vec<&str> {
        bindings
            .iter()
            .map(|binding| binding.channel_id.as_str())
            .collect()
    }

    fn transport_ids(plan: &DeliveryPlan) -> Vec<&str> {
        plan.transports
            .iter()
            .map(|target| target.binding.channel_id.as_str())
            .collect()
    }

    mod write_refusal {
        use super::*;
        use pretty_assertions::assert_eq;

        #[test]
        fn a_read_write_unmuted_source_may_write() {
            assert_eq!(write_refusal(&transport("sms")), None);
        }

        #[test]
        fn a_read_only_source_is_refused_as_read_only() {
            let source = ChannelBinding {
                access: Access::ReadOnly,
                ..transport("sms")
            };
            assert_eq!(write_refusal(&source), Some(WriteRefusal::ReadOnly));
        }

        #[test]
        fn a_muted_source_is_refused_as_muted() {
            let source = ChannelBinding {
                muted: true,
                ..transport("sms")
            };
            assert_eq!(
                write_refusal(&source).map(WriteRefusal::blocked_by),
                Some("source_muted")
            );
        }

        #[test]
        fn a_read_only_and_muted_source_is_refused_as_read_only() {
            let source = ChannelBinding {
                access: Access::ReadOnly,
                muted: true,
                ..transport("sms")
            };
            assert_eq!(write_refusal(&source), Some(WriteRefusal::ReadOnly));
        }

        #[test]
        fn a_source_with_no_access_is_refused_as_read_only() {
            let source = ChannelBinding {
                access: Access::None,
                ..transport("sms")
            };
            assert_eq!(write_refusal(&source), Some(WriteRefusal::ReadOnly));
        }
    }

    mod receives {
        use super::*;

        #[test]
        fn a_channel_does_not_receive_its_own_event() {
            let sms = transport("sms");
            assert!(!receives(&sms, &event_from(&sms)));
        }

        #[test]
        fn a_write_only_channel_does_not_receive() {
            let reader = ChannelBinding {
                access: Access::WriteOnly,
                ..transport("ws")
            };
            assert!(!receives(&reader, &event_from(&transport("sms"))));
        }

        #[test]
        fn a_muted_channel_still_receives() {
            let reader = ChannelBinding {
                muted: true,
                ..agent("ai")
            };
            assert!(receives(&reader, &event_from(&transport("sms"))));
        }

        #[test]
        fn a_channel_the_visibility_excludes_does_not_receive() {
            let mut event = event_from(&transport("sms"));
            event.visibility = Visibility::Intelligence;
            assert!(!receives(&transport("ws"), &event));
        }
    }

    mod history_for {
        use super::*;
        use pretty_assertions::assert_eq;

        #[test]
        fn a_reader_has_another_source_s_visible_event() {
            let timeline = [event_from(&transport("sms"))];
            assert_eq!(history_for(&agent("ai"), &timeline).len(), 1);
        }

        #[test]
        fn a_reader_does_not_have_an_event_withheld_from_it() {
            let mut whispered = event_from(&agent("assistant"));
            whispered.visibility = Visibility::channels([channel("ws_advisor")]).unwrap();
            assert_eq!(
                history_for(&agent("ai"), &[whispered]),
                Vec::<&RoomEvent>::new()
            );
        }

        #[test]
        fn a_reader_has_its_own_event_whatever_its_visibility() {
            let mut whispered = event_from(&agent("assistant"));
            whispered.visibility = Visibility::channels([channel("ws_advisor")]).unwrap();
            assert_eq!(history_for(&agent("assistant"), &[whispered]).len(), 1);
        }

        #[test]
        fn a_reader_does_not_have_its_own_refused_event() {
            let mut refused = event_from(&agent("ai"));
            refused.status = EventStatus::Blocked;
            assert_eq!(
                history_for(&agent("ai"), &[refused]),
                Vec::<&RoomEvent>::new()
            );
        }

        #[test]
        fn a_reader_does_not_have_another_source_s_refused_event() {
            let mut refused = event_from(&transport("sms"));
            refused.status = EventStatus::Blocked;
            assert_eq!(
                history_for(&agent("ai"), &[refused]),
                Vec::<&RoomEvent>::new()
            );
        }
    }

    /// RFC §7.5 rule 8, §10.3, gap G-062: a reader's history carries
    /// nothing of what was deleted.
    mod context_for {
        use super::*;
        use crate::model::{DeleteContent, DeleteType, EditContent, EditSource, Room};
        use pretty_assertions::assert_eq;
        use serde_json::json;

        fn id(event_id: &str) -> EventId {
            EventId::new(event_id).unwrap()
        }

        /// The customer's "I need 5000$" on SMS, `evt_1` at index 0, with its
        /// provider's payload.
        fn message() -> RoomEvent {
            let mut event = event_from(&transport("sms"));
            event.content = EventContent::text("I need 5000$");
            event.parent_event_id = Some(id("evt_0"));
            event
                .source
                .raw_payload
                .insert("Body".to_owned(), json!("I need 5000$"));
            event
                .channel_data
                .insert("quoted".to_owned(), json!("I need 5000$"));
            event
        }

        /// `message()` as an accepted delete leaves it.
        fn deleted() -> RoomEvent {
            let mut event = message();
            event
                .metadata
                .insert(edits::DELETED.to_owned(), json!(true));
            event
        }

        /// The edit `edit_id` of `evt_1`, to `text`.
        fn edit(edit_id: &str, text: &str) -> RoomEvent {
            let mut event = event_from(&transport("sms"));
            event.id = id(edit_id);
            event.index = 1;
            event.event_type = EventType::Edit;
            event.content = EventContent::Edit(EditContent {
                target_event_id: id("evt_1"),
                new_content: Box::new(EventContent::text(text)),
                edit_source: Some(EditSource::Sender),
            });
            event
        }

        /// The delete `delete_id` of `evt_1`.
        fn delete(delete_id: &str) -> RoomEvent {
            let mut event = event_from(&transport("sms"));
            event.id = id(delete_id);
            event.index = 2;
            event.event_type = EventType::Delete;
            event.content = EventContent::Delete(DeleteContent {
                target_event_id: id("evt_1"),
                delete_type: DeleteType::Sender,
                reason: Some("typo".to_owned()),
            });
            event
        }

        fn context(recent_events: Vec<RoomEvent>) -> RoomContext {
            RoomContext {
                recent_events,
                ..RoomContext::new(Room::new(
                    RoomId::new("room_1").unwrap(),
                    Timestamp::UNIX_EPOCH,
                ))
            }
        }

        /// The history `ai` is handed of `timeline`.
        fn history(timeline: Vec<RoomEvent>) -> Vec<RoomEvent> {
            context_for(&agent("ai"), &context(timeline)).recent_events
        }

        #[test]
        fn a_deleted_event_reaches_a_reader_with_its_content_withheld() {
            assert_eq!(history(vec![deleted()])[0].content, EventContent::text(""));
        }

        /// The event keeps its place: the delete that follows names it, and
        /// the reader tells it from an event that said nothing.
        #[test]
        fn a_deleted_event_keeps_its_id_index_thread_and_mark() {
            let handed = history(vec![deleted()]).remove(0);
            assert_eq!(
                (
                    edits::is_deleted(&handed),
                    handed.id,
                    handed.index,
                    handed.parent_event_id
                ),
                (true, id("evt_1"), 0, Some(id("evt_0")))
            );
        }

        /// The provider's original payload holds the text.
        #[test]
        fn a_deleted_event_reaches_a_reader_without_its_provider_s_payload() {
            let handed = history(vec![deleted()]).remove(0);
            assert_eq!(handed.source.raw_payload, Metadata::new());
        }

        /// What a channel needs to delete it natively, and the integrator's
        /// metadata, stay.
        #[test]
        fn a_deleted_event_keeps_its_provider_data_and_metadata() {
            let mut marked = deleted();
            marked.metadata.insert("crm".to_owned(), json!("c_1"));
            marked.source.provider_message_id = Some("SM1".to_owned());
            let handed = history(vec![marked.clone()]).remove(0);
            assert_eq!(
                (
                    handed.source.provider_message_id,
                    handed.channel_data,
                    handed.metadata
                ),
                (Some("SM1".to_owned()), marked.channel_data, marked.metadata)
            );
        }

        #[test]
        fn an_event_not_deleted_reaches_a_reader_whole() {
            assert_eq!(history(vec![message()]), [message()]);
        }

        #[test]
        fn the_delete_itself_reaches_a_reader_whole() {
            assert_eq!(
                history(vec![deleted(), delete("evt_3")])[1],
                delete("evt_3")
            );
        }

        #[test]
        fn an_edit_of_a_live_event_keeps_its_new_content() {
            assert_eq!(
                history(vec![message(), edit("evt_2", "I need 50000$")])[1],
                edit("evt_2", "I need 50000$")
            );
        }

        /// The edit held the deleted event's corrected text.
        #[test]
        fn an_edit_of_a_deleted_event_reaches_a_reader_with_its_new_content_withheld() {
            let handed = history(vec![deleted(), edit("evt_2", "I need 50000$")]).remove(1);
            assert_eq!(handed.content, edit("evt_2", "").content);
        }

        #[test]
        fn an_edit_of_a_deleted_event_reaches_a_reader_without_its_provider_s_payload() {
            let mut edited = edit("evt_2", "I need 50000$");
            edited
                .source
                .raw_payload
                .insert("Body".to_owned(), json!("I need 50000$"));
            let handed = history(vec![deleted(), edited]).remove(1);
            assert_eq!(handed.source.raw_payload, Metadata::new());
        }

        /// The target lies before the window; the delete in it tells.
        #[test]
        fn an_edit_whose_target_a_delete_in_the_window_removed_is_withheld() {
            let handed = history(vec![edit("evt_2", "I need 50000$"), delete("evt_3")]).remove(0);
            assert_eq!(handed.content, edit("evt_2", "").content);
        }

        /// The stored mark decides for an event the window holds: a host's
        /// direct update that removed it gives the event back.
        #[test]
        fn an_event_whose_mark_was_removed_reaches_a_reader_whole() {
            assert_eq!(history(vec![message(), delete("evt_3")])[0], message());
        }

        /// A blocked delete changed nothing.
        #[test]
        fn a_blocked_delete_withholds_nothing() {
            let mut blocked = delete("evt_3");
            blocked.status = EventStatus::Blocked;
            let edited = edit("evt_2", "I need 50000$");
            assert_eq!(history(vec![edited.clone(), blocked]), [edited]);
        }
    }

    mod effective_visibility {
        use super::*;
        use pretty_assertions::assert_eq;

        #[test]
        fn the_source_binding_narrows_the_requested_visibility() {
            let source = ChannelBinding {
                visibility: Visibility::channels([channel("ws_advisor")]).unwrap(),
                ..agent("assistant")
            };
            let bindings = [source.clone(), transport("ws_advisor"), transport("sms")];
            assert_eq!(
                effective_visibility(&Visibility::Transport, &source, &bindings).to_string(),
                "ws_advisor"
            );
        }
    }

    mod plan_targets {
        use super::*;
        use pretty_assertions::assert_eq;

        /// An event from the unbound `system` source, at `depth`.
        fn injected(depth: u32) -> RoomEvent {
            RoomEvent {
                chain_depth: depth,
                ..event_from(&transport("system"))
            }
        }

        fn plan(
            event: &RoomEvent,
            targets: &[ChannelId],
            bindings: &[ChannelBinding],
        ) -> DeliveryPlan {
            plan_targets(event, Some(targets), bindings)
        }

        #[test]
        fn reaches_the_named_channels_only() {
            let bindings = [transport("sms"), transport("ws"), agent("ai")];
            let plan = plan(&injected(0), &[channel("ws")], &bindings);
            assert_eq!(transport_ids(&plan), ["ws"]);
        }

        #[test]
        fn reaches_nobody_when_no_channel_is_named() {
            let bindings = [transport("sms")];
            assert_eq!(
                plan_targets(&injected(0), None, &bindings),
                DeliveryPlan::default()
            );
        }

        #[test]
        fn skips_a_named_channel_that_does_not_read() {
            let write_only = ChannelBinding {
                access: Access::WriteOnly,
                ..transport("ws")
            };
            let plan = plan(&injected(0), &[channel("ws")], &[write_only]);
            assert_eq!(transport_ids(&plan), Vec::<&str>::new());
        }

        #[test]
        fn skips_the_event_s_own_source() {
            let sms = transport("sms");
            let plan = plan(&event_from(&sms), &[channel("sms")], &[sms]);
            assert_eq!(transport_ids(&plan), Vec::<&str>::new());
        }

        #[test]
        fn asks_no_named_intelligence_channel() {
            let plan = plan(&injected(0), &[channel("ai")], &[agent("ai")]);
            assert_eq!(plan, DeliveryPlan::default());
        }

        #[test]
        fn reaches_a_named_transport_beside_a_named_agent() {
            let bindings = [agent("ai"), transport("ws")];
            let plan = plan(&injected(0), &[channel("ai"), channel("ws")], &bindings);
            assert_eq!(
                (transport_ids(&plan), ids(&plan.agents)),
                (vec!["ws"], vec![])
            );
        }

        #[test]
        fn reaches_a_named_transport_at_the_limit() {
            let plan = plan(&injected(4), &[channel("ws")], &[transport("ws")]);
            assert_eq!(transport_ids(&plan), ["ws"]);
        }
    }

    mod plan_delivery {
        use super::*;
        use pretty_assertions::assert_eq;

        fn plan(event: &RoomEvent, bindings: &[ChannelBinding]) -> DeliveryPlan {
            plan_delivery(event, bindings, AgentResponsePolicy::AgentChain, 5)
        }

        #[test]
        fn transports_are_planned_in_channel_id_order() {
            let bindings = [transport("ws"), transport("sms"), transport("email")];
            let plan = plan(&event_from(&bindings[1]), &bindings);
            assert_eq!(transport_ids(&plan), ["email", "ws"]);
        }

        #[test]
        fn a_transport_that_cannot_send_is_told_but_not_delivered_to() {
            let inbound_only = ChannelBinding {
                direction: ChannelDirection::Inbound,
                ..transport("kiosk")
            };
            let bindings = [transport("sms"), inbound_only];
            let plan = plan(&event_from(&bindings[0]), &bindings);
            assert!(!plan.transports[0].delivers);
        }

        #[test]
        fn an_outbound_transport_is_delivered_to() {
            let outbound_only = ChannelBinding {
                direction: ChannelDirection::Outbound,
                ..transport("push")
            };
            let bindings = [transport("sms"), outbound_only];
            let plan = plan(&event_from(&bindings[0]), &bindings);
            assert!(plan.transports[0].delivers);
        }

        #[test]
        fn an_unaddressed_event_solicits_every_receiving_agent() {
            let bindings = [transport("sms"), agent("billing"), agent("triage")];
            let plan = plan(&event_from(&bindings[0]), &bindings);
            assert_eq!(ids(&plan.agents), ["billing", "triage"]);
        }

        #[test]
        fn an_addressed_event_solicits_only_the_named_agents() {
            let bindings = [transport("sms"), agent("billing"), agent("triage")];
            let mut event = event_from(&bindings[0]);
            event.addressed_to = Some(vec![channel("triage")]);
            assert_eq!(ids(&plan(&event, &bindings).agents), ["triage"]);
        }

        #[test]
        fn an_event_addressed_to_nobody_solicits_no_agent() {
            let bindings = [transport("sms"), agent("billing")];
            let mut event = event_from(&bindings[0]);
            event.addressed_to = Some(Vec::new());
            assert_eq!(ids(&plan(&event, &bindings).agents), Vec::<&str>::new());
        }

        #[test]
        fn addressing_an_agent_does_not_narrow_transport_delivery() {
            let bindings = [transport("sms"), transport("ws"), agent("billing")];
            let mut event = event_from(&bindings[0]);
            event.addressed_to = Some(vec![channel("billing")]);
            assert_eq!(transport_ids(&plan(&event, &bindings)), ["ws"]);
        }

        #[test]
        fn addressing_does_not_reach_an_agent_the_visibility_excludes() {
            let bindings = [transport("sms"), agent("billing")];
            let mut event = event_from(&bindings[0]);
            event.addressed_to = Some(vec![channel("billing")]);
            event.visibility = Visibility::Transport;
            assert_eq!(ids(&plan(&event, &bindings).agents), Vec::<&str>::new());
        }

        #[test]
        fn an_agent_s_output_solicits_the_other_agents_under_agent_chain() {
            let bindings = [agent("analyst"), agent("writer")];
            let plan = plan(&event_from(&bindings[0]), &bindings);
            assert_eq!(ids(&plan.agents), ["writer"]);
        }

        #[test]
        fn an_agent_s_unaddressed_output_solicits_no_agent_under_addressed_only() {
            let bindings = [agent("analyst"), agent("writer")];
            let plan = plan_delivery(
                &event_from(&bindings[0]),
                &bindings,
                AgentResponsePolicy::AddressedOnly,
                5,
            );
            assert_eq!(ids(&plan.agents), Vec::<&str>::new());
        }

        #[test]
        fn an_agent_s_addressed_output_solicits_the_named_agent_under_addressed_only() {
            let bindings = [agent("analyst"), agent("writer")];
            let mut event = event_from(&bindings[0]);
            event.addressed_to = Some(vec![channel("writer")]);
            let plan = plan_delivery(&event, &bindings, AgentResponsePolicy::AddressedOnly, 5);
            assert_eq!(ids(&plan.agents), ["writer"]);
        }

        #[test]
        fn a_person_s_message_solicits_agents_under_addressed_only() {
            let bindings = [transport("sms"), agent("writer")];
            let plan = plan_delivery(
                &event_from(&bindings[0]),
                &bindings,
                AgentResponsePolicy::AddressedOnly,
                5,
            );
            assert_eq!(ids(&plan.agents), ["writer"]);
        }

        #[test]
        fn an_agent_whose_response_would_reach_the_limit_is_not_asked() {
            let bindings = [agent("analyst"), agent("writer")];
            let mut event = event_from(&bindings[0]);
            event.chain_depth = 4;
            let plan = plan(&event, &bindings);
            assert_eq!(
                (ids(&plan.agents), ids(&plan.not_asked)),
                (vec![], vec!["writer"])
            );
        }

        #[test]
        fn an_agent_below_the_limit_is_asked() {
            let bindings = [agent("analyst"), agent("writer")];
            let mut event = event_from(&bindings[0]);
            event.chain_depth = 3;
            assert_eq!(ids(&plan(&event, &bindings).agents), ["writer"]);
        }

        #[test]
        fn transports_are_delivered_to_at_the_limit() {
            let bindings = [agent("analyst"), transport("ws")];
            let mut event = event_from(&bindings[0]);
            event.chain_depth = 4;
            assert_eq!(transport_ids(&plan(&event, &bindings)), ["ws"]);
        }
    }
}
