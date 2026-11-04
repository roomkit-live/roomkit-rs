//! Who reads, who writes, and whom a broadcast reaches (RFC §7.5, §10.2,
//! §19.3).

use std::collections::BTreeSet;

use crate::model::{
    AgentResponsePolicy, ChannelBinding, ChannelCategory, ChannelId, EventContent, EventId,
    EventStatus, EventType, Metadata, RoomContext, RoomEvent, TextContent, Visibility,
};
use crate::rules::chain_depth;
use crate::rules::discussion;
use crate::rules::edits;

/// Why a source's event may not be broadcast (RFC §7.5 rule 2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum WriteRefusal {
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
    pub(crate) fn blocked_by(self) -> &'static str {
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
pub(crate) fn write_refusal(source: &ChannelBinding) -> Option<WriteRefusal> {
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
pub(crate) fn receives(reader: &ChannelBinding, event: &RoomEvent) -> bool {
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
pub(crate) fn history_for<'timeline>(
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
pub(crate) fn context_for(reader: &ChannelBinding, whole: &RoomContext) -> RoomContext {
    let deleted = deleted_events(&whole.recent_events);
    RoomContext {
        room: whole.room.clone(),
        bindings: whole.bindings.clone(),
        participants: whole.participants.clone(),
        recent_events: history_for(reader, &whole.recent_events)
            .into_iter()
            .map(|event| withheld(event, &deleted))
            .collect(),
        // The labels of the turns it reads take their ranks from it (RFC
        // §6.4, gap G-089).
        author_register: whole.author_register.clone(),
    }
}

/// Whether an event of `visibility` reaches the channel `target_binding`
/// binds (RFC §7.3), the Python implementation's `visibility_allows`:
/// [`Visibility::admits`] for the binding's channel and category.
pub fn visibility_allows(visibility: &Visibility, target_binding: &ChannelBinding) -> bool {
    visibility.admits(&target_binding.channel_id, target_binding.category)
}

/// `context.recent_events` as the channel `channel_id` may know them (RFC
/// §7.5 rule 8), the Python implementation's `visible_events`: what a
/// caller rebuilding a channel's history by itself hands it, a timeline
/// page set as the context's `recent_events` among them.
///
/// The channel keeps its own accepted events, and another source's whose
/// visibility reaches it ([`visibility_allows`]), the visibility stamped at
/// the event's commit. An event stored `BLOCKED` reaches nobody, its own source
/// included. A deleted event, and an edit of one, stay with their content
/// withheld (RFC §10.3, gap G-062). A channel the context binds no binding
/// to is handed none.
///
/// It answers the visibility half of §7.5 rule 1 only: as in the Python
/// implementation, a caller rebuilding history by itself checks the
/// binding's access, which the pipeline checks where it delivers.
pub fn visible_events(context: &RoomContext, channel_id: &ChannelId) -> Vec<RoomEvent> {
    let Some(reader) = context
        .bindings
        .iter()
        .find(|binding| &binding.channel_id == channel_id)
    else {
        return Vec::new();
    };
    // The deleted marks are read from the received events alone, as
    // Python's `withheld(received_events(...))` reads them (RMK-1102).
    let received: Vec<&RoomEvent> = context
        .recent_events
        .iter()
        .filter(|event| event.status != EventStatus::Blocked)
        .collect();
    let deleted = deleted_events(received.iter().copied());
    received
        .into_iter()
        .filter(|event| {
            &event.source.channel_id == channel_id || visibility_allows(&event.visibility, reader)
        })
        .map(|event| withheld(event, &deleted))
        .collect()
}

/// The events `timeline` shows deleted (RFC §10.3), as the Python
/// implementation's `withheld` reads them: those carrying the mark an
/// accepted delete's commit leaves on its target and on each edit of it,
/// which a host's direct update may have removed since. A DELETE row proves
/// nothing by itself: one committed without step 8a (a host's
/// `commit_event`) was never checked.
fn deleted_events<'timeline>(
    timeline: impl IntoIterator<Item = &'timeline RoomEvent>,
) -> BTreeSet<&'timeline EventId> {
    timeline
        .into_iter()
        .filter(|event| edits::is_deleted(event))
        .map(|event| &event.id)
        .collect()
}

/// `event` as a reader is handed it: whole, unless it was deleted or is an
/// edit of an event that was (gap G-062). What was said is then withheld: a
/// deleted event's content, or an edit's new content, the edit marked
/// deleted itself or not, becomes an empty text, and the provider's
/// original payload, which holds the text, is left out
/// (`source.raw_payload`). The rest stays: the id, index, type, source
/// with the provider's message id, thread, metadata with `metadata.deleted`,
/// and the provider's structured data (`channel_data`), so the reader tells
/// a deleted event from an empty one, the delete that follows names an
/// event the reader holds, and a channel deletes it natively.
fn withheld(event: &RoomEvent, deleted: &BTreeSet<&EventId>) -> RoomEvent {
    let mut handed = event.clone();
    let marked = deleted.contains(&event.id);
    if let EventContent::Edit(edit) = &mut handed.content
        && (marked || deleted.contains(&edit.target_event_id))
    {
        // An edit keeps its form, marked deleted with its target or not:
        // what it said is its new content.
        *edit.new_content = TextContent::new("").into();
    } else if marked {
        handed.content = TextContent::new("").into();
    } else {
        return handed;
    }
    handed.source.raw_payload = Metadata::new();
    handed
}

/// The visibility an event is committed with: what its sender asked,
/// narrowed to what its source binding allows (gap G-009).
pub(crate) fn effective_visibility(
    requested: &Visibility,
    source: &ChannelBinding,
    bindings: &[ChannelBinding],
) -> Visibility {
    requested.intersect(&source.visibility, category_in(bindings))
}

/// The visibility of a response to a message whose answers may go to
/// `within`, the message's `response_visibility` (RFC §5.12, gap G-073):
/// what the response's channel gave it, `given`, narrowed to `within`, a
/// listed channel's category read from the room's `bindings`. It is never
/// wider than either; its commit narrows it once more, to its source
/// binding's ([`effective_visibility`]).
pub(crate) fn answer_visibility(
    given: &Visibility,
    within: &Visibility,
    bindings: &[ChannelBinding],
) -> Visibility {
    given.intersect(within, category_in(bindings))
}

/// Where the answers to an event may go within both `first` and `second`,
/// two of its scopes (RFC §5.12, §7.3, §9.4, gap G-073): their
/// intersection, a listed channel's category read from the room's
/// `bindings`; `None`, no restriction, on either side giving the other's.
/// `internal`, which only the framework writes, reads `none` on either
/// side, so that a scope never carries it: so a response carries its
/// cascade's scope within its channel's, and a hook's modify narrows an
/// event's, never widening or clearing it.
pub(crate) fn narrower_scope(
    first: Option<&Visibility>,
    second: Option<&Visibility>,
    bindings: &[ChannelBinding],
) -> Option<Visibility> {
    match (first.map(as_scope), second.map(as_scope)) {
        (None, None) => None,
        (Some(scope), None) | (None, Some(scope)) => Some(scope),
        (Some(first), Some(second)) => Some(first.intersect(&second, category_in(bindings))),
    }
}

/// `scope` as a response scope holds it: `internal` reads `none`.
fn as_scope(scope: &Visibility) -> Visibility {
    if *scope == Visibility::Internal {
        Visibility::None
    } else {
        scope.clone()
    }
}

/// The category of a channel the room binds among `bindings`, which a
/// category intersected with a list reads (RFC §7.3).
fn category_in(
    bindings: &[ChannelBinding],
) -> impl Fn(&ChannelId) -> Option<ChannelCategory> + Copy {
    |channel| {
        bindings
            .iter()
            .find(|binding| &binding.channel_id == channel)
            .map(|binding| binding.category)
    }
}

/// Whom the broadcast of one event reaches (RFC §10.2 steps 2 and 3c).
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct DeliveryPlan {
    /// The transport channels that receive the event, in channel id order.
    pub transports: Vec<TransportTarget>,
    /// The intelligence channels asked to act on the event, a turn each.
    pub agents: Vec<ChannelBinding>,
    /// The intelligence channels the event solicits but that are not asked,
    /// because their response would reach the chain-depth limit (RFC §8.3).
    pub not_asked: Vec<ChannelBinding>,
    /// In a room a discussion holds: the intelligence channels the event
    /// reaches, which it asks later, one turn at a time (RFC §19.7.5). None
    /// of them is unavailable.
    pub deferred: Vec<ChannelBinding>,
}

/// A transport channel a broadcast reaches.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TransportTarget {
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
/// asked (RFC §8.3), and is recorded as not asked unless the event opens
/// no turn (RFC §6.4), which no agent answers.
pub(crate) fn plan_delivery(
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
                // A tool-call row is an activity record no agent reads.
                if is_tool_call_row(event)
                    || orchestration_internal(event)
                    || !solicits(event, reader, from_agent, policy)
                {
                    continue;
                }
                if !at_limit {
                    plan.agents.push(reader.clone());
                } else if !edits::opens_no_turn(event) {
                    // Only an event a turn would answer is recorded as not
                    // asked: a notice or a change, which no agent answers,
                    // gets no record at the limit (RFC §6.4, §8.3).
                    plan.not_asked.push(reader.clone());
                }
            }
        }
    }
    plan
}

/// [`plan_delivery`] for an event of the room of `context`: in a room a
/// discussion holds, the transports it reaches, and no agent asked, however
/// the event addresses them and at any depth (RFC §19.7.5).
pub(crate) fn plan_in_room(
    event: &RoomEvent,
    context: &RoomContext,
    max_chain_depth: u32,
) -> DeliveryPlan {
    let bindings = &context.bindings;
    if !discussion::holds(&context.room.metadata) {
        let policy = context.room.agent_response_policy;
        return plan_delivery(event, bindings, policy, max_chain_depth);
    }
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
            ChannelCategory::Intelligence => plan.deferred.push(reader.clone()),
        }
    }
    plan
}

/// The channels `plan` reaches, whether it asks them now or not: what a
/// discussion's queue never reaches past (RFC §19.7.5, §10.1 step 12).
pub(crate) fn reach(plan: &DeliveryPlan) -> Vec<ChannelId> {
    let transports = plan.transports.iter().map(|target| &target.binding);
    transports
        .chain(&plan.agents)
        .chain(&plan.not_asked)
        .chain(&plan.deferred)
        .map(|binding| binding.channel_id.clone())
        .collect()
}

/// `answered`, as a discussion's turn hands it: within what its source
/// binding lets through (RFC §7.3). `None` when the room no longer binds its
/// source, or that binding cannot write: the turn is not given (RFC §19.7.5).
pub(crate) fn turn_event(answered: &RoomEvent, context: &RoomContext) -> Option<RoomEvent> {
    let source = context.get_binding(&answered.source.channel_id)?;
    if write_refusal(source).is_some() {
        return None;
    }
    let mut event = answered.clone();
    event.visibility = effective_visibility(&answered.visibility, source, &context.bindings);
    Some(event)
}

/// A discussion's turn on `event`: `agent` alone, against the discussion's
/// own depth limit `max_depth`, never a transport (RFC §19.7.5 rules 9 and
/// 11). `None` when the agent cannot read the event. Past the limit it is
/// not asked, and recorded unless the event opens no turn (§6.4, §8.3).
#[must_use]
pub(crate) fn plan_turn(
    event: &RoomEvent,
    bindings: &[ChannelBinding],
    agent: &ChannelId,
    max_depth: u32,
) -> Option<DeliveryPlan> {
    let binding = bindings
        .iter()
        .find(|binding| &binding.channel_id == agent && receives(binding, event))?;
    let mut plan = DeliveryPlan::default();
    if !chain_depth::reaches_limit(event.chain_depth, max_depth) {
        plan.agents.push(binding.clone());
    } else if !edits::opens_no_turn(event) {
        plan.not_asked.push(binding.clone());
    }
    Some(plan)
}

/// Whether `event` is a tool call's start or end row.
fn is_tool_call_row(event: &RoomEvent) -> bool {
    matches!(
        event.r#type,
        EventType::ToolCallStart | EventType::ToolCallEnd
    )
}

/// The visibility of an event a hook injected for `targets` (RFC §9.4, gap
/// G-031): the targets the room binds, among its `bindings`; `none` when it
/// binds none of them, or when the event is stored only (`None`).
pub(crate) fn targets_visibility(
    targets: Option<&[ChannelId]>,
    bindings: &[ChannelBinding],
) -> Visibility {
    let bound = targets
        .unwrap_or_default()
        .iter()
        .filter(|target| {
            bindings
                .iter()
                .any(|binding| &binding.channel_id == *target)
        })
        .cloned();
    Visibility::channels(bound).unwrap_or(Visibility::None)
}

/// The visibility an event a hook injected for `targets` is committed with
/// (RFC §9.4, gap G-031): `requested`, what the hooks of its own pass left,
/// within `given`, the visibility they were given, and within the targets
/// the room binds now, among its `bindings`. A hook may narrow it, never
/// widen it.
pub(crate) fn injected_visibility(
    requested: &Visibility,
    given: &Visibility,
    targets: Option<&[ChannelId]>,
    bindings: &[ChannelBinding],
) -> Visibility {
    let category_of = category_in(bindings);
    requested
        .intersect(given, category_of)
        .intersect(&targets_visibility(targets, bindings), category_of)
}

/// The plan of `event` delivered to named channels only, such as an event a
/// hook injects (RFC §9.4): each named transport channel bound to the room
/// that reads and that the event's visibility admits, other than the event's
/// own source (RFC §7.5 rule 5), in channel id order; `None` names no
/// channel.
///
/// A named intelligence channel is not asked: an injected event asks no
/// agent (gap G-021). Its visibility names the agent, which reads it in its
/// history at its next turn.
pub(crate) fn plan_targets(
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
                && receives(binding, event)
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

/// Whether `event` asks `agent` to act (RFC §19.3, §19.4). The address
/// decides first, and a conversation router's supervisor, which the event's
/// `_always_process` lists, is asked on top of it (§19.4 step 4); without
/// an address, an agent's output asks no agent under `AddressedOnly` but the
/// supervisor; otherwise the router's `_routed_to` names the one agent asked
/// beside the supervisor, and with no stamp every agent is asked.
fn solicits(
    event: &RoomEvent,
    agent: &ChannelBinding,
    from_agent: bool,
    policy: AgentResponsePolicy,
) -> bool {
    let always = always_processes(event, &agent.channel_id);
    if let Some(addressed) = &event.addressed_to {
        return addressed.contains(&agent.channel_id) || always;
    }
    if from_agent && policy == AgentResponsePolicy::AddressedOnly {
        return always;
    }
    match event.metadata.get(ROUTED_TO) {
        None | Some(serde_json::Value::Null) => true,
        Some(routed) => routed.as_str() == Some(agent.channel_id.as_str()) || always,
    }
}

/// The metadata key marking an event as the orchestration's own record, a
/// handoff's, which asks no agent (RFC §19.6). Only the runtime and code the
/// host runs write it: a sender who wrote it into its message would silence
/// the room's agents, so it is dropped at the door (RFC §10.1).
pub(crate) const ORCHESTRATION_INTERNAL: &str = "_orchestration_internal";

/// Whether `event` is the orchestration's own record: Python's
/// `bool(metadata.get(...))`.
fn orchestration_internal(event: &RoomEvent) -> bool {
    match event.metadata.get(ORCHESTRATION_INTERNAL) {
        None | Some(serde_json::Value::Null | serde_json::Value::Bool(false)) => false,
        Some(serde_json::Value::Number(number)) => number.as_f64() != Some(0.0),
        Some(serde_json::Value::String(text)) => !text.is_empty(),
        Some(serde_json::Value::Array(items)) => !items.is_empty(),
        Some(serde_json::Value::Object(members)) => !members.is_empty(),
        Some(serde_json::Value::Bool(true)) => true,
    }
}

/// The metadata key a conversation router stamps the agent it selected
/// under (RFC §19.4).
const ROUTED_TO: &str = "_routed_to";

/// The metadata key a conversation router lists the agents always asked
/// under, its supervisor (RFC §19.4 step 4).
const ALWAYS_PROCESS: &str = "_always_process";

/// Removes from `event`'s metadata the routing stamps it carries, and names
/// them: only a hook writes them, the router among hooks, so an event's
/// stamps are dropped before its `BEFORE_BROADCAST` hooks run (RFC §19.4),
/// or its sender would choose the agent that answers it.
pub(crate) fn strip_routing_stamps(event: &mut RoomEvent) -> Vec<&'static str> {
    [ROUTED_TO, ALWAYS_PROCESS]
        .into_iter()
        .filter(|key| event.metadata.shift_remove(*key).is_some())
        .collect()
}

/// Whether `event`'s `_always_process` lists `channel_id`.
fn always_processes(event: &RoomEvent, channel_id: &ChannelId) -> bool {
    event
        .metadata
        .get(ALWAYS_PROCESS)
        .and_then(serde_json::Value::as_array)
        .is_some_and(|listed| {
            listed
                .iter()
                .any(|id| id.as_str() == Some(channel_id.as_str()))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        Access, ChannelDirection, ChannelId, ChannelType, EventContent, EventId, EventSource,
        EventType, RoomId, Timestamp,
    };

    fn channel(id: &str) -> ChannelId {
        ChannelId::new(id).unwrap()
    }

    fn binding(id: &str, category: ChannelCategory) -> ChannelBinding {
        let channel_type = match category {
            ChannelCategory::Transport => ChannelType::WEBSOCKET,
            ChannelCategory::Intelligence => ChannelType::AI,
        };
        ChannelBinding::at(
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
        RoomEvent::at(
            EventId::new("evt_1").unwrap(),
            RoomId::new("room_1").unwrap(),
            EventType::Message,
            EventSource::new(source.channel_id.clone(), source.channel_type.clone()),
            TextContent::new("hello"),
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
            event.content = TextContent::new("I need 5000$").into();
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
            event.r#type = EventType::Edit;
            event.content = EventContent::Edit(EditContent {
                target_event_id: id("evt_1"),
                new_content: Box::new(TextContent::new(text).into()),
                edit_source: Some(EditSource::Sender),
            });
            event
        }

        /// The delete `delete_id` of `evt_1`.
        fn delete(delete_id: &str) -> RoomEvent {
            let mut event = event_from(&transport("sms"));
            event.id = id(delete_id);
            event.index = 2;
            event.r#type = EventType::Delete;
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
                ..RoomContext::new(Room::at(
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
            assert_eq!(
                history(vec![deleted()])[0].content,
                TextContent::new("").into()
            );
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

        /// An edit the store marked deleted with its target, as both
        /// implementations' stores do, keeps its edit form, its new
        /// content withheld, whether or not the window holds the target.
        #[test]
        fn an_edit_marked_deleted_keeps_its_form_with_its_new_content_withheld() {
            let mut marked = edit("evt_2", "I need 50000$");
            marked.metadata.insert("deleted".to_owned(), json!(true));
            let handed = [
                history(vec![marked.clone()]),
                history(vec![deleted(), marked]),
            ]
            .map(|mut history| history.pop().unwrap().content);
            assert_eq!(
                handed,
                [edit("evt_2", "").content, edit("evt_2", "").content]
            );
        }

        /// RFC §10.3, Python's `withheld`: the marks decide, never a DELETE
        /// row, which a host commits unchecked: an edit of an event before
        /// the window, unmarked, stays whole beside a delete of it.
        #[test]
        fn visible_events_reads_the_marks_only() {
            let edited = edit("evt_2", "I need 50000$");
            assert_eq!(history(vec![edited.clone(), delete("evt_3")])[0], edited);
        }

        /// The stored mark decides for an event the window holds: a host's
        /// direct update that removed it gives the event back.
        #[test]
        fn an_event_whose_mark_was_removed_reaches_a_reader_whole() {
            assert_eq!(history(vec![message(), delete("evt_3")])[0], message());
        }
    }

    /// RFC §7.5 rule 8, the Python implementation's `visible_events`: a
    /// context's events as a channel may know them, a timeline page set as
    /// its `recent_events` among them.
    mod visible_events {
        use super::*;
        use crate::model::{EditContent, EditSource, Room};
        use pretty_assertions::assert_eq;
        use serde_json::json;

        /// The event `id` from `source`, saying `text`, of `visibility`.
        fn said(
            id: &str,
            source: &ChannelBinding,
            text: &str,
            visibility: Visibility,
        ) -> RoomEvent {
            let mut event = event_from(source);
            event.id = EventId::new(id).unwrap();
            event.content = TextContent::new(text).into();
            event.visibility = visibility;
            event
        }

        fn context(bindings: Vec<ChannelBinding>, recent_events: Vec<RoomEvent>) -> RoomContext {
            RoomContext {
                bindings,
                recent_events,
                ..RoomContext::new(Room::at(
                    RoomId::new("room_1").unwrap(),
                    Timestamp::UNIX_EPOCH,
                ))
            }
        }

        /// The texts of what `reader`, bound as `binding`, knows of `events`.
        fn known(
            binding: ChannelBinding,
            others: &[ChannelBinding],
            events: Vec<RoomEvent>,
        ) -> Vec<String> {
            let reader = binding.channel_id.clone();
            let mut bindings = vec![binding];
            bindings.extend(others.iter().cloned());
            visible_events(&context(bindings, events), &reader)
                .iter()
                .map(|event| crate::rules::transcoding::text_of(&event.content))
                .collect()
        }

        /// The events `ws` reads: its own, whispered to an advisor; one to
        /// every channel; one to agents only; one listing it; a refused one;
        /// a deleted one.
        fn page() -> Vec<RoomEvent> {
            let (ws, sms, ai) = (transport("ws"), transport("sms"), agent("ai"));
            let listing_ws = Visibility::channels([channel("ws")]).unwrap();
            let advisor = Visibility::channels([channel("advisor")]).unwrap();
            let mut refused = said("evt_5", &sms, "refused", Visibility::All);
            refused.status = EventStatus::Blocked;
            let mut deleted = said("evt_6", &sms, "deleted", Visibility::All);
            deleted
                .metadata
                .insert(edits::DELETED.to_owned(), json!(true));
            vec![
                said("evt_1", &ws, "own", advisor),
                said("evt_2", &sms, "to all", Visibility::All),
                said("evt_3", &ai, "to agents", Visibility::Intelligence),
                said("evt_4", &sms, "to ws", listing_ws),
                refused,
                deleted,
            ]
        }

        #[test]
        fn a_channel_knows_its_own_events_and_those_their_visibility_admits() {
            assert_eq!(
                known(transport("ws"), &[transport("sms"), agent("ai")], page()),
                ["own", "to all", "to ws", ""]
            );
        }

        /// RMK-1102: a refused row marked deleted marks nothing, its edit
        /// handed whole, as Python's `visible_events` reads the marks of
        /// the received events alone (`received_events` first).
        #[test]
        fn a_refused_row_marked_deleted_withholds_no_edit_of_it() {
            let sms = transport("sms");
            let mut refused = said("evt_1", &sms, "refused", Visibility::All);
            refused.status = EventStatus::Blocked;
            refused
                .metadata
                .insert(edits::DELETED.to_owned(), json!(true));
            let mut edit = said("evt_2", &sms, "", Visibility::All);
            edit.r#type = EventType::Edit;
            edit.content = EventContent::Edit(EditContent {
                target_event_id: EventId::new("evt_1").unwrap(),
                new_content: Box::new(TextContent::new("corrected").into()),
                edit_source: Some(EditSource::Sender),
            });
            assert_eq!(
                known(transport("ws"), &[sms], vec![refused, edit]),
                ["Correction: corrected"]
            );
        }

        #[test]
        fn a_channel_the_room_does_not_bind_knows_none_of_its_events() {
            assert_eq!(
                visible_events(&context(vec![transport("sms")], page()), &channel("ws")),
                Vec::<RoomEvent>::new()
            );
        }

        /// The Python implementation's `visibility_allows` reads the
        /// binding's channel and category; `admits` answers for them.
        #[test]
        fn a_visibility_reaches_a_binding_by_its_channel_and_category() {
            let listed = Visibility::channels([channel("ws")]).unwrap();
            assert_eq!(
                [
                    visibility_allows(&Visibility::Transport, &transport("ws")),
                    visibility_allows(&Visibility::Transport, &agent("ai")),
                    visibility_allows(&listed, &transport("ws")),
                    visibility_allows(&listed, &transport("sms")),
                ],
                [true, false, true, false]
            );
        }

        /// As in the Python implementation, the binding's access is the
        /// caller's to check: the pipeline checks it where it delivers.
        #[test]
        fn a_channel_s_access_is_left_to_the_caller() {
            let mut write_only = transport("ws");
            write_only.access = Access::WriteOnly;
            assert_eq!(
                known(write_only, &[transport("sms"), agent("ai")], page()),
                ["own", "to all", "to ws", ""]
            );
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

        /// Gap G-031: a target the event's visibility no longer admits, a
        /// hook of its pass having narrowed it, is not reached.
        #[test]
        fn skips_a_named_channel_its_visibility_does_not_admit() {
            let narrowed = RoomEvent {
                visibility: Visibility::channels([channel("ws")]).unwrap(),
                ..injected(0)
            };
            let bindings = [transport("sms"), transport("ws")];
            let plan = plan(&narrowed, &[channel("sms"), channel("ws")], &bindings);
            assert_eq!(transport_ids(&plan), ["ws"]);
        }
    }

    /// RFC §19.7.5: the plans of a room a discussion holds.
    mod a_discussion_s_plans {
        use super::*;
        use pretty_assertions::assert_eq;
        use serde_json::json;

        use crate::model::{Metadata, Room, RoomId, Timestamp};

        fn held_room(bindings: Vec<ChannelBinding>) -> RoomContext {
            let mut room = Room::at(RoomId::new("r1").unwrap(), Timestamp::UNIX_EPOCH);
            room.metadata = Metadata::from_iter([(
                discussion::CONFIG_KEY.to_owned(),
                json!({"agents": ["a", "b"], "max_depth": 5}),
            )]);
            let mut context = RoomContext::new(room);
            context.bindings = bindings;
            context
        }

        fn ids(bindings: &[ChannelBinding]) -> Vec<&str> {
            bindings.iter().map(|b| b.channel_id.as_str()).collect()
        }

        fn room_bindings() -> Vec<ChannelBinding> {
            vec![
                binding("a", ChannelCategory::Intelligence),
                binding("b", ChannelCategory::Intelligence),
                binding("ops", ChannelCategory::Transport),
                binding("sms", ChannelCategory::Transport),
            ]
        }

        // Python: tests/test_discussion_core.py::test_a_room_a_discussion_holds_asks_no_agent_at_broadcast
        #[test]
        fn test_a_room_a_discussion_holds_asks_no_agent_at_broadcast() {
            let context = held_room(room_bindings());
            let mut event = event_from(&transport("ops"));
            event.addressed_to = Some(vec![channel("a")]);
            // Past the limit too: no record, the discussion decides.
            event.chain_depth = 50;

            let plan = plan_in_room(&event, &context, 5);

            assert_eq!(ids(&plan.agents), Vec::<&str>::new());
            assert_eq!(ids(&plan.not_asked), Vec::<&str>::new());
            assert_eq!(ids(&plan.deferred), vec!["a", "b"]);
            let reached: Vec<&str> = plan
                .transports
                .iter()
                .map(|target| target.binding.channel_id.as_str())
                .collect();
            assert_eq!(reached, vec!["sms"]);
            assert_eq!(
                reach(&plan),
                vec![channel("sms"), channel("a"), channel("b")]
            );
        }

        // Python: tests/test_discussion_core.py::test_without_a_discussion_the_room_answers_as_before
        #[test]
        fn test_without_a_discussion_the_room_answers_as_before() {
            let mut context = held_room(room_bindings());
            context.room.metadata = Metadata::default();

            let plan = plan_in_room(&event_from(&transport("ops")), &context, 5);

            assert_eq!(ids(&plan.agents), vec!["a", "b"]);
            assert_eq!(ids(&plan.deferred), Vec::<&str>::new());
        }

        // Python: tests/test_discussion_core.py::test_a_turn_asks_its_one_agent_against_the_discussions_depth_limit
        #[test]
        fn test_a_turn_asks_its_one_agent_against_the_discussions_depth_limit() {
            let bindings = room_bindings();
            let mut deep = event_from(&transport("ops"));
            deep.chain_depth = 4;

            // The kit's limit (3) would stop it; the discussion's own (10) does not.
            let plan = plan_turn(&deep, &bindings, &channel("a"), 10).unwrap();
            assert_eq!(ids(&plan.agents), vec!["a"]);
            assert_eq!(ids(&plan.not_asked), Vec::<&str>::new());
            assert_eq!(plan.transports, Vec::new());

            deep.chain_depth = 9;
            let plan = plan_turn(&deep, &bindings, &channel("a"), 10).unwrap();
            assert_eq!(ids(&plan.agents), Vec::<&str>::new());
            assert_eq!(ids(&plan.not_asked), vec!["a"]);

            // One it cannot read, or that is the event's own, takes no turn.
            assert!(plan_turn(&deep, &bindings, &channel("ghost"), 10).is_none());
            assert!(plan_turn(&deep, &bindings, &channel("ops"), 10).is_none());
        }

        // Python: core/mixins/discussion.py `_plan_discussion_turn`, what
        // the discussion reads before it takes an entry off its queue.
        #[test]
        fn a_turn_needs_a_source_that_can_write_and_an_agent_that_reads() {
            let event = event_from(&transport("ops"));
            let context = held_room(room_bindings());
            assert!(discussion::takes_turn(&event, &context, &channel("a")));
            assert!(!discussion::takes_turn(&event, &context, &channel("ghost")));

            let mut muted = room_bindings();
            muted[2].muted = true;
            let context = held_room(muted);
            assert!(!discussion::takes_turn(&event, &context, &channel("a")));

            let mut detached = room_bindings();
            detached.remove(2);
            let context = held_room(detached);
            assert!(!discussion::takes_turn(&event, &context, &channel("a")));

            // Within what its source's binding lets through (RFC §7.3).
            let mut narrowed = room_bindings();
            narrowed[2].visibility = Visibility::Transport;
            let context = held_room(narrowed);
            assert!(!discussion::takes_turn(&event, &context, &channel("a")));
        }
    }

    /// RFC §9.4, gap G-031: the visibility of an event a hook injects.
    mod an_injected_event_s_visibility {
        use super::*;
        use pretty_assertions::assert_eq;

        fn listed(ids: &[&str]) -> Visibility {
            Visibility::channels(ids.iter().map(|id| channel(id))).unwrap()
        }

        #[test]
        fn names_the_targets_the_room_binds_only() {
            let bindings = [transport("sms"), transport("ws")];
            let targets = [channel("ws"), channel("desk")];
            assert_eq!(
                targets_visibility(Some(&targets), &bindings),
                listed(&["ws"])
            );
        }

        #[test]
        fn is_none_when_the_room_binds_no_target() {
            let targets = [channel("desk")];
            assert_eq!(
                targets_visibility(Some(&targets), &[transport("sms")]),
                Visibility::None
            );
        }

        #[test]
        fn is_none_when_it_is_stored_only() {
            assert_eq!(
                targets_visibility(None, &[transport("sms")]),
                Visibility::None
            );
        }

        #[test]
        fn may_be_narrowed_by_a_hook_of_its_pass() {
            let bindings = [transport("sms"), transport("ws")];
            let targets = [channel("sms"), channel("ws")];
            let given = listed(&["sms", "ws"]);
            assert_eq!(
                injected_visibility(&listed(&["ws"]), &given, Some(&targets), &bindings),
                listed(&["ws"])
            );
        }

        #[test]
        fn is_never_widened_by_a_hook_of_its_pass() {
            let bindings = [transport("sms"), transport("ws"), agent("ai")];
            let targets = [channel("ws")];
            let given = listed(&["ws"]);
            assert_eq!(
                injected_visibility(&Visibility::All, &given, Some(&targets), &bindings),
                listed(&["ws"])
            );
        }

        #[test]
        fn loses_a_target_detached_since_its_hooks_were_given_it() {
            let targets = [channel("sms"), channel("ws")];
            let given = listed(&["sms", "ws"]);
            assert_eq!(
                injected_visibility(&given, &given, Some(&targets), &[transport("ws")]),
                listed(&["ws"])
            );
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

        // Python: tests/test_rfc_arbitrations.py::TestSupervisorAlwaysReceives::test_without_a_supervisor_nothing_is_solicited
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

        /// RFC §8.3: a tool-call row is an activity record no agent
        /// answers, and gets no record at the limit.
        #[test]
        fn a_tool_call_row_asks_no_agent_below_or_at_the_limit() {
            let bindings = [agent("analyst"), agent("writer"), transport("web")];
            let mut row = event_from(&bindings[0]);
            row.r#type = EventType::ToolCallStart;
            let below = plan(&row, &bindings);
            row.chain_depth = 4;
            let at_limit = plan(&row, &bindings);
            assert_eq!(
                (
                    ids(&below.agents),
                    ids(&at_limit.not_asked),
                    below.transports.len()
                ),
                (vec![], vec![], 1)
            );
        }

        // Python: tests/test_streamed_chain_depth.py::test_what_opens_no_turn_at_the_limit_leaves_no_record
        #[test]
        fn what_opens_no_turn_at_the_limit_leaves_no_record() {
            let bindings = [transport("sms"), agent("writer")];
            let mut notice = event_from(&bindings[0]);
            notice.r#type = EventType::System;
            let below = plan(&notice, &bindings);
            notice.chain_depth = 4;
            let at_limit = plan(&notice, &bindings);
            // Below the limit the agent reads the notice, taking no turn.
            assert_eq!(
                (
                    ids(&below.agents),
                    ids(&at_limit.agents),
                    ids(&at_limit.not_asked)
                ),
                (vec!["writer"], vec![], vec![])
            );
            // Nor does a change get one.
            notice.r#type = EventType::Edit;
            assert_eq!(ids(&plan(&notice, &bindings).not_asked), Vec::<&str>::new());
        }

        #[test]
        fn an_agent_below_the_limit_is_asked() {
            let bindings = [agent("analyst"), agent("writer")];
            let mut event = event_from(&bindings[0]);
            event.chain_depth = 3;
            assert_eq!(ids(&plan(&event, &bindings).agents), ["writer"]);
        }

        fn routed(event: &mut RoomEvent, routed_to: &str, always: &[&str]) {
            event
                .metadata
                .insert(ROUTED_TO.to_owned(), serde_json::json!(routed_to));
            event
                .metadata
                .insert(ALWAYS_PROCESS.to_owned(), serde_json::json!(always));
        }

        // Python: tests/test_addressing.py::TestSolicitation::test_address_outranks_the_router
        #[test]
        fn the_address_outranks_the_router() {
            let bindings = [transport("human"), agent("claude-code"), agent("codex")];
            let mut event = event_from(&bindings[0]);
            event.addressed_to = Some(vec![channel("codex")]);
            routed(&mut event, "claude-code", &[]);
            assert_eq!(ids(&plan(&event, &bindings).agents), ["codex"]);
        }

        // Python: tests/test_addressing.py::TestSolicitation::test_supervisor_still_acts_on_an_addressed_event
        #[test]
        fn the_supervisor_still_acts_on_an_addressed_event() {
            let bindings = [
                transport("human"),
                agent("claude-code"),
                agent("codex"),
                agent("supervisor"),
            ];
            let mut event = event_from(&bindings[0]);
            event.addressed_to = Some(vec![channel("codex")]);
            event
                .metadata
                .insert(ALWAYS_PROCESS.to_owned(), serde_json::json!(["supervisor"]));
            assert_eq!(
                ids(&plan(&event, &bindings).agents),
                ["codex", "supervisor"]
            );
        }

        // Python: tests/test_addressing.py::TestSolicitation::test_router_still_decides_when_unaddressed
        #[test]
        fn the_router_still_decides_when_unaddressed() {
            let bindings = [
                transport("human"),
                agent("claude-code"),
                agent("codex"),
                agent("supervisor"),
            ];
            let mut event = event_from(&bindings[0]);
            routed(&mut event, "claude-code", &["supervisor"]);
            assert_eq!(
                ids(&plan(&event, &bindings).agents),
                ["claude-code", "supervisor"]
            );
        }

        // Python: tests/test_rfc_arbitrations.py::TestSupervisorAlwaysReceives::test_addressed_only_still_reaches_the_supervisor
        // Python: tests/test_rfc_arbitrations.py::TestSupervisorAlwaysReceives::test_other_agents_are_still_not_solicited
        #[test]
        fn addressed_only_still_reaches_the_supervisor_alone() {
            let bindings = [agent("agent-a"), agent("other-agent"), agent("supervisor")];
            let mut event = event_from(&bindings[0]);
            event
                .metadata
                .insert(ALWAYS_PROCESS.to_owned(), serde_json::json!(["supervisor"]));
            let plan = plan_delivery(&event, &bindings, AgentResponsePolicy::AddressedOnly, 5);
            assert_eq!(ids(&plan.agents), ["supervisor"]);
        }

        // Python: tests/test_addressing.py::TestSolicitation::test_a_supervisor_stamp_that_is_no_list_asks_no_one_more
        #[test]
        fn a_supervisor_stamp_that_is_no_list_asks_no_one_more() {
            let bindings = [transport("human"), agent("codex"), agent("super")];
            for stamp in [
                serde_json::json!("supervisor"),
                serde_json::json!({"super": true}),
                serde_json::Value::Null,
            ] {
                let mut event = event_from(&bindings[0]);
                event.addressed_to = Some(vec![channel("codex")]);
                event.metadata.insert(ALWAYS_PROCESS.to_owned(), stamp);
                assert_eq!(ids(&plan(&event, &bindings).agents), ["codex"]);
            }
        }

        /// RFC §19.4: only a hook writes the routing stamps, so an event's
        /// own are stripped, the rest of its metadata kept.
        #[test]
        fn an_event_s_routing_stamps_are_stripped_and_named() {
            let bindings = [transport("human"), agent("a")];
            let mut event = event_from(&bindings[0]);
            routed(&mut event, "a", &["a"]);
            event
                .metadata
                .insert("custom".to_owned(), serde_json::json!("kept"));
            assert_eq!(
                strip_routing_stamps(&mut event),
                [ROUTED_TO, ALWAYS_PROCESS]
            );
            assert_eq!(
                serde_json::Value::Object(event.metadata),
                serde_json::json!({"custom": "kept"})
            );
        }

        /// RFC §19.4: the router's `_none_` names no agent, which keeps an
        /// agent's answer from every other agent but the supervisor.
        #[test]
        fn a_stamp_naming_no_agent_asks_the_supervisor_alone() {
            let bindings = [agent("agent-a"), agent("agent-b"), agent("supervisor")];
            let mut event = event_from(&bindings[0]);
            routed(&mut event, "_none_", &["supervisor"]);
            assert_eq!(ids(&plan(&event, &bindings).agents), ["supervisor"]);
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
