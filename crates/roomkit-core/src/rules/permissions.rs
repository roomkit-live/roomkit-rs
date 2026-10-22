//! Who reads, who writes, and whom a broadcast reaches (RFC §7.5, §10.2,
//! §19.3).

use crate::model::{
    AgentResponsePolicy, ChannelBinding, ChannelCategory, EventStatus, RoomEvent, Visibility,
};
use crate::rules::chain_depth;

/// Why a source's event may not be broadcast (RFC §7.5 rule 2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WriteRefusal {
    /// The binding's access does not include writing.
    ReadOnly,
    /// The binding is muted.
    Muted,
}

impl WriteRefusal {
    /// The `blocked_by` value the refused event is stored with.
    pub fn blocked_by(self) -> &'static str {
        match self {
            Self::ReadOnly => "source_read_only",
            Self::Muted => "source_muted",
        }
    }
}

/// Why `source` may not have its events broadcast, `None` when it may (RFC
/// §7.5 rule 2).
///
/// An access without writing is reported before a mute, and an access of
/// `none` is reported as read-only, the RFC naming no other reason (gap
/// G-015).
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
