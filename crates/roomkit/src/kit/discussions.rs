//! The seam between the pipeline and the discussion a room holds (RFC
//! §19.7.5): what the core asks under the room's lock, carried out by the
//! discussion this process runs for the room, and a turn's call.
//!
//! A room is held when its metadata stores a discussion, whichever process
//! installed it (rule 16). The core reads that itself; here is only what it
//! cannot do, the discussion's own queue. A room held with no discussion
//! running here is answered as told, with no turn to give.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit_core::model::{ChannelId, RoomContext, RoomEvent, RoomId};
use roomkit_core::ops::{DiscussionTurn, Inbound};

use super::RoomKit;
use crate::InboundResult;
use crate::channel::BoxFuture;
use crate::driver;
use crate::error::RoomKitError;
use crate::store::ConversationStore;

/// What a discussion running in this process does when the pipeline tells
/// it, always under the room's lock: it edits its stored queue without taking
/// that lock again. What it returns as an error is logged, and never leaves
/// an event undelivered.
pub(crate) trait DiscussionSeam: Send + Sync + 'static {
    /// A message was committed and took its place in the room's lane: queue
    /// whom it asks for, within `reach` (rule 8).
    fn on_committed(
        &self,
        event: RoomEvent,
        reach: Vec<ChannelId>,
        context: RoomContext,
    ) -> BoxFuture<'_, Result<(), RoomKitError>>;

    /// An instruction addressed to its agents: a turn of its own each (rule
    /// 7).
    fn queue_instruction(&self, event: RoomEvent) -> BoxFuture<'_, Result<(), RoomKitError>>;

    /// The turn to give now, if any, its queue edited.
    fn next_turn(
        &self,
        context: RoomContext,
    ) -> BoxFuture<'_, Result<Option<DiscussionTurn>, RoomKitError>>;
}

/// The discussion this process runs for each room.
#[derive(Default)]
pub(crate) struct DiscussionSeams(Mutex<HashMap<RoomId, Arc<dyn DiscussionSeam>>>);

impl fmt::Debug for DiscussionSeams {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DiscussionSeams")
            .finish_non_exhaustive()
    }
}

impl DiscussionSeams {
    fn rooms(&self) -> MutexGuard<'_, HashMap<RoomId, Arc<dyn DiscussionSeam>>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The discussion this process runs for `room_id`.
    pub(crate) fn get(&self, room_id: &RoomId) -> Option<Arc<dyn DiscussionSeam>> {
        self.rooms().get(room_id).cloned()
    }

    /// `seam` runs the discussion of `room_id` from now on.
    #[cfg_attr(not(test), expect(dead_code, reason = "the install's, lot 8c"))]
    pub(crate) fn hold(&self, room_id: RoomId, seam: Arc<dyn DiscussionSeam>) {
        self.rooms().insert(room_id, seam);
    }
}

impl<S: ConversationStore> RoomKit<S> {
    /// Gives the turn the discussion of `room_id` names now, if any, and
    /// waits for it: its agent's answers are the result's, as a new
    /// message's. The result names no event when no turn was given.
    #[cfg_attr(not(test), expect(dead_code, reason = "the driver's, lot 8c"))]
    pub(crate) async fn run_discussion_turn(
        &self,
        room_id: &RoomId,
    ) -> Result<InboundResult, RoomKitError> {
        let turn = Inbound::discussion_turn(self.config(), room_id.clone());
        let ran = driver::run_rerun(&self.inner, "discussion_turn", room_id, turn, None).await;
        Ok(ran.output??.into())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Mutex, OnceLock};
    use std::time::Duration;

    use pretty_assertions::assert_eq;
    use roomkit_core::model::{
        ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelOutput, ChannelType,
        EventStatus, EventType, InboundMessage, Metadata, TextContent,
    };
    use roomkit_core::rules::discussion::{CONFIG_KEY, STATE_KEY};
    use roomkit_core::rules::hook_registry::HookTrigger;
    use serde_json::json;
    use tokio::sync::Notify;

    use super::*;
    use crate::HookOptions;
    use crate::channel::{Channel, ChannelError};
    use crate::discussion_turn::discussion_notes;
    use crate::kit::WeakRoomKit;
    use crate::kit::{AttachChannelOptions, CreateRoomOptions, ProcessInboundOptions};
    use crate::testing::{
        EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions,
    };

    fn id(channel: &str) -> ChannelId {
        ChannelId::new(channel).unwrap()
    }

    fn room_id() -> RoomId {
        RoomId::new("r1").unwrap()
    }

    /// Stands for a discussion holding the room, as Python's `_Held`: records
    /// what it is told, and gives the turn it is handed.
    #[derive(Default)]
    struct Held {
        committed: Mutex<Vec<(RoomEvent, Vec<ChannelId>)>>,
        instructed: Mutex<Vec<RoomEvent>>,
        turn: Mutex<Option<DiscussionTurn>>,
        fails: bool,
    }

    impl DiscussionSeam for Held {
        fn on_committed(
            &self,
            event: RoomEvent,
            reach: Vec<ChannelId>,
            _context: RoomContext,
        ) -> BoxFuture<'_, Result<(), RoomKitError>> {
            self.committed.lock().unwrap().push((event, reach));
            let fails = self.fails;
            Box::pin(async move {
                if fails {
                    return Err(RoomKitError::Value("it fails".to_owned()));
                }
                Ok(())
            })
        }

        fn queue_instruction(&self, event: RoomEvent) -> BoxFuture<'_, Result<(), RoomKitError>> {
            self.instructed.lock().unwrap().push(event);
            let fails = self.fails;
            Box::pin(async move {
                if fails {
                    return Err(RoomKitError::Value("it fails".to_owned()));
                }
                Ok(())
            })
        }

        fn next_turn(
            &self,
            _context: RoomContext,
        ) -> BoxFuture<'_, Result<Option<DiscussionTurn>, RoomKitError>> {
            let turn = self.turn.lock().unwrap().take();
            let fails = self.fails;
            Box::pin(async move {
                if fails {
                    return Err(RoomKitError::Value("it fails".to_owned()));
                }
                Ok(turn)
            })
        }
    }

    /// A room storing a discussion of `a` and `b`, the transport `ops`
    /// recorded, the seam `held` running it.
    async fn held_room(held: &Arc<Held>) -> (RoomKit, RecordingTransport) {
        let kit = RoomKit::new(crate::kit::RoomKitOptions::default());
        let ops = RecordingTransport::new(
            id("ops"),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        );
        kit.register_channel(ops.clone()).unwrap();
        for agent in ["a", "b"] {
            let reply = format!("{agent} answers");
            kit.register_channel(EchoAgent::new(
                id(agent),
                reply,
                EchoAgentOptions::default(),
            ))
            .unwrap();
        }
        let options = CreateRoomOptions {
            room_id: Some(room_id()),
            ..Default::default()
        };
        let mut room = kit.create_room(options).await.unwrap();
        room.metadata = Metadata::from_iter([(
            CONFIG_KEY.to_owned(),
            json!({"agents": ["a", "b"], "max_depth": 5}),
        )]);
        kit.store().update_room(room).await.unwrap();
        for (channel, category) in [
            ("ops", ChannelCategory::Transport),
            ("a", ChannelCategory::Intelligence),
            ("b", ChannelCategory::Intelligence),
        ] {
            let options = AttachChannelOptions {
                category: Some(category),
                ..Default::default()
            };
            kit.attach_channel(&room_id(), &id(channel), options)
                .await
                .unwrap();
        }
        kit.inner
            .discussions
            .hold(room_id(), Arc::<Held>::clone(held));
        (kit, ops)
    }

    fn said(body: &str) -> InboundMessage {
        InboundMessage::new(id("ops"), "oncall", TextContent::new(body))
    }

    async fn say(kit: &RoomKit, message: InboundMessage) -> InboundResult {
        kit.process_inbound(message, ProcessInboundOptions::default())
            .await
            .unwrap()
    }

    fn turn_on(
        answered: RoomEvent,
        agent: &str,
        chain_depth: u32,
        notes: &[&str],
    ) -> DiscussionTurn {
        DiscussionTurn {
            agent: id(agent),
            answered: Box::new(RoomEvent {
                chain_depth,
                ..answered
            }),
            mark: crate::issue_mark(notes.iter().map(|note| (*note).to_owned()).collect()),
            max_depth: 10,
        }
    }

    // Python: tests/test_discussion_core.py::test_a_room_a_discussion_holds_asks_no_agent_at_broadcast
    #[tokio::test]
    async fn a_committed_message_goes_to_the_discussion_within_its_reach() {
        let held = Arc::new(Held::default());
        let (kit, _ops) = held_room(&held).await;
        let mut message = said("@a hello");
        message.addressed_to = Some(vec![id("a"), id("ghost")]);

        let result = say(&kit, message).await;

        assert_eq!(result.response_events, Vec::new());
        assert_eq!(result.unavailable_targets, Some(vec![id("ghost")]));
        let committed = held.committed.lock().unwrap();
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].0.r#type, EventType::Message);
        assert_eq!(committed[0].1, vec![id("a"), id("b")]);
    }

    // Python: core/mixins/lane_execution.py `_queue_for_discussion`
    #[tokio::test]
    async fn a_failure_of_the_discussion_leaves_the_event_delivered() {
        let held = Arc::new(Held {
            fails: true,
            ..Held::default()
        });
        let (kit, ops) = held_room(&held).await;
        let other = RecordingTransport::new(
            id("sms"),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        );
        kit.register_channel(other.clone()).unwrap();
        kit.attach_channel(&room_id(), &id("sms"), AttachChannelOptions::default())
            .await
            .unwrap();

        let result = say(&kit, said("hello")).await;

        assert!(!result.blocked);
        assert!(result.event.is_some());
        crate::testing::idle(&kit).await;
        assert_eq!(other.texts(), vec!["hello".to_owned()]);
        assert_eq!(ops.texts(), Vec::<String>::new());
    }

    // Python: core/mixins/lane_execution.py `_commit`, an instruction to its agents
    #[tokio::test]
    async fn an_instruction_to_an_agent_goes_to_the_discussion_undelivered() {
        let held = Arc::new(Held::default());
        let (kit, _ops) = held_room(&held).await;
        let mut message = said("Introduce yourself");
        message.event_type = EventType::Instruction;
        message.addressed_to = Some(vec![id("a")]);

        let result = say(&kit, message).await;

        assert!(!result.blocked);
        assert_eq!(result.response_events, Vec::new());
        assert_eq!(result.event.unwrap().status, EventStatus::Delivered);
        assert_eq!(held.instructed.lock().unwrap().len(), 1);
        assert_eq!(held.committed.lock().unwrap().len(), 0);
    }

    // Python: core/mixins/lane_execution.py, `queue_instruction` is not caught
    #[tokio::test]
    async fn an_instruction_the_discussion_did_not_take_fails_its_call() {
        let held = Arc::new(Held {
            fails: true,
            ..Held::default()
        });
        let (kit, _ops) = held_room(&held).await;
        let mut message = said("Introduce yourself");
        message.event_type = EventType::Instruction;
        message.addressed_to = Some(vec![id("a")]);

        let result = kit
            .process_inbound(message, ProcessInboundOptions::default())
            .await;

        assert!(result.is_err(), "{result:?}");
    }

    // Python: core/mixins/discussion.py `_drive` backs off on what the
    // discussion raises: a failed look is no quiet "no turn".
    #[tokio::test]
    async fn a_failure_to_name_the_turn_fails_the_turn_s_call() {
        let held = Arc::new(Held {
            fails: true,
            ..Held::default()
        });
        let (kit, _ops) = held_room(&held).await;

        let result = kit.run_discussion_turn(&room_id()).await;

        assert!(result.is_err(), "{result:?}");
    }

    // Python: core/mixins/discussion.py `_plan_discussion_turn`
    #[tokio::test]
    async fn a_turn_on_an_event_whose_source_cannot_write_is_no_turn() {
        let held = Arc::new(Held::default());
        let (kit, ops) = held_room(&held).await;
        let asked = say(&kit, said("hello")).await.event.unwrap();
        crate::testing::idle(&kit).await;
        kit.mute(&room_id(), &id("ops"), crate::kit::MuteOptions::default())
            .await
            .unwrap();

        *held.turn.lock().unwrap() = Some(turn_on(asked, "a", 1, &[]));
        let result = kit.run_discussion_turn(&room_id()).await.unwrap();
        crate::testing::idle(&kit).await;

        assert_eq!(result, InboundResult::default());
        assert_eq!(ops.texts(), Vec::<String>::new());
    }

    /// A transport whose first delivery waits for its gate.
    #[derive(Clone, Default)]
    struct Gated {
        reached: Arc<Notify>,
        gate: Arc<Notify>,
        delivered: Arc<AtomicU32>,
    }

    impl Channel for Gated {
        fn channel_id(&self) -> ChannelId {
            id("slow")
        }

        fn channel_type(&self) -> ChannelType {
            ChannelType::SMS
        }

        fn category(&self) -> ChannelCategory {
            ChannelCategory::Transport
        }

        fn capabilities(&self) -> ChannelCapabilities {
            ChannelCapabilities::default()
        }

        async fn handle_inbound(
            &self,
            _message: InboundMessage,
            _context: RoomContext,
        ) -> Result<RoomEvent, ChannelError> {
            Err("no inbound".into())
        }

        async fn deliver(
            &self,
            _event: RoomEvent,
            _binding: ChannelBinding,
            _context: RoomContext,
        ) -> Result<ChannelOutput, ChannelError> {
            if self.delivered.fetch_add(1, Ordering::Relaxed) == 0 {
                self.reached.notify_one();
                self.gate.notified().await;
            }
            Ok(ChannelOutput::default())
        }
    }

    // Python: core/mixins/discussion.py `_plan_discussion_turn`, its
    // `after_index`; RFC §10.2, §19.7.5 rule 9.
    #[tokio::test]
    async fn a_turn_waits_for_its_event_s_own_delivery() {
        let held = Arc::new(Held::default());
        let (kit, _ops) = held_room(&held).await;
        let gated = Gated::default();
        kit.register_channel(gated.clone()).unwrap();
        kit.attach_channel(&room_id(), &id("slow"), AttachChannelOptions::default())
            .await
            .unwrap();
        let kit = Arc::new(kit);
        let delivering = {
            let kit = Arc::clone(&kit);
            tokio::spawn(async move { say(&kit, said("hello")).await })
        };
        gated.reached.notified().await;
        let options = crate::store::ListEventsOptions::default();
        let stored = kit.store().list_events(&room_id(), options).await.unwrap();
        let asked = stored.last().unwrap().clone();

        *held.turn.lock().unwrap() = Some(turn_on(asked, "a", 1, &[]));
        let mut turn = {
            let kit = Arc::clone(&kit);
            tokio::spawn(async move { kit.run_discussion_turn(&room_id()).await })
        };

        let early = tokio::time::timeout(Duration::from_millis(30), &mut turn).await;
        assert!(
            early.is_err(),
            "the turn ran before its event was delivered"
        );
        gated.gate.notify_one();
        delivering.await.unwrap();
        let answered = turn.await.unwrap().unwrap();
        assert_eq!(answered.response_events.len(), 1);
    }

    /// An agent whose turn calls into its room until a call is refused.
    #[derive(Clone, Default)]
    struct Calling {
        kit: Arc<OnceLock<WeakRoomKit>>,
        made: Arc<AtomicU32>,
    }

    impl Channel for Calling {
        fn channel_id(&self) -> ChannelId {
            id("calling")
        }

        fn channel_type(&self) -> ChannelType {
            ChannelType::AI
        }

        fn category(&self) -> ChannelCategory {
            ChannelCategory::Intelligence
        }

        fn capabilities(&self) -> ChannelCapabilities {
            ChannelCapabilities::default()
        }

        async fn handle_inbound(
            &self,
            _message: InboundMessage,
            _context: RoomContext,
        ) -> Result<RoomEvent, ChannelError> {
            Err("no inbound".into())
        }

        async fn deliver(
            &self,
            _event: RoomEvent,
            _binding: ChannelBinding,
            _context: RoomContext,
        ) -> Result<ChannelOutput, ChannelError> {
            Ok(ChannelOutput::default())
        }

        async fn on_event(
            &self,
            _event: RoomEvent,
            _binding: ChannelBinding,
            _context: RoomContext,
        ) -> Result<ChannelOutput, ChannelError> {
            let kit = self.kit.get().and_then(WeakRoomKit::upgrade).unwrap();
            loop {
                let options = ProcessInboundOptions {
                    room_id: Some(room_id()),
                    ..Default::default()
                };
                let called = kit
                    .process_inbound(said("follow-up"), options)
                    .await
                    .map_err(|error| ChannelError::from(error.to_string()))?;
                if called.event.is_none() {
                    return Ok(ChannelOutput::default());
                }
                self.made.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    // Python: core/mixins/discussion.py `_plan_discussion_turn`,
    // `reentry_budget=10 * max_depth`, one budget (rule 11).
    #[tokio::test]
    async fn a_turn_s_calls_draw_on_the_budget_its_limit_bounds() {
        let held = Arc::new(Held::default());
        let (kit, _ops) = held_room(&held).await;
        let calling = Calling::default();
        kit.register_channel(calling.clone()).unwrap();
        calling.kit.set(kit.downgrade()).unwrap();
        let options = AttachChannelOptions {
            category: Some(ChannelCategory::Intelligence),
            ..Default::default()
        };
        kit.attach_channel(&room_id(), &id("calling"), options)
            .await
            .unwrap();
        let asked = say(&kit, said("hello")).await.event.unwrap();
        crate::testing::idle(&kit).await;

        let mut turn = turn_on(asked, "calling", 0, &[]);
        turn.max_depth = 2;
        *held.turn.lock().unwrap() = Some(turn);
        kit.run_discussion_turn(&room_id()).await.unwrap();

        // Twice the discussion's limit, in tens; the kit's own would be 50.
        assert_eq!(calling.made.load(Ordering::Relaxed), 20);
    }

    // Python: core/mixins/inbound_locked.py, RFC §19.7.5 rule 15
    #[tokio::test]
    async fn an_instruction_to_a_discussion_that_is_over_is_blocked() {
        let held = Arc::new(Held::default());
        let (kit, _ops) = held_room(&held).await;
        let mut room = kit.store().get_room(&room_id()).await.unwrap().unwrap();
        room.metadata
            .insert(STATE_KEY.to_owned(), json!({"over": true}));
        kit.store().update_room(room).await.unwrap();
        let mut message = said("Introduce yourself");
        message.event_type = EventType::Instruction;
        message.addressed_to = Some(vec![id("a")]);

        let result = say(&kit, message).await;

        assert!(result.blocked);
        assert_eq!(result.reason.as_deref(), Some("discussion_over"));
        assert_eq!(result.event, None);
        assert_eq!(held.instructed.lock().unwrap().len(), 0);
    }

    // Python: tests/test_discussion_core.py::test_a_turn_asks_its_one_agent_against_the_discussions_depth_limit
    #[tokio::test]
    async fn test_a_turn_asks_its_one_agent_against_the_discussions_depth_limit() {
        let held = Arc::new(Held::default());
        let (kit, ops) = held_room(&held).await;
        let asked = say(&kit, said("@a @b hello")).await.event.unwrap();
        crate::testing::idle(&kit).await;

        // Deeper than the kit's limit, within the discussion's own.
        *held.turn.lock().unwrap() = Some(turn_on(asked.clone(), "a", 7, &["NOTES"]));
        let answered = kit.run_discussion_turn(&room_id()).await.unwrap();
        crate::testing::idle(&kit).await;

        let bodies: Vec<_> = answered
            .response_events
            .iter()
            .map(|e| e.source.channel_id.clone())
            .collect();
        assert_eq!(bodies, vec![id("a")]);
        // It goes on no transport twice, and is committed as an answer.
        assert_eq!(ops.texts(), vec!["a answers".to_owned()]);

        // Past the discussion's own limit: not asked, and recorded blocked.
        *held.turn.lock().unwrap() = Some(turn_on(asked.clone(), "b", 9, &[]));
        let stopped = kit.run_discussion_turn(&room_id()).await.unwrap();
        crate::testing::idle(&kit).await;
        assert_eq!(stopped.response_events, Vec::new());
        let timeline = kit
            .store()
            .list_events(
                &room_id(),
                crate::store::ListEventsOptions {
                    event_filter: Some(crate::store::EventFilter {
                        include_blocked: true,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let blocked: Vec<_> = timeline
            .iter()
            .filter(|event| event.status == EventStatus::Blocked)
            .map(|event| (event.source.channel_id.clone(), event.blocked_by.clone()))
            .collect();
        assert_eq!(
            blocked,
            vec![(id("b"), Some("event_chain_depth_limit".to_owned()))]
        );
    }

    #[tokio::test]
    async fn no_turn_runs_nothing() {
        let held = Arc::new(Held::default());
        let (kit, _ops) = held_room(&held).await;

        let result = kit.run_discussion_turn(&room_id()).await.unwrap();

        assert_eq!(result, InboundResult::default());
    }

    // Python: HookTrigger.ON_SPEAK_QUEUE takes a SpeakQueueEvent, and no other payload.
    #[tokio::test]
    async fn on_speak_queue_takes_its_own_payload() {
        let kit = RoomKit::new(crate::kit::RoomKitOptions::default());
        let speak_queue = |_: crate::SpeakQueueEvent, _: RoomContext| async {};
        kit.hook(
            HookTrigger::OnSpeakQueue,
            speak_queue,
            HookOptions::default(),
        )
        .unwrap();
        let event = |_: RoomEvent, _: RoomContext| async {};
        assert!(
            kit.hook(HookTrigger::OnSpeakQueue, event, HookOptions::default())
                .is_err()
        );
    }

    #[test]
    fn a_mark_is_read_until_it_is_retired() {
        let mark = crate::issue_mark(vec!["one".to_owned(), String::new()]);
        let mut event = RoomEvent::new(
            room_id(),
            roomkit_core::model::EventSource::new(id("ops"), ChannelType::SMS),
            TextContent::new("hello"),
        );
        event.metadata.insert(
            roomkit_core::rules::discussion::TURN_KEY.to_owned(),
            mark.clone(),
        );
        assert_eq!(discussion_notes(&event), vec!["one".to_owned()]);
        crate::retire_mark(&mark);
        assert_eq!(discussion_notes(&event), Vec::<String>::new());
    }
}
