//! The room lifecycle on the tokio runtime (RFC §5.1, §7.2, §7.5 rule 7,
//! §8.2): room timers on an advancing clock, the background sweeper, a
//! sender routed back to the conversation its timer paused, unregistering a
//! channel, and the room metadata updates channels return.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::{Future, ready};
use std::sync::Arc;
use std::time::Duration;

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventContent, InboundMessage, Metadata, RoomContext, RoomEvent, RoomId, RoomStatus, RoomTimers,
    Timestamp,
};
use roomkit::testing::{EchoAgent, RecordingTransport};
use roomkit::{
    CHANNEL_NOT_REGISTERED, Channel, ChannelDeclaration, ChannelError, ConversationStore,
    FrameworkEvent, HookTrigger, InboundError, KitError, LifecycleError, RoomKit,
    SequentialEnvironment, TransitionReason,
};
use tokio::sync::{broadcast, mpsc};

const START_MILLIS: i64 = 1_792_677_780_000;

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

fn after_start(seconds: i64) -> Timestamp {
    Timestamp::from_unix_millis(START_MILLIS + seconds * 1000).unwrap()
}

fn message(from: &RecordingTransport, text: &str) -> InboundMessage {
    InboundMessage::new(from.id().clone(), "+15551234567", EventContent::text(text))
}

/// A sequential environment the test keeps a handle on, to move the kit's
/// clock while the kit runs.
#[derive(Clone, Debug)]
struct Clock(Arc<SequentialEnvironment>);

impl Clock {
    fn new() -> Self {
        let start = Timestamp::from_unix_millis(START_MILLIS).unwrap();
        Self(Arc::new(SequentialEnvironment::new(start)))
    }

    fn advance(&self, seconds: u64) {
        self.0.advance(Duration::from_secs(seconds));
    }
}

/// A kit on `clock`, with an SMS transport attached to a room whose timers
/// pause it after 60 seconds without activity and close it 120 seconds
/// after the pause.
async fn timed_room(clock: &Clock) -> (RoomKit, RecordingTransport) {
    let kit = RoomKit::new();
    kit.set_environment(Arc::clone(&clock.0));
    let sms = RecordingTransport::new(channel("sms"), ChannelType::Sms);
    kit.register_channel(sms.clone()).unwrap();
    kit.create_room_with(&room_id(), |room| {
        room.timers = RoomTimers {
            inactive_after_seconds: Some(60),
            closed_after_seconds: Some(120),
            last_activity_at: None,
        };
    })
    .await
    .unwrap();
    kit.attach(&room_id(), sms.id()).await.unwrap();
    (kit, sms)
}

/// The next framework event matching `wanted`, or a failure after a second.
async fn next_matching(
    events: &mut broadcast::Receiver<FrameworkEvent>,
    wanted: impl Fn(&FrameworkEvent) -> bool,
) -> FrameworkEvent {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let event = events.recv().await.unwrap();
            if wanted(&event) {
                return event;
            }
        }
    })
    .await
    .unwrap()
}

/// The statuses of `rooms`.
fn statuses(rooms: &[roomkit::model::Room]) -> Vec<RoomStatus> {
    rooms.iter().map(|room| room.status).collect()
}

mod timers {
    use super::*;
    use pretty_assertions::assert_eq;

    #[tokio::test]
    async fn check_timers_pauses_a_silent_room_then_closes_it() {
        let clock = Clock::new();
        let (kit, _sms) = timed_room(&clock).await;
        clock.advance(90);
        let paused = kit.check_timers().await.unwrap();
        clock.advance(120);
        let closed = kit.check_timers().await.unwrap();
        assert_eq!(
            (statuses(&paused), statuses(&closed)),
            (vec![RoomStatus::Paused], vec![RoomStatus::Closed])
        );
    }

    #[tokio::test]
    async fn a_late_check_closes_the_room_at_the_instant_its_close_fell_due() {
        let clock = Clock::new();
        let (kit, _sms) = timed_room(&clock).await;
        clock.advance(3600);
        let checked = kit.check_timers().await.unwrap();
        assert_eq!(
            checked
                .iter()
                .map(|room| (room.paused_at, room.closed_at))
                .collect::<Vec<_>>(),
            [(Some(after_start(60)), Some(after_start(180)))]
        );
    }

    #[tokio::test]
    async fn check_timers_reports_each_transition_with_the_reason_timer() {
        let clock = Clock::new();
        let (kit, _sms) = timed_room(&clock).await;
        let mut events = kit.subscribe();
        clock.advance(3600);
        kit.check_timers().await.unwrap();
        let reported = [events.recv().await.unwrap(), events.recv().await.unwrap()];
        assert_eq!(
            reported,
            [
                FrameworkEvent::RoomPaused {
                    room_id: room_id(),
                    reason: TransitionReason::Timer,
                },
                FrameworkEvent::RoomClosed {
                    room_id: room_id(),
                    reason: Some(TransitionReason::Timer),
                },
            ]
        );
    }

    #[tokio::test]
    async fn check_timers_leaves_a_room_whose_timer_has_not_fallen_due() {
        let clock = Clock::new();
        let (kit, _sms) = timed_room(&clock).await;
        clock.advance(59);
        assert_eq!(kit.check_timers().await.unwrap(), []);
    }

    #[tokio::test]
    async fn a_message_restarts_the_inactivity_timer() {
        let clock = Clock::new();
        let (kit, sms) = timed_room(&clock).await;
        clock.advance(50);
        kit.process_inbound(message(&sms, "Still here"), Some(&room_id()))
            .await
            .unwrap();
        clock.advance(50);
        assert_eq!(kit.check_timers().await.unwrap(), []);
    }

    #[tokio::test]
    async fn a_message_after_the_close_fell_due_is_refused() {
        let clock = Clock::new();
        let (kit, sms) = timed_room(&clock).await;
        clock.advance(200);
        let result = kit
            .process_inbound(message(&sms, "Hello?"), Some(&room_id()))
            .await
            .unwrap();
        let room = kit.store().get_room(&room_id()).await.unwrap().unwrap();
        assert_eq!(
            (result.reason.as_deref(), room.status),
            (Some("room_closed"), RoomStatus::Closed)
        );
    }

    #[tokio::test]
    async fn the_sweeper_applies_a_due_transition_in_the_background() {
        let clock = Clock::new();
        let (kit, _sms) = timed_room(&clock).await;
        let mut events = kit.subscribe();
        clock.advance(90);
        let _sweeper = kit.run_timers(Duration::from_millis(10)).unwrap();
        let paused = next_matching(&mut events, |event| {
            matches!(event, FrameworkEvent::RoomPaused { .. })
        })
        .await;
        assert_eq!(
            paused,
            FrameworkEvent::RoomPaused {
                room_id: room_id(),
                reason: TransitionReason::Timer,
            }
        );
    }

    #[tokio::test]
    async fn a_stopped_sweeper_applies_nothing_more() {
        let clock = Clock::new();
        let (kit, _sms) = timed_room(&clock).await;
        kit.run_timers(Duration::from_millis(10)).unwrap().stop();
        clock.advance(90);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let room = kit.store().get_room(&room_id()).await.unwrap().unwrap();
        assert_eq!(room.status, RoomStatus::Active);
    }

    #[tokio::test]
    async fn the_sweeper_refuses_a_zero_period() {
        let kit = RoomKit::new();
        assert!(matches!(
            kit.run_timers(Duration::ZERO),
            Err(KitError::ZeroPeriod)
        ));
    }

    #[test]
    fn the_sweeper_needs_a_tokio_runtime() {
        let kit = RoomKit::new();
        assert!(matches!(
            kit.run_timers(Duration::from_secs(1)),
            Err(KitError::NoRuntime)
        ));
    }
}

/// RFC §10.4 step 1, gap G-045: a sender who writes again after the timer
/// paused their conversation returns to it, and their message resumes it.
mod a_paused_conversation {
    use super::*;
    use pretty_assertions::assert_eq;

    /// The timed room, with a websocket attached beside its SMS transport.
    async fn with_websocket(clock: &Clock) -> (RoomKit, RecordingTransport, RecordingTransport) {
        let (kit, sms) = timed_room(clock).await;
        let ws = RecordingTransport::new(channel("ws"), ChannelType::Websocket);
        kit.register_channel(ws.clone()).unwrap();
        kit.attach(&room_id(), ws.id()).await.unwrap();
        (kit, sms, ws)
    }

    /// Moves the clock past the room's inactivity timer and checks the
    /// timers: the statuses of the rooms the check changed, and the pause
    /// it reported.
    async fn paused_by_its_timer(
        kit: &RoomKit,
        clock: &Clock,
    ) -> (Vec<RoomStatus>, FrameworkEvent) {
        let mut events = kit.subscribe();
        clock.advance(90);
        let paused = kit.check_timers().await.unwrap();
        let reported = next_matching(&mut events, |event| {
            matches!(event, FrameworkEvent::RoomPaused { .. })
        })
        .await;
        (statuses(&paused), reported)
    }

    #[tokio::test]
    async fn its_sender_writing_again_lands_in_it_and_resumes_it() {
        let clock = Clock::new();
        let (kit, sms, ws) = with_websocket(&clock).await;
        kit.process_inbound(message(&sms, "Bonjour"), None)
            .await
            .unwrap();
        let (paused, reported) = paused_by_its_timer(&kit, &clock).await;
        let result = kit
            .process_inbound(message(&sms, "Still there?"), None)
            .await
            .unwrap();
        let room = kit.store().get_room(&room_id()).await.unwrap().unwrap();
        assert_eq!(
            (
                paused,
                reported,
                result.event.map(|event| event.room_id),
                (room.status, room.paused_at),
                ws.texts()
            ),
            (
                vec![RoomStatus::Paused],
                FrameworkEvent::RoomPaused {
                    room_id: room_id(),
                    reason: TransitionReason::Timer,
                },
                Some(room_id()),
                (RoomStatus::Active, None),
                ["Bonjour", "Still there?"].map(str::to_owned).to_vec()
            )
        );
    }

    /// Gap G-045: step 3 admits no one into a paused room whose binding is
    /// not `group`, so a sender the room never recorded gets a room of their
    /// own, and the paused one is left as it is.
    #[tokio::test]
    async fn a_sender_it_never_recorded_gets_a_room_of_their_own() {
        let clock = Clock::new();
        let (kit, sms, ws) = with_websocket(&clock).await;
        paused_by_its_timer(&kit, &clock).await;
        let result = kit
            .process_inbound(message(&sms, "Bonjour"), None)
            .await
            .unwrap();
        let room = kit.store().get_room(&room_id()).await.unwrap().unwrap();
        assert_eq!(
            (
                result.event.is_some_and(|event| event.room_id != room_id()),
                room.status,
                ws.texts()
            ),
            (true, RoomStatus::Paused, Vec::<String>::new())
        );
    }

    /// Gap G-045: a group binding says the channel is one conversation, so a
    /// second member's message after the pause lands in the room and
    /// resumes it.
    #[tokio::test]
    async fn bound_as_a_group_takes_another_member_s_message_and_is_resumed() {
        let clock = Clock::new();
        let (kit, sms, ws) = with_websocket(&clock).await;
        kit.attach_with(&room_id(), sms.id(), |binding| binding.group = true)
            .await
            .unwrap();
        kit.process_inbound(message(&sms, "Bonjour"), None)
            .await
            .unwrap();
        let (paused, _) = paused_by_its_timer(&kit, &clock).await;
        let other = InboundMessage::new(
            sms.id().clone(),
            "+15550000002",
            EventContent::text("Me too"),
        );
        let result = kit.process_inbound(other, None).await.unwrap();
        let room = kit.store().get_room(&room_id()).await.unwrap().unwrap();
        assert_eq!(
            (
                paused,
                result.event.map(|event| event.room_id),
                (room.status, room.paused_at),
                ws.texts()
            ),
            (
                vec![RoomStatus::Paused],
                Some(room_id()),
                (RoomStatus::Active, None),
                ["Bonjour", "Me too"].map(str::to_owned).to_vec()
            )
        );
    }
}

mod unregistering {
    use super::*;
    use pretty_assertions::assert_eq;

    async fn room_with_sms_and_ws() -> (RoomKit, RecordingTransport, RecordingTransport) {
        let kit = RoomKit::new();
        let sms = RecordingTransport::new(channel("sms"), ChannelType::Sms);
        let ws = RecordingTransport::new(channel("ws"), ChannelType::Websocket);
        kit.register_channel(sms.clone()).unwrap();
        kit.register_channel(ws.clone()).unwrap();
        kit.create_room(&room_id()).await.unwrap();
        kit.attach(&room_id(), sms.id()).await.unwrap();
        kit.attach(&room_id(), ws.id()).await.unwrap();
        (kit, sms, ws)
    }

    #[tokio::test]
    async fn emits_channel_unregistered() {
        let (kit, sms, _ws) = room_with_sms_and_ws().await;
        let mut events = kit.subscribe();
        kit.unregister_channel(sms.id()).unwrap();
        assert_eq!(
            events.recv().await.unwrap(),
            FrameworkEvent::ChannelUnregistered {
                channel_id: channel("sms"),
                channel_type: ChannelType::Sms,
            }
        );
    }

    #[tokio::test]
    async fn refuses_the_channel_s_next_message() {
        let (kit, sms, _ws) = room_with_sms_and_ws().await;
        kit.unregister_channel(sms.id()).unwrap();
        let refused = kit
            .process_inbound(message(&sms, "Hello?"), Some(&room_id()))
            .await;
        assert_eq!(
            refused,
            Err(KitError::Inbound(InboundError::UnknownChannel(channel(
                "sms"
            ))))
        );
    }

    /// Gap G-048: the bindings stay, and a transport whose channel is
    /// unregistered is skipped, as one another process serves may be.
    #[tokio::test]
    async fn skips_a_transport_binding_it_leaves() {
        let (kit, sms, ws) = room_with_sms_and_ws().await;
        kit.unregister_channel(ws.id()).unwrap();
        let mut events = kit.subscribe();
        kit.process_inbound(message(&sms, "Bonjour"), Some(&room_id()))
            .await
            .unwrap();
        let mut failed = 0;
        while let Ok(event) = events.try_recv() {
            if matches!(event, FrameworkEvent::DeliveryFailed { .. }) {
                failed += 1;
            }
        }
        let bindings = kit.store().list_bindings(&room_id()).await.unwrap();
        assert_eq!((failed, bindings.len()), (0, 2));
    }

    /// Gap G-048: an intelligence channel whose channel is unregistered is
    /// reported, the turn it was owed never taken.
    #[tokio::test]
    async fn reports_an_agent_binding_it_leaves_as_a_failed_delivery() {
        let (kit, sms, _ws) = room_with_sms_and_ws().await;
        kit.register_channel(EchoAgent::new(channel("ai"), "Hello"))
            .unwrap();
        kit.attach(&room_id(), &channel("ai")).await.unwrap();
        kit.unregister_channel(&channel("ai")).unwrap();
        let mut events = kit.subscribe();
        kit.process_inbound(message(&sms, "Bonjour"), Some(&room_id()))
            .await
            .unwrap();
        let failed = next_matching(&mut events, |event| {
            matches!(event, FrameworkEvent::DeliveryFailed { .. })
        })
        .await;
        let FrameworkEvent::DeliveryFailed {
            channel_id, error, ..
        } = failed
        else {
            panic!("a delivery failure was expected");
        };
        assert_eq!(
            (channel_id, error.as_str()),
            (channel("ai"), CHANNEL_NOT_REGISTERED)
        );
    }

    /// A `BEFORE_BROADCAST` hook that unregisters `channel_id` while the
    /// message it decides on is under the lock: the call goes on with the
    /// configuration it started with, which still holds the channel.
    fn unregistering_during_the_call(kit: &RoomKit, channel_id: &str) {
        let unregistering = kit.clone();
        let channel_id = channel(channel_id);
        kit.before_broadcast("unplug", move |_, _| {
            unregistering.unregister_channel(&channel_id).unwrap();
            ready(Ok(roomkit::model::HookResult::allow()))
        })
        .unwrap();
    }

    /// Gap G-048: a transport unregistered once the call started is skipped
    /// when the driver finds it gone.
    #[tokio::test]
    async fn skips_a_transport_unregistered_during_the_call() {
        let (kit, sms, _ws) = room_with_sms_and_ws().await;
        unregistering_during_the_call(&kit, "ws");
        let mut events = kit.subscribe();
        kit.process_inbound(message(&sms, "Bonjour"), Some(&room_id()))
            .await
            .unwrap();
        let mut failed = 0;
        while let Ok(event) = events.try_recv() {
            if matches!(event, FrameworkEvent::DeliveryFailed { .. }) {
                failed += 1;
            }
        }
        assert_eq!(failed, 0);
    }

    /// Gap G-048: an intelligence channel unregistered once the call
    /// started is reported when the driver finds it gone.
    #[tokio::test]
    async fn reports_an_agent_unregistered_during_the_call() {
        let (kit, sms, _ws) = room_with_sms_and_ws().await;
        kit.register_channel(EchoAgent::new(channel("ai"), "Hello"))
            .unwrap();
        kit.attach(&room_id(), &channel("ai")).await.unwrap();
        unregistering_during_the_call(&kit, "ai");
        let mut events = kit.subscribe();
        kit.process_inbound(message(&sms, "Bonjour"), Some(&room_id()))
            .await
            .unwrap();
        let failed = next_matching(&mut events, |event| {
            matches!(event, FrameworkEvent::DeliveryFailed { .. })
        })
        .await;
        let FrameworkEvent::DeliveryFailed {
            channel_id, error, ..
        } = failed
        else {
            panic!("a delivery failure was expected");
        };
        assert_eq!(
            (channel_id, error.as_str()),
            (channel("ai"), CHANNEL_NOT_REGISTERED)
        );
    }

    #[tokio::test]
    async fn an_unknown_channel_fails() {
        let kit = RoomKit::new();
        assert_eq!(
            kit.unregister_channel(&channel("sms")),
            Err(KitError::UnknownChannel(channel("sms")))
        );
    }

    #[tokio::test]
    async fn frees_the_id_for_another_channel() {
        let (kit, sms, _ws) = room_with_sms_and_ws().await;
        kit.unregister_channel(sms.id()).unwrap();
        let replacement = RecordingTransport::new(channel("sms"), ChannelType::Sms);
        kit.register_channel(replacement.clone()).unwrap();
        kit.process_inbound(message(&replacement, "Back"), Some(&room_id()))
            .await
            .unwrap();
        assert_eq!(timeline_texts(&kit).await, ["Back"]);
    }

    async fn timeline_texts(kit: &RoomKit) -> Vec<String> {
        kit.store()
            .list_events(&room_id(), true)
            .await
            .unwrap()
            .iter()
            .map(|event| roomkit::text_of(&event.content))
            .collect()
    }
}

/// A transport that tags the room on every event it is told of.
#[derive(Clone, Debug)]
struct Tagger {
    declaration: ChannelDeclaration,
}

impl Tagger {
    fn new() -> Self {
        Self {
            declaration: ChannelDeclaration::new(
                channel("crm"),
                ChannelType::Webhook,
                ChannelCategory::Transport,
                ChannelDirection::Inbound,
            ),
        }
    }
}

impl Channel for Tagger {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn on_event(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let mut revoked = Metadata::new();
        revoked.insert("sms".to_owned(), "now".into());
        let mut output = ChannelOutput::default();
        output
            .metadata_updates
            .insert("crm_contact".to_owned(), "c_42".into());
        output
            .metadata_updates
            .insert("roomkit:detached".to_owned(), revoked.into());
        ready(Ok(output))
    }
}

/// Gap G-050: every creation of a room waits for its `ON_ROOM_CREATED`
/// hooks, the integrator's included.
mod creating {
    use super::*;
    use pretty_assertions::assert_eq;

    /// RFC Appendix B.1: the agent a creation hook attaches is bound once
    /// `create_room` returns, however long the hook takes.
    #[tokio::test]
    async fn returns_once_its_on_room_created_hooks_are_over() {
        let kit = RoomKit::new();
        kit.register_channel(EchoAgent::new(channel("ai"), "Hello"))
            .unwrap();
        let attaching = kit.clone();
        kit.observe("welcome", HookTrigger::OnRoomCreated, move |event, _| {
            let kit = attaching.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                kit.attach(&event.room_id, &channel("ai")).await.unwrap();
            }
        })
        .unwrap();
        kit.create_room(&room_id()).await.unwrap();
        let bindings = kit.store().list_bindings(&room_id()).await.unwrap();
        let bound: Vec<&str> = bindings
            .iter()
            .map(|binding| binding.channel_id.as_str())
            .collect();
        assert_eq!(bound, ["ai"]);
    }
}

mod metadata_updates {
    use super::*;
    use pretty_assertions::assert_eq;

    async fn tagged_room() -> roomkit::model::Room {
        let kit = RoomKit::new();
        let sms = RecordingTransport::new(channel("sms"), ChannelType::Sms);
        kit.register_channel(sms.clone()).unwrap();
        kit.register_channel(Tagger::new()).unwrap();
        kit.create_room(&room_id()).await.unwrap();
        kit.attach(&room_id(), sms.id()).await.unwrap();
        kit.attach(&room_id(), &channel("crm")).await.unwrap();
        kit.process_inbound(message(&sms, "Bonjour"), Some(&room_id()))
            .await
            .unwrap();
        kit.store().get_room(&room_id()).await.unwrap().unwrap()
    }

    #[tokio::test]
    async fn a_channel_s_updates_are_written_on_the_room() {
        let room = tagged_room().await;
        let contact = room
            .metadata
            .get("crm_contact")
            .and_then(|value| value.as_str());
        assert_eq!(contact, Some("c_42"));
    }

    #[tokio::test]
    async fn a_reserved_key_is_refused() {
        let room = tagged_room().await;
        assert!(!room.metadata.contains_key("roomkit:detached"));
    }

    #[tokio::test]
    async fn a_room_created_with_a_reserved_key_is_refused() {
        let kit = RoomKit::new();
        let refused = kit
            .create_room_with(&room_id(), |room| {
                room.metadata
                    .insert("roomkit:detached".to_owned(), Metadata::new().into());
            })
            .await;
        assert_eq!(
            refused,
            Err(KitError::Lifecycle(LifecycleError::ReservedMetadataKey(
                "roomkit:detached".to_owned()
            )))
        );
    }
}

mod bindings {
    use super::*;
    use pretty_assertions::assert_eq;

    async fn sms_and_agent() -> (RoomKit, RecordingTransport, EchoAgent) {
        let kit = RoomKit::new();
        let sms = RecordingTransport::new(channel("sms"), ChannelType::Sms);
        let agent = EchoAgent::new(channel("ai"), "Hello back");
        kit.register_channel(sms.clone()).unwrap();
        kit.register_channel(agent.clone()).unwrap();
        kit.create_room(&room_id()).await.unwrap();
        kit.attach(&room_id(), sms.id()).await.unwrap();
        kit.attach(&room_id(), agent.id()).await.unwrap();
        (kit, sms, agent)
    }

    #[tokio::test]
    async fn a_detached_agent_is_asked_nothing_more() {
        let (kit, sms, agent) = sms_and_agent().await;
        kit.detach(&room_id(), agent.id()).await.unwrap();
        kit.process_inbound(message(&sms, "Bonjour"), Some(&room_id()))
            .await
            .unwrap();
        assert_eq!(sms.texts(), Vec::<String>::new());
    }

    #[tokio::test]
    async fn muting_and_unmuting_fire_their_hooks_in_order() {
        let (kit, _sms, agent) = sms_and_agent().await;
        let (seen, mut fired) = mpsc::unbounded_channel();
        for (name, trigger) in [
            ("on_muted", HookTrigger::OnChannelMuted),
            ("on_unmuted", HookTrigger::OnChannelUnmuted),
        ] {
            let seen = seen.clone();
            kit.observe(name, trigger, move |event, _| {
                let seen = seen.clone();
                async move {
                    seen.send(roomkit::text_of(&event.content)).unwrap();
                }
            })
            .unwrap();
        }
        kit.mute(&room_id(), agent.id()).await.unwrap();
        let muted = next(&mut fired).await;
        kit.unmute(&room_id(), agent.id()).await.unwrap();
        let unmuted = next(&mut fired).await;
        assert_eq!(
            [muted, unmuted],
            ["channel muted".to_owned(), "channel unmuted".to_owned()]
        );
    }

    #[tokio::test]
    async fn a_closed_room_reopened_answers_again() {
        let (kit, sms, _agent) = sms_and_agent().await;
        kit.close_room(&room_id()).await.unwrap();
        kit.reopen_room(&room_id()).await.unwrap();
        kit.process_inbound(message(&sms, "Bonjour"), Some(&room_id()))
            .await
            .unwrap();
        assert_eq!(sms.texts(), ["Hello back"]);
    }

    async fn next<T>(receiver: &mut mpsc::UnboundedReceiver<T>) -> T {
        tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap()
    }
}

#[test]
fn the_sweeper_can_be_held_across_tasks() {
    fn sendable<T: Send + Sync + 'static>() {}
    sendable::<roomkit::TimerSweeper>();
}
