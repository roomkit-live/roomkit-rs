//! The reentrant lock and direct injection on the tokio driver (RFC §13.5,
//! §10.5, §9.5.1 Reentrance, §10.1 step 18): code that runs inside a room's
//! own work, a hook under the lock, an off-lock hook, an agent's turn, a
//! transport's delivery, calls into that room and never waits for itself.
//!
//! Every call is bounded: a test fails instead of hanging when a call waits
//! for its own caller.

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "a test fails on a broken fixture"
)]

use std::future::{Future, ready};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    DirectEvent, EventContent, EventDirection, EventSource, EventStatus, EventType, HookResult,
    InboundMessage, RoomContext, RoomEvent, RoomId, RoomStatus, Timestamp,
};
use roomkit::ops::{ChannelDeclaration, FrameworkEvent, InboundError, default_inbound_event};
use roomkit::rules::routing::RouteQuery;
use roomkit::testing::RecordingTransport;
use roomkit::{
    Channel, ChannelError, ConversationStore, EventDraft, HookExecution, HookSpec, HookTrigger,
    InboundRoomRouter, KitError, RoomKit, RouterError, text_of,
};
use tokio::sync::Notify;

/// How long a call that should not wait may take before the test fails it.
const BOUND: Duration = Duration::from_secs(5);

/// Runs the scenario `$scenario` as the test `$single` on a single-threaded
/// runtime, and as `$threaded` on a multi-threaded one, where the calls of
/// one room's work run in parallel.
macro_rules! on_both_runtimes {
    ($single:ident, $threaded:ident, $scenario:ident) => {
        #[tokio::test]
        async fn $single() {
            $scenario().await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $threaded() {
            $scenario().await;
        }
    };
}

type Answer = Pin<Box<dyn Future<Output = Result<ChannelOutput, ChannelError>> + Send>>;
type Call = Arc<dyn Fn(RoomEvent) -> Answer + Send + Sync>;

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

fn message(text: &str) -> InboundMessage {
    InboundMessage::new(channel("sms"), "+15551234567", EventContent::text(text))
}

/// The console's event `text`.
fn from_console(text: &str) -> DirectEvent {
    DirectEvent::new(channel("ws"), EventContent::text(text))
}

/// A SYNC `BEFORE_BROADCAST` hook named `name`, under the lock or off it.
fn before_broadcast(name: &str, needs_lock: bool) -> HookSpec {
    HookSpec {
        needs_lock,
        ..HookSpec::new(name, HookTrigger::BeforeBroadcast, HookExecution::Sync)
    }
}

/// A channel whose parse, turn or delivery runs a scripted call; a
/// scripted parse runs before the default one.
struct Scripted {
    declaration: ChannelDeclaration,
    parse: Option<Call>,
    on_event: Option<Call>,
    deliver: Option<Call>,
}

impl Scripted {
    /// An agent whose turn runs `on_event`.
    fn agent(id: &str, on_event: Call) -> Self {
        Self {
            declaration: ChannelDeclaration::new(
                channel(id),
                ChannelType::Ai,
                ChannelCategory::Intelligence,
                ChannelDirection::Bidirectional,
            ),
            parse: None,
            on_event: Some(on_event),
            deliver: None,
        }
    }

    /// A transport needing no recipient whose delivery runs `deliver`.
    fn transport(id: &str, deliver: Call) -> Self {
        let declaration = ChannelDeclaration::new(
            channel(id),
            ChannelType::Websocket,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        );
        Self {
            declaration: ChannelDeclaration {
                recipient_key: None,
                ..declaration
            },
            parse: None,
            on_event: None,
            deliver: Some(deliver),
        }
    }

    /// The same channel, running `parse` on each message before its default
    /// parse.
    fn parsing(mut self, parse: Call) -> Self {
        self.parse = Some(parse);
        self
    }
}

impl Channel for Scripted {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        _context: RoomContext,
        draft: EventDraft,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        let event = default_inbound_event(
            &message,
            &self.declaration,
            draft.room_id,
            draft.id,
            draft.now,
        );
        let parse = self.parse.clone();
        async move {
            if let Some(parse) = parse {
                parse(event.clone()).await?;
            }
            Ok(event)
        }
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        match &self.on_event {
            Some(call) => call(event),
            None => Box::pin(ready(Ok(ChannelOutput::default()))),
        }
    }

    fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        match &self.deliver {
            Some(call) => call(event),
            None => Box::pin(ready(Ok(ChannelOutput::default()))),
        }
    }
}

/// A kit with the customer's SMS, the advisor's console `ws` and an
/// `audit` transport that records every event of `room_1`, in order.
async fn room() -> (RoomKit, RecordingTransport) {
    let kit = RoomKit::new();
    let audit = RecordingTransport::new(channel("audit"), ChannelType::Websocket);
    kit.register_channel(RecordingTransport::new(channel("sms"), ChannelType::Sms))
        .unwrap();
    kit.register_channel(RecordingTransport::new(
        channel("ws"),
        ChannelType::Websocket,
    ))
    .unwrap();
    kit.register_channel(audit.clone()).unwrap();
    kit.create_room(&room_id()).await.unwrap();
    for id in ["sms", "ws", "audit"] {
        kit.attach(&room_id(), &channel(id)).await.unwrap();
    }
    (kit, audit)
}

/// Runs `message` into `room_1`, failing the test past [`BOUND`].
async fn send(kit: &RoomKit, text: &str) -> Result<roomkit::ops::InboundResult, KitError> {
    tokio::time::timeout(BOUND, kit.process_inbound(message(text), Some(&room_id())))
        .await
        .unwrap_or_else(|_| panic!("the call waited for itself"))
}

/// The room's timeline, BLOCKED records included, as `index source status
/// text` lines.
async fn timeline(kit: &RoomKit) -> Vec<String> {
    kit.store()
        .list_events(&room_id(), true)
        .await
        .unwrap()
        .iter()
        .map(|event| {
            let status = if event.status == EventStatus::Blocked {
                "blocked"
            } else {
                "delivered"
            };
            format!(
                "#{} {} {status} {}",
                event.index,
                event.source.channel_id,
                text_of(&event.content)
            )
        })
        .collect()
}

/// RFC §13.5, §7.5 rule 2: a hook under the lock changes the room through
/// the kit's own calls, which take the lock again at once.
mod a_locked_hook {
    use super::*;
    use pretty_assertions::assert_eq;

    on_both_runtimes!(
        that_mutes_the_source_blocks_the_event,
        that_mutes_the_source_blocks_the_event_on_threads,
        mutes_the_source
    );

    /// The hook mutes the event's source: the event is stored
    /// `source_muted`.
    async fn mutes_the_source() {
        let (kit, _) = room().await;
        let hook_kit = kit.clone();
        kit.hook(before_broadcast("moderator", true), move |_, _| {
            let kit = hook_kit.clone();
            async move {
                kit.mute(&room_id(), &channel("sms")).await?;
                Ok(HookResult::allow())
            }
        })
        .unwrap();
        let result = send(&kit, "hello").await.unwrap();
        assert_eq!(
            (result.reason, timeline(&kit).await),
            (
                Some("source_muted".to_owned()),
                vec!["#0 sms blocked hello".to_owned()]
            )
        );
    }

    /// The hook closes the room: the event it decides on is refused, as a
    /// closed room refuses any (RFC §5.1, gap G-070).
    #[tokio::test]
    async fn that_closes_the_room_refuses_the_event() {
        let (kit, _) = room().await;
        let mut events = kit.subscribe();
        let hook_kit = kit.clone();
        kit.hook(before_broadcast("farewell", true), move |_, _| {
            let kit = hook_kit.clone();
            async move {
                kit.close_room(&room_id()).await?;
                Ok(HookResult::allow())
            }
        })
        .unwrap();
        let result = send(&kit, "hello").await.unwrap();
        let mut names = Vec::new();
        while let Ok(event) = events.try_recv() {
            if matches!(event, FrameworkEvent::RoomClosed { .. }) {
                names.push("room_closed");
            } else if matches!(event, FrameworkEvent::RoomRefusedEvent { .. }) {
                names.push("room_refused_event");
            }
        }
        assert_eq!(
            (result.reason, timeline(&kit).await.len(), names),
            (
                Some("room_closed".to_owned()),
                0,
                vec!["room_closed", "room_refused_event"]
            )
        );
    }

    on_both_runtimes!(
        that_injects_commits_its_event_ahead,
        that_injects_commits_its_event_ahead_on_threads,
        injects_ahead
    );

    /// The hook injects into its own room: its event commits ahead of the
    /// one the hook decides on, which takes the next index, and the hook
    /// receives it at once. The delivery sets keep index order.
    async fn injects_ahead() {
        let (kit, audit) = room().await;
        let seen = Arc::new(Mutex::new(None));
        let (hook_kit, hook_seen) = (kit.clone(), Arc::clone(&seen));
        kit.hook(
            before_broadcast("notifier", true),
            move |event: RoomEvent, _| {
                let (kit, seen) = (hook_kit.clone(), Arc::clone(&hook_seen));
                async move {
                    if event.source.channel_id == channel("sms") {
                        let injected = kit.send_event(&room_id(), from_console("Heads up")).await?;
                        *seen.lock().unwrap() = Some(injected.index);
                    }
                    Ok(HookResult::allow())
                }
            },
        )
        .unwrap();
        let result = send(&kit, "hello").await.unwrap();
        assert_eq!(
            (
                *seen.lock().unwrap(),
                result.event.map(|event| event.index),
                audit.texts()
            ),
            (
                Some(0),
                Some(1),
                vec!["Heads up".to_owned(), "hello".to_owned()]
            )
        );
    }

    /// The hook injects twice at once: the two entries run one after the
    /// other within the hook's section, each with its own index.
    #[tokio::test]
    async fn that_injects_twice_at_once_commits_both_ahead() {
        let (kit, audit) = room().await;
        let hook_kit = kit.clone();
        kit.hook(
            before_broadcast("notifier", true),
            move |event: RoomEvent, _| {
                let kit = hook_kit.clone();
                async move {
                    if event.source.channel_id == channel("sms") {
                        let room = room_id();
                        let (first, second) = tokio::join!(
                            kit.send_event(&room, from_console("First")),
                            kit.send_event(&room, from_console("Second"))
                        );
                        first?;
                        second?;
                    }
                    Ok(HookResult::allow())
                }
            },
        )
        .unwrap();
        let result = send(&kit, "hello").await.unwrap();
        let mut ahead = audit.texts();
        ahead.truncate(2);
        ahead.sort();
        assert_eq!(
            (
                result.event.map(|event| event.index),
                ahead,
                timeline(&kit).await.len()
            ),
            (Some(2), vec!["First".to_owned(), "Second".to_owned()], 3)
        );
    }

    /// RFC §10.5 `commit_event` under the lock: the record takes the next
    /// index, ahead of the event the hook decides on, and reaches no one.
    #[tokio::test]
    async fn that_commits_a_record_leaves_the_event_the_next_index() {
        let (kit, audit) = room().await;
        let hook_kit = kit.clone();
        kit.hook(
            before_broadcast("tracer", true),
            move |event: RoomEvent, _| {
                let kit = hook_kit.clone();
                async move {
                    let mut record = event.clone();
                    record.id = "trace_1".parse()?;
                    record.content = EventContent::text("trace");
                    kit.commit_event(&room_id(), record, None).await?;
                    Ok(HookResult::allow())
                }
            },
        )
        .unwrap();
        let result = send(&kit, "hello").await.unwrap();
        assert_eq!(
            (
                result.event.map(|event| event.index),
                timeline(&kit).await,
                audit.texts()
            ),
            (
                Some(1),
                vec![
                    "#0 sms delivered trace".to_owned(),
                    "#1 sms delivered hello".to_owned()
                ],
                vec!["hello".to_owned()]
            )
        );
    }

    /// The lock taken again by the hook's own call is let go with the
    /// section's release only: a call from outside, which holds no lease,
    /// waits for the section, so the event is decided before the room
    /// closes.
    #[tokio::test]
    async fn keeps_the_lock_after_its_call_releases_it() {
        let (kit, _) = room().await;
        let muted = Arc::new(Notify::new());
        let (hook_kit, hook_muted) = (kit.clone(), Arc::clone(&muted));
        kit.hook(before_broadcast("moderator", true), move |_, _| {
            let (kit, muted) = (hook_kit.clone(), Arc::clone(&hook_muted));
            async move {
                kit.mute(&room_id(), &channel("sms")).await?;
                muted.notify_one();
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(HookResult::allow())
            }
        })
        .unwrap();
        let outside = {
            let (kit, muted) = (kit.clone(), Arc::clone(&muted));
            tokio::spawn(async move {
                muted.notified().await;
                kit.close_room(&room_id()).await.unwrap();
            })
        };
        let result = send(&kit, "hello").await.unwrap();
        tokio::time::timeout(BOUND, outside).await.unwrap().unwrap();
        assert_eq!(
            (result.reason, timeline(&kit).await),
            (
                Some("source_muted".to_owned()),
                vec!["#0 sms blocked hello".to_owned()]
            )
        );
    }
}

/// RFC §9.5.1 Reentrance: an off-lock hook's injection takes no ticket, so
/// it waits for none its own call holds, and commits ahead.
#[tokio::test]
async fn an_off_lock_hook_injects_without_a_ticket() {
    let (kit, audit) = room().await;
    let hook_kit = kit.clone();
    kit.hook(
        before_broadcast("scanner", false),
        move |event: RoomEvent, _| {
            let kit = hook_kit.clone();
            async move {
                if event.source.channel_id == channel("sms") {
                    kit.send_event(&room_id(), from_console("Scanned")).await?;
                }
                Ok(HookResult::allow())
            }
        },
    )
    .unwrap();
    let result = send(&kit, "hello").await.unwrap();
    assert_eq!(
        (result.event.map(|event| event.index), audit.texts()),
        (Some(1), vec!["Scanned".to_owned(), "hello".to_owned()])
    );
}

on_both_runtimes!(
    an_agent_s_turn_injects_into_its_own_room_and_receives_its_event_at_once,
    an_agent_s_turn_injects_into_its_own_room_on_threads,
    agent_injects
);

/// RFC §10.1 step 18: an agent's turn injects into its own room and
/// receives its event at once, at the commit; the event's delivery follows
/// in the lane, after the turn's own set.
async fn agent_injects() {
    let (kit, audit) = room().await;
    let seen = Arc::new(Mutex::new(None));
    let (turn_kit, turn_seen, turn_audit) = (kit.clone(), Arc::clone(&seen), audit.clone());
    let agent = Scripted::agent(
        "ai",
        Arc::new(move |event: RoomEvent| {
            let (kit, seen, audit) = (turn_kit.clone(), Arc::clone(&turn_seen), turn_audit.clone());
            Box::pin(async move {
                if event.source.channel_id == channel("sms") {
                    let mut thought =
                        DirectEvent::new(channel("ai"), EventContent::text("Let me check."));
                    thought.event_type = EventType::Message;
                    let injected = kit.send_event(&room_id(), thought).await?;
                    let delivered_yet = audit.texts().contains(&"Let me check.".to_owned());
                    *seen.lock().unwrap() = Some((
                        injected.index,
                        injected.status,
                        injected.source.direction,
                        delivered_yet,
                    ));
                }
                Ok(ChannelOutput::default())
            })
        }),
    );
    kit.register_channel(agent).unwrap();
    kit.attach(&room_id(), &channel("ai")).await.unwrap();
    send(&kit, "hello").await.unwrap();
    assert_eq!(
        (*seen.lock().unwrap(), audit.texts()),
        (
            Some((1, EventStatus::Delivered, EventDirection::Outbound, false)),
            vec!["hello".to_owned(), "Let me check.".to_owned()]
        )
    );
}

/// RFC §10.1, §13.5: a channel's call to `process_inbound` for its own room
/// is adopted, returns at its commit with no delivery result, and its
/// delivery follows in the lane.
#[tokio::test]
async fn process_inbound_from_a_channel_into_its_own_room_returns_at_its_commit() {
    let (kit, audit) = room().await;
    let seen = Arc::new(Mutex::new(None));
    let (bot_kit, bot_seen) = (kit.clone(), Arc::clone(&seen));
    let bot = Scripted::transport(
        "bot",
        Arc::new(move |event: RoomEvent| {
            let (kit, seen) = (bot_kit.clone(), Arc::clone(&bot_seen));
            Box::pin(async move {
                if text_of(&event.content) == "hello" {
                    let follow_up = message("follow-up");
                    let result = kit.process_inbound(follow_up, Some(&room_id())).await?;
                    *seen.lock().unwrap() = Some((
                        result.event.map(|event| event.index),
                        result.delivery_results.len(),
                    ));
                }
                Ok(ChannelOutput::default())
            })
        }),
    );
    kit.register_channel(bot).unwrap();
    kit.attach(&room_id(), &channel("bot")).await.unwrap();
    send(&kit, "hello").await.unwrap();
    assert_eq!(
        (*seen.lock().unwrap(), audit.texts()),
        (
            Some((Some(1), 0)),
            vec!["hello".to_owned(), "follow-up".to_owned()]
        )
    );
}

/// RFC §13.5: the lock is reentrant within one execution context only.
mod a_context {
    use super::*;
    use pretty_assertions::assert_eq;

    /// A task a hook spawns without awaiting it as its own runs outside the
    /// hook's context: its call waits for the lock until the section is
    /// over, and the event is decided before the room closes.
    #[tokio::test]
    async fn is_not_handed_to_a_task_a_hook_spawns() {
        let (kit, _) = room().await;
        let spawned = Arc::new(Mutex::new(None));
        let (hook_kit, hook_spawned) = (kit.clone(), Arc::clone(&spawned));
        kit.hook(
            before_broadcast("closer", true),
            move |event: RoomEvent, _| {
                let (kit, spawned) = (hook_kit.clone(), Arc::clone(&hook_spawned));
                async move {
                    if event.source.channel_id == channel("sms") {
                        let task = tokio::spawn(async move { kit.close_room(&room_id()).await });
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        *spawned.lock().unwrap() = Some(task);
                    }
                    Ok(HookResult::allow())
                }
            },
        )
        .unwrap();
        let result = send(&kit, "hello").await.unwrap();
        let task = spawned.lock().unwrap().take().unwrap();
        let closed = tokio::time::timeout(BOUND, task).await.unwrap().unwrap();
        assert_eq!(
            (
                result.event.map(|event| event.status),
                closed.map(|room| room.status).ok()
            ),
            (Some(EventStatus::Delivered), Some(RoomStatus::Closed))
        );
    }

    on_both_runtimes!(
        after_the_release_does_not_bypass_the_lock,
        after_the_release_does_not_bypass_the_lock_on_threads,
        does_not_bypass
    );

    /// An agent's turn runs after its trigger's section released the lock:
    /// its context holds no lease, so its call waits for the section an
    /// entry it adopted holds, rather than run within it. The entry is
    /// decided on the room as it was, then the room closes.
    async fn does_not_bypass() {
        let (kit, _) = room().await;
        let in_hook = Arc::new(Notify::new());
        let hook_entered = Arc::clone(&in_hook);
        kit.hook(
            before_broadcast("slow_policy", true),
            move |event: RoomEvent, _| {
                let entered = Arc::clone(&hook_entered);
                async move {
                    if event.source.channel_id == channel("ai") {
                        entered.notify_one();
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Ok(HookResult::allow())
                }
            },
        )
        .unwrap();
        let outcome = Arc::new(Mutex::new(None));
        let (turn_kit, turn_outcome, turn_in_hook) =
            (kit.clone(), Arc::clone(&outcome), Arc::clone(&in_hook));
        let agent = Scripted::agent(
            "ai",
            Arc::new(move |event: RoomEvent| {
                let (kit, outcome, in_hook) = (
                    turn_kit.clone(),
                    Arc::clone(&turn_outcome),
                    Arc::clone(&turn_in_hook),
                );
                Box::pin(async move {
                    if event.source.channel_id != channel("sms") {
                        return Ok(ChannelOutput::default());
                    }
                    let thought = DirectEvent::new(channel("ai"), EventContent::text("Noted."));
                    let room = room_id();
                    let close = async {
                        in_hook.notified().await;
                        kit.close_room(&room).await
                    };
                    let (injected, closed) = tokio::join!(kit.send_event(&room, thought), close);
                    *outcome.lock().unwrap() = Some((
                        injected.map(|event| event.status).ok(),
                        closed.map(|room| room.status).ok(),
                    ));
                    Ok(ChannelOutput::default())
                })
            }),
        );
        kit.register_channel(agent).unwrap();
        kit.attach(&room_id(), &channel("ai")).await.unwrap();
        send(&kit, "hello").await.unwrap();
        assert_eq!(
            *outcome.lock().unwrap(),
            Some((Some(EventStatus::Delivered), Some(RoomStatus::Closed)))
        );
    }
}

/// Gap G-050, RFC §13.5: an `ON_ROOM_CREATED` hook step 2 awaits runs
/// within the call that created the room: its injection is adopted, and
/// commits ahead of the message that created the room.
#[tokio::test]
async fn an_awaited_creation_hook_injects_into_the_room_being_created() {
    let (kit, _) = room().await;
    let new_room = RoomId::new("room_2").unwrap();
    let observer_kit = kit.clone();
    kit.observe(
        "welcome",
        HookTrigger::OnRoomCreated,
        move |event: RoomEvent, _| {
            let kit = observer_kit.clone();
            async move {
                kit.attach(&event.room_id, &channel("ws")).await.unwrap();
                kit.send_event(&event.room_id, from_console("Welcome"))
                    .await
                    .unwrap();
            }
        },
    )
    .unwrap();
    let created = kit.process_inbound(message("hello"), Some(&new_room));
    let result = tokio::time::timeout(BOUND, created)
        .await
        .unwrap_or_else(|_| panic!("the creation hook waited for its own call"))
        .unwrap();
    let texts: Vec<(u64, String)> = kit
        .store()
        .list_events(&new_room, true)
        .await
        .unwrap()
        .iter()
        .map(|event| (event.index, text_of(&event.content)))
        .collect();
    assert_eq!(
        (result.event.map(|event| event.index), texts),
        (
            Some(1),
            vec![(0, "Welcome".to_owned()), (1, "hello".to_owned())]
        )
    );
}

/// RFC §13.5: the lock is reentrant all the way down a call chain: a
/// locked hook sends a message into its room, which is nested in the hook's
/// section, and the parse of that message injects into the room too, nested
/// in turn. Each commits ahead of the event whose section it runs in.
#[tokio::test]
async fn an_entry_made_by_a_nested_entry_s_parse_is_nested_in_turn() {
    let (kit, _) = room().await;
    let parse_kit = kit.clone();
    let bot = Scripted::transport(
        "bot",
        Arc::new(|_| Box::pin(ready(Ok(ChannelOutput::default())))),
    )
    .parsing(Arc::new(move |_| {
        let kit = parse_kit.clone();
        Box::pin(async move {
            kit.send_event(&room_id(), from_console("Parsed")).await?;
            Ok(ChannelOutput::default())
        })
    }));
    kit.register_channel(bot).unwrap();
    kit.attach(&room_id(), &channel("bot")).await.unwrap();
    let hook_kit = kit.clone();
    kit.hook(
        before_broadcast("relay", true),
        move |event: RoomEvent, _| {
            let kit = hook_kit.clone();
            async move {
                if event.source.channel_id == channel("sms") {
                    let relayed =
                        InboundMessage::new(channel("bot"), "relay", EventContent::text("Relayed"));
                    kit.process_inbound(relayed, Some(&room_id())).await?;
                }
                Ok(HookResult::allow())
            }
        },
    )
    .unwrap();
    send(&kit, "hello").await.unwrap();
    assert_eq!(
        timeline(&kit).await,
        [
            "#0 ws delivered Parsed",
            "#1 bot delivered Relayed",
            "#2 sms delivered hello"
        ]
    );
}

/// RFC §13.5: a task the turn spawns and awaits as its own keeps the turn's
/// context when its future is wrapped with `keep_context`: its injection is
/// adopted, and returns at its commit.
#[tokio::test]
async fn a_task_a_turn_awaits_as_its_own_keeps_the_turn_s_context() {
    let (kit, audit) = room().await;
    let seen = Arc::new(Mutex::new(None));
    let (turn_kit, turn_seen) = (kit.clone(), Arc::clone(&seen));
    let agent = Scripted::agent(
        "ai",
        Arc::new(move |event: RoomEvent| {
            let (kit, seen) = (turn_kit.clone(), Arc::clone(&turn_seen));
            Box::pin(async move {
                if event.source.channel_id != channel("sms") {
                    return Ok(ChannelOutput::default());
                }
                let tool = tokio::spawn(roomkit::keep_context(async move {
                    let thought = DirectEvent::new(channel("ai"), EventContent::text("Looked up."));
                    kit.send_event(&room_id(), thought).await
                }));
                let injected = tool.await??;
                *seen.lock().unwrap() = Some(injected.index);
                Ok(ChannelOutput::default())
            })
        }),
    );
    kit.register_channel(agent).unwrap();
    kit.attach(&room_id(), &channel("ai")).await.unwrap();
    send(&kit, "hello").await.unwrap();
    assert_eq!(
        (*seen.lock().unwrap(), audit.texts()),
        (Some(1), vec!["hello".to_owned(), "Looked up.".to_owned()])
    );
}

/// RFC §8.3: a channel whose parse sends every message it parses back into
/// its room is bounded by the reentry budget: each message is charged as it
/// arrives, before its parse, and the one past the budget is refused.
#[tokio::test]
async fn a_parse_that_calls_into_its_room_for_every_message_is_bounded() {
    let (kit, _) = room().await;
    kit.set_max_chain_depth(1);
    let refused = Arc::new(Mutex::new(Vec::new()));
    let (relay_kit, relay_refused) = (kit.clone(), Arc::clone(&refused));
    let relay = Scripted::transport(
        "relay",
        Arc::new(|_| Box::pin(ready(Ok(ChannelOutput::default())))),
    )
    .parsing(Arc::new(move |_| {
        let (kit, refused) = (relay_kit.clone(), Arc::clone(&relay_refused));
        Box::pin(async move {
            let again = InboundMessage::new(channel("relay"), "relay", EventContent::text("again"));
            let result = kit.process_inbound(again, Some(&room_id())).await?;
            if result.event.is_none() {
                refused.lock().unwrap().push(result.reason);
            }
            Ok(ChannelOutput::default())
        })
    }));
    kit.register_channel(relay).unwrap();
    kit.attach(&room_id(), &channel("relay")).await.unwrap();
    let first = InboundMessage::new(channel("relay"), "relay", EventContent::text("again"));
    tokio::time::timeout(BOUND, kit.process_inbound(first, Some(&room_id())))
        .await
        .unwrap_or_else(|_| panic!("the parse recursed past the budget"))
        .unwrap();
    let stored = timeline(&kit).await.len();
    assert_eq!(
        (refused.lock().unwrap().clone(), stored),
        (vec![Some("reentry_loop_cap".to_owned())], 11)
    );
}

/// A call dropped while an entry its hook made works under its lock leaves
/// the room free: the lock and the lane are let go, and the next call goes
/// through.
#[tokio::test]
async fn a_call_dropped_while_its_entry_works_leaves_the_room_free() {
    let (kit, _) = room().await;
    let hook_kit = kit.clone();
    kit.hook(
        before_broadcast("stuck", true),
        move |event: RoomEvent, _| {
            let kit = hook_kit.clone();
            async move {
                if text_of(&event.content) == "first" {
                    kit.send_event(&room_id(), from_console("Heads up")).await?;
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                }
                Ok(HookResult::allow())
            }
        },
    )
    .unwrap();
    let dropped = tokio::time::timeout(
        Duration::from_millis(50),
        kit.process_inbound(message("first"), Some(&room_id())),
    )
    .await;
    let next = send(&kit, "second").await.unwrap();
    assert_eq!(
        (dropped.is_err(), next.event.map(|event| event.status)),
        (true, Some(EventStatus::Delivered))
    );
}

/// RFC §13.6: the call's lock request, held back while an entry its
/// off-lock hook made holds the lock, is given up on once the call's
/// `process_timeout` passed meanwhile, not granted.
#[tokio::test]
async fn a_lock_request_held_back_past_the_deadline_is_given_up_on() {
    let (kit, _) = room().await;
    kit.set_process_timeout(Some(Duration::from_millis(100)));
    kit.hook(
        before_broadcast("slow", true),
        |event: RoomEvent, _| async move {
            if text_of(&event.content) == "Heads up" {
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            Ok(HookResult::allow())
        },
    )
    .unwrap();
    let hook_kit = kit.clone();
    kit.hook(
        before_broadcast("notifier", false),
        move |event: RoomEvent, _| {
            let kit = hook_kit.clone();
            async move {
                if event.source.channel_id == channel("sms") {
                    // Fired and not awaited: the entry runs beside the check.
                    drop(tokio::spawn(roomkit::keep_context(async move {
                        kit.send_event(&room_id(), from_console("Heads up")).await
                    })));
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Ok(HookResult::allow())
            }
        },
    )
    .unwrap();
    let result = send(&kit, "hello").await.unwrap();
    assert_eq!(
        (result.reason, timeline(&kit).await),
        (
            Some("process_timeout".to_owned()),
            vec!["#0 ws delivered Heads up".to_owned()]
        )
    );
}

/// RFC §9.3, §13.5: a hook that times out while the entry it made still
/// works under its lock is heard only once the entry is settled: the entry
/// commits and is delivered, then the event the hook decided on, the hook
/// failing open.
#[tokio::test]
async fn a_hook_that_times_out_while_its_entry_works_is_heard_after_it() {
    let (kit, audit) = room().await;
    kit.hook(
        before_broadcast("slow", true),
        |event: RoomEvent, _| async move {
            if text_of(&event.content) == "Heads up" {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Ok(HookResult::allow())
        },
    )
    .unwrap();
    let hook_kit = kit.clone();
    let spec = HookSpec {
        timeout: Duration::from_millis(50),
        ..before_broadcast("notifier", true)
    };
    kit.hook(spec, move |event: RoomEvent, _| {
        let kit = hook_kit.clone();
        async move {
            if event.source.channel_id == channel("sms") {
                kit.send_event(&room_id(), from_console("Heads up")).await?;
            }
            Ok(HookResult::allow())
        }
    })
    .unwrap();
    let result = send(&kit, "hello").await.unwrap();
    assert_eq!(
        (
            result.event.map(|event| (event.index, event.status)),
            audit.texts()
        ),
        (
            Some((1, EventStatus::Delivered)),
            vec!["Heads up".to_owned(), "hello".to_owned()]
        )
    );
}

/// Routes every message to `room_1`.
struct ToRoom1;

impl<S: ConversationStore> InboundRoomRouter<S> for ToRoom1 {
    fn route(
        &self,
        _store: &S,
        _query: &RouteQuery,
    ) -> impl Future<Output = Result<Option<RoomId>, RouterError>> + Send {
        ready(Ok(Some(room_id())))
    }
}

/// RFC §13.5: a call that could only wait for what its own caller holds, a
/// message routed back into the room whose lane the caller's call holds,
/// fails instead of hanging.
#[tokio::test]
async fn a_call_that_could_only_wait_for_itself_fails() {
    let (kit, _) = room().await;
    kit.set_router(ToRoom1);
    let refused = Arc::new(Mutex::new(None));
    let (hook_kit, hook_refused) = (kit.clone(), Arc::clone(&refused));
    kit.hook(
        before_broadcast("forwarder", true),
        move |event: RoomEvent, _| {
            let (kit, refused) = (hook_kit.clone(), Arc::clone(&hook_refused));
            async move {
                if text_of(&event.content) == "hello" {
                    let routed = kit.process_inbound(message("forwarded"), None).await;
                    *refused.lock().unwrap() = Some(routed.err());
                }
                Ok(HookResult::allow())
            }
        },
    )
    .unwrap();
    let result = send(&kit, "hello").await.unwrap();
    assert_eq!(
        (
            refused.lock().unwrap().clone(),
            result.event.map(|event| event.index)
        ),
        (
            Some(Some(KitError::Reentrant {
                room_id: room_id(),
                call: "process_inbound",
            })),
            Some(0)
        )
    );
}

/// RFC §10.5: from outside, `send_event` waits for its delivery set and
/// returns the committed event; a refusal before the commit is raised.
mod send_event {
    use super::*;
    use pretty_assertions::assert_eq;

    #[tokio::test]
    async fn returns_the_committed_event_once_delivered() {
        let (kit, audit) = room().await;
        let event = tokio::time::timeout(BOUND, kit.send_event(&room_id(), from_console("Hi")))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (event.index, event.status, audit.texts()),
            (0, EventStatus::Delivered, vec!["Hi".to_owned()])
        );
    }

    #[tokio::test]
    async fn into_a_closed_room_is_refused() {
        let (kit, _) = room().await;
        kit.close_room(&room_id()).await.unwrap();
        let refused = kit.send_event(&room_id(), from_console("Hi")).await;
        assert_eq!(
            refused,
            Err(KitError::Inbound(InboundError::Refused {
                reason: "room_closed".to_owned()
            }))
        );
    }
}

/// RFC §10.5: `commit_event` commits a record no one receives.
#[tokio::test]
async fn commit_event_reaches_no_channel() {
    let (kit, audit) = room().await;
    let source = EventSource::new(
        channel("ws"),
        ChannelType::Websocket,
        EventDirection::Inbound,
    );
    let record = RoomEvent::new(
        "snap_1".parse().unwrap(),
        room_id(),
        EventType::Message,
        source,
        EventContent::text("Snapshot"),
        Timestamp::UNIX_EPOCH,
    );
    let committed = kit.commit_event(&room_id(), record, None).await.unwrap();
    assert_eq!(
        (
            committed.index,
            audit.texts().len(),
            timeline(&kit).await.len()
        ),
        (0, 0, 1)
    );
}
