//! The body of an ASYNC hook a cascade fires once an event's delivery set
//! has run (`AFTER_BROADCAST`, a change's `ON_EVENT_UPDATED` and
//! `ON_EVENT_DELETED`), and of an ASYNC hook of `BEFORE_BROADCAST`, which
//! observes the event its chain allowed, is that cascade's work while it
//! runs (RFC §8.3, §9.2, §9.5.1,
//! §10.1 step 16, §10.3, RMK-954): a call it makes into the room draws one
//! of the cascade's hook calls, a counter kept apart from its passes; a
//! direct change it makes draws one too, refused past them; a call it makes
//! is at least one deeper than its event; a task it starts calls as work of
//! its own once the body has returned.
//!
//! Each looping hook stops itself well past the cap, so that a loop the cap
//! does not stop fails at once rather than run forever.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::{Future, ready};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    DeleteContent, DirectEvent, EditContent, EventContent, EventDirection, EventId, EventSource,
    EventType, EventUpdates, HookResult, InboundMessage, ParticipantId, Post, RoomContext,
    RoomEvent, RoomId,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    Channel, ChannelDeclaration, ChannelError, ConversationStore, HookTrigger, InboundResult,
    InboundRoomRouter, KitError, MutationError, RoomKit, RoomKitOptions, RouteOptions,
    RouterFuture, keep_context, text_of,
};

/// `max_chain_depth` 1: ten passes and ten hook calls per cascade.
const BUDGET: usize = 10;

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("r1").unwrap()
}

/// What a test collects from the hooks it runs.
type Kept<T> = Arc<Mutex<Vec<T>>>;

fn kept<T>() -> Kept<T> {
    Arc::default()
}

fn push<T>(kept: &Kept<T>, item: T) {
    kept.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(item);
}

fn len<T>(kept: &Kept<T>) -> usize {
    kept.lock().unwrap_or_else(PoisonError::into_inner).len()
}

fn taken<T>(kept: &Kept<T>) -> Vec<T> {
    std::mem::take(&mut *kept.lock().unwrap_or_else(PoisonError::into_inner))
}

/// Room `r1` with an SMS, its kit's chain-depth limit `max_chain_depth`.
async fn room(max_chain_depth: u32) -> RoomKit {
    room_with(RoomKitOptions {
        max_chain_depth,
        ..Default::default()
    })
    .await
}

/// [`room`] on a kit built with `options`.
async fn room_with(options: RoomKitOptions) -> RoomKit {
    let kit = RoomKit::new(options);
    kit.register_channel(RecordingTransport::new(channel("sms"), ChannelType::SMS))
        .unwrap();
    kit.create_room(&room_id()).await.unwrap();
    kit.attach(&room_id(), &channel("sms")).await.unwrap();
    kit
}

fn message(text: &str) -> InboundMessage {
    InboundMessage::new(channel("sms"), "+15551234567", EventContent::text(text))
}

/// The customer's `text` from the SMS, sent as `u`.
fn from_u(content: EventContent, event_type: EventType) -> DirectEvent {
    DirectEvent {
        event_type,
        participant_id: Some(ParticipantId::new("u").unwrap()),
        ..DirectEvent::new(channel("sms"), content)
    }
}

/// An agent that answers "Answer." to every event but its own, and counts
/// what it is asked.
#[derive(Clone)]
struct Answering {
    id: ChannelId,
    asked: Kept<RoomEvent>,
}

impl Answering {
    fn new(id: &str) -> Self {
        Self {
            id: channel(id),
            asked: kept(),
        }
    }
}

impl Channel for Answering {
    fn declaration(&self) -> ChannelDeclaration {
        ChannelDeclaration::new(
            self.id.clone(),
            ChannelType::AI,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
        )
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        if event.source.channel_id == self.id {
            return ready(Ok(ChannelOutput::default()));
        }
        push(&self.asked, event.clone());
        let answer = RoomEvent::new(
            event.id.clone(),
            event.room_id.clone(),
            EventType::Message,
            EventSource::new(self.id.clone(), ChannelType::AI, EventDirection::Outbound),
            EventContent::text("Answer."),
            event.created_at,
        );
        ready(Ok(ChannelOutput::respond(vec![answer])))
    }
}

/// RFC §9.5.1, §10.1 step 16: no agent in the room, the hook's own post
/// fires it again: it stops at the cap of its cascade's hook calls.
#[tokio::test]
async fn a_hook_posting_after_every_event_stops_at_the_cap() {
    let kit = room(1).await;
    let posted: Kept<Option<String>> = kept();
    let (hook_kit, hook_posted) = (kit.clone(), Arc::clone(&posted));
    kit.observe("notice", HookTrigger::AfterBroadcast, move |event, _| {
        let (kit, posted) = (hook_kit.clone(), Arc::clone(&hook_posted));
        async move {
            if event.event_type == EventType::Message && len(&posted) <= 2 * BUDGET {
                let notice = Post::new(EventContent::text(format!("Noted {}.", len(&posted))));
                let event = kit.post(&room_id(), notice).await.unwrap();
                push(&posted, event.blocked_by);
            }
        }
    })
    .unwrap();
    kit.process_inbound_in(&room_id(), message("hi"))
        .await
        .unwrap();
    kit.idle().await;
    let mut expected = vec![None; BUDGET];
    expected.push(Some("reentry_loop_cap".to_owned()));
    assert_eq!(taken(&posted), expected);
}

/// RFC §9.2, §9.5.1, decided for step 3 milestone 2: an ASYNC hook of a
/// SYNC trigger, `BEFORE_BROADCAST`, observes the event its chain allowed
/// as the cascade's work: its own post fires it again, and it stops at the
/// cap of its cascade's hook calls, as an `AFTER_BROADCAST` body does. It
/// posted without end (11,364 events in 2 s, the review's probe).
#[tokio::test]
async fn an_observer_of_before_broadcast_posting_on_every_event_stops_at_the_cap() {
    let kit = room(1).await;
    let posted: Kept<Option<String>> = kept();
    let (hook_kit, hook_posted) = (kit.clone(), Arc::clone(&posted));
    kit.observe("notice", HookTrigger::BeforeBroadcast, move |event, _| {
        let (kit, posted) = (hook_kit.clone(), Arc::clone(&hook_posted));
        async move {
            if event.event_type == EventType::Message && len(&posted) <= 2 * BUDGET {
                let notice = Post::new(EventContent::text(format!("Noted {}.", len(&posted))));
                let event = kit.post(&room_id(), notice).await.unwrap();
                push(&posted, event.blocked_by);
            }
        }
    })
    .unwrap();
    kit.process_inbound_in(&room_id(), message("hi"))
        .await
        .unwrap();
    kit.idle().await;
    let mut expected = vec![None; BUDGET];
    expected.push(Some("reentry_loop_cap".to_owned()));
    assert_eq!(taken(&posted), expected);
}

/// RFC §9.2, §10.1 step 18, the light review of step 3 milestone 2: the
/// call an ASYNC hook of `BEFORE_BROADCAST` makes into its room returns once
/// its delivery set has run, with its delivery results, as the Python
/// implementation's does (`py_observer_return.py`: 202 ms, `['other']`):
/// nothing waits for the observer, so its call waits for nothing it holds.
/// It returned at its decision, no delivery result in it.
#[tokio::test]
async fn an_observer_of_before_broadcast_s_call_returns_with_its_delivery_results() {
    let kit = room(5).await;
    kit.register_channel(RecordingTransport::new(
        channel("ws"),
        ChannelType::WEBSOCKET,
    ))
    .unwrap();
    kit.attach(&room_id(), &channel("ws")).await.unwrap();
    let results: Kept<Vec<String>> = kept();
    let (hook_kit, hook_results) = (kit.clone(), Arc::clone(&results));
    kit.observe("watcher", HookTrigger::BeforeBroadcast, move |event, _| {
        let (kit, results) = (hook_kit.clone(), Arc::clone(&hook_results));
        async move {
            if text_of(&event.content) == "hi" {
                let again = kit.process_inbound_in(&room_id(), message("again")).await;
                let delivered = again
                    .unwrap()
                    .delivery_results
                    .keys()
                    .map(ToString::to_string)
                    .collect();
                push(&results, delivered);
            }
        }
    })
    .unwrap();
    kit.process_inbound_in(&room_id(), message("hi"))
        .await
        .unwrap();
    kit.idle().await;
    assert_eq!(taken(&results), vec![vec!["ws".to_owned()]]);
}

/// A router that opens a room for every message.
struct NoRoom;

impl InboundRoomRouter for NoRoom {
    fn route<'a>(
        &'a self,
        _channel_id: &'a ChannelId,
        _channel_type: &'a ChannelType,
        _options: RouteOptions,
    ) -> RouterFuture<'a> {
        Box::pin(ready(Ok(None)))
    }
}

/// RFC §9.5.1: a message the hook sends that opens a room is the
/// cascade's work too, charged before the room is created: a hook that
/// opens a room after every event, each room's message firing it again,
/// stops at the cap.
#[tokio::test]
async fn a_hook_opening_a_room_after_every_event_stops_at_the_cap() {
    let kit = room_with(RoomKitOptions {
        max_chain_depth: 1,
        inbound_router: Some(Arc::new(NoRoom)),
        ..Default::default()
    })
    .await;
    let answers: Kept<Option<String>> = kept();
    let calls: Kept<()> = kept();
    let (hook_kit, hook_answers, hook_calls) =
        (kit.clone(), Arc::clone(&answers), Arc::clone(&calls));
    kit.observe("open", HookTrigger::AfterBroadcast, move |event, _| {
        let (kit, answers, calls) = (
            hook_kit.clone(),
            Arc::clone(&hook_answers),
            Arc::clone(&hook_calls),
        );
        async move {
            if event.event_type == EventType::Message && len(&calls) <= 2 * BUDGET {
                push(&calls, ());
                let result: InboundResult = kit.process_inbound(message("again")).await.unwrap();
                push(&answers, result.reason);
            }
        }
    })
    .unwrap();
    kit.process_inbound_in(&room_id(), message("hi"))
        .await
        .unwrap();
    kit.idle().await;
    let rooms = kit
        .store()
        .list_rooms(&roomkit::RoomFilter::default())
        .await
        .unwrap();
    let refused = taken(&answers)
        .iter()
        .filter(|reason| reason.as_deref() == Some("reentry_loop_cap"))
        .count();
    assert_eq!((refused, rooms.len()), (1, 1 + BUDGET));
}

/// RFC §8.3: a hook that awaits each answer and posts back to the agent:
/// each post is one deeper than the answer that fired the hook, so the
/// round trips end at the depth limit, three answers at the limit of 6.
#[tokio::test]
async fn a_judge_posting_back_after_each_answer_stops_at_the_depth_limit() {
    let kit = room(6).await;
    let agent = Answering::new("a");
    kit.register_channel(agent.clone()).unwrap();
    kit.attach(&room_id(), &channel("a")).await.unwrap();
    let depths: Kept<u32> = kept();
    let (hook_kit, hook_depths) = (kit.clone(), Arc::clone(&depths));
    kit.observe("judge", HookTrigger::AfterBroadcast, move |event, _| {
        let (kit, depths) = (hook_kit.clone(), Arc::clone(&hook_depths));
        async move {
            if event.source.channel_id != channel("a")
                || event.blocked_by.is_some()
                || len(&depths) >= 4 * BUDGET
            {
                return;
            }
            let mut again = Post::new(EventContent::text("@a again"));
            again.addressed_to = vec![channel("a")];
            let posted = kit.post(&room_id(), again).await.unwrap();
            push(&depths, posted.chain_depth);
        }
    })
    .unwrap();
    let mut hello = message("@a hello");
    hello.addressed_to = Some(vec![channel("a")]);
    kit.process_inbound_in(&room_id(), hello).await.unwrap();
    kit.idle().await;
    assert_eq!((len(&agent.asked), taken(&depths)), (3, vec![2, 4, 6]));
}

/// RFC §10.3, §9.5.1: a change's mutation trigger is its cascade's work
/// too: a hook that edits the event again each time it is edited, or that
/// sends a message and deletes it each time one is deleted, stops at the
/// cap of its hook calls.
#[tokio::test]
async fn a_mutation_hook_changing_again_stops_at_the_cap() {
    for deleting in [false, true] {
        let kit = room(1).await;
        let sent: Kept<Option<String>> = kept();
        let (hook_kit, hook_sent) = (kit.clone(), Arc::clone(&sent));
        let trigger = if deleting {
            HookTrigger::OnEventDeleted
        } else {
            HookTrigger::OnEventUpdated
        };
        kit.observe("again", trigger, move |event, _| {
            let (kit, sent) = (hook_kit.clone(), Arc::clone(&hook_sent));
            async move {
                if len(&sent) > 2 * BUDGET {
                    return;
                }
                let n = len(&sent);
                if deleting {
                    let mut another =
                        from_u(EventContent::text(format!("m{n}")), EventType::Message);
                    another.addressed_to = Some(Vec::new());
                    let another = kit.send_event(&room_id(), another).await.unwrap();
                    push(&sent, another.blocked_by.clone());
                    if another.blocked_by.is_none() {
                        let delete = delete_of(&another.id);
                        let deleted = kit.send_event(&room_id(), delete).await.unwrap();
                        push(&sent, deleted.blocked_by);
                    }
                } else {
                    let edit = edit_of(&event.id, &format!("v{n}"));
                    let edited = kit.send_event(&room_id(), edit).await.unwrap();
                    push(&sent, edited.blocked_by);
                }
            }
        })
        .unwrap();
        let first = kit
            .send_event(
                &room_id(),
                from_u(EventContent::text("v"), EventType::Message),
            )
            .await
            .unwrap();
        let change = if deleting {
            delete_of(&first.id)
        } else {
            edit_of(&first.id, "v")
        };
        kit.send_event(&room_id(), change).await.unwrap();
        kit.idle().await;
        let mut expected = vec![None; BUDGET];
        expected.push(Some("reentry_loop_cap".to_owned()));
        assert_eq!(taken(&sent), expected, "deleting {deleting}");
    }
}

/// The customer's edit of `target` to `text`.
fn edit_of(target: &EventId, text: &str) -> DirectEvent {
    let edit = EditContent::new(target.clone(), EventContent::text(text));
    from_u(EventContent::Edit(edit), EventType::Edit)
}

/// The customer's delete of `target`.
fn delete_of(target: &EventId) -> DirectEvent {
    let delete = DeleteContent::new(target.clone());
    from_u(EventContent::Delete(delete), EventType::Delete)
}

/// RFC §9.5.1: a notice, one post for each message, draws one hook call of
/// that message's cascade: more messages than a cascade holds hook calls
/// each get theirs.
#[tokio::test]
async fn a_hook_posting_once_per_message_posts_for_every_message() {
    let kit = room(1).await;
    let posted: Kept<Option<String>> = kept();
    let (hook_kit, hook_posted) = (kit.clone(), Arc::clone(&posted));
    kit.observe("receipt", HookTrigger::AfterBroadcast, move |event, _| {
        let (kit, posted) = (hook_kit.clone(), Arc::clone(&hook_posted));
        async move {
            if event.source.channel_id == channel("sms") {
                let receipt = kit
                    .post(&room_id(), Post::new(EventContent::text("Received.")))
                    .await
                    .unwrap();
                push(&posted, receipt.blocked_by);
            }
        }
    })
    .unwrap();
    for n in 0..=BUDGET {
        kit.process_inbound_in(&room_id(), message(&format!("message {n}")))
            .await
            .unwrap();
    }
    kit.idle().await;
    assert_eq!(taken(&posted), vec![None; BUDGET + 1]);
}

/// How a hook's body ends.
#[derive(Clone, Copy, Debug)]
enum Ends {
    Returns,
    Raises,
    TimesOut,
}

/// RFC §9.5.1: once the hook's body is over, whether it returned, raised or
/// ran past its timeout, a task it started that kept its context sends at
/// depth 0 with a budget of its own for each call, as a call from outside
/// the room's work.
#[tokio::test]
async fn a_task_the_hook_starts_calls_as_work_of_its_own() {
    for ends in [Ends::Returns, Ends::Raises, Ends::TimesOut] {
        let kit = room(1).await;
        let sent: Kept<(u32, Option<String>)> = kept();
        let over = Arc::new(tokio::sync::Notify::new());
        let (hook_kit, hook_sent, hook_over) = (kit.clone(), Arc::clone(&sent), Arc::clone(&over));
        let mut spec = roomkit::HookSpec::new(
            "start",
            HookTrigger::AfterBroadcast,
            roomkit::HookExecution::Async,
        );
        spec.timeout = Duration::from_millis(10);
        kit.hook(spec, move |event, _| {
            let (kit, sent, over) = (
                hook_kit.clone(),
                Arc::clone(&hook_sent),
                Arc::clone(&hook_over),
            );
            async move {
                if text_of(&event.content) != "hi" {
                    return Ok(HookResult::allow());
                }
                tokio::spawn(keep_context(async move {
                    over.notified().await;
                    for n in 0..=2 * BUDGET {
                        let later = DirectEvent::new(
                            channel("sms"),
                            EventContent::text(format!("later {n}")),
                        );
                        let event = kit.send_event(&room_id(), later).await.unwrap();
                        push(&sent, (event.chain_depth, event.blocked_by));
                    }
                }));
                match ends {
                    Ends::Returns => Ok(HookResult::allow()),
                    Ends::Raises => Err("the hook's body fails".into()),
                    Ends::TimesOut => std::future::pending().await,
                }
            }
        })
        .unwrap();
        kit.process_inbound_in(&room_id(), message("hi"))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        over.notify_one();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while len(&sent) <= 2 * BUDGET && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(taken(&sent), vec![(0, None); 2 * BUDGET + 1], "{ends:?}");
    }
}

/// RFC §8.3, §10.5: a task the hook starts begins at depth 0 unless it
/// names the chain it continues: posting one deeper than the answer that
/// fired the hook, the round trips end at `max_chain_depth` (4 here), the
/// agent not asked about the second post.
#[tokio::test]
async fn a_task_the_hook_starts_continues_the_chain_it_names() {
    let kit = room(4).await;
    let agent = Answering::new("ai");
    kit.register_channel(agent.clone()).unwrap();
    kit.attach(&room_id(), &channel("ai")).await.unwrap();
    let mut events = kit.subscribe();
    let failures: Kept<KitError> = kept();
    let (hook_kit, hook_failures) = (kit.clone(), Arc::clone(&failures));
    kit.observe("judge", HookTrigger::AfterBroadcast, move |event, _| {
        let (kit, failures) = (hook_kit.clone(), Arc::clone(&hook_failures));
        async move {
            if event.source.channel_id != channel("ai") || event.blocked_by.is_some() {
                return;
            }
            // Not kept: the task's calls are work of its own.
            tokio::spawn(async move {
                let mut again = Post::new(EventContent::text("Try again."));
                again.addressed_to = vec![channel("ai")];
                again.chain_depth = event.chain_depth + 1;
                if let Err(error) = kit.post(&room_id(), again).await {
                    push(&failures, error);
                }
            });
        }
    })
    .unwrap();
    kit.process_inbound_in(&room_id(), message("hi"))
        .await
        .unwrap();
    let limited = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match events.recv().await {
                Ok(roomkit::FrameworkEvent::ChainDepthExceeded { .. }) | Err(_) => return,
                Ok(_) => {}
            }
        }
    });
    limited.await.unwrap();
    kit.idle().await;
    let messages: Vec<(String, u32, Option<String>)> = kit
        .store()
        .list_events(&room_id(), &every_row())
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.event_type == EventType::Message)
        .map(|event| {
            let source = event.source.channel_id.as_str().to_owned();
            (source, event.chain_depth, event.blocked_by)
        })
        .collect();
    let expected: Vec<(String, u32, Option<String>)> = [
        ("sms", 0, None),
        ("ai", 1, None),
        ("host", 2, None),
        ("ai", 3, None),
        ("host", 4, None),
        ("ai", 5, Some("event_chain_depth_limit")),
    ]
    .into_iter()
    .map(|(source, depth, blocked)| (source.to_owned(), depth, blocked.map(str::to_owned)))
    .collect();
    assert_eq!((taken(&failures), messages), (Vec::new(), expected));
}

/// RFC §8.3: three agents answer one another to the depth limit of 5, 45
/// answers, and a hook posts a notice after each: 45 passes and 45 hook
/// calls, each under its cap of 50, so every answer and every notice
/// lands. On one counter, notices would have been blocked.
#[tokio::test]
async fn a_notice_per_answer_costs_the_agents_no_answer() {
    let kit = room(5).await;
    for n in 0..3 {
        let id = format!("agent-{n}");
        kit.register_channel(Answering::new(&id)).unwrap();
        kit.attach(&room_id(), &channel(&id)).await.unwrap();
    }
    let notices: Kept<Option<String>> = kept();
    let (hook_kit, hook_notices) = (kit.clone(), Arc::clone(&notices));
    kit.observe("notice", HookTrigger::AfterBroadcast, move |event, _| {
        let (kit, notices) = (hook_kit.clone(), Arc::clone(&hook_notices));
        async move {
            if event.source.channel_id.as_str().starts_with("agent-") && event.blocked_by.is_none()
            {
                let mut noted = DirectEvent::new(channel("sms"), EventContent::text("Noted."));
                noted.addressed_to = Some(Vec::new());
                let noted = kit.send_event(&room_id(), noted).await.unwrap();
                push(&notices, noted.blocked_by);
            }
        }
    })
    .unwrap();
    kit.process_inbound_in(&room_id(), message("hi"))
        .await
        .unwrap();
    kit.idle().await;
    let answers: Vec<Option<String>> = kit
        .store()
        .list_events(&room_id(), &every_row())
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.source.channel_id.as_str().starts_with("agent-"))
        .map(|event| event.blocked_by)
        .filter(|blocked| blocked.as_deref() != Some("event_chain_depth_limit"))
        .collect();
    assert_eq!((answers, taken(&notices)), (vec![None; 45], vec![None; 45]));
}

/// RFC §10.3: an `ON_EVENT_UPDATED` hook that updates its event again on
/// every update, or an `ON_EVENT_DELETED` hook that sends a message and
/// deletes it on every delete, started from outside the room's work, runs
/// on the budget of the first change, ten hook calls: the update loop's
/// eleventh run's update is refused, nothing written, and the delete
/// loop's sixth run's message is stored capped. Neither solicits an agent,
/// so no depth limit stops them.
#[tokio::test]
async fn a_hook_changing_its_room_on_every_change_stops_at_the_cap() {
    for deleting in [false, true] {
        let kit = room(1).await;
        // A refused change's error, or `None` for a message stored capped.
        let refused: Kept<Option<KitError>> = kept();
        let ran: Kept<()> = kept();
        let trigger = if deleting {
            HookTrigger::OnEventDeleted
        } else {
            HookTrigger::OnEventUpdated
        };
        let (hook_kit, hook_refused, hook_ran) =
            (kit.clone(), Arc::clone(&refused), Arc::clone(&ran));
        kit.observe("again", trigger, move |event, _| {
            let (kit, refused, ran) = (
                hook_kit.clone(),
                Arc::clone(&hook_refused),
                Arc::clone(&hook_ran),
            );
            async move {
                push(&ran, ());
                if len(&ran) > 4 * BUDGET {
                    return;
                }
                let changed = if deleting {
                    let mut x = DirectEvent::new(channel("sms"), EventContent::text("x"));
                    x.addressed_to = Some(Vec::new());
                    let sent = kit.send_event(&room_id(), x).await.unwrap();
                    if sent.blocked_by.is_some() {
                        push(&refused, None);
                        return;
                    }
                    kit.delete_event(&room_id(), &sent.id, true)
                        .await
                        .map(|_| ())
                } else {
                    let mut metadata = event.metadata.clone();
                    metadata.insert("n".to_owned(), len(&ran).into());
                    let updates = EventUpdates {
                        metadata: Some(metadata),
                        ..EventUpdates::default()
                    };
                    kit.update_event(&room_id(), &event.id, updates)
                        .await
                        .map(|_| ())
                };
                if let Err(error) = changed {
                    push(&refused, Some(error));
                }
            }
        })
        .unwrap();
        let first = kit
            .send_event(
                &room_id(),
                DirectEvent::new(channel("sms"), EventContent::text("v")),
            )
            .await
            .unwrap();
        if deleting {
            kit.delete_event(&room_id(), &first.id, true).await.unwrap();
        } else {
            let mut metadata = first.metadata.clone();
            metadata.insert("n".to_owned(), 0.into());
            let updates = EventUpdates {
                metadata: Some(metadata),
                ..EventUpdates::default()
            };
            kit.update_event(&room_id(), &first.id, updates)
                .await
                .unwrap();
        }
        kit.idle().await;
        let expected = if deleting {
            (BUDGET / 2 + 1, vec![None])
        } else {
            let capped = KitError::Mutation(MutationError::ReentryLoopCap(room_id()));
            (BUDGET + 1, vec![Some(capped)])
        };
        assert_eq!(
            (len(&ran), taken(&refused)),
            expected,
            "deleting {deleting}"
        );
    }
}

/// RFC §10.3: a hook's body spends its cascade's thirty hook calls
/// (depth limit 3) updating another event, then deletes the message the
/// agent answered: refused once, the message kept, the agent asked once.
#[tokio::test]
async fn a_direct_change_past_the_cap_writes_nothing() {
    for deleting in [false, true] {
        let kit = room(3).await;
        let agent = Answering::new("ai");
        kit.register_channel(agent.clone()).unwrap();
        kit.attach(&room_id(), &channel("ai")).await.unwrap();
        let mut spare = DirectEvent::new(channel("sms"), EventContent::text("spare"));
        spare.addressed_to = Some(Vec::new());
        let spare = kit.send_event(&room_id(), spare).await.unwrap();
        let refused: Kept<KitError> = kept();
        let (hook_kit, hook_refused) = (kit.clone(), Arc::clone(&refused));
        kit.observe("bulk", HookTrigger::AfterBroadcast, move |event, _| {
            let (kit, refused, spare) = (
                hook_kit.clone(),
                Arc::clone(&hook_refused),
                spare.id.clone(),
            );
            async move {
                if text_of(&event.content) != "hi" {
                    return;
                }
                for n in 0..30 {
                    let mut metadata = roomkit::model::Metadata::new();
                    metadata.insert("n".to_owned(), n.into());
                    let updates = EventUpdates {
                        metadata: Some(metadata),
                        ..EventUpdates::default()
                    };
                    kit.update_event(&room_id(), &spare, updates).await.unwrap();
                }
                let one_more = if deleting {
                    kit.delete_event(&room_id(), &event.id, true)
                        .await
                        .map(|_| ())
                } else {
                    let updates = EventUpdates {
                        content: Some(EventContent::text("changed")),
                        ..EventUpdates::default()
                    };
                    kit.update_event(&room_id(), &event.id, updates)
                        .await
                        .map(|_| ())
                };
                if let Err(error) = one_more {
                    push(&refused, error);
                }
            }
        })
        .unwrap();
        let hi = kit
            .send_event(
                &room_id(),
                DirectEvent::new(channel("sms"), EventContent::text("hi")),
            )
            .await
            .unwrap();
        kit.idle().await;
        let stored = kit.store().get_event(&hi.id).await.unwrap();
        assert_eq!(
            (
                taken(&refused),
                stored.map(|event| text_of(&event.content)),
                len(&agent.asked)
            ),
            (
                vec![KitError::Mutation(MutationError::ReentryLoopCap(room_id()))],
                Some("hi".to_owned()),
                1
            ),
            "deleting {deleting}"
        );
    }
}

/// A transport whose delivery of the customer's "go" updates `target` sixty
/// times.
struct Updating {
    kit: RoomKit,
    target: Arc<Mutex<Option<EventId>>>,
    failures: Kept<KitError>,
}

impl Channel for Updating {
    fn declaration(&self) -> ChannelDeclaration {
        let mut declaration = ChannelDeclaration::new(
            channel("calling"),
            ChannelType::WEBSOCKET,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        );
        declaration.recipient_key = None;
        declaration
    }

    fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let (kit, failures) = (self.kit.clone(), Arc::clone(&self.failures));
        let target = self
            .target
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        async move {
            if let (Some(target), "go") = (target, text_of(&event.content).as_str()) {
                update_sixty_times(&kit, &target, &failures).await;
            }
            Ok(ChannelOutput::default())
        }
    }
}

async fn update_sixty_times(kit: &RoomKit, target: &EventId, failures: &Kept<KitError>) {
    for n in 0..60 {
        let mut metadata = roomkit::model::Metadata::new();
        metadata.insert("n".to_owned(), n.into());
        let updates = EventUpdates {
            metadata: Some(metadata),
            ..EventUpdates::default()
        };
        if let Err(error) = kit.update_event(&room_id(), target, updates).await {
            push(failures, error);
            return;
        }
    }
}

/// RFC §10.3: a transport's delivery, or a SYNC hook under the lock, that
/// makes sixty direct updates, six times the cap of ten, never fails: only
/// an ASYNC hook's body draws on the hook calls.
#[tokio::test]
async fn work_outside_a_hook_body_draws_nothing() {
    for locked_hook in [false, true] {
        let kit = room(1).await;
        let failures: Kept<KitError> = kept();
        let target_id = Arc::new(Mutex::new(None));
        if locked_hook {
            let (hook_kit, hook_failures, hook_target) =
                (kit.clone(), Arc::clone(&failures), Arc::clone(&target_id));
            kit.before_broadcast("updates", move |event, _| {
                let (kit, failures) = (hook_kit.clone(), Arc::clone(&hook_failures));
                let target = hook_target
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .clone();
                async move {
                    if let (Some(target), "go") = (target, text_of(&event.content).as_str()) {
                        update_sixty_times(&kit, &target, &failures).await;
                    }
                    Ok(HookResult::allow())
                }
            })
            .unwrap();
        } else {
            kit.register_channel(Updating {
                kit: kit.clone(),
                target: Arc::clone(&target_id),
                failures: Arc::clone(&failures),
            })
            .unwrap();
            kit.attach(&room_id(), &channel("calling")).await.unwrap();
        }
        let target = kit
            .send_event(
                &room_id(),
                DirectEvent::new(channel("sms"), EventContent::text("target")),
            )
            .await
            .unwrap();
        *target_id.lock().unwrap_or_else(PoisonError::into_inner) = Some(target.id.clone());
        kit.send_event(
            &room_id(),
            DirectEvent::new(channel("sms"), EventContent::text("go")),
        )
        .await
        .unwrap();
        kit.idle().await;
        let stored = kit.store().get_event(&target.id).await.unwrap().unwrap();
        assert_eq!(
            (taken(&failures), stored.metadata.get("n").cloned()),
            (Vec::new(), Some(59.into())),
            "locked hook {locked_hook}"
        );
    }
}

/// RFC §8.3, §10.3: a SYNC hook under the lock updates an event on each of
/// the agent's answers, and an `ON_EVENT_UPDATED` body posts "@a again" to
/// the agent. The update is made in the answer's cascade, so its trigger's
/// body draws on that cascade's budget, however soon the body that posted
/// is over: at the depth limit of 3, thirty posts land, each at depth 1,
/// and the agent's answer to the last, its thirty-first, is blocked
/// `reentry_loop_cap`, past the cascade's thirty passes.
#[tokio::test]
async fn a_locked_hook_s_update_whose_body_posts_back_stops_at_the_cap() {
    let kit = room(3).await;
    let agent = Answering::new("a");
    kit.register_channel(agent.clone()).unwrap();
    kit.attach(&room_id(), &channel("a")).await.unwrap();
    let mut target = DirectEvent::new(channel("sms"), EventContent::text("target"));
    target.addressed_to = Some(Vec::new());
    let target = kit.send_event(&room_id(), target).await.unwrap().id;
    let hook_kit = kit.clone();
    kit.before_broadcast("touch", move |event, _| {
        let (kit, target) = (hook_kit.clone(), target.clone());
        async move {
            if event.source.channel_id == channel("a") {
                let mut metadata = roomkit::model::Metadata::new();
                metadata.insert("by".to_owned(), event.id.as_str().into());
                let updates = EventUpdates {
                    metadata: Some(metadata),
                    ..EventUpdates::default()
                };
                kit.update_event(&room_id(), &target, updates).await?;
            }
            Ok(HookResult::allow())
        }
    })
    .unwrap();
    let posted: Kept<(u32, Option<String>)> = kept();
    let (hook_kit, hook_posted) = (kit.clone(), Arc::clone(&posted));
    kit.observe("again", HookTrigger::OnEventUpdated, move |_, _| {
        let (kit, posted) = (hook_kit.clone(), Arc::clone(&hook_posted));
        async move {
            if len(&posted) > 200 {
                return;
            }
            let mut again = Post::new(EventContent::text("@a again"));
            again.addressed_to = vec![channel("a")];
            let event = kit.post(&room_id(), again).await.unwrap();
            push(&posted, (event.chain_depth, event.blocked_by));
        }
    })
    .unwrap();
    let mut hello = message("@a hello");
    hello.addressed_to = Some(vec![channel("a")]);
    kit.process_inbound_in(&room_id(), hello).await.unwrap();
    kit.idle().await;
    let answers: Vec<Option<String>> = kit
        .store()
        .list_events(&room_id(), &every_row())
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.source.channel_id == channel("a"))
        .map(|event| event.blocked_by)
        .collect();
    let mut capped = vec![None; 3 * BUDGET];
    capped.push(Some("reentry_loop_cap".to_owned()));
    assert_eq!(
        (taken(&posted), answers),
        (vec![(1, None); 3 * BUDGET], capped)
    );
}

/// RFC §10.3: a direct change made from a locked section or an admission
/// draws nothing, though its call was made by a hook's body: the body of
/// an `AFTER_BROADCAST` hook sends "x", and a `BEFORE_BROADCAST` hook,
/// under the lock or off it, makes fifteen updates on "x", past the ten
/// hook calls of the cascade: every one is written.
#[tokio::test]
async fn a_hook_of_a_call_a_body_makes_draws_nothing() {
    for needs_lock in [true, false] {
        let kit = room(1).await;
        let mut target = DirectEvent::new(channel("sms"), EventContent::text("target"));
        target.addressed_to = Some(Vec::new());
        let target = kit.send_event(&room_id(), target).await.unwrap().id;
        let hook_kit = kit.clone();
        kit.observe("send", HookTrigger::AfterBroadcast, move |event, _| {
            let kit = hook_kit.clone();
            async move {
                if text_of(&event.content) == "hi" {
                    let mut x = DirectEvent::new(channel("sms"), EventContent::text("x"));
                    x.addressed_to = Some(Vec::new());
                    kit.send_event(&room_id(), x).await.unwrap();
                }
            }
        })
        .unwrap();
        let failures: Kept<KitError> = kept();
        let (hook_kit, hook_failures, hook_target) =
            (kit.clone(), Arc::clone(&failures), target.clone());
        let mut spec = roomkit::HookSpec::new(
            "updates",
            HookTrigger::BeforeBroadcast,
            roomkit::HookExecution::Sync,
        );
        spec.needs_lock = needs_lock;
        kit.hook(spec, move |event, _| {
            let (kit, failures, target) = (
                hook_kit.clone(),
                Arc::clone(&hook_failures),
                hook_target.clone(),
            );
            async move {
                if text_of(&event.content) == "x" {
                    for n in 0..15 {
                        let mut metadata = roomkit::model::Metadata::new();
                        metadata.insert("n".to_owned(), n.into());
                        let updates = EventUpdates {
                            metadata: Some(metadata),
                            ..EventUpdates::default()
                        };
                        if let Err(error) = kit.update_event(&room_id(), &target, updates).await {
                            push(&failures, error);
                        }
                    }
                }
                Ok(HookResult::allow())
            }
        })
        .unwrap();
        kit.send_event(
            &room_id(),
            DirectEvent::new(channel("sms"), EventContent::text("hi")),
        )
        .await
        .unwrap();
        kit.idle().await;
        let stored = kit.store().get_event(&target).await.unwrap().unwrap();
        assert_eq!(
            (taken(&failures), stored.metadata.get("n").cloned()),
            (Vec::new(), Some(14.into())),
            "needs_lock {needs_lock}"
        );
    }
}

/// Every row of a room's timeline, the refused ones included.
fn every_row() -> roomkit::EventQuery {
    roomkit::EventQuery {
        limit: u64::MAX,
        event_filter: Some(roomkit::EventFilter {
            include_blocked: true,
            ..roomkit::EventFilter::default()
        }),
        ..roomkit::EventQuery::default()
    }
}
