//! The organization a call acts for (RFC §17.2, step 5 decision 6): every
//! call takes it in its options, `organization_id`, as the Python
//! implementation's calls take it. A room of another organization, or of
//! none, reads as one that does not exist, as Python reads it (decision 9),
//! and nothing is written in it, its due timers included; a call that
//! names no organization reaches every room.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::sync::Arc;
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{
    Access, ChannelId, ChannelType, EventSource, InboundMessage, Metadata, RoomEvent, RoomId,
    RoomStatus, RoomTimers, TextContent, Timestamp, Visibility,
};
use roomkit::testing::{RecordingTransport, SequentialEnvironment, kit_with_environment};
use roomkit::{
    ArchiveRoomOptions, AttachChannelOptions, CheckAllTimersOptions, CheckRoomTimersOptions,
    CloseRoomOptions, CommitEventOptions, ConversationStore, CreateRoomOptions, DeleteEventOptions,
    DetachChannelOptions, GetBindingOptions, GetRoomOptions, GetTimelineOptions, InboundError,
    KitError, LifecycleError, ListBindingsOptions, ListMembersOptions, MutationError, MuteOptions,
    PauseRoomOptions, PostOptions, ProcessInboundOptions, ReopenRoomOptions, ResumeRoomOptions,
    RoomKit, RoomKitOptions, SendEventOptions, SetAccessOptions, SetVisibilityOptions, StoreError,
    UnmuteOptions, UpdateBindingMetadataOptions, UpdateEventOptions,
};

const START_MILLIS: i64 = 1_792_677_780_000;

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

/// The organization whose calls reach no room of the tests'.
const CLINIC: &str = "clinic";

fn clinic() -> String {
    CLINIC.to_owned()
}

/// A kit on a clock the test moves, with an SMS and a WebSocket transport
/// registered, and `room_1` of `owner`, none when `None`, its SMS attached
/// and its timers pausing it after 60 seconds without activity.
async fn room_of(owner: Option<&str>) -> (RoomKit, Arc<SequentialEnvironment>) {
    let clock = Arc::new(SequentialEnvironment::new(
        Timestamp::from_unix_millis(START_MILLIS).unwrap(),
    ));
    let kit = kit_with_environment(RoomKitOptions::default(), Arc::clone(&clock));
    for (id, channel_type) in [("sms", ChannelType::SMS), ("ws", ChannelType::WEBSOCKET)] {
        kit.register_channel(RecordingTransport::new(channel(id), channel_type))
            .unwrap();
    }
    let organization_id = owner.map(str::to_owned);
    let room = CreateRoomOptions {
        room_id: Some(room_id()),
        organization_id: organization_id.clone(),
        timers: Some(RoomTimers {
            inactive_after_seconds: Some(60),
            ..RoomTimers::default()
        }),
        ..Default::default()
    };
    kit.create_room(room).await.unwrap();
    let sms = AttachChannelOptions {
        organization_id,
        ..Default::default()
    };
    kit.attach_channel(&room_id(), &channel("sms"), sms)
        .await
        .unwrap();
    (kit, clock)
}

/// A record of `room_1`, from its WebSocket.
fn record(text: &str) -> RoomEvent {
    let source = EventSource::new(channel("ws"), ChannelType::WEBSOCKET);
    RoomEvent {
        id: "record_1".parse().unwrap(),
        created_at: Timestamp::UNIX_EPOCH,
        ..RoomEvent::new(room_id(), source, TextContent::new(text))
    }
}

fn hi() -> InboundMessage {
    InboundMessage::new(channel("sms"), "+15551234567", TextContent::new("Hi"))
}

/// `room_1` as the store holds it: its status, its event count, its
/// attach's lifecycle record among them (gap G-046), and its bindings as
/// `(channel, muted)`, by channel.
async fn as_stored(kit: &RoomKit) -> (RoomStatus, u64, Vec<(String, bool)>) {
    let room = kit.store().get_room(&room_id()).await.unwrap().unwrap();
    let bindings = kit.store().list_bindings(&room_id()).await.unwrap();
    let mut bindings: Vec<_> = bindings
        .into_iter()
        .map(|binding| (binding.channel_id.as_str().to_owned(), binding.muted))
        .collect();
    bindings.sort();
    (room.status, room.event_count, bindings)
}

/// What a call came to, as the Python implementation's outcomes are named:
/// `RoomNotFound`, or the value it returned when it found nothing.
fn outcome<T: std::fmt::Debug>(result: Result<T, KitError>) -> String {
    match result {
        Err(
            KitError::Store(StoreError::RoomNotFound(_))
            | KitError::Lifecycle(LifecycleError::RoomNotFound(_))
            | KitError::Inbound(InboundError::RoomNotFound(_))
            | KitError::Mutation(MutationError::RoomNotFound(_)),
        ) => "RoomNotFound".to_owned(),
        Err(other) => format!("{other:?}"),
        Ok(value) => format!("{value:?}"),
    }
}

/// Each of `clinic`'s calls on `room_1`, scoped to it, and what it came to;
/// `committed` is an event the room holds.
async fn clinic_calls(kit: &RoomKit, committed: &RoomEvent) -> Vec<(&'static str, String)> {
    let room = room_id();
    let mut called = room_calls(kit, &room).await;
    called.extend(binding_changes(kit, &room).await);
    called.extend(binding_reads(kit, &room).await);
    called.extend(event_calls(kit, &room, committed).await);
    called
}

/// `clinic`'s read and lifecycle changes of `room`, and its timer checks.
async fn room_calls(kit: &RoomKit, room: &RoomId) -> Vec<(&'static str, String)> {
    let read = GetRoomOptions {
        organization_id: Some(clinic()),
    };
    let pause = PauseRoomOptions {
        organization_id: Some(clinic()),
    };
    let resume = ResumeRoomOptions {
        organization_id: Some(clinic()),
    };
    let reopen = ReopenRoomOptions {
        organization_id: Some(clinic()),
    };
    let close = CloseRoomOptions {
        organization_id: Some(clinic()),
    };
    let archive = ArchiveRoomOptions {
        organization_id: Some(clinic()),
    };
    let check = CheckRoomTimersOptions {
        organization_id: Some(clinic()),
    };
    let check_all = CheckAllTimersOptions {
        organization_id: Some(clinic()),
    };
    vec![
        (
            "get_room",
            outcome(kit.get_room(room, read).await.map(|room| room.id)),
        ),
        ("pause_room", outcome(kit.pause_room(room, pause).await)),
        ("resume_room", outcome(kit.resume_room(room, resume).await)),
        ("reopen_room", outcome(kit.reopen_room(room, reopen).await)),
        ("close_room", outcome(kit.close_room(room, close).await)),
        (
            "archive_room",
            outcome(kit.archive_room(room, archive).await),
        ),
        (
            "check_room_timers",
            outcome(kit.check_room_timers(room, check).await),
        ),
        (
            "check_all_timers",
            outcome(kit.check_all_timers(check_all).await),
        ),
    ]
}

/// `clinic`'s changes of the bindings of `room`.
async fn binding_changes(kit: &RoomKit, room: &RoomId) -> Vec<(&'static str, String)> {
    let (sms, ws) = (channel("sms"), channel("ws"));
    let attach = AttachChannelOptions {
        organization_id: Some(clinic()),
        ..AttachChannelOptions::default()
    };
    let detach = DetachChannelOptions {
        organization_id: Some(clinic()),
    };
    let mute = MuteOptions {
        organization_id: Some(clinic()),
    };
    let unmute = UnmuteOptions {
        organization_id: Some(clinic()),
    };
    let access = SetAccessOptions {
        organization_id: Some(clinic()),
    };
    let visibility = SetVisibilityOptions {
        organization_id: Some(clinic()),
    };
    let tagged = UpdateBindingMetadataOptions {
        organization_id: Some(clinic()),
    };
    let tag = Metadata::from_iter([("tag".to_owned(), "x".into())]);
    vec![
        (
            "attach_channel",
            outcome(kit.attach_channel(room, &ws, attach).await),
        ),
        (
            "detach_channel",
            outcome(kit.detach_channel(room, &sms, detach).await),
        ),
        ("mute", outcome(kit.mute(room, &sms, mute).await)),
        ("unmute", outcome(kit.unmute(room, &sms, unmute).await)),
        (
            "set_access",
            outcome(kit.set_access(room, &sms, Access::ReadOnly, access).await),
        ),
        (
            "set_visibility",
            outcome(
                kit.set_visibility(room, &sms, Visibility::None, visibility)
                    .await,
            ),
        ),
        (
            "update_binding_metadata",
            outcome(kit.update_binding_metadata(room, &sms, tag, tagged).await),
        ),
    ]
}

/// `clinic`'s reads of the bindings and members of `room`.
async fn binding_reads(kit: &RoomKit, room: &RoomId) -> Vec<(&'static str, String)> {
    let binding = GetBindingOptions {
        organization_id: Some(clinic()),
    };
    let bindings = ListBindingsOptions {
        organization_id: Some(clinic()),
    };
    let members = ListMembersOptions {
        organization_id: Some(clinic()),
        ..ListMembersOptions::default()
    };
    vec![
        (
            "get_binding",
            outcome(kit.get_binding(room, &channel("sms"), binding).await),
        ),
        (
            "list_bindings",
            outcome(kit.list_bindings(room, bindings).await),
        ),
        (
            "list_members",
            outcome(kit.list_members(room, members).await),
        ),
    ]
}

/// `clinic`'s calls on the events of `room`, `committed` one of them.
async fn event_calls(
    kit: &RoomKit,
    room: &RoomId,
    committed: &RoomEvent,
) -> Vec<(&'static str, String)> {
    let (sms, x) = (channel("sms"), || TextContent::new("x"));
    let timeline = GetTimelineOptions {
        organization_id: Some(clinic()),
        ..GetTimelineOptions::default()
    };
    let send = SendEventOptions {
        organization_id: Some(clinic()),
        ..SendEventOptions::default()
    };
    let post = PostOptions {
        organization_id: Some(clinic()),
        ..PostOptions::default()
    };
    let commit = CommitEventOptions {
        organization_id: Some(clinic()),
    };
    let inbound = ProcessInboundOptions {
        room_id: Some(room.clone()),
        organization_id: Some(clinic()),
        ..ProcessInboundOptions::default()
    };
    let update = UpdateEventOptions {
        content: Some(TextContent::new("Changed").into()),
        organization_id: Some(clinic()),
        ..UpdateEventOptions::default()
    };
    let delete = DeleteEventOptions {
        organization_id: Some(clinic()),
        ..DeleteEventOptions::default()
    };
    let id = &committed.id;
    vec![
        (
            "get_timeline",
            outcome(kit.get_timeline(room, timeline).await),
        ),
        (
            "send_event",
            outcome(kit.send_event(room, &sms, x(), send).await),
        ),
        ("post", outcome(kit.post(room, x(), post).await)),
        (
            "commit_event",
            outcome(kit.commit_event(room, record("Note"), commit).await),
        ),
        (
            "process_inbound",
            outcome(kit.process_inbound(hi(), inbound).await),
        ),
        (
            "update_event",
            outcome(kit.update_event(room, id, update).await),
        ),
        (
            "delete_event",
            outcome(kit.delete_event(room, id, delete).await),
        ),
    ]
}

/// RFC §17.2, §5.1: every scoped call on a room of another organization,
/// or of none, gets the Python implementation's outcome
/// (tasks/probes/step5-lot1/other_org.py): `RoomNotFound`, but `None` from
/// `update_event`, no id from `delete_event` and no room from
/// `check_all_timers`; and writes nothing in the room, its due timers
/// included.
#[tokio::test]
async fn another_organizations_room_is_not_found() {
    for owner in [Some("bank"), None] {
        let (kit, clock) = room_of(owner).await;
        let committed = kit
            .commit_event(&room_id(), record("Kept"), CommitEventOptions::default())
            .await
            .unwrap();
        clock.advance(Duration::from_secs(120));
        let called = clinic_calls(&kit, &committed).await;
        let python = |name: &str| match name {
            "update_event" => "None".to_owned(),
            "delete_event" | "check_all_timers" => "[]".to_owned(),
            _ => "RoomNotFound".to_owned(),
        };
        let expected: Vec<(&str, String)> = called
            .iter()
            .map(|(name, _)| (*name, python(name)))
            .collect();
        // The event update_event and delete_event named, as it was stored.
        let kept = kit.store().get_event(&committed.id).await.unwrap();
        assert_eq!(
            (called, as_stored(&kit).await, kept),
            (
                expected,
                // The attach's record and the one record the test commits.
                (RoomStatus::Active, 2, vec![("sms".to_owned(), false)]),
                Some(committed)
            ),
            "a room of {owner:?}"
        );
    }
}

/// RFC §17.2: a room a call for an organization creates belongs to it.
#[tokio::test]
async fn a_room_an_organization_creates_belongs_to_it() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let options = CreateRoomOptions {
        room_id: Some(room_id()),
        organization_id: Some("bank".to_owned()),
        ..Default::default()
    };
    kit.create_room(options).await.unwrap();
    let room = kit.store().get_room(&room_id()).await.unwrap().unwrap();
    assert_eq!(room.organization_id.as_deref(), Some("bank"));
}

/// RFC §17.2, §13.4: from the room's own work, a hook under its lock, an
/// injection for another organization under a key the room's console holds
/// fails as not found, as it would without the key: the original is never
/// handed to it.
#[tokio::test]
async fn a_replay_is_never_handed_to_another_organization() {
    let (kit, _) = room_of(Some("bank")).await;
    let bank = AttachChannelOptions {
        organization_id: Some("bank".to_owned()),
        ..Default::default()
    };
    kit.attach_channel(&room_id(), &channel("ws"), bank)
        .await
        .unwrap();
    let held = |organization: &str| SendEventOptions {
        idempotency_key: Some("WS1".to_owned()),
        organization_id: Some(organization.to_owned()),
        ..Default::default()
    };
    let balance = || TextContent::new("Your balance is 5000$");
    kit.send_event(&room_id(), &channel("ws"), balance(), held("bank"))
        .await
        .unwrap();
    let answers = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (hook_kit, hook_answers) = (kit.clone(), Arc::clone(&answers));
    kit.before_broadcast("relay", move |event, _| {
        let (kit, answers) = (hook_kit.clone(), Arc::clone(&hook_answers));
        async move {
            if event.source.channel_id == channel("sms") {
                let replayed = kit
                    .send_event(&room_id(), &channel("ws"), balance(), held("clinic"))
                    .await
                    .map(|event| event.id);
                answers.lock().unwrap().push(replayed);
            }
            Ok(roomkit::model::HookResult::allow())
        }
    })
    .unwrap();
    let options = ProcessInboundOptions {
        room_id: Some(room_id()),
        organization_id: Some("bank".to_owned()),
        ..ProcessInboundOptions::default()
    };
    kit.process_inbound(hi(), options).await.unwrap();
    assert_eq!(
        std::mem::take(&mut *answers.lock().unwrap()),
        [Err(KitError::Inbound(
            InboundError::RoomNotFound(room_id())
        ))]
    );
}

/// RFC §17.2: an organization's calls reach its own rooms, its reads
/// included.
#[tokio::test]
async fn an_organization_s_calls_reach_its_own_rooms() {
    let (kit, _clock) = room_of(Some("bank")).await;
    let bank = || Some("bank".to_owned());
    let ws = AttachChannelOptions {
        organization_id: bank(),
        ..Default::default()
    };
    kit.attach_channel(&room_id(), &channel("ws"), ws)
        .await
        .unwrap();
    kit.mute(
        &room_id(),
        &channel("sms"),
        MuteOptions {
            organization_id: bank(),
        },
    )
    .await
    .unwrap();
    let commit = CommitEventOptions {
        organization_id: bank(),
    };
    kit.commit_event(&room_id(), record("Note"), commit)
        .await
        .unwrap();
    kit.pause_room(
        &room_id(),
        PauseRoomOptions {
            organization_id: bank(),
        },
    )
    .await
    .unwrap();
    let read = GetTimelineOptions {
        organization_id: bank(),
        ..Default::default()
    };
    let timeline = kit.get_timeline(&room_id(), read).await.unwrap();
    let bindings = kit
        .list_bindings(
            &room_id(),
            ListBindingsOptions {
                organization_id: bank(),
            },
        )
        .await
        .map(|bindings| bindings.len());
    let members = kit
        .list_members(
            &room_id(),
            ListMembersOptions {
                organization_id: bank(),
                ..ListMembersOptions::default()
            },
        )
        .await
        .map(|members| members.len());
    let binding = kit
        .get_binding(
            &room_id(),
            &channel("ws"),
            GetBindingOptions {
                organization_id: bank(),
            },
        )
        .await
        .map(|binding| binding.channel_id);
    assert_eq!(
        (
            as_stored(&kit).await,
            timeline.len(),
            (bindings, members, binding)
        ),
        // Three lifecycle records and the record; the pause writes none.
        (
            (
                RoomStatus::Paused,
                4,
                vec![("sms".to_owned(), true), ("ws".to_owned(), false)]
            ),
            4,
            (Ok(2), Ok(0), Ok(channel("ws")))
        )
    );
}

/// RFC §17.2: a call that names no organization reaches every room.
#[tokio::test]
async fn a_call_naming_no_organization_reaches_an_organization_s_room() {
    let (kit, _clock) = room_of(Some("bank")).await;
    kit.commit_event(&room_id(), record("Note"), CommitEventOptions::default())
        .await
        .unwrap();
    kit.pause_room(&room_id(), PauseRoomOptions::default())
        .await
        .unwrap();
    assert_eq!(
        as_stored(&kit).await,
        (RoomStatus::Paused, 2, vec![("sms".to_owned(), false)])
    );
}
