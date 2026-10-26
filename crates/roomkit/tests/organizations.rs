//! The kit acting for one organization (RFC §17.2, decision 2 of step 2b):
//! every call of an organization's handle reaches its rooms only, a room of
//! another organization, or of none, never changed, its due timers
//! included; the kit itself acts for none.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::sync::Arc;
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{
    BindingUpdate, ChannelId, ChannelType, DirectEvent, EventContent, EventDirection, EventSource,
    EventType, EventUpdates, InboundMessage, ParticipantId, RoomEvent, RoomId, RoomOptions,
    RoomStatus, Timestamp, Visibility,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    ConversationStore, InboundError, KitError, LifecycleError, MutationError, OrganizationKit,
    RoomFilter, RoomKit, SequentialEnvironment, StoreError,
};

const START_MILLIS: i64 = 1_792_677_780_000;

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

/// A kit on a clock the test moves, with an SMS and a WebSocket transport
/// registered, and `room_1` created by `owner`, none when `None`, its SMS
/// attached and its timers pausing it after 60 seconds without activity.
async fn room_of(owner: Option<&str>) -> (RoomKit, Arc<SequentialEnvironment>) {
    let clock = Arc::new(SequentialEnvironment::new(
        Timestamp::from_unix_millis(START_MILLIS).unwrap(),
    ));
    let kit = RoomKit::new();
    kit.set_environment(Arc::clone(&clock));
    for (id, channel_type) in [("sms", ChannelType::SMS), ("ws", ChannelType::WEBSOCKET)] {
        kit.register_channel(RecordingTransport::new(channel(id), channel_type))
            .unwrap();
    }
    let timers = || {
        let mut options = RoomOptions::default();
        options.inactive_after_seconds = Some(60);
        options
    };
    if let Some(owner) = owner {
        let owner = kit.for_organization(owner);
        owner.create_room_with(&room_id(), timers()).await.unwrap();
        owner.attach(&room_id(), &channel("sms")).await.unwrap();
    } else {
        kit.create_room_with(&room_id(), timers()).await.unwrap();
        kit.attach(&room_id(), &channel("sms")).await.unwrap();
    }
    (kit, clock)
}

/// A record of `room_1`, from its WebSocket.
fn record(text: &str) -> RoomEvent {
    let source = EventSource::new(
        channel("ws"),
        ChannelType::WEBSOCKET,
        EventDirection::Inbound,
    );
    RoomEvent::new(
        "record_1".parse().unwrap(),
        room_id(),
        EventType::Message,
        source,
        EventContent::text(text),
        Timestamp::UNIX_EPOCH,
    )
}

/// What `clinic`'s lifecycle changes of `room_1` fail with, each.
async fn lifecycle_refusals(clinic: &OrganizationKit) -> Vec<Option<KitError>> {
    let (room, sms, ws) = (room_id(), channel("sms"), channel("ws"));
    let hidden = BindingUpdate::visibility(Visibility::None);
    vec![
        clinic.attach(&room, &ws).await.err(),
        clinic.detach(&room, &sms).await.err(),
        clinic.update_binding(&room, &sms, hidden).await.err(),
        clinic.mute(&room, &sms).await.err(),
        clinic.pause_room(&room).await.err(),
        clinic.resume_room(&room).await.err(),
        clinic.reopen_room(&room).await.err(),
        clinic.close_room(&room).await.err(),
        clinic.archive_room(&room).await.err(),
    ]
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

#[tokio::test]
async fn a_room_an_organization_creates_belongs_to_it() {
    let kit = RoomKit::new();
    kit.for_organization("bank")
        .create_room(&room_id())
        .await
        .unwrap();
    let room = kit.store().get_room(&room_id()).await.unwrap().unwrap();
    assert_eq!(room.organization_id.as_deref(), Some("bank"));
}

/// RFC §17.2: a room of another organization, or of none, is not found by
/// an organization's lifecycle changes, and none of them writes in it, its
/// due timers included.
#[tokio::test]
async fn a_lifecycle_change_never_reaches_a_room_that_is_not_its_organization_s() {
    for owner in [Some("bank"), None] {
        let (kit, clock) = room_of(owner).await;
        clock.advance(Duration::from_secs(120));
        let refusals = lifecycle_refusals(&kit.for_organization("clinic")).await;
        let not_found = || Some(KitError::Lifecycle(LifecycleError::RoomNotFound(room_id())));
        assert_eq!(
            (refusals, as_stored(&kit).await),
            (
                (0..9).map(|_| not_found()).collect::<Vec<_>>(),
                (RoomStatus::Active, 1, vec![("sms".to_owned(), false)])
            ),
            "a room of {owner:?}"
        );
    }
}

/// RFC §17.2, §10.5: another organization's room takes no record, and its
/// due timers are not applied for the refused call.
#[tokio::test]
async fn a_record_is_never_committed_to_another_organization_s_room() {
    let (kit, clock) = room_of(Some("bank")).await;
    clock.advance(Duration::from_secs(120));
    let refused = kit
        .for_organization("clinic")
        .commit_event(&room_id(), record("Note"))
        .await;
    assert_eq!(
        (refused, as_stored(&kit).await),
        (
            Err(KitError::Mutation(MutationError::RoomNotFound(room_id()))),
            (RoomStatus::Active, 1, vec![("sms".to_owned(), false)])
        )
    );
}

/// RFC §17.2, gaps G-069 and G-077: another organization's room refuses a
/// message as not its own, and an injected event fails as not found, with
/// nothing written in it, its due timers included.
#[tokio::test]
async fn an_event_is_never_brought_into_another_organization_s_room() {
    let (kit, clock) = room_of(Some("bank")).await;
    clock.advance(Duration::from_secs(120));
    let clinic = kit.for_organization("clinic");
    let message = InboundMessage::new(channel("sms"), "+15551234567", EventContent::text("Hi"));
    let refused = (
        clinic.process_inbound_in(&room_id(), message).await.err(),
        clinic
            .send_event(
                &room_id(),
                DirectEvent::new(channel("sms"), EventContent::text("Hi")),
            )
            .await
            .err(),
    );
    let inbound = |error| Some(KitError::Inbound(error));
    assert_eq!(
        (refused, as_stored(&kit).await),
        (
            (
                inbound(InboundError::OtherOrganization(room_id())),
                inbound(InboundError::RoomNotFound(room_id()))
            ),
            (RoomStatus::Active, 1, vec![("sms".to_owned(), false)])
        )
    );
}

/// RFC §17.2, §13.4: from the room's own work, a hook under its lock, an
/// injection for another organization under a key the room's console holds
/// fails as not found, as it would without the key: the original is never
/// handed to it.
#[tokio::test]
async fn a_replay_is_never_handed_to_another_organization() {
    let (kit, _) = room_of(Some("bank")).await;
    let bank = kit.for_organization("bank");
    bank.attach(&room_id(), &channel("ws")).await.unwrap();
    let mut held = DirectEvent::new(channel("ws"), EventContent::text("Your balance is 5000$"));
    held.idempotency_key = Some("WS1".to_owned());
    bank.send_event(&room_id(), held.clone()).await.unwrap();
    let answers = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (hook_kit, hook_answers) = (kit.clone(), Arc::clone(&answers));
    kit.before_broadcast("relay", move |event, _| {
        let (kit, answers, held) = (hook_kit.clone(), Arc::clone(&hook_answers), held.clone());
        async move {
            if event.source.channel_id == channel("sms") {
                let replayed = kit
                    .for_organization("clinic")
                    .send_event(&room_id(), held)
                    .await
                    .map(|event| event.id);
                answers.lock().unwrap().push(replayed);
            }
            Ok(roomkit::model::HookResult::allow())
        }
    })
    .unwrap();
    let message = InboundMessage::new(channel("sms"), "+15551234567", EventContent::text("Hi"));
    bank.process_inbound_in(&room_id(), message).await.unwrap();
    assert_eq!(
        std::mem::take(&mut *answers.lock().unwrap()),
        [Err(KitError::Inbound(
            InboundError::RoomNotFound(room_id())
        ))]
    );
}

/// RFC §17.2, gap G-063: another organization's room holds no stored event
/// of the caller's, so an update or a delete changes nothing.
#[tokio::test]
async fn a_stored_event_of_another_organization_s_room_is_never_changed() {
    let (kit, _clock) = room_of(Some("bank")).await;
    let message = InboundMessage::new(channel("sms"), "+15551234567", EventContent::text("Hi"));
    let committed = kit
        .for_organization("bank")
        .process_inbound_in(&room_id(), message)
        .await
        .unwrap()
        .event
        .unwrap();
    let clinic = kit.for_organization("clinic");
    let mut updates = EventUpdates::default();
    updates.content = Some(EventContent::text("Changed"));
    let changed = (
        clinic
            .update_event(&room_id(), &committed.id, updates)
            .await
            .unwrap(),
        clinic
            .delete_event(&room_id(), &committed.id, true)
            .await
            .unwrap(),
    );
    let stored = kit.store().get_event(&committed.id).await.unwrap();
    assert_eq!(
        (changed, stored.map(|event| event.content)),
        ((None, vec![]), Some(EventContent::text("Hi")))
    );
}

#[tokio::test]
async fn an_organization_s_calls_reach_its_own_rooms() {
    let (kit, _clock) = room_of(Some("bank")).await;
    let bank = kit.for_organization("bank");
    bank.attach(&room_id(), &channel("ws")).await.unwrap();
    bank.mute(&room_id(), &channel("sms")).await.unwrap();
    bank.commit_event(&room_id(), record("Note")).await.unwrap();
    bank.pause_room(&room_id()).await.unwrap();
    assert_eq!(
        as_stored(&kit).await,
        // Three lifecycle records and the record; the pause writes none.
        (
            RoomStatus::Paused,
            4,
            vec![("sms".to_owned(), true), ("ws".to_owned(), false)]
        )
    );
}

/// RFC §17.2: an organization reads its own rooms, their timelines,
/// bindings and participants; another organization's room, or one of none,
/// reads as a room that does not exist, a read of its roster failing as
/// not found.
#[tokio::test]
async fn an_organization_reads_its_own_rooms_only() {
    for (reader, own) in [("bank", true), ("clinic", false)] {
        let (kit, _clock) = room_of(Some("bank")).await;
        let committed = kit.commit_event(&room_id(), record("Note")).await.unwrap();
        let reader = kit.for_organization(reader);
        let room = room_id();
        let not_found = || KitError::Store(StoreError::RoomNotFound(room_id()));
        let read = (
            reader.get_room(&room).await.unwrap().map(|room| room.id),
            reader
                .list_rooms(&RoomFilter::default())
                .await
                .unwrap()
                .len(),
            reader
                .get_event(&committed.id)
                .await
                .unwrap()
                .map(|event| event.id),
            reader
                .list_events(&room, true)
                .await
                .map(|events| events.len()),
            reader
                .list_bindings(&room)
                .await
                .map(|bindings| bindings.len()),
            reader
                .list_participants(&room)
                .await
                .map(|people| people.len()),
            reader
                .get_participant(&room, &ParticipantId::new("u").unwrap())
                .await,
        );
        let expected = if own {
            (
                Some(room_id()),
                1,
                Some(committed.id.clone()),
                // The attach's lifecycle record, then the record.
                Ok(2),
                Ok(1),
                Ok(0),
                Ok(None),
            )
        } else {
            (
                None,
                0,
                None,
                Err(not_found()),
                Err(not_found()),
                Err(not_found()),
                Err(not_found()),
            )
        };
        assert_eq!(read, expected, "read by {}", reader.organization_id());
    }
}

/// RFC §17.2: the kit acts for no organization, and reaches every room.
#[tokio::test]
async fn the_kit_reaches_an_organization_s_room() {
    let (kit, _clock) = room_of(Some("bank")).await;
    kit.commit_event(&room_id(), record("Note")).await.unwrap();
    kit.pause_room(&room_id()).await.unwrap();
    assert_eq!(
        as_stored(&kit).await,
        (RoomStatus::Paused, 2, vec![("sms".to_owned(), false)])
    );
}
