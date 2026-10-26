//! The routing lookups and the rooms a channel reaches (RFC §10.4 step 1,
//! §14.1, gap G-058): the sender is found by their address or by the
//! identity the caller resolved for them, the newest room winning.

use pretty_assertions::assert_eq;
use roomkit::model::{IdentityId, Participant, Room, RoomId, RoomStatus, Timestamp};
use roomkit::{ConversationStore, RoomFilter};

use super::{binding, channel, participant_id, room_id, with_room};
use crate::Store;

const ALICE: &str = "+15550000001";

fn marie() -> IdentityId {
    IdentityId::new("id_marie").unwrap()
}

/// A store holding `named`, rooms created a second apart in that order,
/// each with an SMS binding naming the id it gives, or no one.
async fn bound(named: &[(&str, Option<&str>)]) -> Store {
    let store = crate::store().await;
    for (offset, (room, correspondent)) in named.iter().enumerate() {
        let room_id = RoomId::new(*room).unwrap();
        let created = Timestamp::from_unix_millis(i64::try_from(offset).unwrap() * 1000).unwrap();
        store
            .create_room(Room::new(room_id.clone(), created))
            .await
            .unwrap();
        let mut binding = binding("sms");
        binding.room_id = room_id;
        binding.participant_id = correspondent.map(participant_id);
        store.store_binding(binding).await.unwrap();
    }
    store
}

/// The room the binding lookup finds for ALICE on SMS, with `identity`
/// resolved for her, under `filter`.
async fn by_binding(
    store: &Store,
    identity: Option<&IdentityId>,
    filter: &RoomFilter,
) -> Option<String> {
    store
        .find_room_id_by_binding(&channel("sms"), &participant_id(ALICE), identity, filter)
        .await
        .unwrap()
        .map(|room| room.to_string())
}

#[tokio::test]
async fn the_newest_room_whose_binding_names_the_participant_is_found() {
    let store = bound(&[
        ("older", Some(ALICE)),
        ("newer", Some(ALICE)),
        ("newest", Some("+15550000002")),
    ])
    .await;
    assert_eq!(
        by_binding(&store, None, &RoomFilter::default()).await,
        Some("newer".to_owned())
    );
}

#[tokio::test]
async fn a_binding_of_another_channel_finds_no_room() {
    let store = bound(&[("room_1", Some(ALICE))]).await;
    let found = store
        .find_room_id_by_binding(
            &channel("whatsapp"),
            &participant_id(ALICE),
            None,
            &RoomFilter::default(),
        )
        .await;
    assert_eq!(found, Ok(None));
}

#[tokio::test]
async fn a_room_the_filter_refuses_is_not_found_by_its_binding() {
    let store = bound(&[("room_1", Some(ALICE))]).await;
    let filter = RoomFilter {
        status: Some(RoomStatus::Paused),
        ..RoomFilter::default()
    };
    assert_eq!(by_binding(&store, None, &filter).await, None);
}

#[tokio::test]
async fn a_binding_naming_the_sender_s_identity_is_found() {
    let store = bound(&[("by_identity", Some("id_marie"))]).await;
    assert_eq!(
        (
            by_binding(&store, None, &RoomFilter::default()).await,
            by_binding(&store, Some(&marie()), &RoomFilter::default()).await
        ),
        (None, Some("by_identity".to_owned()))
    );
}

#[tokio::test]
async fn the_newest_room_either_name_finds_wins() {
    let identity_newer = bound(&[
        ("by_address", Some(ALICE)),
        ("by_identity", Some("id_marie")),
    ])
    .await;
    let address_newer = bound(&[
        ("by_identity", Some("id_marie")),
        ("by_address", Some(ALICE)),
    ])
    .await;
    let filter = RoomFilter::default();
    assert_eq!(
        (
            by_binding(&identity_newer, Some(&marie()), &filter).await,
            by_binding(&address_newer, Some(&marie()), &filter).await
        ),
        (
            Some("by_identity".to_owned()),
            Some("by_address".to_owned())
        )
    );
}

fn jean() -> Participant {
    Participant::new(
        participant_id("p_jean"),
        room_id(),
        channel("sms"),
        Timestamp::UNIX_EPOCH,
    )
}

#[tokio::test]
async fn a_participant_reached_through_the_channel_finds_its_room() {
    let store = with_room().await;
    let mut participant = jean();
    participant.identity_id = Some(IdentityId::new("id_jean").unwrap());
    store.create_participant(participant).await.unwrap();
    let by_id = store
        .find_room_id_by_participant(&channel("sms"), &jean().id, None, &RoomFilter::default())
        .await
        .unwrap();
    // Another address, the identity the caller resolved for it.
    let by_identity = store
        .find_room_id_by_participant(
            &channel("sms"),
            &participant_id("+15559999999"),
            Some(&IdentityId::new("id_jean").unwrap()),
            &RoomFilter::default(),
        )
        .await
        .unwrap();
    assert_eq!((by_id, by_identity), (Some(room_id()), Some(room_id())));
}

#[tokio::test]
async fn a_participant_reached_through_another_channel_finds_no_room() {
    let store = with_room().await;
    store.create_participant(jean()).await.unwrap();
    let found = store
        .find_room_id_by_participant(&channel("email"), &jean().id, None, &RoomFilter::default())
        .await;
    assert_eq!(found, Ok(None));
}

#[tokio::test]
async fn listing_the_rooms_a_channel_reaches_skips_the_others() {
    let store = bound(&[("bound", None)]).await;
    let joined = RoomId::new("joined").unwrap();
    store
        .create_room(Room::new(joined.clone(), Timestamp::UNIX_EPOCH))
        .await
        .unwrap();
    store
        .create_participant(Participant::new(
            participant_id(ALICE),
            joined,
            channel("sms"),
            Timestamp::UNIX_EPOCH,
        ))
        .await
        .unwrap();
    store
        .create_room(Room::new(
            RoomId::new("elsewhere").unwrap(),
            Timestamp::UNIX_EPOCH,
        ))
        .await
        .unwrap();
    let filter = RoomFilter {
        channel_id: Some(channel("sms")),
        ..RoomFilter::default()
    };
    let listed = store.list_rooms(&filter).await.unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|room| room.id.as_str())
            .collect::<Vec<_>>(),
        ["bound", "joined"]
    );
}
