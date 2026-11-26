//! The routing lookups and the rooms a channel reaches (RFC §10.4 step 1,
//! §14.1, gap G-058): the sender is found by their address or by the
//! identity the caller resolved for them, the newest room winning.

use crate::model::{IdentityId, Participant, Room, RoomId, RoomStatus, Timestamp};
use crate::store::{
    FindRoomIdByBindingOptions, FindRoomIdByParticipantOptions, FindRoomIdsByChannelOptions,
};
use crate::{ConversationStore, RoomFilter};
use serde_json::json;

use super::{Held, Open, binding, channel, participant_id, room_id, with_room};

const ALICE: &str = "+15550000001";

fn marie() -> IdentityId {
    IdentityId::new("id_marie").unwrap()
}

/// A store holding `named`, rooms created a second apart in that order,
/// each with an SMS binding naming the id it gives, or no one.
async fn bound<T: Held>(open: Open<'_, T>, named: &[(&str, Option<&str>)]) -> T {
    let store = open().await;
    for (offset, (room, correspondent)) in named.iter().enumerate() {
        let room_id = RoomId::new(*room).unwrap();
        let created = Timestamp::from_unix_millis(i64::try_from(offset).unwrap() * 1000).unwrap();
        let room = Room {
            created_at: created,
            updated_at: created,
            ..Room::new(room_id.clone())
        };
        store.create_room(room).await.unwrap();
        let mut binding = binding("sms");
        binding.room_id = room_id;
        binding.participant_id = correspondent.map(participant_id);
        store.add_binding(binding).await.unwrap();
    }
    store
}

/// The room the binding lookup finds for ALICE on SMS, with `identity`
/// resolved for her, under `filter`.
async fn by_binding<T: Held>(
    store: &T,
    identity: Option<&IdentityId>,
    filter: &RoomFilter,
) -> Option<String> {
    let options = FindRoomIdByBindingOptions {
        identity_id: identity.cloned(),
        room_filter: Some(filter.clone()),
        recipient_key: None,
    };
    store
        .find_room_id_by_binding(&channel("sms"), ALICE, options)
        .await
        .unwrap()
        .map(|room| room.to_string())
}

async fn the_newest_room_whose_binding_names_the_participant_is_found<T: Held>(open: Open<'_, T>) {
    let store = bound(
        open,
        &[
            ("older", Some(ALICE)),
            ("newer", Some(ALICE)),
            ("newest", Some("+15550000002")),
        ],
    )
    .await;
    assert_eq!(
        by_binding(&store, None, &RoomFilter::default()).await,
        Some("newer".to_owned())
    );
}

async fn a_binding_of_another_channel_finds_no_room<T: Held>(open: Open<'_, T>) {
    let store = bound(open, &[("room_1", Some(ALICE))]).await;
    let found = store
        .find_room_id_by_binding(
            &channel("whatsapp"),
            ALICE,
            FindRoomIdByBindingOptions::default(),
        )
        .await;
    assert_eq!(found, Ok(None));
}

async fn a_room_the_filter_refuses_is_not_found_by_its_binding<T: Held>(open: Open<'_, T>) {
    let store = bound(open, &[("room_1", Some(ALICE))]).await;
    let filter = RoomFilter {
        statuses: Some(vec![RoomStatus::Paused]),
        ..RoomFilter::default()
    };
    assert_eq!(by_binding(&store, None, &filter).await, None);
}

async fn a_binding_naming_the_sender_s_identity_is_found<T: Held>(open: Open<'_, T>) {
    let store = bound(open, &[("by_identity", Some("id_marie"))]).await;
    assert_eq!(
        (
            by_binding(&store, None, &RoomFilter::default()).await,
            by_binding(&store, Some(&marie()), &RoomFilter::default()).await
        ),
        (None, Some("by_identity".to_owned()))
    );
}

async fn the_newest_room_either_name_finds_wins<T: Held>(open: Open<'_, T>) {
    let identity_newer = bound(
        open,
        &[
            ("by_address", Some(ALICE)),
            ("by_identity", Some("id_marie")),
        ],
    )
    .await;
    let address_newer = bound(
        open,
        &[
            ("by_identity", Some("id_marie")),
            ("by_address", Some(ALICE)),
        ],
    )
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
    Participant {
        joined_at: Timestamp::UNIX_EPOCH,
        ..Participant::new(participant_id("p_jean"), room_id(), channel("sms"))
    }
}

async fn a_participant_reached_through_the_channel_finds_its_room<T: Held>(open: Open<'_, T>) {
    let store = with_room(open).await;
    let mut participant = jean();
    participant.identity_id = Some(IdentityId::new("id_jean").unwrap());
    store.add_participant(participant).await.unwrap();
    let by_id = store
        .find_room_id_by_participant(
            &channel("sms"),
            jean().id.as_str(),
            FindRoomIdByParticipantOptions::default(),
        )
        .await
        .unwrap();
    // Another address, the identity the caller resolved for it.
    let resolved = FindRoomIdByParticipantOptions {
        identity_id: Some(IdentityId::new("id_jean").unwrap()),
        room_filter: None,
    };
    let by_identity = store
        .find_room_id_by_participant(&channel("sms"), "+15559999999", resolved)
        .await
        .unwrap();
    assert_eq!((by_id, by_identity), (Some(room_id()), Some(room_id())));
}

async fn a_participant_reached_through_another_channel_finds_no_room<T: Held>(open: Open<'_, T>) {
    let store = with_room(open).await;
    store.add_participant(jean()).await.unwrap();
    let found = store
        .find_room_id_by_participant(
            &channel("email"),
            jean().id.as_str(),
            FindRoomIdByParticipantOptions::default(),
        )
        .await;
    assert_eq!(found, Ok(None));
}

// Python: tests/test_shared_channel_routing.py::TestAnIdentifiedCorrespondent::test_an_identity_does_not_pull_a_number_into_a_room_of_another_channel
async fn an_identity_does_not_pull_a_number_into_a_room_of_another_channel<T: Held>(
    open: Open<'_, T>,
) {
    let store = with_room(open).await;
    let mut member = jean();
    member.identity_id = Some(IdentityId::new("id_jean").unwrap());
    store.add_participant(member).await.unwrap();
    let resolved = FindRoomIdByParticipantOptions {
        identity_id: Some(IdentityId::new("id_jean").unwrap()),
        room_filter: None,
    };
    let found = store
        .find_room_id_by_participant(&channel("email"), ALICE, resolved)
        .await;
    assert_eq!(found, Ok(None));
}

// Python: tests/test_shared_channel_routing.py::TestEitherNameFindsTheNewestRoom::test_an_older_room_joined_through_the_channel_is_found_past_a_newer_one
async fn an_older_room_joined_through_the_channel_is_found_past_a_newer_one<T: Held>(
    open: Open<'_, T>,
) {
    let store = bound(open, &[("older", None), ("newer", None)]).await;
    for (room, through) in [("older", "sms"), ("newer", "email")] {
        let member = Participant {
            joined_at: Timestamp::UNIX_EPOCH,
            ..Participant::new(
                participant_id("p_jean"),
                RoomId::new(room).unwrap(),
                channel(through),
            )
        };
        store.add_participant(member).await.unwrap();
    }
    let found = store
        .find_room_id_by_participant(
            &channel("sms"),
            "p_jean",
            FindRoomIdByParticipantOptions::default(),
        )
        .await;
    assert_eq!(found, Ok(Some(RoomId::new("older").unwrap())));
}

// Python: tests/test_shared_channel_routing.py::TestAPausedConversation::test_the_newest_room_wins_whatever_its_status
async fn the_newest_room_wins_whatever_its_status<T: Held>(open: Open<'_, T>) {
    let store = bound(open, &[("older", Some(ALICE)), ("newer", Some(ALICE))]).await;
    let newer = RoomId::new("newer").unwrap();
    let mut room = store.get_room(&newer).await.unwrap().unwrap();
    room.status = RoomStatus::Paused;
    store.update_room(room).await.unwrap();
    assert_eq!(
        by_binding(&store, None, &RoomFilter::default()).await,
        Some("newer".to_owned())
    );
}

/// Python's `find_room_ids_by_channel`: the rooms binding the channel, the
/// oldest first, at most `limit` (2 by default), of a group binding or not
/// when `group` names it; a room holding a participant reached through the
/// channel, and binding none, is not among them.
async fn the_rooms_binding_a_channel_are_found_oldest_first<T: Held>(open: Open<'_, T>) {
    let store = bound(open, &[("first", None), ("second", None), ("third", None)]).await;
    let mut group = store
        .get_binding(&RoomId::new("third").unwrap(), &channel("sms"))
        .await
        .unwrap()
        .unwrap();
    group.group = true;
    store.update_binding(group).await.unwrap();
    let joined = RoomId::new("joined").unwrap();
    store.create_room(Room::new(joined.clone())).await.unwrap();
    store
        .add_participant(Participant::new(
            participant_id(ALICE),
            joined,
            channel("sms"),
        ))
        .await
        .unwrap();
    let found = async |options: FindRoomIdsByChannelOptions| {
        let rooms = store
            .find_room_ids_by_channel(&channel("sms"), options)
            .await
            .unwrap();
        rooms.iter().map(ToString::to_string).collect::<Vec<_>>()
    };
    let every = || FindRoomIdsByChannelOptions {
        limit: 10,
        ..FindRoomIdsByChannelOptions::default()
    };
    let groups = FindRoomIdsByChannelOptions {
        group: Some(true),
        ..every()
    };
    let not_groups = FindRoomIdsByChannelOptions {
        group: Some(false),
        ..every()
    };
    assert_eq!(
        (
            found(FindRoomIdsByChannelOptions::default()).await,
            found(every()).await,
            found(groups).await,
            found(not_groups).await
        ),
        (
            vec!["first".to_owned(), "second".to_owned()],
            vec!["first".to_owned(), "second".to_owned(), "third".to_owned()],
            vec!["third".to_owned()],
            vec!["first".to_owned(), "second".to_owned()]
        )
    );
}

/// RFC §5.7, Python's `names_by_recipient`: with the channel's recipient
/// key, a binding naming no correspondent, not a group's, names the
/// address its metadata holds under the key; without the key, it names
/// none.
async fn a_binding_names_its_recipient_under_the_recipient_key<T: Held>(open: Open<'_, T>) {
    let store = bound(open, &[("recipient", None), ("group", None)]).await;
    for (room, group) in [("recipient", false), ("group", true)] {
        let mut binding = store
            .get_binding(&RoomId::new(room).unwrap(), &channel("sms"))
            .await
            .unwrap()
            .unwrap();
        binding
            .metadata
            .insert("phone_number".to_owned(), json!(ALICE));
        binding.group = group;
        store.update_binding(binding).await.unwrap();
    }
    let keyed = FindRoomIdByBindingOptions {
        recipient_key: Some("phone_number".to_owned()),
        ..FindRoomIdByBindingOptions::default()
    };
    let found = (
        store
            .find_room_id_by_binding(&channel("sms"), ALICE, keyed)
            .await,
        store
            .find_room_id_by_binding(
                &channel("sms"),
                ALICE,
                FindRoomIdByBindingOptions::default(),
            )
            .await,
    );
    assert_eq!(
        found,
        (Ok(Some(RoomId::new("recipient").unwrap())), Ok(None))
    );
}

/// This module's checks, in order.
pub(super) async fn check<T: Held>(open: Open<'_, T>) {
    the_newest_room_whose_binding_names_the_participant_is_found(open).await;
    a_binding_of_another_channel_finds_no_room(open).await;
    a_room_the_filter_refuses_is_not_found_by_its_binding(open).await;
    a_binding_naming_the_sender_s_identity_is_found(open).await;
    the_newest_room_either_name_finds_wins(open).await;
    a_participant_reached_through_the_channel_finds_its_room(open).await;
    a_participant_reached_through_another_channel_finds_no_room(open).await;
    an_identity_does_not_pull_a_number_into_a_room_of_another_channel(open).await;
    an_older_room_joined_through_the_channel_is_found_past_a_newer_one(open).await;
    the_newest_room_wins_whatever_its_status(open).await;
    the_rooms_binding_a_channel_are_found_oldest_first(open).await;
    a_binding_names_its_recipient_under_the_recipient_key(open).await;
}
