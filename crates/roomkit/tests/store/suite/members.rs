//! A room's bindings and participants (RFC §5.5, §5.7, §14.1).

use pretty_assertions::assert_eq;
use roomkit::model::{Participant, Timestamp};
use roomkit::{ConversationStore, StoreError};
use serde_json::json;

use super::{binding, channel, participant_id, room_id, with_room};

#[tokio::test]
async fn a_binding_is_read_as_it_was_stored() {
    let store = with_room().await;
    let mut stored = binding("sms");
    stored.participant_id = Some(participant_id("+15550000001"));
    stored
        .metadata
        .insert("phone_number".to_owned(), json!("+15550000001"));
    store.store_binding(stored.clone()).await.unwrap();
    assert_eq!(store.list_bindings(&room_id()).await, Ok(vec![stored]));
}

/// A binding stored again replaces the room's binding of its channel.
#[tokio::test]
async fn a_binding_stored_again_replaces_the_one_of_its_channel() {
    let store = with_room().await;
    store.store_binding(binding("sms")).await.unwrap();
    let mut muted = binding("sms");
    muted.muted = true;
    store.store_binding(muted.clone()).await.unwrap();
    assert_eq!(store.list_bindings(&room_id()).await, Ok(vec![muted]));
}

#[tokio::test]
async fn a_deleted_binding_is_gone() {
    let store = with_room().await;
    store.store_binding(binding("sms")).await.unwrap();
    store
        .delete_binding(&room_id(), &channel("sms"))
        .await
        .unwrap();
    assert_eq!(store.list_bindings(&room_id()).await.unwrap(), []);
}

#[tokio::test]
async fn a_binding_of_a_missing_room_is_refused() {
    let store = crate::store().await;
    assert_eq!(
        store.store_binding(binding("sms")).await,
        Err(StoreError::RoomNotFound(room_id()))
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
async fn a_context_holds_the_room_s_participants() {
    let store = with_room().await;
    store.create_participant(jean()).await.unwrap();
    let context = store.load_context(&room_id(), 10).await.unwrap().unwrap();
    assert_eq!(context.participants, [jean()]);
}

#[tokio::test]
async fn a_participant_id_cannot_be_created_twice_in_a_room() {
    let store = with_room().await;
    store.create_participant(jean()).await.unwrap();
    assert_eq!(
        store.create_participant(jean()).await,
        Err(StoreError::ParticipantExists {
            room_id: room_id(),
            participant_id: jean().id,
        })
    );
}

#[tokio::test]
async fn a_participant_is_found_within_its_room() {
    let store = with_room().await;
    store.create_participant(jean()).await.unwrap();
    assert_eq!(
        store.get_participant(&room_id(), &jean().id).await,
        Ok(Some(jean()))
    );
}

/// RFC §14.1 `list_participants`: in the order they were created, an
/// update leaving its participant in its place.
#[tokio::test]
async fn an_updated_participant_is_written_whole_in_its_place() {
    let store = with_room().await;
    let marie = Participant {
        id: participant_id("p_marie"),
        ..jean()
    };
    store.create_participant(jean()).await.unwrap();
    store.create_participant(marie.clone()).await.unwrap();
    let renamed = Participant {
        display_name: Some("Jean Tremblay".to_owned()),
        ..jean()
    };
    store.update_participant(renamed.clone()).await.unwrap();
    assert_eq!(
        store.list_participants(&room_id()).await,
        Ok(vec![renamed, marie])
    );
}

#[tokio::test]
async fn updating_a_participant_the_room_does_not_have_is_refused() {
    let store = with_room().await;
    assert_eq!(
        store.update_participant(jean()).await,
        Err(StoreError::ParticipantNotFound {
            room_id: room_id(),
            participant_id: jean().id,
        })
    );
}

#[tokio::test]
async fn a_deleted_participant_is_gone() {
    let store = with_room().await;
    store.create_participant(jean()).await.unwrap();
    store
        .delete_participant(&room_id(), &jean().id)
        .await
        .unwrap();
    assert_eq!(store.list_participants(&room_id()).await, Ok(Vec::new()));
}
