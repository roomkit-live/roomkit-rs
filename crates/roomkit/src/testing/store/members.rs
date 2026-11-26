//! A room's bindings and participants (RFC §5.5, §5.7, §14.1).

use crate::model::{
    ChannelBinding, ChannelCapabilities, Participant, RateLimit, RetryPolicy, RoomId, Timestamp,
};
use crate::store::{GetEventCountOptions, ListEventsOptions, ListTasksOptions};
use crate::{ConversationStore, StoreError};
use serde_json::json;

use super::{Held, Open, binding, channel, participant_id, room_id, with_room};

async fn a_binding_is_read_as_it_was_stored<T: Held>(open: Open<'_, T>) {
    let store = with_room(open).await;
    let mut stored = binding("sms");
    stored.participant_id = Some(participant_id("+15550000001"));
    stored
        .metadata
        .insert("phone_number".to_owned(), json!("+15550000001"));
    store.add_binding(stored.clone()).await.unwrap();
    assert_eq!(store.list_bindings(&room_id()).await, Ok(vec![stored]));
}

/// A binding stored again replaces the room's binding of its channel, in
/// its place: the room lists its bindings in the order they were attached,
/// as the Python implementation's stores do.
async fn a_binding_stored_again_replaces_the_one_of_its_channel_in_its_place<T: Held>(
    open: Open<'_, T>,
) {
    let store = with_room(open).await;
    store.add_binding(binding("sms")).await.unwrap();
    store.add_binding(binding("ws")).await.unwrap();
    let mut muted = binding("sms");
    muted.muted = true;
    store.add_binding(muted.clone()).await.unwrap();
    assert_eq!(
        store.list_bindings(&room_id()).await,
        Ok(vec![muted, binding("ws")])
    );
}

/// A binding its model refuses, which a struct literal can hold, is refused
/// by the store, nothing written: a rate limit, a retry delay or a
/// capability limit not above 0. Stored, it would fail every later read of
/// the room's bindings, as Python's model refuses it when read (probes
/// `tasks/probes/step5-lot2/invalid_stored_binding.py` and
/// `tasks/probes/step5-lot2-review-A/model_bounds_written.py`).
async fn a_binding_its_model_refuses_is_never_written<T: Held>(open: Open<'_, T>) {
    let store = with_room(open).await;
    let limited = ChannelBinding {
        rate_limit: Some(RateLimit {
            max_per_second: Some(0.0),
            ..RateLimit::default()
        }),
        ..binding("sms")
    };
    let retried = ChannelBinding {
        retry_policy: Some(RetryPolicy {
            exponential_base: f64::NAN,
            ..RetryPolicy::default()
        }),
        ..binding("sms")
    };
    let capped = ChannelBinding {
        capabilities: ChannelCapabilities {
            max_buttons: Some(0),
            ..ChannelCapabilities::default()
        },
        ..binding("sms")
    };
    let mut written = Vec::new();
    for refused in [limited, retried, capped] {
        written.push(store.add_binding(refused).await);
    }
    let refused = |name: &str, value: &str| {
        Err(StoreError::Value(format!(
            "{name}: Input should be greater than 0 [input_value={value}]"
        )))
    };
    assert_eq!(
        (written, store.list_bindings(&room_id()).await),
        (
            vec![
                refused("max_per_second", "0.0"),
                refused("exponential_base", "nan"),
                refused("max_buttons", "0"),
            ],
            Ok(Vec::new())
        )
    );
}

/// Python's `remove_binding`: whether the room held the binding, `false`
/// for one it does not hold and for a room the store does not hold.
async fn a_removed_binding_is_gone_and_reported<T: Held>(open: Open<'_, T>) {
    let store = with_room(open).await;
    store.add_binding(binding("sms")).await.unwrap();
    let missing = RoomId::new("room_9").unwrap();
    let removed = [
        store.remove_binding(&room_id(), &channel("sms")).await,
        store.remove_binding(&room_id(), &channel("sms")).await,
        store.remove_binding(&missing, &channel("sms")).await,
    ];
    assert_eq!(
        (removed, store.list_bindings(&room_id()).await),
        ([Ok(true), Ok(false), Ok(false)], Ok(Vec::new()))
    );
}

async fn a_binding_of_a_missing_room_is_refused<T: Held>(open: Open<'_, T>) {
    let store = open().await;
    assert_eq!(
        store.add_binding(binding("sms")).await,
        Err(StoreError::RoomNotFound(room_id()))
    );
}

fn jean() -> Participant {
    Participant {
        joined_at: Timestamp::UNIX_EPOCH,
        ..Participant::new(participant_id("p_jean"), room_id(), channel("sms"))
    }
}

/// Python's `load_room_context`: the room, its bindings and its
/// participants; nothing for a room the store does not hold.
async fn the_room_s_context_holds_its_bindings_and_participants<T: Held>(open: Open<'_, T>) {
    let store = with_room(open).await;
    store.add_binding(binding("sms")).await.unwrap();
    store.add_participant(jean()).await.unwrap();
    let (room, bindings, participants) = store.load_room_context(&room_id()).await.unwrap();
    let missing = store
        .load_room_context(&RoomId::new("room_9").unwrap())
        .await
        .unwrap();
    assert_eq!(
        (room.map(|room| room.id), bindings, participants, missing),
        (
            Some(room_id()),
            vec![binding("sms")],
            vec![jean()],
            (None, Vec::new(), Vec::new())
        )
    );
}

/// Python's store, severity rows 3 and 10: a participant of a room the
/// store does not hold is refused; a read of such a room finds nothing.
async fn a_room_the_store_does_not_hold_has_nothing_and_takes_no_participant<T: Held>(
    open: Open<'_, T>,
) {
    let store = open().await;
    let added = store.add_participant(jean()).await;
    let reads = (
        store.list_bindings(&room_id()).await,
        store.list_participants(&room_id()).await,
        store.get_participant(&room_id(), &jean().id).await,
        store.get_binding(&room_id(), &channel("sms")).await,
        store
            .list_tasks(&room_id(), ListTasksOptions::default())
            .await
            .map(|tasks| tasks.len()),
        store
            .list_observations(&room_id())
            .await
            .map(|observations| observations.len()),
        store
            .list_events(&room_id(), ListEventsOptions::default())
            .await
            .map(|events| events.len()),
        store
            .get_event_count(&room_id(), GetEventCountOptions::default())
            .await,
    );
    assert_eq!(
        (added, reads),
        (
            Err(StoreError::RoomNotFound(room_id())),
            (
                Ok(Vec::new()),
                Ok(Vec::new()),
                Ok(None),
                Ok(None),
                Ok(0),
                Ok(0),
                Ok(0),
                Ok(0)
            )
        )
    );
}

async fn a_participant_id_cannot_be_created_twice_in_a_room<T: Held>(open: Open<'_, T>) {
    let store = with_room(open).await;
    store.add_participant(jean()).await.unwrap();
    assert_eq!(
        store.add_participant(jean()).await,
        Err(StoreError::ParticipantExists {
            room_id: room_id(),
            participant_id: jean().id,
        })
    );
}

async fn a_participant_is_found_within_its_room<T: Held>(open: Open<'_, T>) {
    let store = with_room(open).await;
    store.add_participant(jean()).await.unwrap();
    assert_eq!(
        store.get_participant(&room_id(), &jean().id).await,
        Ok(Some(jean()))
    );
}

/// RFC §14.1 `list_participants`: in the order they were created, an
/// update leaving its participant in its place.
async fn an_updated_participant_is_written_whole_in_its_place<T: Held>(open: Open<'_, T>) {
    let store = with_room(open).await;
    let marie = Participant {
        id: participant_id("p_marie"),
        ..jean()
    };
    store.add_participant(jean()).await.unwrap();
    store.add_participant(marie.clone()).await.unwrap();
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

async fn updating_a_participant_the_room_does_not_have_is_refused<T: Held>(open: Open<'_, T>) {
    let store = with_room(open).await;
    assert_eq!(
        store.update_participant(jean()).await,
        Err(StoreError::ParticipantNotFound {
            room_id: room_id(),
            participant_id: jean().id,
        })
    );
}

async fn a_deleted_participant_is_gone<T: Held>(open: Open<'_, T>) {
    let store = with_room(open).await;
    store.add_participant(jean()).await.unwrap();
    store
        .delete_participant(&room_id(), &jean().id)
        .await
        .unwrap();
    assert_eq!(store.list_participants(&room_id()).await, Ok(Vec::new()));
}

/// This module's checks, in order.
pub(super) async fn check<T: Held>(open: Open<'_, T>) {
    a_binding_is_read_as_it_was_stored(open).await;
    a_binding_stored_again_replaces_the_one_of_its_channel_in_its_place(open).await;
    a_binding_its_model_refuses_is_never_written(open).await;
    a_removed_binding_is_gone_and_reported(open).await;
    a_binding_of_a_missing_room_is_refused(open).await;
    the_room_s_context_holds_its_bindings_and_participants(open).await;
    a_room_the_store_does_not_hold_has_nothing_and_takes_no_participant(open).await;
    a_participant_id_cannot_be_created_twice_in_a_room(open).await;
    a_participant_is_found_within_its_room(open).await;
    an_updated_participant_is_written_whole_in_its_place(open).await;
    updating_a_participant_the_room_does_not_have_is_refused(open).await;
    a_deleted_participant_is_gone(open).await;
}
