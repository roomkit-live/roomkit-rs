//! RFC §14.1: a store that keeps no private state of its own keeps it in
//! the room's metadata, which clients may read, and the trait's provided
//! methods say so once (RMK-1101). A binary of its own: the warning is
//! said once per store type in a process, so no other test may say it
//! first.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

#[path = "store/abstract_only_store.rs"]
mod abstract_only_store;
#[path = "support/warnings.rs"]
mod warnings;

use pretty_assertions::assert_eq;
use roomkit::ConversationStore;
use roomkit::model::{Metadata, Room, RoomId};
use serde_json::json;

use crate::abstract_only_store::AbstractOnly;
use crate::warnings::Warnings;

/// Python: `tests/test_author_register.py::TestTheStoreKeepsItApart::test_a_store_without_its_methods_keeps_it_in_the_metadata`,
/// on the store's own calls: the provided methods keep the state in the
/// room's metadata, and say once that it is not private there.
#[tokio::test(flavor = "current_thread")]
async fn a_store_without_its_methods_keeps_it_in_the_metadata() {
    let store = AbstractOnly::default();
    let room_id = RoomId::new("r1").unwrap();
    store.create_room(Room::new(room_id.clone())).await.unwrap();
    let register = Metadata::from_iter([("sources".to_owned(), json!(["a", "b"]))]);
    let (warnings, capturing) = Warnings::capture();

    store
        .set_private_state(&room_id, "author_register", Some(register.clone()))
        .await
        .unwrap();
    let read = store
        .get_private_state(&room_id, "author_register")
        .await
        .unwrap();
    drop(capturing);

    let room = store.get_room(&room_id).await.unwrap().unwrap();
    assert_eq!(
        (
            warnings.containing("get_private_state()").len(),
            read,
            room.metadata.get("author_register").cloned()
        ),
        (1, Some(register.clone()), Some(json!(register)))
    );
}
