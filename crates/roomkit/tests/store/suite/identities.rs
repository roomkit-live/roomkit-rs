//! Identities and the addresses they use (RFC §5.6, §14.1, §17.2): an
//! address belongs to one identity within an organization, and may belong
//! to another in each other organization (gap G-060), so that resolving it
//! is never a guess.

use pretty_assertions::assert_eq;
use roomkit::model::{ChannelType, Identity, IdentityId};
use roomkit::{ConversationStore, StoreError};
use serde_json::json;

use crate::Store;

const ALICE: &str = "+15550000001";

fn id(id: &str) -> IdentityId {
    IdentityId::new(id).unwrap()
}

/// The identity `id` of `organization`, using ALICE's number on SMS.
fn identity(name: &str, organization: Option<&str>) -> Identity {
    let mut identity = Identity {
        organization_id: organization.map(str::to_owned),
        ..Identity::new(id(name))
    };
    identity.link(ChannelType::SMS, ALICE);
    identity
}

/// A store holding `identities`.
async fn holding(identities: Vec<Identity>) -> Store {
    let store = crate::store().await;
    for identity in identities {
        store.create_identity(identity).await.unwrap();
    }
    store
}

/// The id of the identity of `organization` ALICE's number resolves to.
async fn resolved(store: &Store, organization: Option<&str>) -> Option<String> {
    store
        .resolve_identity(
            &ChannelType::SMS,
            ALICE,
            roomkit::store::ResolveIdentityOptions {
                organization_id: organization.map(str::to_owned),
            },
        )
        .await
        .unwrap()
        .map(|identity| identity.id.to_string())
}

#[tokio::test]
async fn an_identity_is_read_as_it_was_created() {
    let mut created = identity("id_jean", Some("bank"));
    created.display_name = Some("Jean".to_owned());
    created.external_id = Some("crm_42".to_owned());
    created.metadata.insert("tier".to_owned(), json!("gold"));
    let store = holding(vec![created.clone()]).await;
    assert_eq!(store.get_identity(&id("id_jean")).await, Ok(Some(created)));
}

#[tokio::test]
async fn an_address_resolves_to_the_identity_of_the_caller_s_organization() {
    let store = holding(vec![
        identity("id_bank", Some("bank")),
        identity("id_clinic", Some("clinic")),
    ])
    .await;
    assert_eq!(
        resolved(&store, Some("clinic")).await,
        Some("id_clinic".to_owned())
    );
}

#[tokio::test]
async fn an_identity_of_an_organization_does_not_resolve_for_a_caller_naming_none() {
    let store = holding(vec![identity("id_bank", Some("bank"))]).await;
    assert_eq!(resolved(&store, None).await, None);
}

#[tokio::test]
async fn an_identity_of_no_organization_resolves_for_a_caller_naming_none() {
    let store = holding(vec![identity("id_alice", None)]).await;
    assert_eq!(resolved(&store, None).await, Some("id_alice".to_owned()));
}

#[tokio::test]
async fn an_address_on_another_channel_type_does_not_resolve() {
    let store = holding(vec![identity("id_alice", None)]).await;
    assert_eq!(
        store
            .resolve_identity(
                &ChannelType::WHATSAPP,
                ALICE,
                roomkit::store::ResolveIdentityOptions::default()
            )
            .await,
        Ok(None)
    );
}

/// RFC §14.1: the same address may belong to a different identity in
/// each organization, and to one only within an organization.
#[tokio::test]
async fn an_address_another_identity_of_the_organization_uses_is_refused() {
    let store = holding(vec![identity("id_one", Some("bank"))]).await;
    assert_eq!(
        store
            .create_identity(identity("id_two", Some("bank")))
            .await,
        Err(StoreError::AddressLinked {
            channel_type: ChannelType::SMS,
        })
    );
}

/// An identity is never created over an id another organization holds: the
/// stored one, and where it resolves, stay as they are (RFC §5.6, §17.2).
#[tokio::test]
async fn an_id_another_organization_holds_is_refused() {
    let held = identity("id_alice", Some("acme"));
    let store = holding(vec![held.clone()]).await;
    let mut intruder = Identity::new(id("id_alice"));
    intruder.organization_id = Some("globex".to_owned());
    assert_eq!(
        store.create_identity(intruder).await,
        Err(StoreError::IdentityNotFound(id("id_alice")))
    );
    assert_eq!(store.get_identity(&id("id_alice")).await, Ok(Some(held)));
    assert_eq!(
        resolved(&store, Some("acme")).await.as_deref(),
        Some("id_alice")
    );
}

/// A replaced identity stops resolving at an address it no longer holds.
#[tokio::test]
async fn a_replaced_identity_stops_resolving_at_what_it_no_longer_holds() {
    let store = holding(vec![identity("id_alice", Some("acme"))]).await;
    let mut replaced = Identity::new(id("id_alice"));
    replaced.organization_id = Some("acme".to_owned());
    replaced.link(ChannelType::SMS, "+15550000002");
    store.create_identity(replaced).await.unwrap();
    assert_eq!(resolved(&store, Some("acme")).await, None);
}

/// No organization counts as another, either way.
#[tokio::test]
async fn no_organization_counts_as_another() {
    for (stored, given) in [(None, Some("acme")), (Some("acme"), None)] {
        let held = identity("id_alice", stored);
        let store = holding(vec![held.clone()]).await;
        let mut other = Identity::new(id("id_alice"));
        other.organization_id = given.map(str::to_owned);
        assert_eq!(
            store.create_identity(other).await,
            Err(StoreError::IdentityNotFound(id("id_alice")))
        );
        assert_eq!(store.get_identity(&id("id_alice")).await, Ok(Some(held)));
    }
}

/// One write: a replacement refused for an address another identity holds leaves
/// what it held.
#[tokio::test]
async fn a_replacement_refused_for_an_address_keeps_the_old_ones() {
    let first = identity("id_alice", Some("acme"));
    let mut other = Identity::new(id("id_bob"));
    other.organization_id = Some("acme".to_owned());
    other.link(ChannelType::SMS, "+15550000002");
    let store = holding(vec![first.clone(), other]).await;
    let mut replaced = Identity::new(id("id_alice"));
    replaced.organization_id = Some("acme".to_owned());
    replaced.link(ChannelType::SMS, "+15550000002");
    assert_eq!(
        store.create_identity(replaced).await,
        Err(StoreError::AddressLinked {
            channel_type: ChannelType::SMS,
        })
    );
    assert_eq!(store.get_identity(&id("id_alice")).await, Ok(Some(first)));
    assert_eq!(
        resolved(&store, Some("acme")).await.as_deref(),
        Some("id_alice")
    );
}

/// The empty string and none both name no organization, as Python's stores read
/// them: for a read, a link and an unlink, whichever form the identity has.
#[tokio::test]
async fn an_empty_organization_is_no_organization() {
    for (stored, given) in [(None, Some("")), (Some(""), None)] {
        let store = holding(vec![identity("id_alice", stored)]).await;
        assert_eq!(
            resolved(&store, given).await.as_deref(),
            Some("id_alice"),
            "stored {stored:?}, read under {given:?}"
        );
        let linked = store
            .link_address(
                &id("id_alice"),
                &ChannelType::SMS,
                "+15550000002",
                roomkit::store::LinkAddressOptions {
                    organization_id: given.map(str::to_owned),
                },
            )
            .await;
        assert_eq!(linked, Ok(()), "stored {stored:?}, linked under {given:?}");
        let unlinked = store
            .unlink_address(
                &id("id_alice"),
                &ChannelType::SMS,
                "+15550000002",
                roomkit::store::UnlinkAddressOptions {
                    organization_id: given.map(str::to_owned),
                },
            )
            .await;
        assert_eq!(
            unlinked,
            Ok(()),
            "stored {stored:?}, unlinked under {given:?}"
        );
    }
}

/// Python's `create_identity`: an identity created under an id the store
/// holds replaces the stored one.
#[tokio::test]
async fn an_identity_created_again_replaces_the_stored_one() {
    let store = holding(vec![identity("id_alice", None)]).await;
    let mut renamed = Identity::new(id("id_alice"));
    renamed.display_name = Some("Alice B.".to_owned());
    let created = store.create_identity(renamed.clone()).await;
    assert_eq!(
        (created, store.get_identity(&id("id_alice")).await),
        (Ok(renamed.clone()), Ok(Some(renamed)))
    );
}

#[tokio::test]
async fn a_linked_address_resolves_to_its_identity() {
    let store = holding(vec![Identity::new(id("id_jean"))]).await;
    store
        .link_address(
            &id("id_jean"),
            &ChannelType::SMS,
            ALICE,
            roomkit::store::LinkAddressOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(resolved(&store, None).await, Some("id_jean".to_owned()));
}

#[tokio::test]
async fn linking_to_an_identity_of_another_organization_is_refused() {
    let store = holding(vec![Identity {
        organization_id: Some("bank".to_owned()),
        ..Identity::new(id("id_jean"))
    }])
    .await;
    assert_eq!(
        store
            .link_address(
                &id("id_jean"),
                &ChannelType::SMS,
                ALICE,
                roomkit::store::LinkAddressOptions {
                    organization_id: Some("clinic".to_owned())
                }
            )
            .await,
        Err(StoreError::IdentityNotFound(id("id_jean")))
    );
}

/// RFC §5.6: moving an address is an unlink, then a link.
#[tokio::test]
async fn an_unlinked_address_can_be_linked_to_another_identity() {
    let store = holding(vec![identity("id_one", None), Identity::new(id("id_two"))]).await;
    store
        .unlink_address(
            &id("id_one"),
            &ChannelType::SMS,
            ALICE,
            roomkit::store::UnlinkAddressOptions::default(),
        )
        .await
        .unwrap();
    store
        .link_address(
            &id("id_two"),
            &ChannelType::SMS,
            ALICE,
            roomkit::store::LinkAddressOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(resolved(&store, None).await, Some("id_two".to_owned()));
}

#[tokio::test]
async fn an_unlinked_address_resolves_to_no_one() {
    let store = holding(vec![identity("id_one", None)]).await;
    store
        .unlink_address(
            &id("id_one"),
            &ChannelType::SMS,
            ALICE,
            roomkit::store::UnlinkAddressOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(resolved(&store, None).await, None);
}

#[tokio::test]
async fn unlinking_an_address_the_identity_does_not_use_changes_nothing() {
    let store = holding(vec![identity("id_one", None)]).await;
    store
        .unlink_address(
            &id("id_one"),
            &ChannelType::WHATSAPP,
            ALICE,
            roomkit::store::UnlinkAddressOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(resolved(&store, None).await, Some("id_one".to_owned()));
}

#[tokio::test]
async fn unlinking_from_an_identity_of_another_organization_is_refused() {
    let store = holding(vec![identity("id_bank", Some("bank"))]).await;
    assert_eq!(
        store
            .unlink_address(
                &id("id_bank"),
                &ChannelType::SMS,
                ALICE,
                roomkit::store::UnlinkAddressOptions {
                    organization_id: Some("clinic".to_owned())
                }
            )
            .await,
        Err(StoreError::IdentityNotFound(id("id_bank")))
    );
}

// Python: tests/test_store_memory.py::TestIdentityOperations::test_get_nonexistent
#[tokio::test]
async fn an_unknown_identity_reads_as_none() {
    let store = crate::store().await;
    assert_eq!(store.get_identity(&id("nope")).await, Ok(None));
}

// Python: tests/test_store_memory.py::TestIdentityOperations::test_multiple_addresses_same_channel
#[tokio::test]
async fn two_addresses_on_one_channel_resolve_to_their_identity() {
    let mut created = Identity::new(id("id1"));
    created.link(ChannelType::SMS, "+15551111111");
    created.link(ChannelType::SMS, "+15552222222");
    let store = holding(vec![created]).await;
    let mut found = Vec::new();
    for address in ["+15551111111", "+15552222222"] {
        let identity = store
            .resolve_identity(
                &ChannelType::SMS,
                address,
                roomkit::store::ResolveIdentityOptions::default(),
            )
            .await
            .unwrap();
        found.push(identity.map(|identity| identity.id.to_string()));
    }
    assert_eq!(found, [Some("id1".to_owned()), Some("id1".to_owned())]);
}

// Python: tests/test_store_identity_scoping.py::TestOneIdentityPerAddress::test_an_unknown_identity_is_refused
#[tokio::test]
async fn linking_to_an_unknown_identity_is_refused_and_links_nothing() {
    let store = crate::store().await;
    let linked = store
        .link_address(
            &id("id_ghost"),
            &ChannelType::SMS,
            ALICE,
            roomkit::store::LinkAddressOptions {
                organization_id: Some("acme".to_owned()),
            },
        )
        .await;
    assert_eq!(
        (linked, resolved(&store, Some("acme")).await),
        (Err(StoreError::IdentityNotFound(id("id_ghost"))), None)
    );
}

// Python: tests/test_store_identity_scoping.py::TestUnlinkingAnAddress::test_the_conversations_that_named_the_address_stay_as_they_are
#[tokio::test]
async fn unlinking_an_address_leaves_the_conversations_that_named_it() {
    use roomkit::model::{ChannelBinding, Participant, ParticipantId, Room, RoomId, Timestamp};

    let store = holding(vec![identity("id_jean", Some("acme"))]).await;
    let room = RoomId::new("room_1").unwrap();
    store
        .create_room(Room {
            organization_id: Some("acme".to_owned()),
            ..Room::new(room.clone())
        })
        .await
        .unwrap();
    let sms = roomkit::model::ChannelId::new("sms").unwrap();
    let binding = ChannelBinding {
        attached_at: Timestamp::UNIX_EPOCH,
        participant_id: Some(ParticipantId::new(ALICE).unwrap()),
        ..ChannelBinding::new(sms.clone(), room.clone(), ChannelType::SMS)
    };
    let member = Participant {
        joined_at: Timestamp::UNIX_EPOCH,
        identity_id: Some(id("id_jean")),
        ..Participant::new(
            ParticipantId::new(ALICE).unwrap(),
            room.clone(),
            sms.clone(),
        )
    };
    let binding = store.add_binding(binding).await.unwrap();
    let member = store.add_participant(member).await.unwrap();
    store
        .unlink_address(
            &id("id_jean"),
            &ChannelType::SMS,
            ALICE,
            roomkit::store::UnlinkAddressOptions {
                organization_id: Some("acme".to_owned()),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        (
            store.get_binding(&room, &sms).await,
            store
                .get_participant(&room, &ParticipantId::new(ALICE).unwrap())
                .await,
        ),
        (Ok(Some(binding)), Ok(Some(member)))
    );
}

#[tokio::test]
async fn unlinking_from_an_unknown_identity_is_refused() {
    let store = crate::store().await;
    assert_eq!(
        store
            .unlink_address(
                &id("id_ghost"),
                &ChannelType::SMS,
                ALICE,
                roomkit::store::UnlinkAddressOptions::default()
            )
            .await,
        Err(StoreError::IdentityNotFound(id("id_ghost")))
    );
}

#[tokio::test]
async fn linking_an_address_another_identity_of_the_organization_uses_is_refused() {
    let store = holding(vec![identity("id_one", None), Identity::new(id("id_two"))]).await;
    assert!(matches!(
        store
            .link_address(
                &id("id_two"),
                &ChannelType::SMS,
                ALICE,
                roomkit::store::LinkAddressOptions::default()
            )
            .await,
        Err(StoreError::AddressLinked { .. })
    ));
}
