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
