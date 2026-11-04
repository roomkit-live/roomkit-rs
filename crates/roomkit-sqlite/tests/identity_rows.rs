//! Python: `tests/test_store_sqlite.py::test_a_replacement_removes_a_row_of_another_scope_for_a_kept_address`:
//! a replacement leaves no row of another scope for an address it keeps, one a
//! file's migration left unscoped (RFC §17.2). This store's read already refuses
//! such a row, so the test reads the file.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod support;

use roomkit::ConversationStore;
use roomkit::model::{ChannelType, Identity, IdentityId};
use roomkit_sqlite::{SqliteStore, SqliteStoreOptions};
use rusqlite::Connection;

use crate::support::TempFile;

#[tokio::test]
async fn a_replacement_removes_a_row_of_another_scope_for_a_kept_address() {
    let file = TempFile::new("identity-rows");
    let store = SqliteStore::new(SqliteStoreOptions {
        path: file.path().into(),
    });
    let mut identity = Identity::new(IdentityId::new("i1").unwrap());
    identity.organization_id = Some("acme".to_owned());
    identity.link(ChannelType::SMS, "+1555");
    store.create_identity(identity.clone()).await.unwrap();
    let connection = Connection::open(file.path()).unwrap();
    connection
        .execute(
            "INSERT INTO identity_addresses (channel_type, address, identity_id, organization_id)
             VALUES ('sms', '+1555', 'i1', '')",
            [],
        )
        .unwrap();
    store.create_identity(identity).await.unwrap();
    let scopes: Vec<String> = connection
        .prepare("SELECT organization_id FROM identity_addresses WHERE identity_id = 'i1'")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(scopes, ["acme"]);
    store.close().await;
}

/// A row under a key the model does not read (`Slack`, which Python takes as
/// written) stays when the identity is written again: its JSON keeps the key too.
#[tokio::test]
async fn a_replacement_keeps_the_row_of_a_key_the_model_does_not_read() {
    let file = TempFile::new("identity-unread-key");
    let store = SqliteStore::new(SqliteStoreOptions {
        path: file.path().into(),
    });
    let mut identity = Identity::new(IdentityId::new("i1").unwrap());
    identity.organization_id = Some("acme".to_owned());
    identity.link(ChannelType::SMS, "+1555");
    store.create_identity(identity.clone()).await.unwrap();
    let connection = Connection::open(file.path()).unwrap();
    connection
        .execute_batch(
            "INSERT INTO identity_addresses (channel_type, address, identity_id, organization_id)
             VALUES ('Slack', 'U07', 'i1', 'acme');
             UPDATE identities SET data = json_set(data, '$.channel_addresses.Slack', json('[\"U07\"]'))
             WHERE id = 'i1';",
        )
        .unwrap();
    store.create_identity(identity).await.unwrap();
    let kept: i64 = connection
        .query_row(
            "SELECT count(*) FROM identity_addresses WHERE channel_type = 'Slack' AND address = 'U07'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(kept, 1);
    store.close().await;
}

/// Python: `test_opening_moves_an_unscoped_address_to_its_identity_s_organization`
/// (HITL 0f96a597, RFC §17.2): a row version 3's migration left unscoped moves to
/// the organization of its identity, once, if the identity still declares the
/// address; one whose organization holds the address already yields to that row. A
/// link since revoked is not restored: its row goes (HITL 6928c418). An identity of
/// no organization keeps its unscoped row, and a row of another organization is left
/// alone, which no read serves.
#[tokio::test]
async fn opening_moves_an_unscoped_address_to_its_identity_s_organization() {
    let file = TempFile::new("identity-unscoped");
    let store = SqliteStore::new(SqliteStoreOptions {
        path: file.path().into(),
    });
    for (id, organization, address) in [
        ("alice", Some("acme"), None),
        ("bob", Some("acme"), Some("+2")),
        ("carol", None, Some("+3")),
    ] {
        let mut identity = Identity::new(IdentityId::new(id).unwrap());
        identity.organization_id = organization.map(str::to_owned);
        if let Some(address) = address {
            identity.link(ChannelType::SMS, address);
        }
        store.create_identity(identity).await.unwrap();
    }
    store.close().await;
    {
        let connection = Connection::open(file.path()).unwrap();
        connection
            .execute_batch(
                "INSERT INTO identity_addresses (channel_type, address, identity_id, organization_id)
                 VALUES ('sms', '+1', 'alice', ''), ('sms', '+2', 'alice', ''),
                        ('sms', '+4', 'alice', 'globex'), ('sms', '+5', 'alice', '');
                 UPDATE identities SET data = json_set(data, '$.channel_addresses',
                     json('{\"sms\": [\"+1\", \"+2\"]}')) WHERE id = 'alice';
                 PRAGMA user_version = 13;",
            )
            .unwrap();
    }
    let reopened = SqliteStore::new(SqliteStoreOptions {
        path: file.path().into(),
    });
    assert!(
        reopened
            .get_identity(&IdentityId::new("alice").unwrap())
            .await
            .unwrap()
            .is_some()
    );
    reopened.close().await;
    let connection = Connection::open(file.path()).unwrap();
    let stored: Vec<(String, String, String)> = connection
        .prepare(
            "SELECT address, identity_id, organization_id FROM identity_addresses ORDER BY 1, 3",
        )
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let row = |address: &str, id: &str, organization: &str| {
        (address.to_owned(), id.to_owned(), organization.to_owned())
    };
    assert_eq!(
        stored,
        [
            row("+1", "alice", "acme"),
            row("+2", "bob", "acme"),
            row("+3", "carol", ""),
            row("+4", "alice", "globex"),
        ]
    );
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, 14);
}

/// Python: `test_an_unlink_removes_the_identity_s_rows_in_every_scope`: a link
/// revoked is revoked, a row a file's migration left in another scope with it, so
/// that no later rewrite can restore it (RFC §5.6).
#[tokio::test]
async fn an_unlink_removes_the_identity_s_rows_in_every_scope() {
    let file = TempFile::new("identity-unlink");
    let store = SqliteStore::new(SqliteStoreOptions {
        path: file.path().into(),
    });
    let mut identity = Identity::new(IdentityId::new("i1").unwrap());
    identity.organization_id = Some("acme".to_owned());
    identity.link(ChannelType::SMS, "+1");
    store.create_identity(identity.clone()).await.unwrap();
    let connection = Connection::open(file.path()).unwrap();
    connection
        .execute(
            "INSERT INTO identity_addresses (channel_type, address, identity_id, organization_id)
             VALUES ('sms', '+1', 'i1', '')",
            [],
        )
        .unwrap();
    store
        .unlink_address(
            &identity.id,
            &ChannelType::SMS,
            "+1",
            roomkit::store::UnlinkAddressOptions {
                organization_id: Some("acme".to_owned()),
            },
        )
        .await
        .unwrap();
    let left: i64 = connection
        .query_row("SELECT count(*) FROM identity_addresses", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(left, 0);
    store.close().await;
}
