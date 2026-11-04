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
