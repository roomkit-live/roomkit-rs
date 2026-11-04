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
