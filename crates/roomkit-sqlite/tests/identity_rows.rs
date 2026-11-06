//! Python: `tests/test_store_sqlite.py::test_a_replacement_removes_a_row_of_another_scope_for_a_kept_address`:
//! a replacement leaves no row of another scope for an address it keeps, one a
//! file's migration left unscoped (RFC §17.2). This store's read already refuses
//! such a row, so the test reads the file.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod support;
#[path = "../../roomkit/tests/support/warnings.rs"]
mod warnings;

use roomkit::model::{
    ChannelId, ChannelType, Identity, IdentityId, Participant, ParticipantId, Room, RoomId,
};
use roomkit::store::{LinkAddressOptions, ResolveIdentityOptions, UnlinkAddressOptions};
use roomkit::{ConversationStore, ResolveParticipantOptions, RoomKit, RoomKitOptions, StoreError};
use roomkit_sqlite::{SqliteStore, SqliteStoreOptions};
use rusqlite::Connection;
use rusqlite::types::Value as Column;

use crate::support::TempFile;
use crate::warnings::Warnings;

/// Python's `_UNREADABLE_RECORDS`: records of the identity i1 the model cannot read,
/// each naming another organization's address, the last no UTF-8.
fn unreadable_records() -> Vec<Column> {
    vec![
        Column::Text(r#"{"email": "a@globex.example"}"#.to_owned()),
        Column::Text(
            r#"{"id": "i1", "email": "a@globex.example", "channel_addresses": "x"}"#.to_owned(),
        ),
        Column::Text(r#"["a@globex.example"]"#.to_owned()),
        Column::Text("not json, a@globex.example".to_owned()),
        Column::Blob(b"\xff{\"id\": \"i1\", \"email\": \"a@globex.example\"}".to_vec()),
    ]
}

/// Python's `_spoil_the_record`: writes `record` over the identity i1's, as text
/// whatever it holds.
fn spoil_the_record(file: &TempFile, record: &Column) {
    Connection::open(file.path())
        .unwrap()
        .execute(
            "UPDATE identities SET data = CAST(? AS TEXT) WHERE id = 'i1'",
            [record],
        )
        .unwrap();
}

/// A store whose identity i1, of globex, has a record the model cannot read.
async fn with_unreadable(record: &Column) -> (TempFile, SqliteStore) {
    let file = TempFile::new("identity-unreadable");
    let store = SqliteStore::new(SqliteStoreOptions {
        path: file.path().into(),
    });
    let mut holder = Identity::new(IdentityId::new("i1").unwrap());
    holder.organization_id = Some("globex".to_owned());
    store.create_identity(holder).await.unwrap();
    spoil_the_record(&file, record);
    (file, store)
}

/// `error`'s message, each of its causes' after it.
fn full_text(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut cause = error.source();
    while let Some(current) = cause {
        text.push_str(": ");
        text.push_str(&current.to_string());
        cause = current.source();
    }
    text
}

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
            UnlinkAddressOptions {
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

/// Python: `test_a_row_of_another_scope_than_its_identity_resolves_for_nobody` (RFC
/// §17.2): an identity is read in its own organization only, whatever a row a file's
/// migration left or a direct write says.
#[tokio::test]
async fn a_row_of_another_scope_than_its_identity_resolves_for_nobody() {
    for (held_in, stored) in [("", Some("acme")), ("globex", Some("acme")), ("acme", None)] {
        let file = TempFile::new("identity-scope");
        let store = SqliteStore::new(SqliteStoreOptions {
            path: file.path().into(),
        });
        let mut identity = Identity::new(IdentityId::new("i1").unwrap());
        identity.organization_id = stored.map(str::to_owned);
        store.create_identity(identity).await.unwrap();
        Connection::open(file.path())
            .unwrap()
            .execute(
                "INSERT INTO identity_addresses (channel_type, address, identity_id, organization_id)
                 VALUES ('sms', '+1555', 'i1', ?)",
                [held_in],
            )
            .unwrap();
        for scope in [None, Some("acme"), Some("globex")] {
            let found = store
                .resolve_identity(
                    &ChannelType::SMS,
                    "+1555",
                    ResolveIdentityOptions {
                        organization_id: scope.map(str::to_owned),
                    },
                )
                .await
                .unwrap();
            assert_eq!(found, None, "row in {held_in:?}, read in {scope:?}");
        }
        store.close().await;
    }
}

/// Python: `test_a_row_no_read_serves_leaves_its_address_free` (RFC §5.6): an address
/// is refused for what another identity of the organization holds. A row an older
/// release left, whose identity is of another organization, resolves for nobody: the
/// organization's own identity takes the address.
#[tokio::test]
async fn a_row_no_read_serves_leaves_its_address_free() {
    for (held_in, stored) in [("", Some("acme")), ("globex", Some("acme")), ("acme", None)] {
        for taking in ["create_identity", "link_address"] {
            let file = TempFile::new("identity-free");
            let store = SqliteStore::new(SqliteStoreOptions {
                path: file.path().into(),
            });
            let mut identity = Identity::new(IdentityId::new("i1").unwrap());
            identity.organization_id = stored.map(str::to_owned);
            store.create_identity(identity).await.unwrap();
            let connection = Connection::open(file.path()).unwrap();
            connection
                .execute(
                    "INSERT INTO identity_addresses (channel_type, address, identity_id, organization_id)
                     VALUES ('sms', '+1555', 'i1', ?)",
                    [held_in],
                )
                .unwrap();
            let scope = (!held_in.is_empty()).then(|| held_in.to_owned());
            let mut dana = Identity::new(IdentityId::new("dana").unwrap());
            dana.organization_id = scope.clone();
            if taking == "create_identity" {
                dana.link(ChannelType::SMS, "+1555");
                store.create_identity(dana).await.unwrap();
            } else {
                store.create_identity(dana).await.unwrap();
                store
                    .link_address(
                        &IdentityId::new("dana").unwrap(),
                        &ChannelType::SMS,
                        "+1555",
                        LinkAddressOptions {
                            organization_id: scope.clone(),
                        },
                    )
                    .await
                    .unwrap();
            }
            let found = store
                .resolve_identity(
                    &ChannelType::SMS,
                    "+1555",
                    ResolveIdentityOptions {
                        organization_id: scope,
                    },
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(found.id.as_str(), "dana", "row in {held_in:?}, {taking}");
            store.close().await;
        }
    }
}

/// Python: `test_a_holder_the_model_cannot_read_keeps_its_address` (RFC §5.6): the
/// refusal stands, and quotes no part of the holder's record, which may be another
/// organization's: each of Python's unreadable records, the one no UTF-8 among them.
#[tokio::test]
async fn a_holder_the_model_cannot_read_keeps_its_address() {
    for record in unreadable_records() {
        let (file, store) = with_unreadable(&record).await;
        let connection = Connection::open(file.path()).unwrap();
        connection
            .execute(
                "INSERT INTO identity_addresses (channel_type, address, identity_id, organization_id)
                 VALUES ('sms', '+1555', 'i1', 'acme')",
                [],
            )
            .unwrap();
        let mut dana = Identity::new(IdentityId::new("dana").unwrap());
        dana.organization_id = Some("acme".to_owned());
        dana.link(ChannelType::SMS, "+1555");
        assert_eq!(
            store.create_identity(dana).await,
            Err(StoreError::AddressLinked {
                channel_type: ChannelType::SMS
            }),
            "{record:?}"
        );
        store.close().await;
    }
}

/// Python: `test_a_record_the_model_cannot_read_serves_no_read` (HITL 4ee4d298): it
/// resolves to nobody, for any organization, and neither the caller nor the log reads
/// the record, which may be another organization's.
#[tokio::test]
async fn a_record_the_model_cannot_read_serves_no_read() {
    for record in unreadable_records() {
        let (file, store) = with_unreadable(&record).await;
        Connection::open(file.path())
            .unwrap()
            .execute(
                "INSERT INTO identity_addresses (channel_type, address, identity_id, organization_id)
                 VALUES ('sms', '+1555', 'i1', 'acme')",
                [],
            )
            .unwrap();
        let (warnings, capture) = Warnings::capture();
        let resolved = store
            .resolve_identity(
                &ChannelType::SMS,
                "+1555",
                ResolveIdentityOptions {
                    organization_id: Some("acme".to_owned()),
                },
            )
            .await;
        drop(capture);
        store.close().await;
        assert_eq!(resolved, Ok(None), "{record:?}");
        let logged = warnings.containing("i1");
        assert_eq!(logged.len(), 1, "{record:?}");
        assert!(
            !logged[0].contains("globex") && !logged[0].contains("+1555"),
            "{logged:?}"
        );
    }
}

/// Python: `test_a_read_by_id_of_a_record_the_model_cannot_read_quotes_none_of_it` (HITL
/// 47b7dfc6): the read fails with a fixed error, the id alone, whatever the caller's
/// organization: the record may be another organization's. Python's `ValueError` is
/// [`StoreError::Value`].
#[tokio::test]
async fn a_read_by_id_of_a_record_the_model_cannot_read_quotes_none_of_it() {
    const FIXED: &str = "Identity i1 has a record the model cannot read";
    let acme = || Some("acme".to_owned());
    let i1 = IdentityId::new("i1").unwrap();
    for record in unreadable_records() {
        for door in [
            "get_identity",
            "create_identity",
            "link_address",
            "unlink_address",
            "resolve_participant",
        ] {
            let (_file, store) = with_unreadable(&record).await;
            let refused: String = match door {
                "get_identity" => full_text(&store.get_identity(&i1).await.unwrap_err()),
                "create_identity" => {
                    let mut again = Identity::new(i1.clone());
                    again.organization_id = acme();
                    let refused = store.create_identity(again).await.unwrap_err();
                    assert_eq!(refused, StoreError::Value(FIXED.to_owned()), "{record:?}");
                    full_text(&refused)
                }
                "link_address" => full_text(
                    &store
                        .link_address(
                            &i1,
                            &ChannelType::SMS,
                            "+2",
                            LinkAddressOptions {
                                organization_id: acme(),
                            },
                        )
                        .await
                        .unwrap_err(),
                ),
                "unlink_address" => full_text(
                    &store
                        .unlink_address(
                            &i1,
                            &ChannelType::SMS,
                            "+2",
                            UnlinkAddressOptions {
                                organization_id: acme(),
                            },
                        )
                        .await
                        .unwrap_err(),
                ),
                _ => {
                    let room = RoomId::new("r1").unwrap();
                    let mut held = Room::new(room.clone());
                    held.organization_id = acme();
                    store.create_room(held).await.unwrap();
                    store
                        .add_participant(Participant::new(
                            ParticipantId::new("p1").unwrap(),
                            room.clone(),
                            ChannelId::new("sms").unwrap(),
                        ))
                        .await
                        .unwrap();
                    let kit = RoomKit::new(RoomKitOptions::with_store(store.clone()));
                    let refused = kit
                        .resolve_participant(
                            &room,
                            &ParticipantId::new("p1").unwrap(),
                            &i1,
                            ResolveParticipantOptions {
                                organization_id: acme(),
                                ..ResolveParticipantOptions::default()
                            },
                        )
                        .await
                        .unwrap_err();
                    kit.close().await.unwrap();
                    full_text(&refused)
                }
            };
            store.close().await;
            assert!(refused.ends_with(FIXED), "{door}, {record:?}: {refused}");
            assert!(!refused.contains("globex"), "{door}, {record:?}: {refused}");
        }
    }
}

/// Python: `test_the_rewrite_takes_only_a_list_for_a_declaration`: a declaration no
/// API writes (a text, a map) declares nothing: the row goes, as on Postgres, and the
/// file still opens.
#[tokio::test]
async fn the_rewrite_takes_only_a_list_for_a_declaration() {
    let file = TempFile::new("identity-declaration");
    let store = SqliteStore::new(SqliteStoreOptions {
        path: file.path().into(),
    });
    let mut identity = Identity::new(IdentityId::new("alice").unwrap());
    identity.organization_id = Some("acme".to_owned());
    store.create_identity(identity).await.unwrap();
    store.close().await;
    let connection = Connection::open(file.path()).unwrap();
    connection
        .execute_batch(
            "INSERT INTO identity_addresses (channel_type, address, identity_id, organization_id)
             VALUES ('sms', 'a@b.c', 'alice', ''), ('mms', '+2', 'alice', '');
             UPDATE identities SET data = json_set(data, '$.channel_addresses',
                 json('{\"sms\": \"a@b.c\", \"mms\": {\"k\": \"+2\"}}')) WHERE id = 'alice';
             PRAGMA user_version = 13;",
        )
        .unwrap();
    let reopened = SqliteStore::new(SqliteStoreOptions {
        path: file.path().into(),
    });
    let opened = reopened.room_exists(&RoomId::new("any").unwrap()).await;
    assert_eq!(opened, Ok(false));
    reopened.close().await;
    let left: i64 = connection
        .query_row("SELECT count(*) FROM identity_addresses", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(left, 0);
}
