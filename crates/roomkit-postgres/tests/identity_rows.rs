//! An identity row the model cannot read, on a real database: Python's tests of
//! the same names in `tests/test_store_postgres.py` (HITL 4ee4d298, 47b7dfc6,
//! 70fef50d). Each test skips itself without `POSTGRES_DSN`.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod support;
#[path = "../../roomkit/tests/support/warnings.rs"]
mod warnings;

use roomkit::model::{
    ChannelId, ChannelType, Identity, IdentityId, Participant, ParticipantId, Room, RoomId,
};
use roomkit::store::{LinkAddressOptions, ResolveIdentityOptions, UnlinkAddressOptions};
use roomkit::{
    ConversationStore, ResolveParticipantOptions, RoomKit, RoomKitError, RoomKitOptions, StoreError,
};
use roomkit_postgres::PostgresStore;

use crate::support::{TempSchema, full_text};
use crate::warnings::Warnings;

/// Python's `_UNREADABLE_COLUMNS`: a JSONB column of the identity i1 of another
/// shape than its field's, each quoting an address of globex's.
const UNREADABLE_COLUMNS: [(&str, &str); 5] = [
    ("channel_addresses", r#"["a@globex.example"]"#),
    ("channel_addresses", r#"{"sms": "a@globex.example"}"#),
    (
        "channel_addresses",
        r#"{"sms": ["+1555"], "SMS": "a@globex.example"}"#,
    ),
    ("external_ids", r#""a@globex.example""#),
    ("metadata", r#"["a@globex.example"]"#),
];

const FIXED: &str = "Identity i1 has a record the model cannot read";

/// A store on a schema of its own, holding globex's identity i1 with the
/// address `+1555`, its `column` spoiled with `value` (Python's
/// `_spoil_the_row`).
async fn with_unreadable(column: &str, value: &str) -> (TempSchema, PostgresStore) {
    let schema = TempSchema::new().await;
    let store = schema.initialized_store().await;
    let mut i1 = Identity::new(IdentityId::new("i1").unwrap());
    i1.organization_id = Some("globex".to_owned());
    i1.link(ChannelType::SMS, "+1555");
    store.create_identity(i1).await.unwrap();
    schema
        .client()
        .await
        .execute(
            &format!("UPDATE identities SET {column} = $1::text::jsonb WHERE id = 'i1'"),
            &[&value],
        )
        .await
        .unwrap();
    (schema, store)
}

/// Python: `test_a_record_the_model_cannot_read_serves_no_read` (HITL 4ee4d298,
/// 70fef50d): it resolves to nobody, and neither the caller nor the log reads
/// the row, which may be another organization's.
#[tokio::test]
async fn a_record_the_model_cannot_read_serves_no_read() {
    if support::dsn().is_none() {
        return;
    }
    for (column, value) in UNREADABLE_COLUMNS {
        let (_schema, store) = with_unreadable(column, value).await;
        let (warnings, capture) = Warnings::capture();
        let resolved = store
            .resolve_identity(
                &ChannelType::SMS,
                "+1555",
                ResolveIdentityOptions {
                    organization_id: Some("globex".to_owned()),
                },
            )
            .await;
        drop(capture);
        store.close().await;
        assert_eq!(resolved, Ok(None), "{column} {value}");
        let logged = warnings.containing("i1");
        assert_eq!(logged.len(), 1, "{column} {value}: {logged:?}");
        assert!(!logged[0].contains("a@globex.example"), "{logged:?}");
    }
}

/// Python: `test_a_read_by_id_of_a_record_the_model_cannot_read_quotes_none_of_it`
/// (HITL 47b7dfc6, 70fef50d): the read fails with a fixed error, the id alone,
/// as on `SqliteStore`: the row may be another organization's. Python's
/// `ValueError` is [`StoreError::Value`], and the kit's `RoomKitError::Value`.
#[tokio::test]
async fn a_read_by_id_of_a_record_the_model_cannot_read_quotes_none_of_it() {
    if support::dsn().is_none() {
        return;
    }
    let globex = || Some("globex".to_owned());
    let i1 = IdentityId::new("i1").unwrap();
    for (column, value) in UNREADABLE_COLUMNS {
        for door in [
            "get_identity",
            "link_address",
            "unlink_address",
            "resolve_participant",
        ] {
            let (_schema, store) = with_unreadable(column, value).await;
            let fixed = StoreError::Value(FIXED.to_owned());
            let refused: String = match door {
                "get_identity" => {
                    let refused = store.get_identity(&i1).await.unwrap_err();
                    assert_eq!(refused, fixed, "{column} {value}");
                    full_text(&refused)
                }
                "link_address" => {
                    let options = LinkAddressOptions {
                        organization_id: globex(),
                    };
                    let refused = store
                        .link_address(&i1, &ChannelType::SMS, "+2", options)
                        .await
                        .unwrap_err();
                    assert_eq!(refused, fixed, "{column} {value}");
                    full_text(&refused)
                }
                "unlink_address" => {
                    let options = UnlinkAddressOptions {
                        organization_id: globex(),
                    };
                    let refused = store
                        .unlink_address(&i1, &ChannelType::SMS, "+2", options)
                        .await
                        .unwrap_err();
                    assert_eq!(refused, fixed, "{column} {value}");
                    full_text(&refused)
                }
                _ => {
                    let room = RoomId::new("r1").unwrap();
                    let mut held = Room::new(room.clone());
                    held.organization_id = globex();
                    store.create_room(held).await.unwrap();
                    let p1 = ParticipantId::new("p1").unwrap();
                    let sms = ChannelId::new("sms").unwrap();
                    store
                        .add_participant(Participant::new(p1.clone(), room.clone(), sms))
                        .await
                        .unwrap();
                    // No organization named: a caller naming one learns nothing of
                    // the row (HITL 3f36d6c8).
                    let kit = RoomKit::new(RoomKitOptions::with_store(store.clone()));
                    let options = ResolveParticipantOptions::default();
                    let refused = kit
                        .resolve_participant(&room, &p1, &i1, options)
                        .await
                        .unwrap_err();
                    kit.close().await.unwrap();
                    assert!(
                        matches!(&refused, RoomKitError::Value(text) if text == FIXED),
                        "{column} {value}: {refused:?}"
                    );
                    full_text(&refused)
                }
            };
            store.close().await;
            assert!(refused.ends_with(FIXED), "{door}, {column}: {refused}");
            assert!(
                !refused.contains("a@globex.example"),
                "{door}, {column}: {refused}"
            );
        }
    }
}

/// Python: `test_a_row_with_an_empty_id_serves_no_read`: a row no API writes,
/// its id empty, which the model refuses: it resolves to nobody, and the log
/// names its id as it is.
#[tokio::test]
async fn a_row_with_an_empty_id_serves_no_read() {
    if support::dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let store = schema.initialized_store().await;
    schema
        .client()
        .await
        .batch_execute(
            "INSERT INTO identities (id, organization_id, channel_addresses) \
             VALUES ('', 'globex', '{\"sms\": [\"+1555\"]}'); \
             INSERT INTO identity_addresses (channel_type, address, identity_id, organization_id) \
             VALUES ('sms', '+1555', '', 'globex')",
        )
        .await
        .unwrap();
    let (warnings, capture) = Warnings::capture();
    let resolved = store
        .resolve_identity(
            &ChannelType::SMS,
            "+1555",
            ResolveIdentityOptions {
                organization_id: Some("globex".to_owned()),
            },
        )
        .await;
    drop(capture);
    store.close().await;
    assert_eq!(resolved, Ok(None));
    assert_eq!(
        warnings
            .containing("Identity  has a record the model cannot read")
            .len(),
        1
    );
}

/// Python: `test_a_replacement_repairs_a_row_the_model_cannot_read`: a
/// replacement of an identity whose row holds a member the model refuses,
/// under a key that is no channel type's name, writes the addresses it
/// declares, and the row reads again.
#[tokio::test]
async fn a_replacement_repairs_a_row_the_model_cannot_read() {
    if support::dsn().is_none() {
        return;
    }
    let (_schema, store) = with_unreadable(
        "channel_addresses",
        r#"{"sms": ["+1555"], "SMS": "a@globex.example"}"#,
    )
    .await;
    let mut i1 = Identity::new(IdentityId::new("i1").unwrap());
    i1.organization_id = Some("globex".to_owned());
    i1.link(ChannelType::SMS, "+1555");
    store.create_identity(i1.clone()).await.unwrap();
    let read = store.get_identity(&i1.id).await;
    store.close().await;
    assert_eq!(read, Ok(Some(i1)));
}
