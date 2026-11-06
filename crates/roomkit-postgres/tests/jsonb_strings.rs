//! A JSONB column that holds a JSON string, on a real database: Python's tests
//! of the same names in `tests/test_store_postgres.py` (HITL f9737c41). Python's
//! pool decodes a JSONB column, and its mappers decode a string once more, so
//! that such a column reads as the value its text holds. Each test skips itself
//! without `POSTGRES_DSN`.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod support;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelId, ChannelType, EventId, EventSource, Identity, IdentityId, Metadata,
    Observation, ObservationId, Participant, ParticipantId, RateLimit, RetryPolicy, Room,
    RoomEvent, RoomId, RoomTimers, Task, TaskId, TextContent, Timestamp, Verification,
    VerificationRequest,
};
use roomkit::store::{
    CommitEventOptions, GetVerificationOptions, LinkAddressOptions, ResolveIdentityOptions,
    UnlinkAddressOptions,
};
use roomkit::{ConversationStore, StoreError};
use roomkit_postgres::PostgresStore;
use serde_json::{Value, json};

use crate::support::TempSchema;

/// A table with a JSONB column, and the row of it the tests write (Python's
/// `_JSONB_ROW_KEYS`).
#[derive(Clone, Copy, Debug)]
enum Table {
    Rooms,
    Events,
    Bindings,
    Participants,
    Identities,
    Verifications,
    VerificationRequests,
    Tasks,
    Observations,
}

impl Table {
    fn name(self) -> &'static str {
        match self {
            Self::Rooms => "rooms",
            Self::Events => "events",
            Self::Bindings => "bindings",
            Self::Participants => "participants",
            Self::Identities => "identities",
            Self::Verifications => "verifications",
            Self::VerificationRequests => "verification_requests",
            Self::Tasks => "tasks",
            Self::Observations => "observations",
        }
    }

    fn row(self) -> &'static str {
        match self {
            Self::Rooms => "id = 'r1'",
            Self::Events => "id = 'e1'",
            Self::Bindings => "room_id = 'r1' AND channel_id = 'ch1'",
            Self::Participants => "room_id = 'r1' AND id = 'p1'",
            Self::Identities => "id = 'i1'",
            Self::Verifications => "identity_id = 'i1'",
            Self::VerificationRequests => "id = 'q1'",
            Self::Tasks => "id = 't1'",
            Self::Observations => "id = 'o1'",
        }
    }
}

/// Python's `_JSONB_COLUMNS`: each JSONB column a read of the store decodes.
const JSONB_COLUMNS: [(Table, &str); 19] = [
    (Table::Rooms, "metadata"),
    (Table::Rooms, "timers"),
    (Table::Events, "content"),
    (Table::Events, "source_extra"),
    (Table::Events, "metadata"),
    (Table::Events, "channel_data"),
    (Table::Events, "delivery_results"),
    (Table::Bindings, "capabilities"),
    (Table::Bindings, "metadata"),
    (Table::Bindings, "rate_limit"),
    (Table::Bindings, "retry_policy"),
    (Table::Participants, "metadata"),
    (Table::Identities, "channel_addresses"),
    (Table::Identities, "external_ids"),
    (Table::Identities, "metadata"),
    (Table::Verifications, "data"),
    (Table::VerificationRequests, "data"),
    (Table::Tasks, "metadata"),
    (Table::Observations, "metadata"),
];

/// A write over a row, after a read of it (Python's `_write_the_row_again`).
#[derive(Clone, Copy, Debug)]
enum Write {
    UpdateRoom,
    CommitEvent,
    AddBinding,
    UpdateBinding,
    PutVerification,
    UpdateVerificationRequest,
    CreateIdentity,
}

fn members(value: Value) -> Metadata {
    serde_json::from_value(value).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("r1").unwrap()
}

fn channel() -> ChannelId {
    ChannelId::new("ch1").unwrap()
}

fn identity_id() -> IdentityId {
    IdentityId::new("i1").unwrap()
}

/// Python's `_a_row_of_each_table`: a row in each table with a JSONB column,
/// each such column an object with members.
async fn a_row_of_each_table(store: &PostgresStore) {
    let at: Timestamp = "2026-01-01T12:00:00Z".parse().unwrap();
    let mut room = Room::new(room_id());
    room.metadata = members(json!({"topic": "refund"}));
    room.timers = RoomTimers {
        inactive_after_seconds: Some(60),
        ..RoomTimers::default()
    };
    store.create_room(room).await.unwrap();
    let mut source = EventSource::new(channel(), ChannelType::SMS);
    source.external_id = Some("x1".to_owned());
    source.raw_payload = members(json!({"sid": "SM1"}));
    let mut event = RoomEvent::new(room_id(), source, TextContent::new("hello"));
    event.id = EventId::new("e1").unwrap();
    event.metadata = members(json!({"k": "v"}));
    event.channel_data = members(json!({"thread_id": "th1"}));
    event.delivery_results = members(json!({"ch2": {"status": "sent"}}));
    store.add_event(event).await.unwrap();
    let mut binding = ChannelBinding::new(channel(), room_id(), ChannelType::SMS);
    binding.metadata = members(json!({"k": "v"}));
    binding.rate_limit = Some(RateLimit {
        max_per_second: Some(2.0),
        ..RateLimit::default()
    });
    binding.retry_policy = Some(RetryPolicy {
        max_retries: 5,
        ..RetryPolicy::default()
    });
    store.add_binding(binding).await.unwrap();
    let mut participant = Participant::new(ParticipantId::new("p1").unwrap(), room_id(), channel());
    participant.metadata = members(json!({"k": "v"}));
    store.add_participant(participant).await.unwrap();
    let mut identity = Identity::new(identity_id());
    identity.organization_id = Some("acme".to_owned());
    identity.link(ChannelType::SMS, "+1555");
    identity
        .external_ids
        .insert("crm".to_owned(), "c1".to_owned());
    identity.metadata = members(json!({"k": "v"}));
    store.create_identity(identity).await.unwrap();
    let mut verification = Verification::new(identity_id());
    verification.method = Some("otp".to_owned());
    verification.verified_at = Some(at);
    store.put_verification(verification).await.unwrap();
    let expires_at: Timestamp = "2026-01-01T12:05:00Z".parse().unwrap();
    let request = VerificationRequest::new(
        "q1",
        identity_id(),
        room_id(),
        channel(),
        "otp",
        "h",
        at,
        at,
        expires_at,
        3,
    );
    store.create_verification_request(request).await.unwrap();
    let mut task = Task::new(TaskId::new("t1").unwrap(), room_id(), "Follow up");
    task.metadata = members(json!({"k": "v"}));
    store.add_task(task).await.unwrap();
    let mut observation = Observation::new(
        ObservationId::new("o1").unwrap(),
        room_id(),
        channel(),
        "calm",
    );
    observation.metadata = members(json!({"k": "v"}));
    store.add_observation(observation).await.unwrap();
}

/// Python's `_read_the_row`: the model the row of `table` reads as, as JSON.
async fn read_the_row(store: &PostgresStore, table: Table) -> Result<Value, StoreError> {
    let read = match table {
        Table::Rooms => serde_json::to_value(store.get_room(&room_id()).await?),
        Table::Events => serde_json::to_value(store.get_event(&EventId::new("e1").unwrap()).await?),
        Table::Bindings => serde_json::to_value(store.get_binding(&room_id(), &channel()).await?),
        Table::Participants => serde_json::to_value(
            store
                .get_participant(&room_id(), &ParticipantId::new("p1").unwrap())
                .await?,
        ),
        Table::Identities => serde_json::to_value(store.get_identity(&identity_id()).await?),
        Table::Verifications => serde_json::to_value(
            store
                .get_verification(&identity_id(), GetVerificationOptions::default())
                .await?,
        ),
        Table::VerificationRequests => {
            serde_json::to_value(store.get_verification_request("q1").await?)
        }
        Table::Tasks => serde_json::to_value(store.get_task(&TaskId::new("t1").unwrap()).await?),
        Table::Observations => serde_json::to_value(store.list_observations(&room_id()).await?),
    };
    Ok(read.unwrap())
}

/// Python's `_held_as_a_string`: writes the row's `column` over as a JSON
/// string of its own text, as a hand could.
async fn held_as_a_string(schema: &TempSchema, table: Table, column: &str) {
    let client = schema.client().await;
    let (row, table) = (table.row(), table.name());
    client
        .execute(
            &format!("UPDATE {table} SET {column} = to_jsonb({column}::text) WHERE {row}"),
            &[],
        )
        .await
        .unwrap();
    let held: String = client
        .query_one(
            &format!("SELECT jsonb_typeof({column}) FROM {table} WHERE {row}"),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(held, "string", "{table}.{column}");
}

/// Python: `test_a_jsonb_string_holding_json_reads_as_its_value` (HITL
/// f9737c41): the column reads as the value its text holds, as the object did.
#[tokio::test]
async fn a_jsonb_string_holding_json_reads_as_its_value() {
    if support::dsn().is_none() {
        return;
    }
    let mut misread = Vec::new();
    for (table, column) in JSONB_COLUMNS {
        let schema = TempSchema::new().await;
        let store = schema.initialized_store().await;
        a_row_of_each_table(&store).await;
        let written = read_the_row(&store, table).await.unwrap();
        held_as_a_string(&schema, table, column).await;
        let read = read_the_row(&store, table).await;
        if !matches!(&read, Ok(read) if *read == written) {
            misread.push(format!("{}.{column}: {read:?}", table.name()));
        }
    }
    assert!(misread.is_empty(), "{misread:#?}");
}

/// Python's `_column_value`: the row's `column`, and the JSON type it holds.
async fn column_value(schema: &TempSchema, table: Table, column: &str) -> (Value, String) {
    let row = schema
        .client()
        .await
        .query_one(
            &format!(
                "SELECT {column}, jsonb_typeof({column}) FROM {} WHERE {}",
                table.name(),
                table.row()
            ),
            &[],
        )
        .await
        .unwrap();
    (row.get(0), row.get(1))
}

/// Python's `_write_the_row_again`: writes over the row through `write`, with
/// the model the row reads as; a commit adds an event to the room r1, not its
/// activity.
async fn write_the_row_again(store: &PostgresStore, write: Write) -> Result<(), StoreError> {
    match write {
        Write::CommitEvent => {
            let later = RoomEvent::new(
                room_id(),
                EventSource::new(channel(), ChannelType::SMS),
                TextContent::new("later"),
            );
            let options = CommitEventOptions {
                activity: false,
                ..CommitEventOptions::default()
            };
            store.commit_event(&room_id(), later, options).await?;
        }
        Write::UpdateRoom => {
            let room = store.get_room(&room_id()).await?.unwrap();
            store.update_room(room).await?;
        }
        Write::AddBinding => {
            let binding = store.get_binding(&room_id(), &channel()).await?.unwrap();
            store.add_binding(binding).await?;
        }
        Write::UpdateBinding => {
            let binding = store.get_binding(&room_id(), &channel()).await?.unwrap();
            store.update_binding(binding).await?;
        }
        Write::PutVerification => {
            let verification = store
                .get_verification(&identity_id(), GetVerificationOptions::default())
                .await?
                .unwrap();
            store.put_verification(verification).await?;
        }
        Write::UpdateVerificationRequest => {
            let request = store.get_verification_request("q1").await?.unwrap();
            store.update_verification_request(request).await?;
        }
        Write::CreateIdentity => {
            let identity = store.get_identity(&identity_id()).await?.unwrap();
            store.create_identity(identity).await?;
        }
    }
    Ok(())
}

/// Python: `test_a_write_over_a_jsonb_string_leaves_an_object` (HITL
/// f9737c41): a write over the row holds the object the column held, which
/// both implementations read, not the string, nor the string and the object
/// as one array.
#[tokio::test]
async fn a_write_over_a_jsonb_string_leaves_an_object() {
    if support::dsn().is_none() {
        return;
    }
    let mut misheld = Vec::new();
    for (table, column, write) in [
        (Table::Rooms, "timers", Write::UpdateRoom),
        (Table::Rooms, "timers", Write::CommitEvent),
        (Table::Bindings, "capabilities", Write::AddBinding),
        (Table::Bindings, "rate_limit", Write::AddBinding),
        (Table::Bindings, "retry_policy", Write::AddBinding),
        (Table::Bindings, "capabilities", Write::UpdateBinding),
        (Table::Bindings, "rate_limit", Write::UpdateBinding),
        (Table::Bindings, "retry_policy", Write::UpdateBinding),
        (Table::Verifications, "data", Write::PutVerification),
        (
            Table::VerificationRequests,
            "data",
            Write::UpdateVerificationRequest,
        ),
        (
            Table::Identities,
            "channel_addresses",
            Write::CreateIdentity,
        ),
    ] {
        let schema = TempSchema::new().await;
        let store = schema.initialized_store().await;
        a_row_of_each_table(&store).await;
        let written = column_value(&schema, table, column).await;
        assert_eq!(written.1, "object");
        held_as_a_string(&schema, table, column).await;
        let wrote = write_the_row_again(&store, write).await;
        let held = column_value(&schema, table, column).await;
        if wrote.is_err() || held != written {
            misheld.push(format!(
                "{}.{column} by {write:?}: {wrote:?}, {held:?}",
                table.name()
            ));
        }
    }
    assert!(misheld.is_empty(), "{misheld:#?}");
}

/// Python: `test_an_address_change_over_a_jsonb_string_writes_an_object` (HITL
/// f9737c41): a link or an unlink reads the identity's addresses as the value
/// the string holds, and writes the changed object, the address rows with it.
#[tokio::test]
async fn an_address_change_over_a_jsonb_string_writes_an_object() {
    if support::dsn().is_none() {
        return;
    }
    for link in [true, false] {
        let schema = TempSchema::new().await;
        let store = schema.initialized_store().await;
        a_row_of_each_table(&store).await;
        held_as_a_string(&schema, Table::Identities, "channel_addresses").await;
        let acme = || Some("acme".to_owned());
        let address = if link { "+2" } else { "+1555" };
        if link {
            let options = LinkAddressOptions {
                organization_id: acme(),
            };
            store
                .link_address(&identity_id(), &ChannelType::SMS, address, options)
                .await
                .unwrap();
        } else {
            let options = UnlinkAddressOptions {
                organization_id: acme(),
            };
            store
                .unlink_address(&identity_id(), &ChannelType::SMS, address, options)
                .await
                .unwrap();
        }
        let held = if link {
            json!({"sms": ["+1555", "+2"]})
        } else {
            json!({})
        };
        assert_eq!(
            column_value(&schema, Table::Identities, "channel_addresses").await,
            (held.clone(), "object".to_owned()),
            "link {link}"
        );
        let identity = store.get_identity(&identity_id()).await.unwrap().unwrap();
        assert_eq!(
            serde_json::to_value(&identity.channel_addresses).unwrap(),
            held,
            "link {link}"
        );
        let options = ResolveIdentityOptions {
            organization_id: acme(),
        };
        let resolved = store
            .resolve_identity(&ChannelType::SMS, address, options)
            .await
            .unwrap();
        assert_eq!(resolved.is_some(), link, "link {link}");
    }
}

/// Python: `test_an_event_update_over_a_jsonb_string_holding_no_json_writes_it`
/// (HITL f9737c41): an update reads nothing of the row it writes over, as
/// Python's: a content and delivery results that hold no JSON text are no
/// refusal, and the content is written.
#[tokio::test]
async fn an_event_update_over_a_jsonb_string_holding_no_json_writes_it() {
    if support::dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let store = schema.initialized_store().await;
    a_row_of_each_table(&store).await;
    let event = store
        .get_event(&EventId::new("e1").unwrap())
        .await
        .unwrap()
        .unwrap();
    schema
        .client()
        .await
        .execute(
            "UPDATE events SET content = to_jsonb('not json'::text), \
             delivery_results = to_jsonb('not json'::text) WHERE id = 'e1'",
            &[],
        )
        .await
        .unwrap();
    store.update_event(event).await.unwrap();
    assert_eq!(
        column_value(&schema, Table::Events, "content").await,
        (
            json!({"type": "text", "body": "hello", "language": null}),
            "object".to_owned()
        )
    );
}

/// Rust only, as Python's write replaces what it does not read: a merge over a
/// JSON string keeps a member of the value it holds that the model does not
/// read, as over an object (`a_commit_and_a_verification_keep_what_the_model_does_not_read`).
#[tokio::test]
async fn a_merge_over_a_jsonb_string_keeps_what_the_model_does_not_read() {
    if support::dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let store = schema.initialized_store().await;
    a_row_of_each_table(&store).await;
    schema
        .client()
        .await
        .batch_execute(
            "UPDATE rooms SET timers = to_jsonb((timers || '{\"later\": 1}')::text);
             UPDATE identities SET channel_addresses =
                 to_jsonb((channel_addresses || '{\"SMS\": [\"+9\"]}')::text);",
        )
        .await
        .unwrap();
    let room = store.get_room(&room_id()).await.unwrap().unwrap();
    store.update_room(room).await.unwrap();
    let identity = store.get_identity(&identity_id()).await.unwrap().unwrap();
    store.create_identity(identity).await.unwrap();
    let (timers, _) = column_value(&schema, Table::Rooms, "timers").await;
    let (addresses, _) = column_value(&schema, Table::Identities, "channel_addresses").await;
    assert_eq!(timers["later"], json!(1));
    assert_eq!(addresses["SMS"], json!(["+9"]));
}
