//! [`PostgresStore`], the [`ConversationStore`] over a Postgres database.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use deadpool_postgres::Pool;
use roomkit::RoomFilter;
use roomkit::model::{
    ChannelBinding, ChannelId, ChannelType, EventId, Identity, IdentityId, Metadata, Observation,
    Participant, ParticipantId, Room, RoomEvent, RoomId, RoomStatus, Task, TaskId, ThreadSummary,
    Timestamp, Verification, VerificationRequest,
};
use roomkit::store::{
    AdvanceDeliveredIndexOptions, CommitEventOptions, DeleteEventOptions, FindLatestRoomOptions,
    FindRoomIdByBindingOptions, FindRoomIdByChannelOptions, FindRoomIdByParticipantOptions,
    FindRoomIdsByChannelOptions, FindRoomsOptions, GetEventCountOptions, GetVerificationOptions,
    LinkAddressOptions, ListEventsOptions, ListRoomsOptions, ListTasksOptions,
    PatchRoomMetadataOptions, ResolveIdentityOptions, UnlinkAddressOptions,
};
use roomkit::telemetry::TelemetryProvider;
use roomkit::{ConversationStore, StoreError, written};

use crate::connect::Tls;
use crate::error::{Failure, NoConnection};
use crate::lent::{Lent, cancellable};
use crate::options::{
    DedupeEventIndicesOptions, DropLegacyParentIndexOptions, InitOptions, MigrateOptions,
    PostgresStoreOptions,
};
use crate::spans::QuerySpans;
use crate::{
    events, identities, lookups, markers, members, pool, rooms, rows, schema, side_effects,
    timeline, verifications,
};

/// How long an operation waits for a connection: Python's `_acquire_timeout`.
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a close gives the connections in use: Python's `_close_timeout`.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

/// A conversation store in a Postgres database, on the Python
/// implementation's schema (see the [crate] documentation): Python's
/// `PostgresStore`.
///
/// Every read returns copies and every write copies in (RFC §14.4). Each
/// method that writes is one statement or one transaction: a commit's event,
/// its room's counters and its target are written together or not at all
/// (RFC §14.3), even across processes. Its clones share the pool.
#[derive(Clone)]
pub struct PostgresStore {
    shared: Arc<Shared>,
}

struct Shared {
    dsn: Option<String>,
    pool: RwLock<Option<Pool>>,
    /// What a cancel request connects with, for a pool the store opened.
    tls: RwLock<Option<Tls>>,
    owns_pool: bool,
    telemetry: QuerySpans,
}

impl fmt::Debug for PostgresStore {
    /// Writes no DSN: one holds a password.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresStore")
            .field("owns_pool", &self.shared.owns_pool)
            .finish_non_exhaustive()
    }
}

impl PostgresStore {
    /// The store of `options.dsn`, or of `options.pool`, which stays the
    /// integrator's: no I/O happens here. [`init`](Self::init) must run
    /// before any other operation.
    pub fn new(options: PostgresStoreOptions) -> Self {
        let PostgresStoreOptions { dsn, pool } = options;
        Self {
            shared: Arc::new(Shared {
                dsn,
                owns_pool: pool.is_none(),
                pool: RwLock::new(pool),
                tls: RwLock::new(None),
                telemetry: QuerySpans::default(),
            }),
        }
    }

    /// The [`RoomEvent`] a row of the `events` table stores: Python's
    /// `event_from_row`, for a host that reads events with a query of its own
    /// and reads them as the store would. The row carries every column of
    /// `events`; columns beyond them are ignored.
    ///
    /// # Errors
    ///
    /// Fails [`StoreError::Unavailable`] for a row without those columns, or
    /// one the model refuses.
    pub fn event_from_row(row: &tokio_postgres::Row) -> Result<RoomEvent, StoreError> {
        Ok(rows::event(row)?)
    }

    /// Creates the connection pool if needed, and ensures the schema exists:
    /// Python's `init(min_size=2, max_size=10)`. It runs only additive,
    /// idempotent schema maintenance, under an advisory lock, in one
    /// transaction, so a fleet restarting at once runs it one process after
    /// the other, and a library upgrade destroys no data but, once, address rows no
    /// read serves: one whose organization holds the address, or whose identity no
    /// longer declares it (RFC §17.2).
    ///
    /// # Errors
    ///
    /// Fails [`StoreError::Unavailable`], its cause a
    /// [`PostgresSchemaError`](crate::PostgresSchemaError) for a version 1
    /// schema, which is left as it is ([`migrate`](Self::migrate) moves it),
    /// else what failed; [`StoreError::Value`] for sizes or a connection
    /// string asyncpg refuses.
    pub async fn init(&self, options: InitOptions) -> Result<(), StoreError> {
        cancellable(async {
            if self.pool().is_none() {
                let (pool, tls) = pool::open(
                    self.shared.dsn.as_deref(),
                    options.min_size,
                    options.max_size,
                )
                .await?;
                *self
                    .shared
                    .tls
                    .write()
                    .unwrap_or_else(PoisonError::into_inner) = tls;
                // Kept before the schema is read, as Python keeps it: a schema
                // `init` refuses is one `migrate` moves, on this pool.
                *self
                    .shared
                    .pool
                    .write()
                    .unwrap_or_else(PoisonError::into_inner) = Some(pool);
            }
            let mut client = self.client().await?;
            Ok(schema::prepare(&mut client).await?)
        })
        .await
    }

    /// The explicit, opt-in schema migration, which never runs on its own:
    /// Python's `migrate(*, dry_run=True, confirm=False)`. Only
    /// `dry_run: false` with `confirm: true` drops a version 1 schema's
    /// tables, irreversibly; its report is Python's `dict`, with
    /// `detected_version`, `action` (`noop`, `dry_run`, `migrated`) and
    /// `dropped_tables`.
    ///
    /// # Errors
    ///
    /// Fails [`StoreError::Unavailable`], its cause a
    /// [`PostgresSchemaError`](crate::PostgresSchemaError) for a run without
    /// `confirm`, else what failed.
    pub async fn migrate(&self, options: MigrateOptions) -> Result<Metadata, StoreError> {
        cancellable(async {
            let mut client = self.client().await?;
            Ok(schema::migrate(&mut client, options.dry_run, options.confirm).await?)
        })
        .await
    }

    /// Repairs duplicate event indexes a release before could leave, then
    /// enforces their uniqueness, in one transaction: Python's
    /// `dedupe_event_indices(*, dry_run=True)`. Renumbering shifts indexes,
    /// so read markers may be off afterwards: run it in a maintenance window,
    /// on a backup first. Its report is Python's `dict`, with `action`
    /// (`dry_run`, `noop`, `repaired`), `duplicate_rows`, `affected_rooms`
    /// and `now_unique`.
    ///
    /// # Errors
    ///
    /// Fails [`StoreError::Unavailable`] when the database fails.
    pub async fn dedupe_event_indices(
        &self,
        options: DedupeEventIndicesOptions,
    ) -> Result<Metadata, StoreError> {
        cancellable(async {
            let mut client = self.client().await?;
            Ok(schema::dedupe_event_indices(&mut client, options.dry_run).await?)
        })
        .await
    }

    /// Drops the single-column `idx_events_parent` the composite index
    /// supersedes, which [`init`](Self::init) never does: Python's
    /// `drop_legacy_parent_index(*, dry_run=True)`. Its report is Python's
    /// `dict`, with `action` (`dry_run`, `noop`, `dropped`).
    ///
    /// # Errors
    ///
    /// Fails [`StoreError::Unavailable`] when the database fails.
    pub async fn drop_legacy_parent_index(
        &self,
        options: DropLegacyParentIndexOptions,
    ) -> Result<Metadata, StoreError> {
        cancellable(async {
            let mut client = self.client().await?;
            Ok(schema::drop_legacy_parent_index(&mut client, options.dry_run).await?)
        })
        .await
    }

    /// The pool, if the store holds one.
    fn pool(&self) -> Option<Pool> {
        self.shared
            .pool
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// `work` under its `store.<operation>` span, Python's `_query_span`, its
    /// statement cancelled on the server if its caller cuts it. A plain function:
    /// another future around `around`'s would triple what a call holds.
    fn spanned<'a, T: 'a>(
        &'a self,
        operation: &'a str,
        table: &'a str,
        work: impl Future<Output = Result<T, StoreError>> + 'a,
    ) -> impl Future<Output = Result<T, StoreError>> + 'a {
        self.shared
            .telemetry
            .around(operation, table, cancellable(work))
    }

    /// A connection of the pool, waited for at most [`ACQUIRE_TIMEOUT`], lent to
    /// the operation running: one its caller cuts has its statement cancelled on
    /// the server, as asyncpg cancels it (HITL 3ebe4596).
    async fn client(&self) -> Result<Lent, StoreError> {
        let Some(pool) = self.pool() else {
            return Err(StoreError::unavailable(NoConnection::NotInitialized));
        };
        let tls = self
            .shared
            .tls
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        match tokio::time::timeout(ACQUIRE_TIMEOUT, pool.get()).await {
            Ok(client) => Ok(Lent::new(client.map_err(StoreError::unavailable)?, tls)),
            Err(_) => Err(StoreError::unavailable(NoConnection::TimedOut)),
        }
    }
}

/// `outcome` for a refusal Python raises once the operation's span ended, `ok`: the
/// span's failure is what else went wrong.
fn after_the_span(outcome: Result<(), Failure>) -> Result<Result<(), StoreError>, StoreError> {
    match outcome {
        Ok(()) => Ok(Ok(())),
        Err(Failure::Refused(refused)) => Ok(Err(refused)),
        Err(failure) => Err(failure.into()),
    }
}

/// `outcome` for a row the model refuses: Python decodes a read's row once its span
/// ended, `ok`.
fn decoded_after_the_span<T>(
    outcome: Result<T, Failure>,
) -> Result<Result<T, StoreError>, StoreError> {
    match outcome {
        Ok(read) => Ok(Ok(read)),
        Err(unread @ (Failure::Json(_) | Failure::OutOfRange(_))) => Ok(Err(unread.into())),
        Err(failure) => Err(failure.into()),
    }
}

/// What [`ConversationStore::load_room_context`] reads: Python's `(room,
/// bindings, participants)`.
type RoomRead = (Option<Room>, Vec<ChannelBinding>, Vec<Participant>);

impl ConversationStore for PostgresStore {
    /// Python's `is_process_local = False`: the database outlives the
    /// process, and other processes share it.
    fn is_process_local(&self) -> bool {
        false
    }

    /// Python's `store._telemetry`, which the kit sets: what `store.<operation>`
    /// spans go to.
    fn set_telemetry(&self, provider: Arc<dyn TelemetryProvider>, record_sensitive_data: bool) {
        self.shared.telemetry.set(provider, record_sensitive_data);
    }

    async fn create_room(&self, room: Room) -> Result<Room, StoreError> {
        self.spanned("create_room", "rooms", async {
            let client = self.client().await?;
            after_the_span(rooms::create(&*client, &room).await)
        })
        .await??;
        Ok(room)
    }

    async fn get_room(&self, room_id: &RoomId) -> Result<Option<Room>, StoreError> {
        self.spanned("get_room", "rooms", async {
            let client = self.client().await?;
            decoded_after_the_span(rooms::get(&*client, room_id).await)
        })
        .await?
    }

    async fn update_room_if(
        &self,
        room: Room,
        status: RoomStatus,
        paused_at: Option<Timestamp>,
        last_activity_at: Option<Timestamp>,
    ) -> Result<Option<Room>, StoreError> {
        self.spanned("update_room_if", "rooms", async {
            let mut client = self.client().await?;
            Ok(rooms::update_if(&mut client, room, status, paused_at, last_activity_at).await?)
        })
        .await
    }

    async fn room_exists(&self, room_id: &RoomId) -> Result<bool, StoreError> {
        self.spanned("room_exists", "rooms", async {
            let client = self.client().await?;
            Ok(rooms::exists(&*client, room_id).await?)
        })
        .await
    }

    async fn update_room(&self, room: Room) -> Result<Room, StoreError> {
        self.spanned("update_room", "rooms", async {
            let client = self.client().await?;
            Ok(rooms::write(&*client, room).await?)
        })
        .await
    }

    /// One statement, as Python's store patches it: no commit falls between
    /// the read and the write.
    async fn patch_room_metadata(
        &self,
        room_id: &RoomId,
        patch: Metadata,
        options: PatchRoomMetadataOptions,
    ) -> Result<Option<Room>, StoreError> {
        self.spanned("patch_room_metadata", "rooms", async {
            let client = self.client().await?;
            Ok(
                rooms::patch_metadata(&*client, room_id, patch, &options.unset, written::now())
                    .await?,
            )
        })
        .await
    }

    async fn delete_room(&self, room_id: &RoomId) -> Result<bool, StoreError> {
        self.spanned("delete_room", "rooms", async {
            let client = self.client().await?;
            Ok(rooms::delete(&*client, room_id).await?)
        })
        .await
    }

    async fn list_rooms(&self, options: ListRoomsOptions) -> Result<Vec<Room>, StoreError> {
        self.spanned("list_rooms", "rooms", async {
            let client = self.client().await?;
            decoded_after_the_span(lookups::list(&*client, options.offset, options.limit).await)
        })
        .await?
    }

    async fn find_rooms(&self, options: FindRoomsOptions) -> Result<Vec<Room>, StoreError> {
        self.spanned("find_rooms", "rooms", async {
            let client = self.client().await?;
            decoded_after_the_span(lookups::find(&*client, options).await)
        })
        .await?
    }

    async fn find_latest_room(
        &self,
        participant_id: &ParticipantId,
        options: FindLatestRoomOptions,
    ) -> Result<Option<Room>, StoreError> {
        cancellable(async {
            let client = self.client().await?;
            Ok(lookups::find_latest(&*client, participant_id, &options).await?)
        })
        .await
    }

    async fn find_room_id_by_channel(
        &self,
        channel_id: &ChannelId,
        options: FindRoomIdByChannelOptions,
    ) -> Result<Option<RoomId>, StoreError> {
        let ids = FindRoomIdsByChannelOptions {
            room_filter: Some(RoomFilter {
                statuses: options.status.map(|status| vec![status]),
                organization_id: None,
            }),
            limit: 1,
            group: None,
        };
        let found = self.find_room_ids_by_channel(channel_id, ids).await?;
        Ok(found.into_iter().next())
    }

    async fn find_room_ids_by_channel(
        &self,
        channel_id: &ChannelId,
        options: FindRoomIdsByChannelOptions,
    ) -> Result<Vec<RoomId>, StoreError> {
        cancellable(async {
            let client = self.client().await?;
            Ok(lookups::find_ids_by_channel(&*client, channel_id, &options).await?)
        })
        .await
    }

    async fn find_room_id_by_binding(
        &self,
        channel_id: &ChannelId,
        participant_id: &str,
        options: FindRoomIdByBindingOptions,
    ) -> Result<Option<RoomId>, StoreError> {
        cancellable(async {
            let client = self.client().await?;
            Ok(lookups::find_by_binding(&*client, channel_id, participant_id, &options).await?)
        })
        .await
    }

    async fn find_room_id_by_participant(
        &self,
        channel_id: &ChannelId,
        participant_id: &str,
        options: FindRoomIdByParticipantOptions,
    ) -> Result<Option<RoomId>, StoreError> {
        cancellable(async {
            let client = self.client().await?;
            Ok(
                lookups::find_by_participant(&*client, channel_id, participant_id, &options)
                    .await?,
            )
        })
        .await
    }

    async fn get_delivered_index(&self, room_id: &RoomId) -> Result<i64, StoreError> {
        self.spanned("get_delivered_index", "rooms", async {
            let client = self.client().await?;
            Ok(rooms::delivered_index(&*client, room_id).await?)
        })
        .await
    }

    async fn advance_delivered_index(
        &self,
        room_id: &RoomId,
        index: u64,
        options: AdvanceDeliveredIndexOptions,
    ) -> Result<bool, StoreError> {
        self.spanned("advance_delivered_index", "rooms", async {
            let client = self.client().await?;
            Ok(rooms::advance(&*client, room_id, index, options.force).await?)
        })
        .await
    }

    async fn get_private_state(
        &self,
        room_id: &RoomId,
        key: &str,
    ) -> Result<Option<Metadata>, StoreError> {
        self.spanned("get_private_state", "private_state", async {
            let client = self.client().await?;
            Ok(rooms::private_state(&*client, room_id, key).await?)
        })
        .await
    }

    async fn set_private_state(
        &self,
        room_id: &RoomId,
        key: &str,
        value: Option<Metadata>,
    ) -> Result<(), StoreError> {
        self.spanned("set_private_state", "private_state", async {
            let client = self.client().await?;
            Ok(rooms::set_private_state(&*client, room_id, key, value.as_ref()).await?)
        })
        .await
    }

    async fn add_binding(&self, binding: ChannelBinding) -> Result<ChannelBinding, StoreError> {
        written::binding(&binding)?;
        self.spanned("add_binding", "bindings", async {
            let client = self.client().await?;
            members::add_binding(&*client, &binding).await?;
            Ok(binding)
        })
        .await
    }

    async fn get_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<Option<ChannelBinding>, StoreError> {
        self.spanned("get_binding", "bindings", async {
            let client = self.client().await?;
            decoded_after_the_span(members::binding(&*client, room_id, channel_id).await)
        })
        .await?
    }

    async fn binding_exists(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<bool, StoreError> {
        self.spanned("binding_exists", "bindings", async {
            let client = self.client().await?;
            Ok(members::binding_exists(&*client, room_id, channel_id).await?)
        })
        .await
    }

    async fn update_binding(&self, binding: ChannelBinding) -> Result<ChannelBinding, StoreError> {
        written::binding(&binding)?;
        self.spanned("update_binding", "bindings", async {
            let client = self.client().await?;
            members::update_binding(&*client, &binding).await?;
            Ok(binding)
        })
        .await
    }

    async fn remove_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<bool, StoreError> {
        self.spanned("remove_binding", "bindings", async {
            let client = self.client().await?;
            Ok(members::remove_binding(&*client, room_id, channel_id).await?)
        })
        .await
    }

    async fn list_bindings(&self, room_id: &RoomId) -> Result<Vec<ChannelBinding>, StoreError> {
        self.spanned("list_bindings", "bindings", async {
            let client = self.client().await?;
            decoded_after_the_span(members::bindings(&*client, room_id).await)
        })
        .await?
    }

    /// The three reads over one connection, one checkout instead of three.
    async fn load_room_context(&self, room_id: &RoomId) -> Result<RoomRead, StoreError> {
        self.spanned("load_room_context", "rooms", async {
            let client = self.client().await?;
            let read: Result<RoomRead, Failure> = async {
                let Some(room) = rooms::get(&*client, room_id).await? else {
                    return Ok((None, Vec::new(), Vec::new()));
                };
                Ok((
                    Some(room),
                    members::bindings(&*client, room_id).await?,
                    members::participants(&*client, room_id).await?,
                ))
            }
            .await;
            decoded_after_the_span(read)
        })
        .await?
    }

    async fn add_participant(&self, participant: Participant) -> Result<Participant, StoreError> {
        self.spanned("add_participant", "participants", async {
            let client = self.client().await?;
            members::add_participant(&*client, &participant).await?;
            Ok(participant)
        })
        .await
    }

    async fn get_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> Result<Option<Participant>, StoreError> {
        self.spanned("get_participant", "participants", async {
            let client = self.client().await?;
            decoded_after_the_span(members::participant(&*client, room_id, participant_id).await)
        })
        .await?
    }

    async fn update_participant(
        &self,
        participant: Participant,
    ) -> Result<Participant, StoreError> {
        self.spanned("update_participant", "participants", async {
            let client = self.client().await?;
            after_the_span(members::update_participant(&*client, &participant).await)
        })
        .await??;
        Ok(participant)
    }

    async fn delete_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> Result<(), StoreError> {
        self.spanned("delete_participant", "participants", async {
            let client = self.client().await?;
            Ok(members::delete_participant(&*client, room_id, participant_id).await?)
        })
        .await
    }

    async fn list_participants(&self, room_id: &RoomId) -> Result<Vec<Participant>, StoreError> {
        self.spanned("list_participants", "participants", async {
            let client = self.client().await?;
            decoded_after_the_span(members::participants(&*client, room_id).await)
        })
        .await?
    }

    async fn create_identity(&self, identity: Identity) -> Result<Identity, StoreError> {
        cancellable(async {
            let mut client = self.client().await?;
            identities::create(&mut client, &identity).await?;
            Ok(identity)
        })
        .await
    }

    async fn get_identity(&self, identity_id: &IdentityId) -> Result<Option<Identity>, StoreError> {
        cancellable(async {
            let client = self.client().await?;
            Ok(identities::get(&*client, identity_id).await?)
        })
        .await
    }

    async fn resolve_identity(
        &self,
        channel_type: &ChannelType,
        address: &str,
        options: ResolveIdentityOptions,
    ) -> Result<Option<Identity>, StoreError> {
        cancellable(async {
            let client = self.client().await?;
            Ok(identities::resolve(
                &*client,
                channel_type,
                address,
                options.organization_id.as_deref(),
            )
            .await?)
        })
        .await
    }

    async fn link_address(
        &self,
        identity_id: &IdentityId,
        channel_type: &ChannelType,
        address: &str,
        options: LinkAddressOptions,
    ) -> Result<(), StoreError> {
        cancellable(async {
            let mut client = self.client().await?;
            Ok(identities::link(
                &mut client,
                identity_id,
                channel_type,
                address,
                options.organization_id.as_deref(),
            )
            .await?)
        })
        .await
    }

    async fn unlink_address(
        &self,
        identity_id: &IdentityId,
        channel_type: &ChannelType,
        address: &str,
        options: UnlinkAddressOptions,
    ) -> Result<(), StoreError> {
        cancellable(async {
            let mut client = self.client().await?;
            Ok(identities::unlink(
                &mut client,
                identity_id,
                channel_type,
                address,
                options.organization_id.as_deref(),
            )
            .await?)
        })
        .await
    }

    async fn get_verification(
        &self,
        identity_id: &IdentityId,
        options: GetVerificationOptions,
    ) -> Result<Option<Verification>, StoreError> {
        cancellable(async {
            let client = self.client().await?;
            Ok(
                verifications::get(&*client, identity_id, options.organization_id.as_deref())
                    .await?,
            )
        })
        .await
    }

    async fn put_verification(
        &self,
        verification: Verification,
    ) -> Result<Verification, StoreError> {
        cancellable(async {
            let client = self.client().await?;
            verifications::put(&*client, &verification).await?;
            Ok(verification)
        })
        .await
    }

    async fn list_verifications_in_force(&self) -> Result<Vec<Verification>, StoreError> {
        cancellable(async {
            let client = self.client().await?;
            Ok(verifications::in_force(&*client).await?)
        })
        .await
    }

    async fn create_verification_request(
        &self,
        request: VerificationRequest,
    ) -> Result<VerificationRequest, StoreError> {
        cancellable(async {
            let client = self.client().await?;
            verifications::create_request(&*client, &request).await?;
            Ok(request)
        })
        .await
    }

    async fn get_verification_request(
        &self,
        request_id: &str,
    ) -> Result<Option<VerificationRequest>, StoreError> {
        cancellable(async {
            let client = self.client().await?;
            Ok(verifications::request(&*client, request_id).await?)
        })
        .await
    }

    async fn update_verification_request(
        &self,
        request: VerificationRequest,
    ) -> Result<VerificationRequest, StoreError> {
        cancellable(async {
            let client = self.client().await?;
            verifications::update_request(&*client, &request).await?;
            Ok(request)
        })
        .await
    }

    async fn check_idempotency(
        &self,
        room_id: &RoomId,
        key: &str,
        channel_id: Option<&ChannelId>,
    ) -> Result<bool, StoreError> {
        cancellable(async {
            let client = self.client().await?;
            Ok(events::key_held(&*client, room_id, channel_id, key).await?)
        })
        .await
    }

    async fn get_event_by_idempotency_key(
        &self,
        room_id: &RoomId,
        key: &str,
        channel_id: Option<&ChannelId>,
    ) -> Result<Option<RoomEvent>, StoreError> {
        self.spanned("get_event_by_idempotency_key", "events", async {
            let client = self.client().await?;
            decoded_after_the_span(events::by_key(&*client, room_id, channel_id, key).await)
        })
        .await?
    }

    async fn commit_event(
        &self,
        room_id: &RoomId,
        mut event: RoomEvent,
        options: CommitEventOptions,
    ) -> Result<RoomEvent, StoreError> {
        let CommitEventOptions {
            mut target,
            activity,
        } = options;
        written::event(&mut event)?;
        if let Some(target) = &mut target {
            written::event(target)?;
        }
        self.spanned("commit_event", "events", async {
            let at = activity.then(written::now);
            let mut client = self.client().await?;
            // Boxed: a commit's statements make a future past what callers hold on
            // their stacks.
            Ok(Box::pin(events::commit(&mut client, room_id, event, target, at)).await?)
        })
        .await
    }

    async fn add_event(&self, mut event: RoomEvent) -> Result<RoomEvent, StoreError> {
        written::event(&mut event)?;
        self.spanned("add_event", "events", async {
            let mut client = self.client().await?;
            Ok(Box::pin(events::add(&mut client, event)).await?)
        })
        .await
    }

    async fn add_event_auto_index(
        &self,
        room_id: &RoomId,
        mut event: RoomEvent,
    ) -> Result<RoomEvent, StoreError> {
        written::event(&mut event)?;
        self.spanned("add_event_auto_index", "events", async {
            let mut client = self.client().await?;
            Ok(Box::pin(events::add_auto_indexed(&mut client, room_id, event)).await?)
        })
        .await
    }

    async fn record_delivery_results(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        results: Metadata,
    ) -> Result<(), StoreError> {
        self.spanned("record_delivery_results", "events", async {
            let client = self.client().await?;
            Ok(events::record_results(&*client, room_id, event_id, results).await?)
        })
        .await
    }

    async fn write_change_target(
        &self,
        room_id: &RoomId,
        mut target: RoomEvent,
    ) -> Result<(), StoreError> {
        written::event(&mut target)?;
        self.spanned("write_change_target", "events", async {
            let mut client = self.client().await?;
            Ok(Box::pin(events::write_target(&mut client, room_id, target)).await?)
        })
        .await
    }

    async fn get_event(&self, event_id: &EventId) -> Result<Option<RoomEvent>, StoreError> {
        self.spanned("get_event", "events", async {
            let client = self.client().await?;
            decoded_after_the_span(events::get(&*client, event_id).await)
        })
        .await?
    }

    async fn update_event(&self, mut event: RoomEvent) -> Result<RoomEvent, StoreError> {
        written::event(&mut event)?;
        self.spanned("update_event", "events", async {
            let mut client = self.client().await?;
            Ok(Box::pin(events::update(&mut client, event)).await?)
        })
        .await
    }

    async fn delete_event(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        options: DeleteEventOptions,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        self.spanned("delete_event", "events", async {
            let mut client = self.client().await?;
            Ok(events::delete(&mut client, room_id, event_id, options.cascade_replies).await?)
        })
        .await
    }

    async fn get_event_count(
        &self,
        room_id: &RoomId,
        options: GetEventCountOptions,
    ) -> Result<u64, StoreError> {
        let filter = options.event_filter;
        if let Some(filter) = &filter {
            filter.validate()?;
        }
        self.spanned("get_event_count", "events", async {
            let client = self.client().await?;
            Ok(timeline::count(&*client, room_id, filter.as_ref()).await?)
        })
        .await
    }

    async fn list_events(
        &self,
        room_id: &RoomId,
        options: ListEventsOptions,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        options.validate()?;
        self.spanned("list_events", "events", async {
            let client = self.client().await?;
            decoded_after_the_span(timeline::page(&*client, room_id, &options).await)
        })
        .await?
    }

    async fn get_thread_summaries(
        &self,
        room_id: &RoomId,
        root_event_ids: &[EventId],
    ) -> Result<BTreeMap<EventId, ThreadSummary>, StoreError> {
        if root_event_ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        self.spanned("get_thread_summaries", "events", async {
            let client = self.client().await?;
            Ok(timeline::thread_summaries(&*client, room_id, root_event_ids).await?)
        })
        .await
    }

    async fn mark_read(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        event_id: &EventId,
    ) -> Result<(), StoreError> {
        cancellable(async {
            let client = self.client().await?;
            Ok(markers::mark(&*client, room_id, channel_id, event_id).await?)
        })
        .await
    }

    async fn mark_all_read(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<(), StoreError> {
        cancellable(async {
            let client = self.client().await?;
            Ok(markers::mark_all(&*client, room_id, channel_id).await?)
        })
        .await
    }

    async fn get_unread_count(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<u64, StoreError> {
        cancellable(async {
            let client = self.client().await?;
            Ok(markers::unread(&*client, room_id, channel_id).await?)
        })
        .await
    }

    async fn list_read_markers(
        &self,
        room_id: &RoomId,
    ) -> Result<BTreeMap<ChannelId, u64>, StoreError> {
        cancellable(async {
            let client = self.client().await?;
            Ok(markers::list(&*client, room_id).await?)
        })
        .await
    }

    async fn add_task(&self, task: Task) -> Result<Task, StoreError> {
        cancellable(async {
            let client = self.client().await?;
            side_effects::add_task(&*client, &task).await?;
            Ok(task)
        })
        .await
    }

    async fn get_task(&self, task_id: &TaskId) -> Result<Option<Task>, StoreError> {
        cancellable(async {
            let client = self.client().await?;
            Ok(side_effects::task(&*client, task_id).await?)
        })
        .await
    }

    async fn update_task(&self, task: Task) -> Result<Task, StoreError> {
        cancellable(async {
            let client = self.client().await?;
            side_effects::update_task(&*client, &task).await?;
            Ok(task)
        })
        .await
    }

    async fn add_observation(&self, observation: Observation) -> Result<Observation, StoreError> {
        cancellable(async {
            let client = self.client().await?;
            side_effects::add_observation(&*client, &observation).await?;
            Ok(observation)
        })
        .await
    }

    async fn list_tasks(
        &self,
        room_id: &RoomId,
        options: ListTasksOptions,
    ) -> Result<Vec<Task>, StoreError> {
        cancellable(async {
            let client = self.client().await?;
            Ok(side_effects::tasks(&*client, room_id, options.status).await?)
        })
        .await
    }

    async fn list_observations(&self, room_id: &RoomId) -> Result<Vec<Observation>, StoreError> {
        cancellable(async {
            let client = self.client().await?;
            Ok(side_effects::observations(&*client, room_id).await?)
        })
        .await
    }

    /// Closes the pool if the store opened it, the connections still in use
    /// given a second to come back: Python's `close()` as the kit's close
    /// runs it. A pool handed in stays its owner's. A second close does
    /// nothing; [`init`](Self::init) opens a new pool.
    async fn close(&self) {
        if !self.shared.owns_pool {
            return;
        }
        let pool = self
            .shared
            .pool
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(pool) = pool {
            pool::close_own_pool(&pool, CLOSE_TIMEOUT, "PostgresStore").await;
        }
    }
}
