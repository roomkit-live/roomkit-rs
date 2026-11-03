//! The room lifecycle (RFC §5.1): the kit's calls for a room's creation,
//! its read and its transitions, each for the organization its options
//! name (RFC §17.2).

use std::sync::Arc;
use std::sync::atomic::Ordering;

use roomkit_core::model::{Room, RoomId};
use roomkit_core::ops::{
    AnnounceRoomCreated, ArchiveRoom, CloseRoom, CreateRoom, PauseRoom, ReopenRoom, ResumeRoom,
};
use roomkit_core::rules::organizations;

use super::RoomKit;
use super::events::room_work;
use super::options::{
    ArchiveRoomOptions, CloseRoomOptions, CreateRoomOptions, GetRoomOptions, PauseRoomOptions,
    ReopenRoomOptions, ResumeRoomOptions,
};
use crate::driver;
use crate::error::RoomKitError;
use crate::orchestration::Orchestration;
use crate::store::{ConversationStore, StoreError};

impl<S: ConversationStore> RoomKit<S> {
    /// Creates a room (RFC §5.1): its id the one `options` names or a
    /// generated one, its timers, its agent response policy (the kit's
    /// unless named) and its metadata, belonging to the organization the
    /// options name. A key of its metadata only the framework writes, under
    /// `roomkit:` or of the room's private state, is dropped with a warning,
    /// the rest written. Its status, instants and counters are a new
    /// room's.
    ///
    /// Then runs its `ON_ROOM_CREATED` hooks and waits for them, each within
    /// its timeout, before it emits `room_created` (RFC §10.1 step 2): what
    /// they set up, the agent they attach for instance, is in place when the
    /// call returns (RFC Appendix B.1, gap G-050).
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::RoomAlreadyExists`] when the room exists
    /// already, nothing written, as Python raises `RoomAlreadyExistsError`;
    /// [`RoomKitError::Value`] for a room id holding a NUL character, which
    /// leads the keys of the kit's other locks in a shared lock manager
    /// (RFC §13.5), as Python's `ValueError`.
    /// With a strategy (`options.orchestration`, or the kit's), the room is
    /// written, then installed, then announced, as Python's: its
    /// `ON_ROOM_CREATED` hooks see what the install set up (RFC §19.7). An
    /// install that fails returns what
    /// [`install_strategy`](Self::install_strategy) returns, the room kept,
    /// unannounced, holding the strategy only when the install could not be
    /// read or taken back whole.
    pub async fn create_room(&self, options: CreateRoomOptions<S>) -> Result<Room, RoomKitError> {
        let orchestration = options
            .orchestration
            .clone()
            .unwrap_or_else(|| self.inner.default_orchestration.clone());
        let forgets = options
            .room_id
            .as_ref()
            .is_some_and(|room_id| self.holds_strategy_record(room_id));
        if orchestration.is_none() && !forgets {
            return self.create_room_record(options, true).await;
        }
        // Boxed: a room created with no strategy keeps a small future.
        Box::pin(self.create_room_with_strategy(options, orchestration)).await
    }

    /// The room `options` describes, written, then the strategy recorded
    /// for its id taken back, its strategy installed and its creation
    /// announced, in that order: its `ON_ROOM_CREATED` hooks see what the
    /// install set up (RFC §19.7).
    async fn create_room_with_strategy(
        &self,
        options: CreateRoomOptions<S>,
        orchestration: Option<Arc<dyn Orchestration<S>>>,
    ) -> Result<Room, RoomKitError> {
        let room = self.create_room_record(options, false).await?;
        // A strategy recorded for this id belonged to a room that is gone.
        self.forget_strategy(&room.id).await?;
        if let Some(strategy) = orchestration {
            // The path install_strategy() takes: claimed, recorded (RFC §19.7).
            Box::pin(self.install_on(&room.id, strategy, room.organization_id.clone())).await?;
        }
        let announce = AnnounceRoomCreated::new(self.config(), room.id.clone());
        let environment = self.inner.environment();
        driver::run_on(&self.inner, environment, "create_room", announce)
            .await?
            .map_err(RoomKitError::from)?;
        Ok(room)
    }

    /// The room `options` describes, written, its creation announced when
    /// `announced`.
    async fn create_room_record(
        &self,
        options: CreateRoomOptions<S>,
        announced: bool,
    ) -> Result<Room, RoomKitError> {
        let inner = &self.inner;
        let environment = inner.environment();
        let config = self.config();
        let room_id = options.room_id.unwrap_or_else(|| environment.new_room_id());
        let room = Room {
            organization_id: options.organization_id,
            timers: options.timers.unwrap_or_default(),
            agent_response_policy: options
                .agent_response_policy
                .unwrap_or(config.agent_response_policy),
            metadata: options.metadata,
            ..Room::at(room_id, environment.now())
        };
        let room_id = room.id.clone();
        let create = CreateRoom::new(config, room);
        let create = if announced {
            create
        } else {
            create.unannounced()
        };
        let created = driver::run_on(inner, environment, "create_room", create).await?;
        created.map_err(|error| match RoomKitError::from(error) {
            taken if taken.recorded_refusal() == Some("RoomAlreadyExists") => {
                RoomKitError::RoomAlreadyExists(room_id)
            }
            error => error,
        })
    }

    /// The room `room_id`, as the store holds it.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::Store`] with [`StoreError::RoomNotFound`] when the
    /// room does not exist or is not of the organization `options` names
    /// (RFC §17.2), and when the store fails the read.
    pub async fn get_room(
        &self,
        room_id: &RoomId,
        options: GetRoomOptions,
    ) -> Result<Room, RoomKitError> {
        let room = self
            .store()
            .get_room(room_id)
            .await
            .map_err(|error| self.read_failed(error))?;
        room.filter(|room| organizations::admits(options.organization_id.as_deref(), room))
            .ok_or_else(|| StoreError::RoomNotFound(room_id.clone()).into())
    }

    /// What a read the store failed answers: [`RoomKitError::KitClosed`]
    /// once the kit's close released its store, as the Python
    /// implementation's kit answers a read against the store it closed
    /// (RFC §12.10.4); the store's error otherwise.
    pub(super) fn read_failed(&self, error: StoreError) -> RoomKitError {
        let released = self.inner.store_released.load(Ordering::Acquire);
        if released && matches!(error, StoreError::Unavailable(_)) {
            return RoomKitError::KitClosed;
        }
        error.into()
    }

    /// Checks that the organization `organization_id` reaches the room
    /// `room_id`, when it names one: the read a scoped call makes first, as
    /// the Python implementation's do.
    pub(super) async fn reached(
        &self,
        room_id: &RoomId,
        organization_id: Option<&str>,
    ) -> Result<(), RoomKitError> {
        let Some(organization_id) = organization_id else {
            return Ok(());
        };
        let options = GetRoomOptions {
            organization_id: Some(organization_id.to_owned()),
        };
        self.get_room(room_id, options).await.map(drop)
    }

    /// Pauses an active room (RFC §5.1); it still accepts events, the first
    /// one delivered resumes it. A paused room is left as it is. Returns the
    /// room as it now stands.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::RoomNotFound`] when the room does not exist or
    /// is not of the organization `options` names, and
    /// [`RoomKitError::TransitionRefused`], nothing written, when it is
    /// CLOSED or ARCHIVED (gap G-074).
    pub async fn pause_room(
        &self,
        room_id: &RoomId,
        options: PauseRoomOptions,
    ) -> Result<Room, RoomKitError> {
        let pause = PauseRoom::new(self.config(), room_id.clone(), options.organization_id);
        let pause = match room_work(room_id) {
            Some((draw, floor)) => pause.made_in(draw, floor),
            None => pause,
        };
        Ok(driver::run(&self.inner, "pause_room", pause).await??)
    }

    /// Resumes a paused room (RFC §5.1), restarting its inactivity timer. An
    /// active room is left as it is. Returns the room as it now stands.
    ///
    /// # Errors
    ///
    /// As [`pause_room`](Self::pause_room).
    pub async fn resume_room(
        &self,
        room_id: &RoomId,
        options: ResumeRoomOptions,
    ) -> Result<Room, RoomKitError> {
        let resume = ResumeRoom::new(self.config(), room_id.clone(), options.organization_id);
        let resume = match room_work(room_id) {
            Some((draw, floor)) => resume.made_in(draw, floor),
            None => resume,
        };
        Ok(driver::run(&self.inner, "resume_room", resume).await??)
    }

    /// Reopens a closed room: it accepts events again (RFC §5.1). Returns
    /// the room as it now stands.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::RoomNotFound`] when the room does not exist or
    /// is not of the organization `options` names, and
    /// [`RoomKitError::TransitionRefused`], nothing written, when it is not
    /// CLOSED: an archived room is never left (gap G-074).
    pub async fn reopen_room(
        &self,
        room_id: &RoomId,
        options: ReopenRoomOptions,
    ) -> Result<Room, RoomKitError> {
        let reopen = ReopenRoom::new(self.config(), room_id.clone(), options.organization_id);
        let reopen = match room_work(room_id) {
            Some((draw, floor)) => reopen.made_in(draw, floor),
            None => reopen,
        };
        Ok(driver::run(&self.inner, "reopen_room", reopen).await??)
    }

    /// Closes a room: it refuses every new event from now on (RFC §5.1). A
    /// closed room is left as it is. Returns the room as it now stands.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::RoomNotFound`] when the room does not exist or
    /// is not of the organization `options` names, and
    /// [`RoomKitError::TransitionRefused`], nothing written, when it is
    /// ARCHIVED: an archived room is never left (gap G-074).
    pub async fn close_room(
        &self,
        room_id: &RoomId,
        options: CloseRoomOptions,
    ) -> Result<Room, RoomKitError> {
        let close = CloseRoom::new(self.config(), room_id.clone(), options.organization_id);
        let close = match room_work(room_id) {
            Some((draw, floor)) => close.made_in(draw, floor),
            None => close,
        };
        Ok(driver::run(&self.inner, "close_room", close).await??)
    }

    /// Archives a room: a terminal, read-only state it never leaves (RFC
    /// §5.1). An archived room is left as it is. Returns the room as it now
    /// stands.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::RoomNotFound`] when the room does not exist or is
    /// not of the organization `options` names.
    pub async fn archive_room(
        &self,
        room_id: &RoomId,
        options: ArchiveRoomOptions,
    ) -> Result<Room, RoomKitError> {
        let archive = ArchiveRoom::new(self.config(), room_id.clone(), options.organization_id);
        let archive = match room_work(room_id) {
            Some((draw, floor)) => archive.made_in(draw, floor),
            None => archive,
        };
        Ok(driver::run(&self.inner, "archive_room", archive).await??)
    }
}
