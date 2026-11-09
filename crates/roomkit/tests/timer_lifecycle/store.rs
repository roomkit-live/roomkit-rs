//! A controllable store boundary for lifecycle races, cancellation and sweeps.

#[path = "forward.rs"]
mod forward;

use roomkit::model::{Room, RoomId, RoomStatus, Timestamp};
use roomkit::{ConversationStore, InMemoryStore, StoreError, store};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;

#[derive(Debug, Default)]
pub(super) struct TimerStore {
    pub(super) store: InMemoryStore,
    pub(super) before_write: AtomicBool,
    pub(super) before_read: AtomicBool,
    pub(super) after_write: AtomicBool,
    pub(super) after_listing: AtomicBool,
    pub(super) delete_listed: AtomicBool,
    pub(super) fail_room: Mutex<Option<RoomId>>,
    pub(super) reached: Notify,
    pub(super) release: Notify,
    pub(super) conditional_writes: AtomicUsize,
}

impl TimerStore {
    async fn hold_once(&self, armed: &AtomicBool) {
        if armed.swap(false, Ordering::SeqCst) {
            self.reached.notify_one();
            self.release.notified().await;
        }
    }

    async fn read_room(&self, room_id: &RoomId) -> Result<Option<Room>, StoreError> {
        self.hold_once(&self.before_read).await;
        if self.fail_room.lock().unwrap().as_ref() == Some(room_id) {
            return Err(StoreError::Value("timer read failed".to_owned()));
        }
        self.store.get_room(room_id).await
    }

    async fn find(&self, options: store::FindRoomsOptions) -> Result<Vec<Room>, StoreError> {
        let rooms = self.store.find_rooms(options).await?;
        if self.delete_listed.swap(false, Ordering::SeqCst) {
            self.store.delete_room(&rooms[0].id).await?;
        }
        self.hold_once(&self.after_listing).await;
        Ok(rooms)
    }

    async fn conditional_write(
        &self,
        room: Room,
        status: RoomStatus,
        paused_at: Option<Timestamp>,
        last_activity_at: Option<Timestamp>,
    ) -> Result<Option<Room>, StoreError> {
        self.conditional_writes.fetch_add(1, Ordering::SeqCst);
        self.hold_once(&self.before_write).await;
        let written = self
            .store
            .update_room_if(room, status, paused_at, last_activity_at)
            .await?;
        self.hold_once(&self.after_write).await;
        Ok(written)
    }
}
