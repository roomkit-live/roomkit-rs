//! Turns which may already be running on a worker while its admission is being acknowledged.
use crate::turns::{SteerAddress, lock};
use roomkit_core::model::{ChannelId, RoomId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;

#[derive(Debug, Default)]
pub(super) struct Admissions(Mutex<HashMap<Arc<str>, Pending>>);
#[derive(Debug)]
struct Pending {
    room: RoomId,
    channel: ChannelId,
    done: watch::Sender<()>,
}
pub(in crate::turns) struct Admission<'a> {
    pending: &'a Admissions,
    id: Arc<str>,
}
impl Drop for Admission<'_> {
    fn drop(&mut self) {
        lock(&self.pending.0).remove(&self.id);
    }
}
impl Admissions {
    pub(super) fn start<'a>(
        &'a self,
        id: &Arc<str>,
        room: &RoomId,
        channel: &ChannelId,
    ) -> Admission<'a> {
        lock(&self.0).insert(
            Arc::clone(id),
            Pending {
                room: room.clone(),
                channel: channel.clone(),
                done: watch::channel(()).0,
            },
        );
        Admission {
            pending: self,
            id: Arc::clone(id),
        }
    }
    pub(super) async fn wait(&self, address: SteerAddress<'_>, timeout: Duration) {
        let mut pending: Vec<_> = lock(&self.0)
            .iter()
            .filter(|(turn, p)| {
                p.room == *address.room
                    && p.channel == *address.channel
                    && address.turn.is_none_or(|target| target == turn.as_ref())
            })
            .map(|(_, p)| p.done.subscribe())
            .collect();
        drop(
            tokio::time::timeout(timeout, async {
                for status in &mut pending {
                    let _closed = status.changed().await;
                }
            })
            .await,
        );
    }
}
