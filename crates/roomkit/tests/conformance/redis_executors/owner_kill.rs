//! The run's kit killed as the owner of its turns (README: the `owner.kill`
//! call, `expect.killed_owner_frames`).
//!
//! The kit's turn backend hands the owner each turn through a link that, once
//! a step killed the owner, carries nothing either way: the executors hear
//! nothing more from the kit, and what they send it is never read, only its
//! type kept. A killed process releases nothing either, so the executor of a
//! turn learns of the death from its owner's silence alone (RFC §10.6.8):
//! each link the owner held is kept until the run ends, as dropping it would
//! release it. The kit itself runs on: what it does of its turns afterwards,
//! as its own liveness checks fail them, reaches no executor and is not
//! observed.
//!
//! Wrapped, the Redis backend is the kit's application transport rather
//! than its native one: the owner's side drives the same public link either
//! way, as Python's kit does.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use roomkit::RoomKitError;
use roomkit::turns::wire::{Control, Frame, TurnRequest};
use roomkit::turns::{
    ChannelControl, ChannelResult, ExecutorInfo, RoomOwner, ServedExecutor, TurnBackend,
    TurnFuture, TurnLink, TurnTimeouts,
};
use tokio::task::JoinHandle;

use crate::identity::RunSlot;
use crate::run::lock;

/// Whether the run's kit was killed as its turns' owner, and its links.
pub(super) struct OwnerKill {
    inner: Arc<dyn TurnBackend>,
    killed: Arc<AtomicBool>,
    run: RunSlot,
    /// Every link the owner was handed, kept until the run ends.
    links: Mutex<Vec<Arc<dyn TurnLink>>>,
    /// The tasks reading on for the dead owner.
    drains: Mutex<Vec<JoinHandle<()>>>,
}

impl OwnerKill {
    pub(super) fn new(inner: Arc<dyn TurnBackend>, run: RunSlot) -> Self {
        Self {
            inner,
            killed: Arc::default(),
            run,
            links: Mutex::default(),
            drains: Mutex::default(),
        }
    }

    /// Kills the owner: each link it holds is read on for it, which keeps
    /// the type of what its executor sends and hands the owner nothing.
    pub(super) fn kill(&self) {
        self.killed.store(true, Ordering::SeqCst);
        let links = lock(&self.links).clone();
        let drains = links.into_iter().map(|link| {
            let run = self.run.clone();
            tokio::spawn(async move {
                while let Ok((_, frame)) = link.receive().await {
                    heard(&run, &frame);
                }
            })
        });
        lock(&self.drains).extend(drains);
    }

    /// The owner's link to a turn `link` carries, which a kill cuts.
    fn owned(&self, link: Arc<dyn TurnLink>) -> Arc<dyn TurnLink> {
        lock(&self.links).push(Arc::clone(&link));
        Arc::new(OwnerLink {
            link,
            killed: Arc::clone(&self.killed),
            run: self.run.clone(),
        })
    }

    /// Stops reading for the dead owner and lets its links go: the run is over.
    pub(super) fn close(&self) {
        for drain in lock(&self.drains).drain(..) {
            drain.abort();
        }
        lock(&self.links).clear();
    }
}

/// Records the type of a frame that reached the dead owner.
fn heard(run: &RunSlot, frame: &Frame) {
    let Some(run) = run.run() else {
        return;
    };
    let kind = serde_json::to_value(frame)
        .ok()
        .and_then(|value| value["type"].as_str().map(str::to_owned))
        .unwrap_or_else(|| "unreadable".to_owned());
    lock(&run.observed).killed_owner_frames.insert(kind);
}

impl TurnBackend for OwnerKill {
    fn submit(
        &self,
        request: TurnRequest,
    ) -> TurnFuture<'_, Result<Arc<dyn TurnLink>, RoomKitError>> {
        Box::pin(async move { Ok(self.owned(self.inner.submit(request).await?)) })
    }

    fn submit_to(
        &self,
        request: TurnRequest,
        executor: ExecutorInfo,
    ) -> TurnFuture<'_, Result<Arc<dyn TurnLink>, RoomKitError>> {
        Box::pin(async move { Ok(self.owned(self.inner.submit_to(request, executor).await?)) })
    }

    fn serve(&self, executor: Arc<dyn ServedExecutor>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        self.inner.serve(executor)
    }

    fn control(
        &self,
        message: ChannelControl,
    ) -> TurnFuture<'_, Result<Option<ChannelResult>, RoomKitError>> {
        self.inner.control(message)
    }

    fn attend(&self, owner: Arc<dyn RoomOwner>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        self.inner.attend(owner)
    }

    fn claim_presence<'a>(
        &'a self,
        channel_id: &'a str,
        room_id: &'a str,
        online: Option<bool>,
    ) -> Option<TurnFuture<'a, Result<bool, RoomKitError>>> {
        self.inner.claim_presence(channel_id, room_id, online)
    }

    fn executors(&self) -> Option<TurnFuture<'_, Result<Vec<ExecutorInfo>, RoomKitError>>> {
        self.inner.executors()
    }

    fn executors_now(&self) -> Option<TurnFuture<'_, Result<Vec<ExecutorInfo>, RoomKitError>>> {
        self.inner.executors_now()
    }

    fn timeouts(&self) -> Option<TurnTimeouts> {
        self.inner.timeouts()
    }

    fn close(&self) -> TurnFuture<'_, Result<(), RoomKitError>> {
        self.inner.close()
    }
}

/// The owner's link to one turn, which carries nothing once it was killed.
struct OwnerLink {
    link: Arc<dyn TurnLink>,
    killed: Arc<AtomicBool>,
    run: RunSlot,
}

impl OwnerLink {
    fn killed(&self) -> bool {
        self.killed.load(Ordering::SeqCst)
    }
}

impl TurnLink for OwnerLink {
    fn request(&self) -> &TurnRequest {
        self.link.request()
    }

    fn taken(&self) -> bool {
        self.link.taken()
    }

    /// The next frame while the owner lives; once killed, it reads on for
    /// the dead owner and hands it nothing.
    fn receive(&self) -> TurnFuture<'_, Result<(u64, Frame), RoomKitError>> {
        Box::pin(async move {
            loop {
                let received = self.link.receive().await;
                if !self.killed() {
                    return received;
                }
                match received {
                    Ok((_, frame)) => heard(&self.run, &frame),
                    Err(_) => std::future::pending::<()>().await,
                }
            }
        })
    }

    fn send(&self, message: Control) -> TurnFuture<'_, Result<(), RoomKitError>> {
        if self.killed() {
            return Box::pin(async { Ok(()) });
        }
        self.link.send(message)
    }

    fn release(&self) {
        if !self.killed() {
            self.link.release();
        }
    }

    fn beat_heard(&self) -> bool {
        !self.killed() && self.link.beat_heard()
    }
}
