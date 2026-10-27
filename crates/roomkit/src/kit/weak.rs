//! A kit held without keeping it alive, for what the kit itself holds.

use std::sync::{Arc, Weak};

use super::{Inner, RoomKit};
use crate::store::InMemoryStore;

/// A kit held without keeping it alive, as a [`Weak`] holds an [`Arc`]'s
/// value: what the kit holds, a hook, a channel, holds this rather than a
/// [`RoomKit`] clone to call into its kit.
///
/// The kit holds its hooks and its channels, so one holding a [`RoomKit`]
/// clone makes a cycle: once the host drops its kit, neither the kit nor
/// its store is ever freed, and a store's file or connections stay open. A
/// weak kit makes none: [`upgrade`](Self::upgrade) gives the kit back while
/// any clone of it lives, and `None` once the host dropped the last one.
///
/// The kit's own work holds it too, while it runs: a background task (an
/// observer, an inbound call in flight) and a [`TimerSweeper`](crate::TimerSweeper)
/// keep the kit alive. Inside a hook or a channel the kit runs, `upgrade`
/// therefore always succeeds; `None` matters only to work detached from
/// the kit, a task the hook spawned that outlives it. For a release at a
/// known point, the store's file closed included, call
/// [`shutdown`](RoomKit::shutdown), then drop the kit.
///
/// An `ON_ROOM_CREATED` hook attaching the agent to each new room (RFC
/// Appendix B.1):
///
/// ```
/// use roomkit::model::ChannelId;
/// use roomkit::testing::EchoAgent;
/// use roomkit::{HookTrigger, RoomKit};
///
/// let kit = RoomKit::new(Default::default());
/// let agent = ChannelId::new("ai")?;
/// kit.register_channel(EchoAgent::new(agent.clone(), "You said: "))?;
/// let attaching = kit.downgrade();
/// kit.observe("attach_agent", HookTrigger::OnRoomCreated, move |event, _| {
///     let (kit, agent) = (attaching.upgrade(), agent.clone());
///     async move {
///         if let Some(kit) = kit {
///             // The hook's failure would be logged.
///             let _attached = kit.attach_channel(&event.room_id, &agent, Default::default()).await;
///         }
///     }
/// })?;
///
/// // The hook does not keep the kit: dropped, the kit is freed.
/// let watching = kit.downgrade();
/// drop(kit);
/// assert!(watching.upgrade().is_none());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug)]
pub struct WeakRoomKit<S = InMemoryStore> {
    inner: Weak<Inner<S>>,
}

impl<S> Clone for WeakRoomKit<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Weak::clone(&self.inner),
        }
    }
}

impl<S> RoomKit<S> {
    /// The kit held weakly, for a hook or a channel the kit holds, which a
    /// [`RoomKit`] clone would keep from ever being freed (see
    /// [`WeakRoomKit`]).
    pub fn downgrade(&self) -> WeakRoomKit<S> {
        WeakRoomKit {
            inner: Arc::downgrade(&self.inner),
        }
    }
}

impl<S> WeakRoomKit<S> {
    /// The kit, while any [`RoomKit`] clone of it lives; `None` once the
    /// last one is dropped.
    pub fn upgrade(&self) -> Option<RoomKit<S>> {
        self.inner.upgrade().map(|inner| RoomKit { inner })
    }
}
