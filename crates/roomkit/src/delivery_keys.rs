//! The deliveries a caller named by a key (RFC §10.1 "Explicit delivery
//! cancellation"), the Python implementation's `core/_delivery_keys.py`.
//!
//! A caller that sets `InboundMessage::delivery_key` can cancel the
//! message's delivery from anywhere in the process, without holding its
//! handle: an agent's tool passes the key along as text. A key names every
//! delivery in flight that carries it, from the call's start until its
//! delivery is over: a delivery cancelled before its event is committed is
//! committed and delivered all the same, and none of its turns starts. A
//! cancellation scoped to an organization reaches only the deliveries of its
//! rooms (RFC §17.2); a delivery whose room is not known yet, a call that
//! named no organization before its room is read, takes it once its room
//! proves the organization's, uncounted.
//!
//! The delivery of a message held for verification (RFC §11.7.3) is over
//! only once its release is answered (RFC §11.7.6): the key names it past
//! its run, until the release or the policy's last moment to release it,
//! and then names the run of its answer. A held delivery cancelled at any
//! point is answered by no agent; the cancellation is also recorded on the
//! held event, for a release another process runs.

use std::collections::HashMap;
use std::future::{Future, poll_fn};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use roomkit_core::model::{EventId, RoomId};
use roomkit_core::ops::InboundResult;
use roomkit_core::rules::verification::HELD_KEY;
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::turns::{Canceller, WeakCanceller};

/// A request to cut a delivery's turns, with its reason: what a
/// cancellation by key, or a handle's `cancel`, asks of the run that
/// delivers (RFC §10.1 "Explicit delivery cancellation").
#[derive(Debug, Default)]
pub(crate) struct CancelRequest {
    state: Mutex<Requested>,
    changed: Notify,
}

#[derive(Debug, Default)]
struct Requested {
    reason: Option<String>,
    waker: Option<Waker>,
    turns: Vec<FollowedTurn>,
}

/// Keeps a turn joined only while its output or stream is actually read.
/// A stream handed back but still unread is cleaned up by the driver.
#[derive(Debug)]
pub(crate) struct Following {
    _active: Arc<()>,
}

#[derive(Debug)]
struct FollowedTurn {
    turn: WeakCanceller,
    active: Weak<()>,
}

impl FollowedTurn {
    fn live(&self) -> bool {
        self.active.strong_count() > 0 && self.turn.live()
    }

    fn cancel(&self, reason: &str) {
        if let Some(_active) = self.active.upgrade() {
            self.turn.cancel(reason);
        }
    }
}

impl CancelRequest {
    /// Asks for the cut, for `reason`: whether this call asked first.
    pub(crate) fn request(&self, reason: &str) -> bool {
        let (turns, waker) = {
            let mut state = locked(&self.state);
            if state.reason.is_some() {
                return false;
            }
            state.reason = Some(reason.to_owned());
            (std::mem::take(&mut state.turns), state.waker.take())
        };
        // The cut crosses before the caller can release a target or a tool.
        // The driver still owns admission, records and cleanup of the cut.
        for turn in turns {
            turn.cancel(reason);
        }
        self.changed.notify_waiters();
        if let Some(waker) = waker {
            waker.wake();
        }
        true
    }

    /// Joins a turn to its delivery's cut, or cuts it immediately when the
    /// request preceded submission. No strong reference retains the turn.
    pub(crate) fn follows(&self, turn: &Canceller) -> Following {
        let turn = turn.downgrade();
        let active = Arc::new(());
        let mut state = locked(&self.state);
        if let Some(reason) = state.reason.clone() {
            drop(state);
            turn.cancel(&reason);
        } else {
            state.turns.retain(FollowedTurn::live);
            state.turns.push(FollowedTurn {
                turn,
                active: Arc::downgrade(&active),
            });
        }
        Following { _active: active }
    }

    /// Why the cut was asked for, once it was.
    pub(crate) fn reason(&self) -> Option<String> {
        locked(&self.state).reason.clone()
    }

    /// Wakes every active reader independently of the driver's next poll.
    /// Register before checking the reason so no request falls between them.
    pub(crate) async fn cancelled(&self) {
        let changed = self.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        poll_fn(|cx| {
            // A turn's end can wake the read while request is still sending
            // other controls, before its notification: the reason wins.
            if self.reason().is_some() {
                Poll::Ready(())
            } else {
                changed.as_mut().poll(cx)
            }
        })
        .await;
    }

    /// Whether the cut was asked for; `cx` is woken once it is otherwise.
    pub(crate) fn poll(&self, cx: &Context<'_>) -> bool {
        let mut state = locked(&self.state);
        if state.reason.is_some() {
            state.waker = None;
            return true;
        }
        state.waker.clone_from(&Some(cx.waker().clone()));
        false
    }
}

/// A held event a delivery waits for the release of: its room and its id.
pub(crate) type HeldEvent = (RoomId, EventId);

/// What records on a held event that its delivery was cancelled, with the
/// reason: the kit's write on the event's hold mark, off the canceller.
pub(crate) type Recorder = Box<dyn Fn(HeldEvent, String) + Send + Sync>;

/// A delivery, from its call's start: the organization of its room once
/// read, the cut it asks of the run that delivers it once that run is
/// known, and past that run the event it holds for verification, until its
/// release; Python's `NamedDelivery`. Its key, when it has one, names it.
#[derive(Debug)]
pub(crate) struct Named {
    key: Option<String>,
    keys: Weak<DeliveryKeys>,
    state: Mutex<NamedState>,
}

#[derive(Debug, Default)]
struct NamedState {
    organization_id: Option<String>,
    /// Whether the delivery's room is known: from the start for a call
    /// that names its organization, which lands in one of its rooms or is
    /// refused (RFC §17.2), once its room is read otherwise.
    located: bool,
    /// Scoped cancellations waiting for the room's organization, each with
    /// its reason.
    requests: Vec<(String, String)>,
    /// A cancellation received while no run delivers it.
    cancelled: Option<String>,
    /// The cut of the run that delivers it, while one does.
    target: Option<Arc<CancelRequest>>,
    /// The event it holds for verification, and the last moment it may be
    /// released (RFC §11.7.6).
    held: Option<(HeldEvent, Instant)>,
}

impl NamedState {
    /// Why the delivery was cancelled, if it was.
    fn cancelled(&self) -> Option<String> {
        self.target
            .as_ref()
            .and_then(|target| target.reason())
            .or_else(|| self.cancelled.clone())
    }
}

impl Named {
    /// Cancels the delivery for a caller acting for `organization_id`, any
    /// caller when `None`: whether this call cancelled it. A scoped request
    /// on a delivery whose room is not known yet waits for it and is not
    /// counted: until then the delivery is no organization's. A held
    /// delivery no run delivers has its cancellation recorded on its event.
    fn cancel(&self, reason: &str, organization_id: Option<&str>) -> bool {
        let (target, recorded) = {
            let mut state = locked(&self.state);
            if let Some(organization) = organization_id {
                if !state.located {
                    state
                        .requests
                        .push((organization.to_owned(), reason.to_owned()));
                    return false;
                }
                if state.organization_id.as_deref() != Some(organization) {
                    return false;
                }
            }
            if state.cancelled().is_some() {
                return false;
            }
            state.cancelled = Some(reason.to_owned());
            let target = state.target.clone();
            let recorded = target
                .is_none()
                .then(|| state.held.as_ref().map(|(held, _)| held.clone()))
                .flatten();
            (target, recorded)
        };
        // Sending a control may invoke a transport tap: hold no name lock.
        if let Some(target) = target {
            target.request(reason);
        }
        if let Some(held) = recorded {
            self.record(held, reason);
        }
        true
    }

    /// The delivery's room is read, of `organization_id`: a scoped
    /// cancellation that waited for it cancels the delivery when it is that
    /// organization's. A delivery located already stays so.
    pub(crate) fn locate(&self, organization_id: Option<&str>) {
        let mut state = locked(&self.state);
        if state.located {
            return;
        }
        state.located = true;
        state.organization_id = organization_id.map(str::to_owned);
        let requests = std::mem::take(&mut state.requests);
        let asked = requests
            .into_iter()
            .find(|(owner, _)| Some(owner.as_str()) == organization_id);
        let Some((_, reason)) = asked else {
            return;
        };
        let reason = state.cancelled().unwrap_or(reason);
        state.cancelled = Some(reason.clone());
        let target = state.target.clone();
        drop(state);
        if let Some(target) = target {
            target.request(&reason);
        }
    }

    /// Makes `target` the cut of the run that delivers it: asked at once
    /// when the delivery was cancelled already.
    fn attach(&self, target: &Arc<CancelRequest>) {
        let reason = {
            let mut state = locked(&self.state);
            let reason = state.cancelled();
            state.cancelled.clone_from(&reason);
            state.target = Some(Arc::clone(target));
            reason
        };
        if let Some(reason) = reason {
            target.request(&reason);
        }
    }

    /// The call that delivers it is decided, `result`: a committed event the
    /// pipeline held keeps it named until its release, or until the
    /// policy's last moment for it, when the policy releases held messages
    /// (RFC §11.7.6); Python's `_name_held`. Decided under the room's lock,
    /// so no release reaches the event before.
    pub(crate) fn decided(self: &Arc<Self>, result: &InboundResult) {
        let Some(event) = result.event.as_ref().filter(|_| !result.blocked) else {
            return;
        };
        if !event
            .metadata
            .get(HELD_KEY)
            .is_some_and(serde_json::Value::is_object)
        {
            return;
        }
        if let Some(keys) = self.keys.upgrade() {
            keys.hold(self, (event.room_id.clone(), event.id.clone()));
        }
    }

    /// The run whose cut is `target` is over: the delivery is over, unless
    /// it holds its event, whose release it waits for, its cancellation
    /// recorded on the event; a later run that delivers it now ends it
    /// instead. Python's `_over`.
    fn over(&self, target: &Arc<CancelRequest>) {
        let (cancelled, held) = {
            let mut state = locked(&self.state);
            if !state
                .target
                .as_ref()
                .is_some_and(|own| Arc::ptr_eq(own, target))
            {
                return;
            }
            state.target = None;
            let cancelled = target.reason().or_else(|| state.cancelled.take());
            state.cancelled.clone_from(&cancelled);
            (cancelled, state.held.as_ref().map(|(held, _)| held.clone()))
        };
        match (held, cancelled) {
            (Some(held), Some(reason)) => self.record(held, &reason),
            // No key names it past its run: nothing can cancel it now.
            (Some((_, event_id)), None) if self.key.is_none() => {
                if let Some(keys) = self.keys.upgrade() {
                    keys.unhold(&event_id);
                }
            }
            _ => {}
        }
        self.release();
    }

    /// Stops naming the delivery, unless a run still delivers it or it
    /// holds its event: their end does then.
    fn release(&self) {
        let free = {
            let state = locked(&self.state);
            state.target.is_none() && state.held.is_none()
        };
        if free && let Some(keys) = self.keys.upgrade() {
            keys.release(self);
        }
    }

    /// Records on `held` that the delivery was cancelled for `reason`.
    fn record(&self, held: HeldEvent, reason: &str) {
        if let Some(keys) = self.keys.upgrade() {
            keys.record(held, reason);
        }
    }

    /// Why the delivery was cancelled, if it was.
    pub(crate) fn cancelled(&self) -> Option<String> {
        locked(&self.state).cancelled()
    }

    /// Whether its room is known: its call named its organization, or its
    /// room was read.
    pub(crate) fn is_located(&self) -> bool {
        locked(&self.state).located
    }

    /// Whether a key names it.
    pub(crate) fn has_key(&self) -> bool {
        self.key.is_some()
    }

    /// Cancels the delivery for `reason` from the code that named it, as any
    /// caller would: whether this call cancelled it. A delivery no run
    /// delivers yet is cut once one does, before its turns start.
    pub(crate) fn cut(&self, reason: &str) -> bool {
        self.cancel(reason, None)
    }
}

/// A delivery's name held by the code that runs it: dropped, that code is
/// over, and so is the delivery, but for a held event's.
#[derive(Debug)]
pub(crate) struct NamedGuard {
    named: Arc<Named>,
    /// The cut of the run that delivers it, once attached.
    target: Option<Arc<CancelRequest>>,
}

impl NamedGuard {
    /// A guard of its own on `named`, for code that runs its delivery
    /// beside the code that named it: the run that adopted its call.
    pub(crate) fn of(named: Arc<Named>) -> Self {
        Self {
            named,
            target: None,
        }
    }

    /// The delivery it names.
    pub(crate) fn named(&self) -> &Arc<Named> {
        &self.named
    }

    /// Makes `target` the cut of the run that delivers it, until the guard
    /// is dropped.
    pub(crate) fn attach(&mut self, target: Arc<CancelRequest>) {
        self.named.attach(&target);
        self.target = Some(target);
    }
}

impl Drop for NamedGuard {
    fn drop(&mut self) {
        match &self.target {
            Some(target) => self.named.over(target),
            None => self.named.release(),
        }
    }
}

/// Which deliveries each key names, in flight or holding their event for
/// verification; Python's `DeliveryKeys`.
#[derive(Default)]
pub(crate) struct DeliveryKeys {
    named: Mutex<HashMap<String, Vec<Arc<Named>>>>,
    /// The deliveries holding their event for verification, by event.
    held: Mutex<HashMap<EventId, Arc<Named>>>,
    /// How long a held event may wait for its release, when the kit's
    /// policy releases held messages (RFC §11.7.6).
    lifetime: OnceLock<Duration>,
    /// What records a held delivery's cancellation on its event.
    recorder: OnceLock<Recorder>,
}

impl std::fmt::Debug for DeliveryKeys {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeliveryKeys")
            .field("named", &self.named)
            .field("held", &self.held)
            .field("lifetime", &self.lifetime)
            .finish_non_exhaustive()
    }
}

impl DeliveryKeys {
    /// Holds released events' deliveries for `lifetime` at most, and
    /// records a held delivery's cancellation through `recorder`: the kit's
    /// policy releases held messages (RFC §11.7.6).
    pub(crate) fn releasing(&self, lifetime: Duration, recorder: Recorder) {
        let _ = self.lifetime.set(lifetime);
        drop(self.recorder.set(recorder));
    }

    /// Names the delivery of a call acting for `organization_id`, by `key`
    /// when it has one, until the guard is dropped and, for a delivery that
    /// holds its event, until its release.
    pub(crate) fn naming(
        self: &Arc<Self>,
        key: Option<&str>,
        organization_id: Option<&str>,
    ) -> NamedGuard {
        let named = Arc::new(Named {
            key: key.map(str::to_owned),
            keys: Arc::downgrade(self),
            state: Mutex::new(NamedState {
                organization_id: organization_id.map(str::to_owned),
                located: organization_id.is_some(),
                ..NamedState::default()
            }),
        });
        if let Some(key) = key {
            locked(&self.named)
                .entry(key.to_owned())
                .or_default()
                .push(Arc::clone(&named));
        }
        NamedGuard {
            named,
            target: None,
        }
    }

    /// Cancels every delivery `key` names, only those of `organization_id`'s
    /// rooms when given: how many this call cancelled, none for a key naming
    /// nothing (over, or never sent) or deliveries cancelled already.
    pub(crate) fn cancel(&self, key: &str, reason: &str, organization_id: Option<&str>) -> usize {
        self.expire();
        let named: Vec<Arc<Named>> = locked(&self.named).get(key).cloned().unwrap_or_default();
        named
            .iter()
            .filter(|named| named.cancel(reason, organization_id))
            .count()
    }

    /// `named` holds `held` for verification: its delivery is over once the
    /// event is released ([`unhold`](Self::unhold)), or once the policy's
    /// lifetime passed and nothing may release it. Nothing is held when the
    /// policy releases nothing.
    fn hold(&self, named: &Arc<Named>, held: HeldEvent) {
        let Some(lifetime) = self.lifetime.get() else {
            return;
        };
        self.expire();
        let event_id = held.1.clone();
        locked(&named.state).held = Some((held, Instant::now() + *lifetime));
        locked(&self.held).insert(event_id, Arc::clone(named));
    }

    /// The delivery that held `event_id`, now released (RFC §11.7.6), named
    /// until the guard is dropped: the run of its answer, or nothing; `None`
    /// when this process holds no delivery for the event.
    pub(crate) fn unhold(&self, event_id: &EventId) -> Option<NamedGuard> {
        let named = locked(&self.held).remove(event_id)?;
        locked(&named.state).held = None;
        Some(NamedGuard {
            named,
            target: None,
        })
    }

    /// Records on `held` that its delivery was cancelled for `reason`.
    fn record(&self, held: HeldEvent, reason: &str) {
        if let Some(recorder) = self.recorder.get() {
            recorder(held, reason.to_owned());
        }
    }

    /// Stops naming `named` by its key: its delivery is over.
    fn release(&self, named: &Named) {
        let Some(key) = &named.key else {
            return;
        };
        let mut keys = locked(&self.named);
        let Some(entries) = keys.get_mut(key) else {
            return;
        };
        entries.retain(|entry| !std::ptr::eq(Arc::as_ptr(entry), named));
        if entries.is_empty() {
            keys.remove(key);
        }
    }

    /// Releases the held deliveries whose event nothing may release now.
    fn expire(&self) {
        let now = Instant::now();
        let expired: Vec<EventId> = locked(&self.held)
            .iter()
            .filter(|(_, named)| {
                locked(&named.state)
                    .held
                    .as_ref()
                    .is_some_and(|(_, until)| *until <= now)
            })
            .map(|(event_id, _)| event_id.clone())
            .collect();
        for event_id in expired {
            drop(self.unhold(&event_id));
        }
    }

    /// Whether any key names a delivery, or any delivery holds its event.
    #[cfg(test)]
    fn is_empty(&self) -> bool {
        locked(&self.named).is_empty() && locked(&self.held).is_empty()
    }
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reader_woken_by_its_turn_sees_the_cut_before_notification() {
        use std::future::Future;
        use std::task::{Context, Waker};

        let cancel = CancelRequest::default();
        let mut waiting = std::pin::pin!(cancel.cancelled());
        let mut cx = Context::from_waker(Waker::noop());
        assert!(waiting.as_mut().poll(&mut cx).is_pending());

        // request publishes the reason before sending controls. A turn's
        // end can wake the reader before request reaches notify_waiters.
        locked(&cancel.state).reason = Some("requested_stop".to_owned());
        assert!(waiting.as_mut().poll(&mut cx).is_ready());
    }

    fn held(event: &str) -> HeldEvent {
        (RoomId::new("room-1").unwrap(), EventId::new(event).unwrap())
    }

    // Python: tests/test_delivery_cancel_turns.py::test_a_key_names_every_delivery_in_flight_and_nothing_after
    #[test]
    fn a_key_names_every_delivery_until_its_guard_drops() {
        let keys = Arc::new(DeliveryKeys::default());
        assert_eq!(keys.cancel("never sent", "stopped", None), 0);
        let first = keys.naming(Some("task"), None);
        let second = keys.naming(Some("task"), None);
        let unnamed = keys.naming(None, None);
        assert_eq!(keys.cancel("task", "stopped", None), 2);
        assert_eq!(keys.cancel("task", "stopped", None), 0);
        drop((first, second, unnamed));
        assert!(keys.is_empty());
    }

    // Python: tests/test_delivery_cancel_turns.py::test_a_scoped_cancel_before_the_commit_reaches_only_its_organization
    #[test]
    fn a_scoped_cancel_before_the_room_is_read_waits_for_it() {
        let keys = Arc::new(DeliveryKeys::default());
        let mut guard = keys.naming(Some("t"), None);
        let cut = Arc::new(CancelRequest::default());
        guard.attach(Arc::clone(&cut));
        assert_eq!(keys.cancel("t", "elsewhere", Some("org-2")), 0);
        assert_eq!(keys.cancel("t", "stopped", Some("org-1")), 0);
        assert_eq!(cut.reason(), None);
        guard.named().locate(Some("org-1"));
        assert_eq!(cut.reason().as_deref(), Some("stopped"));
        assert_eq!(keys.cancel("t", "again", Some("org-1")), 0);
    }

    // Python: tests/test_delivery_cancel_turns.py::test_a_delivery_cancelled_before_its_commit_starts_no_turn
    #[test]
    fn a_cancel_before_the_run_is_known_reaches_it_once_attached() {
        let keys = Arc::new(DeliveryKeys::default());
        let mut guard = keys.naming(Some("a"), Some("org-1"));
        assert_eq!(keys.cancel("a", "stopped", Some("org-1")), 1);
        let cut = Arc::new(CancelRequest::default());
        guard.attach(Arc::clone(&cut));
        assert_eq!(cut.reason().as_deref(), Some("stopped"));
        assert_eq!(keys.cancel("a", "stopped", None), 0);
    }

    // Python: core/_delivery_keys.py::NamedDelivery.attach reads cancelled
    // from the current cascade before handing the name to the next one.
    #[test]
    fn a_cancel_of_the_current_run_survives_handing_its_name_to_another() {
        let keys = Arc::new(DeliveryKeys::default());
        let mut guard = keys.naming(Some("a"), None);
        let first = Arc::new(CancelRequest::default());
        guard.attach(Arc::clone(&first));
        first.request("stopped");
        let next = Arc::new(CancelRequest::default());
        guard.attach(Arc::clone(&next));
        assert_eq!(next.reason().as_deref(), Some("stopped"));
        assert_eq!(keys.cancel("a", "again", None), 0);
    }

    // Python: tests/test_verification_release_cancel.py::test_a_held_message_past_its_release_age_is_named_no_more
    #[tokio::test(start_paused = true)]
    async fn a_held_delivery_past_its_release_age_is_named_no_more() {
        let keys = Arc::new(DeliveryKeys::default());
        keys.releasing(Duration::from_millis(20), Box::new(|_, _| {}));
        let mut guard = keys.naming(Some("task-42"), None);
        guard.attach(Arc::new(CancelRequest::default()));
        keys.hold(guard.named(), held("held"));
        drop(guard);
        assert!(!keys.is_empty(), "its run over, the key still names it");

        tokio::time::advance(Duration::from_millis(50)).await;

        assert_eq!(keys.cancel("task-42", "stopped", None), 0);
        assert!(keys.is_empty());
    }

    // Python: tests/test_verification_release_cancel.py::test_a_cancelled_held_message_is_released_with_no_answer
    #[tokio::test(start_paused = true)]
    async fn a_held_delivery_cancelled_once_its_run_is_over_is_recorded_and_unheld_cancelled() {
        let keys = Arc::new(DeliveryKeys::default());
        let writes = Arc::new(Mutex::new(Vec::new()));
        let into = Arc::clone(&writes);
        let record: Recorder = Box::new(move |held, reason| locked(&into).push((held, reason)));
        keys.releasing(Duration::from_secs(3600), record);
        let mut guard = keys.naming(Some("task-42"), None);
        guard.attach(Arc::new(CancelRequest::default()));
        keys.hold(guard.named(), held("held"));
        drop(guard);

        assert_eq!(keys.cancel("task-42", "user_stopped", None), 1);

        assert_eq!(
            *locked(&writes),
            [(held("held"), "user_stopped".to_owned())]
        );
        let released = keys.unhold(&EventId::new("held").unwrap()).unwrap();
        assert_eq!(
            released.named().cancelled().as_deref(),
            Some("user_stopped")
        );
        drop(released);
        assert!(keys.is_empty());
    }
}
