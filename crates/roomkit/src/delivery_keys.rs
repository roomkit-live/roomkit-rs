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

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Waker};

/// A request to cut a delivery's turns, with its reason: what a
/// cancellation by key, or a handle's `request_cancel`, asks of the run that
/// delivers (RFC §10.1 "Explicit delivery cancellation").
#[derive(Debug, Default)]
pub(crate) struct CancelRequest(Mutex<Requested>);

#[derive(Debug, Default)]
struct Requested {
    reason: Option<String>,
    waker: Option<Waker>,
}

impl CancelRequest {
    /// Asks for the cut, for `reason`: whether this call asked first.
    pub(crate) fn request(&self, reason: &str) -> bool {
        let mut state = locked(&self.0);
        if state.reason.is_some() {
            return false;
        }
        state.reason = Some(reason.to_owned());
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
        true
    }

    /// Why the cut was asked for, once it was.
    pub(crate) fn reason(&self) -> Option<String> {
        locked(&self.0).reason.clone()
    }

    /// Whether the cut was asked for; `cx` is woken once it is otherwise.
    pub(crate) fn poll(&self, cx: &Context<'_>) -> bool {
        let mut state = locked(&self.0);
        if state.reason.is_some() {
            state.waker = None;
            return true;
        }
        state.waker.clone_from(&Some(cx.waker().clone()));
        false
    }
}

/// A delivery a key names, from its call's start: the organization of its
/// room once read, and the cut it asks of the run that delivers it once
/// that run is known; Python's `NamedDelivery`.
#[derive(Debug)]
pub(crate) struct Named {
    key: String,
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
    /// A cancellation received while no run delivers it yet.
    cancelled: Option<String>,
    /// The cut of the run that delivers it, once known.
    target: Option<Arc<CancelRequest>>,
}

impl Named {
    /// Cancels the delivery for a caller acting for `organization_id`, any
    /// caller when `None`: whether this call cancelled it. A scoped request
    /// on a delivery whose room is not known yet waits for it and is not
    /// counted: until then the delivery is no organization's.
    fn cancel(&self, reason: &str, organization_id: Option<&str>) -> bool {
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
        match &state.target {
            Some(target) => target.request(reason),
            None if state.cancelled.is_some() => false,
            None => {
                state.cancelled = Some(reason.to_owned());
                true
            }
        }
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
        match &state.target {
            Some(target) => {
                target.request(&reason);
            }
            None => {
                state.cancelled.get_or_insert(reason);
            }
        }
    }

    /// Makes `target` the cut of the run that delivers it: asked at once
    /// when the delivery was cancelled already.
    pub(crate) fn attach(&self, target: Arc<CancelRequest>) {
        let mut state = locked(&self.state);
        if let Some(reason) = &state.cancelled {
            target.request(reason);
        }
        state.target = Some(target);
    }
}

/// The name of a delivery, held while it is in flight: dropped, the key
/// names it no more.
#[derive(Debug)]
pub(crate) struct NamedGuard {
    keys: Arc<DeliveryKeys>,
    named: Arc<Named>,
}

impl NamedGuard {
    /// The delivery it names.
    pub(crate) fn named(&self) -> &Arc<Named> {
        &self.named
    }
}

impl Drop for NamedGuard {
    fn drop(&mut self) {
        self.keys.release(&self.named);
    }
}

/// Which deliveries each key names, Python's `DeliveryKeys`.
#[derive(Debug, Default)]
pub(crate) struct DeliveryKeys {
    named: Mutex<HashMap<String, Vec<Arc<Named>>>>,
}

impl DeliveryKeys {
    /// Names the delivery of a call acting for `organization_id` by `key`
    /// until the guard is dropped; nothing for no key.
    pub(crate) fn naming(
        self: &Arc<Self>,
        key: Option<&str>,
        organization_id: Option<&str>,
    ) -> Option<NamedGuard> {
        let key = key?;
        let named = Arc::new(Named {
            key: key.to_owned(),
            state: Mutex::new(NamedState {
                organization_id: organization_id.map(str::to_owned),
                located: organization_id.is_some(),
                ..NamedState::default()
            }),
        });
        locked(&self.named)
            .entry(key.to_owned())
            .or_default()
            .push(Arc::clone(&named));
        Some(NamedGuard {
            keys: Arc::clone(self),
            named,
        })
    }

    /// Cancels every delivery `key` names, only those of `organization_id`'s
    /// rooms when given: how many this call cancelled, none for a key naming
    /// nothing (over, or never sent) or deliveries cancelled already.
    pub(crate) fn cancel(&self, key: &str, reason: &str, organization_id: Option<&str>) -> usize {
        let named: Vec<Arc<Named>> = locked(&self.named).get(key).cloned().unwrap_or_default();
        named
            .iter()
            .filter(|named| named.cancel(reason, organization_id))
            .count()
    }

    /// Stops naming `named` by its key: its delivery is over.
    fn release(&self, named: &Arc<Named>) {
        let mut keys = locked(&self.named);
        let Some(entries) = keys.get_mut(&named.key) else {
            return;
        };
        entries.retain(|entry| !Arc::ptr_eq(entry, named));
        if entries.is_empty() {
            keys.remove(&named.key);
        }
    }

    /// Whether any key names a delivery.
    #[cfg(test)]
    fn is_empty(&self) -> bool {
        locked(&self.named).is_empty()
    }
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Python: tests/test_delivery_cancel_turns.py::test_a_key_names_every_delivery_in_flight_and_nothing_after
    #[test]
    fn a_key_names_every_delivery_until_its_guard_drops() {
        let keys = Arc::new(DeliveryKeys::default());
        assert_eq!(keys.cancel("never sent", "stopped", None), 0);
        let first = keys.naming(Some("task"), None);
        let second = keys.naming(Some("task"), None);
        assert_eq!(keys.cancel("task", "stopped", None), 2);
        assert_eq!(keys.cancel("task", "stopped", None), 0);
        drop((first, second));
        assert!(keys.is_empty());
        assert!(keys.naming(None, None).is_none());
    }

    // Python: tests/test_delivery_cancel_turns.py::test_a_scoped_cancel_before_the_commit_reaches_only_its_organization
    #[test]
    fn a_scoped_cancel_before_the_room_is_read_waits_for_it() {
        let keys = Arc::new(DeliveryKeys::default());
        let guard = keys.naming(Some("t"), None).unwrap();
        let cut = Arc::new(CancelRequest::default());
        guard.named().attach(Arc::clone(&cut));
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
        let guard = keys.naming(Some("a"), Some("org-1")).unwrap();
        assert_eq!(keys.cancel("a", "stopped", Some("org-1")), 1);
        let cut = Arc::new(CancelRequest::default());
        guard.named().attach(Arc::clone(&cut));
        assert_eq!(cut.reason().as_deref(), Some("stopped"));
        assert_eq!(keys.cancel("a", "stopped", None), 0);
    }
}
