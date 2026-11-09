//! Cancelling the deliveries a caller named by a key (RFC §10.1 "Explicit
//! delivery cancellation"): Python's `InboundMixin.cancel_delivery`.

use super::RoomKit;
use crate::driver::CALLER_CANCELLED;
use crate::store::ConversationStore;

/// What [`RoomKit::cancel_delivery`] takes beside the key: Python's
/// `cancel_delivery(key, *, reason='caller_cancelled', organization_id=None)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelDeliveryOptions {
    /// Python `reason='caller_cancelled'`: why, each delivery's
    /// `cancellation_reason`.
    pub reason: String,
    /// Python `organization_id=None`: the organization the caller acts for
    /// (RFC §17.2); only the deliveries of its rooms are cancelled.
    pub organization_id: Option<String>,
}

impl Default for CancelDeliveryOptions {
    fn default() -> Self {
        Self {
            reason: CALLER_CANCELLED.to_owned(),
            organization_id: None,
        }
    }
}

impl<S: ConversationStore> RoomKit<S> {
    /// Cancels the deliveries the key names of the messages sent with
    /// `InboundMessage { delivery_key: Some(key), .. }` (RFC §10.1 "Explicit
    /// delivery cancellation"): how many it cancelled, 0 when the key names
    /// nothing (the deliveries are over, or were never sent), or only
    /// deliveries cancelled already. Python's `cancel_delivery(key, *,
    /// reason='caller_cancelled', organization_id=None)`.
    ///
    /// A key names its delivery from the start of
    /// [`process_inbound`](Self::process_inbound) until the delivery is
    /// over. Each delivery's agents stop: a turn not started yet never
    /// starts, a running one is cut, and nothing answers it after. The
    /// message itself is still delivered to the room's other channels and
    /// its `AFTER_BROADCAST` hooks fire; one cancelled before its commit is
    /// committed all the same. Returns at once, from anywhere, a hook under
    /// the room's lock or a tool handler included; each delivery's caller
    /// reads its cancelled end on its result, its `cancellation_reason` the
    /// reason given, once its turns are over; a call refused before its
    /// commit reports its refusal instead.
    ///
    /// With `organization_id`, only the deliveries of that organization's
    /// rooms are cancelled, so two tenants that chose the same key never
    /// cancel each other's (RFC §17.2). A delivery whose call named no
    /// organization belongs to none until the call reads its room: a scoped
    /// cancellation reaching it earlier is applied once the room proves the
    /// organization's, and is not counted.
    ///
    /// The key names, it does not authorize: check who may cancel before
    /// calling.
    #[expect(
        clippy::unused_async,
        clippy::unused_async_trait_impl,
        reason = "Python's method is a coroutine: the call is awaited"
    )]
    pub async fn cancel_delivery(&self, key: &str, options: CancelDeliveryOptions) -> usize {
        if self.inner.open().is_err() {
            return 0;
        }
        let CancelDeliveryOptions {
            reason,
            organization_id,
        } = options;
        self.inner
            .delivery_keys
            .cancel(key, &reason, organization_id.as_deref())
    }
}
