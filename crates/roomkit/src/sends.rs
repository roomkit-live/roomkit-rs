//! What guards a transport's sends (RFC §10.2 step 3d, §13.1, §13.2), as
//! the Python implementation's `EventRouter` guards them: the channel's
//! circuit breaker, its binding's rate limit, then its binding's retry
//! policy around `deliver()` (RMK-1048).
//!
//! - The breaker is the channel's, every room's sends sharing it: five
//!   failed sends in a row open it, and its sends are then skipped, failed
//!   `DeliveryFailed`, `circuit_breaker_open`, until 60 seconds later one
//!   probe is let through; a send that succeeds closes it. Its opening and
//!   its closing are framework events (`circuit_breaker_opened`,
//!   `circuit_breaker_closed`), emitted without waiting for their handlers.
//! - The rate limit is a bucket of tokens per channel, refilled at the
//!   binding's rate and holding a second of it (one at least): a send
//!   waits for its token.
//! - The retry policy sends again a failure that may be retried: the
//!   error's own word first (`retryable`), then the policy's list of
//!   classes, the error's class or a base of it; the delay doubles from
//!   the policy's base, up to its most.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use roomkit_core::model::{
    ChannelBinding, ChannelId, ChannelOutput, RateLimit, RetryPolicy, RoomContext, RoomEvent,
};
use roomkit_core::ops::FrameworkEvent as CoreEvent;
use tokio::time::Instant;

use crate::channel::{ChannelError, DynChannel};
use crate::error::RoomKitError;

/// The failed sends in a row that open a channel's breaker.
const FAILURE_THRESHOLD: u32 = 5;

/// How long an open breaker skips the channel's sends before it lets one
/// probe through.
const RECOVERY: Duration = Duration::from_secs(60);

/// How many seconds of its rate a channel's bucket holds.
const BURST_SECONDS: f64 = 1.0;

/// What a send skipped by an open breaker fails with: Python's code and
/// message.
pub(crate) const CIRCUIT_OPEN: (&str, &str) = ("DeliveryFailed", "circuit_breaker_open");

/// The breakers and the buckets of the kit's transports, by channel.
#[derive(Debug, Default)]
pub(crate) struct Sends {
    breakers: Mutex<HashMap<ChannelId, Breaker>>,
    buckets: Mutex<HashMap<ChannelId, Bucket>>,
}

/// A send the channel's open breaker skipped: nothing was sent.
#[derive(Debug)]
pub(crate) struct CircuitOpen;

impl Sends {
    /// Sends `event` through `channel` as `binding` guards it, the
    /// breaker's transitions emitted through `emit`: the channel's answer,
    /// or its last failure.
    ///
    /// # Errors
    ///
    /// Returns [`CircuitOpen`] when the channel's breaker skipped the send.
    pub(crate) async fn deliver(
        &self,
        channel: &dyn DynChannel,
        binding: ChannelBinding,
        event: RoomEvent,
        context: RoomContext,
        emit: impl Fn(CoreEvent),
    ) -> Result<Result<ChannelOutput, ChannelError>, CircuitOpen> {
        let channel_id = binding.channel_id.clone();
        if !self.breaker(&channel_id).allows(Instant::now()) {
            tracing::warn!(target: "roomkit::delivery", channel_id = %channel_id, "the channel's circuit breaker is open: the send is skipped");
            return Err(CircuitOpen);
        }
        if let Some(limit) = &binding.rate_limit {
            self.wait_for_token(&channel_id, limit).await;
        }
        let policy = binding.retry_policy.clone();
        let answered = match &policy {
            Some(policy) => {
                retrying(policy, || {
                    channel.deliver(event.clone(), binding.clone(), context.clone())
                })
                .await
            }
            None => channel.deliver(event, binding, context).await,
        };
        let transition = match &answered {
            Ok(_) => self.breaker(&channel_id).succeeded(),
            Err(_) => self.breaker(&channel_id).failed(Instant::now()),
        };
        match transition {
            Some(Transition::Opened) => emit(CoreEvent::circuit_breaker_opened(channel_id)),
            Some(Transition::Closed) => emit(CoreEvent::circuit_breaker_closed(channel_id)),
            None => {}
        }
        Ok(answered)
    }

    /// Waits until the channel's bucket holds a token for a send under
    /// `limit`, and takes it.
    async fn wait_for_token(&self, channel_id: &ChannelId, limit: &RateLimit) {
        let Some(rate) = rate_per_second(limit) else {
            return;
        };
        loop {
            if self.bucket(channel_id).take(rate, Instant::now()) {
                return;
            }
            tokio::time::sleep(Duration::from_secs_f64(1.0 / rate)).await;
        }
    }

    fn breaker(&self, channel_id: &ChannelId) -> Guard<'_, Breaker> {
        Guard::of(lock(&self.breakers), channel_id)
    }

    fn bucket(&self, channel_id: &ChannelId) -> Guard<'_, Bucket> {
        Guard::of(lock(&self.buckets), channel_id)
    }
}

/// One channel's entry of a map, held locked.
struct Guard<'a, T> {
    map: MutexGuard<'a, HashMap<ChannelId, T>>,
    channel_id: ChannelId,
}

impl<'a, T: Default> Guard<'a, T> {
    fn of(map: MutexGuard<'a, HashMap<ChannelId, T>>, channel_id: &ChannelId) -> Self {
        Self {
            map,
            channel_id: channel_id.clone(),
        }
    }

    fn entry(&mut self) -> &mut T {
        self.map.entry(self.channel_id.clone()).or_default()
    }
}

impl Guard<'_, Breaker> {
    fn allows(&mut self, now: Instant) -> bool {
        self.entry().allows(now)
    }

    fn succeeded(&mut self) -> Option<Transition> {
        self.entry().succeeded()
    }

    fn failed(&mut self, now: Instant) -> Option<Transition> {
        self.entry().failed(now)
    }
}

impl Guard<'_, Bucket> {
    fn take(&mut self, rate: f64, now: Instant) -> bool {
        self.entry().take(rate, now)
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A breaker's transition, which a framework event reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Transition {
    Opened,
    Closed,
}

/// A channel's circuit breaker (roomkit's `CircuitBreaker`): closed, open
/// once its failures in a row reach the threshold, half-open, one probe let
/// through, once the recovery passed. A probe that never ended, its send
/// cut by the kit's close, lets another through once the recovery passed
/// again since it went, where Python's breaker would stay open for good.
#[derive(Debug, Default)]
struct Breaker {
    failures: u32,
    opened_at: Option<Instant>,
    probe_at: Option<Instant>,
}

impl Breaker {
    /// Whether a send may be attempted at `now`.
    fn allows(&mut self, now: Instant) -> bool {
        let Some(opened_at) = self.opened_at else {
            return true;
        };
        let recovered = |since: Instant| now.duration_since(since) >= RECOVERY;
        if recovered(opened_at) && self.probe_at.is_none_or(recovered) {
            self.probe_at = Some(now);
            return true;
        }
        false
    }

    /// A send succeeded: the breaker closes, which is a transition when it
    /// was open.
    fn succeeded(&mut self) -> Option<Transition> {
        let was_open = self.opened_at.is_some();
        *self = Self::default();
        was_open.then_some(Transition::Closed)
    }

    /// A send failed at `now`: past the threshold the breaker opens again,
    /// a transition when it was closed.
    fn failed(&mut self, now: Instant) -> Option<Transition> {
        self.failures = self.failures.saturating_add(1);
        if self.failures < FAILURE_THRESHOLD {
            return None;
        }
        let was_open = self.opened_at.is_some();
        self.opened_at = Some(now);
        self.probe_at = None;
        (!was_open).then_some(Transition::Opened)
    }
}

/// A channel's bucket of tokens (roomkit's `TokenBucketRateLimiter`).
#[derive(Debug, Default)]
struct Bucket {
    /// Its tokens and when they were counted; `None` before its first send,
    /// when it is full.
    held: Option<(f64, Instant)>,
}

impl Bucket {
    /// Takes a token at `rate` per second, at `now`, if it holds one.
    fn take(&mut self, rate: f64, now: Instant) -> bool {
        let most = (rate * BURST_SECONDS).max(1.0);
        let (tokens, counted) = self.held.unwrap_or((most, now));
        let tokens = (tokens + now.duration_since(counted).as_secs_f64() * rate).min(most);
        if tokens >= 1.0 {
            self.held = Some((tokens - 1.0, now));
            true
        } else {
            self.held = Some((tokens, now));
            false
        }
    }
}

/// The sends a second `limit` allows, its most precise bound first; `None`
/// for no bound.
fn rate_per_second(limit: &RateLimit) -> Option<f64> {
    let rate = limit
        .max_per_second
        .or(limit.max_per_minute.map(|per_minute| per_minute / 60.0))
        .or(limit.max_per_hour.map(|per_hour| per_hour / 3600.0))?;
    (rate.is_finite() && rate > 0.0).then_some(rate)
}

/// `send`, tried again under `policy` while it fails with an error that may
/// be retried (roomkit's `retry_with_backoff`); its last answer.
async fn retrying<F, Fut>(policy: &RetryPolicy, mut send: F) -> Result<ChannelOutput, ChannelError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<ChannelOutput, ChannelError>>,
{
    let mut attempt: u32 = 0;
    loop {
        let error = match send().await {
            Ok(output) => return Ok(output),
            Err(error) => error,
        };
        if !retryable(&error, policy) || attempt >= policy.max_retries {
            return Err(error);
        }
        let delay = delay(policy, attempt);
        tracing::warn!(
            target: "roomkit::delivery",
            attempt = attempt + 1,
            of = policy.max_retries + 1,
            delay_seconds = delay.as_secs_f64(),
            "a send failed: it is tried again"
        );
        tokio::time::sleep(delay).await;
        attempt += 1;
    }
}

/// The delay before the try after `attempt`: the base grown by the
/// exponent at each attempt, at most the policy's most.
fn delay(policy: &RetryPolicy, attempt: u32) -> Duration {
    let grown = policy.base_delay_seconds
        * policy
            .exponential_base
            .powi(i32::try_from(attempt).unwrap_or(i32::MAX));
    let seconds = grown.min(policy.max_delay_seconds);
    Duration::try_from_secs_f64(seconds).unwrap_or(Duration::ZERO)
}

/// Whether `error` may be tried again under `policy` (RFC §13.2): the
/// error's own word first, a kit error's `retryable`; then the policy's
/// list, its class or a base of it, every error retried without one.
fn retryable(error: &ChannelError, policy: &RetryPolicy) -> bool {
    let kit_error = error.downcast_ref::<RoomKitError>();
    if let Some(declared) = kit_error.and_then(RoomKitError::retryable) {
        return declared;
    }
    let Some(classes) = &policy.retryable_errors else {
        return true;
    };
    let class = kit_error.map_or_else(|| "ChannelError".to_owned(), RoomKitError::class_name);
    let builtin = matches!(
        kit_error,
        Some(
            RoomKitError::Value(_) | RoomKitError::Cancelled | RoomKitError::ExceptionGroup { .. }
        )
    );
    let mut names = vec![class.as_str()];
    if kit_error.is_some() && !builtin {
        names.push("RoomKitError");
    }
    // `CancelledError` is a `BaseException` alone.
    if !matches!(kit_error, Some(RoomKitError::Cancelled)) {
        names.push("Exception");
    }
    names.push("BaseException");
    classes
        .iter()
        .any(|listed| names.contains(&listed.as_str()))
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    /// The breaker's states (roomkit's `CircuitBreaker`): open at the fifth
    /// failure in a row, one probe through once the recovery passed; a
    /// probe that never ended lets another through a recovery later, so a
    /// send the kit's close cut cannot leave the channel closed for good.
    #[test]
    fn an_open_breaker_lets_one_probe_through_per_recovery() {
        let start = Instant::now();
        let at = |seconds: u64| start + Duration::from_secs(seconds);
        let mut breaker = Breaker::default();
        let opened: Vec<Option<Transition>> = (0..5).map(|_| breaker.failed(at(0))).collect();
        let allowed = [
            breaker.allows(at(59)),
            breaker.allows(at(60)),
            breaker.allows(at(61)),
            breaker.allows(at(119)),
            breaker.allows(at(120)),
        ];
        assert_eq!(
            (
                opened.last().copied().flatten(),
                allowed,
                breaker.succeeded()
            ),
            (
                Some(Transition::Opened),
                [false, true, false, false, true],
                Some(Transition::Closed)
            )
        );
    }
}
