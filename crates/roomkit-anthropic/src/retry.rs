//! Anthropic SDK transport retry policy: retry response headers and failures
//! before streaming, never replay a body already handed to the caller.

use std::hash::{BuildHasher, Hasher};
use std::time::Duration;

use jiff::Timestamp;
use jiff::fmt::rfc2822::DateTimeParser;
use jiff::fmt::strtime::BrokenDownTime;
use jiff::tz::TimeZone;
use reqwest::StatusCode;
use reqwest::header::{HeaderMap, HeaderValue};
use roomkit_core::ops::{Environment, SystemEnvironment};

/// The first wait of a try the endpoint named none for, in seconds: the
/// SDK's `INITIAL_RETRY_DELAY`.
const INITIAL_RETRY_DELAY: f64 = 0.5;
/// The longest such wait, in seconds: the SDK's `MAX_RETRY_DELAY`.
const MAX_RETRY_DELAY: f64 = 8.0;
/// Clamp an extreme remote delay to a representable timer duration.
const MAX_RETRY_AFTER_DELAY: f64 = 4_294_967.0;

/// Whether an answer of `status` with `headers` is worth another try: the
/// SDK's `_should_retry`: `x-should-retry` decides, then the status.
pub(crate) fn should_retry(status: StatusCode, headers: &HeaderMap) -> bool {
    match headers.get("x-should-retry").map(HeaderValue::as_bytes) {
        Some(b"true") => return true,
        Some(b"false") => return false,
        _ => {}
    }
    let status = status.as_u16();
    matches!(status, 408 | 409 | 429) || status >= 500
}

/// How long to wait before the try after `retries_taken` tries, answered
/// with `headers` or not answered at all: the SDK's
/// `_calculate_retry_timeout`.
pub(crate) fn delay(retries_taken: u32, headers: Option<&HeaderMap>) -> Duration {
    let asked = headers.and_then(|headers| retry_after(headers, now_seconds()));
    delay_within(retries_taken, asked, random_unit())
}

/// The wait the endpoint `asked`, within reason, else the backoff after
/// `retries_taken` tries, less up to a quarter of it by `random`, in `0..=1`.
fn delay_within(retries_taken: u32, asked: Option<f64>, random: f64) -> Duration {
    let reasonable = |wait: &f64| !wait.is_nan() && *wait > 0.0;
    let seconds = asked.filter(reasonable).map_or_else(
        || {
            let tries = i32::try_from(retries_taken.min(1000)).unwrap_or(1000);
            let backoff = INITIAL_RETRY_DELAY * 2.0_f64.powi(tries);
            backoff.min(MAX_RETRY_DELAY) * (1.0 - 0.25 * random)
        },
        |wait| wait.min(MAX_RETRY_AFTER_DELAY),
    );
    Duration::try_from_secs_f64(seconds).unwrap_or(Duration::ZERO)
}

/// The seconds `headers` ask to wait, at `now` seconds since the epoch: the
/// SDK's `_parse_retry_after_header`. `retry-after-ms` first, then
/// `retry-after` as a number, then as a date.
fn retry_after(headers: &HeaderMap, now: f64) -> Option<f64> {
    let text = |name: &str| headers.get(name)?.to_str().ok();
    if let Some(millis) = text("retry-after-ms").and_then(python_float) {
        return Some(millis / 1000.0);
    }
    let after = text("retry-after")?;
    python_float(after).or_else(|| Some(date_seconds(after)? - now))
}

/// `text` as Python's `float()` reads what an endpoint writes: spaces
/// around it are no part of it; underscores may separate digits.
fn python_float(text: &str) -> Option<f64> {
    let text = text.trim();
    let bytes = text.as_bytes();
    if bytes.iter().enumerate().any(|(at, byte)| {
        *byte == b'_'
            && !(at > 0
                && bytes.get(at - 1).is_some_and(u8::is_ascii_digit)
                && bytes.get(at + 1).is_some_and(u8::is_ascii_digit))
    }) {
        return None;
    }
    text.replace('_', "").parse().ok()
}

/// The seconds since the epoch of the date `text`, in any of HTTP's three
/// forms (RFC 9110 §5.6.7), its weekday not held against it, as Python's
/// `parsedate_tz` reads them.
#[expect(
    clippy::cast_precision_loss,
    reason = "a date's seconds since the epoch are far within an f64's exact integers"
)]
fn date_seconds(text: &str) -> Option<f64> {
    let text = text.trim();
    let dated = text
        .split_once(',')
        .map_or(text, |(_, dated)| dated.trim_start());
    let date = DateTimeParser::new()
        .parse_timestamp(dated)
        .ok()
        .or_else(|| obsolete_date("%d-%b-%y %H:%M:%S GMT", dated))
        .or_else(|| {
            // `Sun Nov  6 08:49:37 1994`.
            let (_, dated) = text.split_once(' ')?;
            obsolete_date("%b %e %H:%M:%S %Y", dated)
        });
    Some(date?.as_second() as f64)
}

/// The instant `text` writes in the UTC of `format`.
fn obsolete_date(format: &str, text: &str) -> Option<Timestamp> {
    let written = BrokenDownTime::parse(format, text)
        .ok()?
        .to_datetime()
        .ok()?;
    TimeZone::UTC.to_timestamp(written).ok()
}

/// The seconds since the epoch now.
#[expect(
    clippy::cast_precision_loss,
    reason = "today's milliseconds since the epoch are far within an f64's exact integers"
)]
fn now_seconds() -> f64 {
    SystemEnvironment.now().as_unix_millis() as f64 / 1000.0
}

/// A number in `0..=1`, for a wait's jitter: no more random than a hasher's
/// keys, which is all a jitter needs and takes no dependency.
fn random_unit() -> f64 {
    let bits = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    let high = u32::try_from(bits >> 32).unwrap_or(u32::MAX);
    f64::from(high) / f64::from(u32::MAX)
}
