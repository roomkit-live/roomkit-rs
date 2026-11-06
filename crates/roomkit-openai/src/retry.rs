//! The retries a request takes below the channel's retry policy: what
//! Python's `OpenAIConfig(max_retries=...)` hands the openai SDK, as its
//! request loop reads an answer (`openai._base_client`, 2.54).
//!
//! A try that could not reach the endpoint is tried again, and one answered
//! with a status [`should_retry`] says is worth it, each after [`delay`]. A
//! whole answer's body counts in the try, a stream's does not, read once its
//! head came. An answer to a whole request under another content type than
//! JSON's reads as a stream here, so its body is out of the try where the
//! SDK's is in it (RMK-1353).

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
/// The longest wait an endpoint can ask for, in seconds: past it, the
/// answer is final (the SDK's `MAX_RETRY_AFTER_DELAY`).
const MAX_RETRY_AFTER_DELAY: f64 = 120.0;

/// Whether an answer of `status` with `headers` is worth another try: the
/// SDK's `_should_retry`. A wait past [`MAX_RETRY_AFTER_DELAY`] says no
/// whatever else; then `x-should-retry` decides, then the status.
pub(crate) fn should_retry(status: StatusCode, headers: &HeaderMap) -> bool {
    let asked = retry_after(headers, now_seconds());
    if asked.is_some_and(|wait| wait.is_finite() && wait > MAX_RETRY_AFTER_DELAY) {
        return false;
    }
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
    let reasonable = |wait: &f64| wait.is_finite() && *wait > 0.0 && *wait <= MAX_RETRY_AFTER_DELAY;
    let seconds = asked.filter(reasonable).unwrap_or_else(|| {
        let tries = i32::try_from(retries_taken.min(1000)).unwrap_or(1000);
        let backoff = INITIAL_RETRY_DELAY * 2.0_f64.powi(tries);
        backoff.min(MAX_RETRY_DELAY) * (1.0 - 0.25 * random)
    });
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
/// around it are no part of it. Python's `1_000` is left out, which no
/// endpoint writes.
fn python_float(text: &str) -> Option<f64> {
    text.trim().parse().ok()
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

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use reqwest::header::HeaderName;

    use super::*;

    fn headers(given: &[(&'static str, &str)]) -> HeaderMap {
        given
            .iter()
            .map(|(name, value)| {
                (
                    HeaderName::from_static(name),
                    HeaderValue::from_str(value).unwrap(),
                )
            })
            .collect()
    }

    fn seconds(seconds: f64) -> Duration {
        Duration::from_secs_f64(seconds)
    }

    // openai._base_client: _calculate_retry_timeout, its exponential backoff
    // capped at MAX_RETRY_DELAY, its jitter taking up to a quarter off.
    #[test]
    fn the_backoff_doubles_from_half_a_second_to_eight() {
        let delays: Vec<Duration> = (0..7).map(|taken| delay_within(taken, None, 0.0)).collect();
        assert_eq!(
            delays,
            [0.5, 1.0, 2.0, 4.0, 8.0, 8.0, 8.0].map(seconds).to_vec()
        );
        assert_eq!(delay_within(0, None, 1.0), seconds(0.375));
        assert_eq!(delay_within(u32::MAX, None, 0.5), seconds(7.0));
    }

    // openai._base_client: _calculate_retry_timeout, a wait the endpoint
    // asked within (0, MAX_RETRY_AFTER_DELAY] taken as it is, any other
    // leaving the backoff.
    #[test]
    fn a_wait_the_endpoint_asked_replaces_the_backoff_within_two_minutes() {
        let backoff = seconds(0.5);
        for (asked, waited) in [
            (0.001, seconds(0.001)),
            (120.0, seconds(120.0)),
            (120.5, backoff),
            (0.0, backoff),
            (-3.0, backoff),
            (f64::NAN, backoff),
            (f64::INFINITY, backoff),
        ] {
            assert_eq!(delay_within(0, Some(asked), 0.0), waited, "{asked}");
        }
    }

    // openai._base_client: _parse_retry_after_header.
    #[test]
    fn a_wait_is_read_from_its_headers_in_the_sdk_s_order() {
        let now = 1_000_000.0;
        let read = |given: &[(&'static str, &str)]| retry_after(&headers(given), now);
        assert_eq!(
            read(&[("retry-after-ms", "1500"), ("retry-after", "9")]),
            Some(1.5)
        );
        assert_eq!(
            read(&[("retry-after-ms", "soon"), ("retry-after", "9")]),
            Some(9.0)
        );
        assert_eq!(read(&[("retry-after", " 2.5 ")]), Some(2.5));
        assert_eq!(read(&[("retry-after", "soon")]), None);
        assert_eq!(read(&[]), None);
        // 1970-01-12T13:46:49Z, nine seconds past the clock, in HTTP's three
        // forms, a wrong weekday not held against it.
        for date in [
            "Mon, 12 Jan 1970 13:46:49 GMT",
            "Tue, 12 Jan 1970 13:46:49 GMT",
            "Monday, 12-Jan-70 13:46:49 GMT",
            "Mon Jan 12 13:46:49 1970",
            "12 Jan 1970 13:46:49 GMT",
            "Mon,12 Jan 1970 13:46:49 GMT",
            "12 Jan 1970 13:46:49 +0000",
        ] {
            assert_eq!(read(&[("retry-after", date)]), Some(9.0), "{date}");
        }
        assert_eq!(
            read(&[("retry-after", "Mon Jan  5 00:00:00 1970")]),
            Some(345_600.0 - now)
        );
    }

    // HITL 08856192: what the SDK reads of a wait and this crate leaves to its
    // backoff, written in docs/architecture.md.
    #[test]
    fn a_wait_the_sdk_reads_and_this_crate_does_not() {
        let read = |after: &str| retry_after(&headers(&[("retry-after", after)]), 1_000_000.0);
        // Python's `float()` reads an underscore between digits.
        assert_eq!(read("1_000"), None);
        // `parsedate_tz` reads a date with no time zone, and a four-digit year in
        // the obsolete form.
        assert_eq!(read("Mon, 12 Jan 1970 00:00:00"), None);
        assert_eq!(read("Mon, 12-Jan-1970 00:00:00 GMT"), None);
    }

    // openai._base_client: _should_retry.
    #[test]
    fn a_status_is_worth_a_retry_as_the_sdk_reads_it() {
        let worth = |status: u16, given: &[(&'static str, &str)]| {
            should_retry(StatusCode::from_u16(status).unwrap(), &headers(given))
        };
        for status in [408, 409, 429, 500, 503, 599, 600] {
            assert!(worth(status, &[]), "{status}");
        }
        for status in [400, 401, 403, 404, 422] {
            assert!(!worth(status, &[]), "{status}");
        }
        assert!(worth(400, &[("x-should-retry", "true")]));
        assert!(!worth(503, &[("x-should-retry", "false")]));
        assert!(worth(503, &[("x-should-retry", "TRUE")]));
        // Past two minutes the endpoint's wait says no, over its own header.
        assert!(!worth(
            429,
            &[("retry-after", "121"), ("x-should-retry", "true")]
        ));
        assert!(worth(429, &[("retry-after", "120")]));
        assert!(worth(429, &[("retry-after-ms", "nan")]));
    }

    #[test]
    fn a_wait_s_jitter_stays_within_a_unit() {
        for _ in 0..100 {
            let random = random_unit();
            assert!((0.0..=1.0).contains(&random), "{random}");
        }
    }
}
