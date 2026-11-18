//! What guards a transport's sends (RFC §10.2 step 3d, §13.1, §13.2,
//! RMK-1048), as the Python implementation's `EventRouter` guards them
//! (probed in `tasks/probes/RMK-1048/py_send_guards.py` on roomkit
//! 38ef0625): its binding's retry policy, its binding's rate limit and the
//! channel's circuit breaker.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

#[path = "support/warnings.rs"]
mod warnings;

use std::future::{Future, ready};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, DeliveryResult,
    DeliveryResultStatus, InboundMessage, Metadata, ProviderResult, RateLimit, RetryPolicy,
    RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CreateRoomOptions, FrameworkEvent,
    ProcessInboundOptions, RoomKit, RoomKitError, RoomKitOptions,
};
use serde_json::json;
use tokio::time::Instant;

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("r").unwrap()
}

/// How a send of [`Flaky`] fails.
#[derive(Clone, Copy)]
enum Failure {
    /// A plain error, `flaky`: a boxed error, of no Python class.
    Plain,
    /// A provider's refusal that says it may not be retried.
    Refused,
    /// A refusal the channel issues before contacting its provider.
    NoRecipient,
    /// A provider answer carrying the same code as a pre-send refusal.
    ProviderNoRecipient,
    /// Five provider failures followed by a refusal before the recovery send.
    MissingOnProbe,
    /// The send's own cancel, Python's `CancelledError`.
    Cancelled,
}

/// A transport whose first `failures` sends fail as `failure`, noting when
/// each send was made.
#[derive(Clone)]
struct Flaky {
    failures: usize,
    failure: Failure,
    sent_at: Arc<Mutex<Vec<Instant>>>,
}

impl Flaky {
    fn new(failures: usize, failure: Failure) -> Self {
        Self {
            failures,
            failure,
            sent_at: Arc::default(),
        }
    }

    fn sends(&self) -> usize {
        self.sent_at.lock().unwrap().len()
    }

    /// The time between two sends, in milliseconds.
    fn gaps(&self) -> Vec<u128> {
        let sent_at = self.sent_at.lock().unwrap();
        sent_at
            .iter()
            .zip(sent_at.iter().skip(1))
            .map(|(earlier, later)| later.duration_since(*earlier).as_millis())
            .collect()
    }
}

impl Channel for Flaky {
    fn channel_id(&self) -> ChannelId {
        channel("ws")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::WEBSOCKET
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Transport
    }

    fn recipient_key(&self) -> Option<String> {
        None
    }

    fn on_event(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let mut sent_at = self.sent_at.lock().unwrap();
        sent_at.push(Instant::now());
        let answer = if sent_at.len() > self.failures {
            Ok(ChannelOutput::empty())
        } else {
            Err(match self.failure {
                Failure::MissingOnProbe if sent_at.len() < self.failures => "provider busy".into(),
                Failure::MissingOnProbe | Failure::NoRecipient => {
                    RoomKitError::no_recipient("ws", "customer").into()
                }
                Failure::ProviderNoRecipient => RoomKitError::ProviderDelivery {
                    result: missing_recipient_result(),
                }
                .into(),
                Failure::Plain => "flaky".into(),
                Failure::Cancelled => RoomKitError::Cancelled.into(),
                Failure::Refused => RoomKitError::ProviderDelivery {
                    result: ProviderResult {
                        error: Some("rejected".to_owned()),
                        metadata: serde_json::from_value::<Metadata>(
                            json!({ "code": "E42", "retryable": false }),
                        )
                        .unwrap(),
                        ..ProviderResult::new(false)
                    },
                }
                .into(),
            })
        };
        ready(answer)
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }
}

/// What a delivery to `ws` read: its status, and its error's code, message
/// and retryability.
type Read = (DeliveryResultStatus, Option<(String, String, bool)>);

/// Sends `messages` messages to a room where `ws` is bound as `options`
/// says; what each delivery to it read, and the framework events of its
/// breaker, in order. Between two messages, `pause` passes.
async fn sent(
    ws: &Flaky,
    messages: usize,
    pause: Duration,
    options: AttachChannelOptions,
) -> (Vec<Read>, Vec<(String, Metadata)>) {
    let (deliveries, events) = send_results(ws, messages, |_| pause, options).await;
    let read = deliveries
        .into_iter()
        .map(|delivery| {
            (
                delivery.status,
                delivery
                    .error
                    .map(|error| (error.code, error.message, error.retryable)),
            )
        })
        .collect();
    (read, events)
}

async fn send_results(
    ws: &Flaky,
    messages: usize,
    pause: impl Fn(usize) -> Duration,
    options: AttachChannelOptions,
) -> (Vec<DeliveryResult>, Vec<(String, Metadata)>) {
    let kit = RoomKit::new(RoomKitOptions::default());
    let breaker_events: Arc<Mutex<Vec<(String, Metadata)>>> = Arc::default();
    for name in ["circuit_breaker_opened", "circuit_breaker_closed"] {
        let noting = Arc::clone(&breaker_events);
        kit.on(name, move |event: FrameworkEvent| {
            noting
                .lock()
                .unwrap()
                .push((event.r#type.clone(), event.data.clone()));
            async {}
        })
        .unwrap();
    }
    let sms = RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    kit.register_channel(ws.clone()).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room_id(), &channel("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    kit.attach_channel(&room_id(), &channel("ws"), options)
        .await
        .unwrap();
    let mut read = Vec::new();
    for n in 0..messages {
        if n > 0 {
            tokio::time::sleep(pause(n)).await;
        }
        let text = TextContent::new(format!("m{n}"));
        let message = InboundMessage::new(channel("sms"), "+15550001", text);
        let routed = ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        };
        let result = kit.process_inbound(message, routed).await.unwrap();
        let delivery = result
            .delivery_results
            .get(&channel("ws"))
            .cloned()
            .unwrap();
        read.push(delivery);
    }
    kit.close().await.unwrap();
    let events = breaker_events.lock().unwrap().clone();
    (read, events)
}

fn retrying(policy: RetryPolicy) -> AttachChannelOptions {
    AttachChannelOptions {
        retry_policy: Some(policy),
        ..Default::default()
    }
}

fn policy(max_retries: u32, retryable_errors: Option<&[&str]>) -> RetryPolicy {
    RetryPolicy {
        max_retries,
        base_delay_seconds: 0.01,
        retryable_errors: retryable_errors
            .map(|names| names.iter().map(|n| (*n).to_owned()).collect()),
        ..RetryPolicy::default()
    }
}

fn sent_ok() -> Read {
    (DeliveryResultStatus::Sent, None)
}

fn failed(code: &str, message: &str, retryable: bool) -> Read {
    (
        DeliveryResultStatus::Failed,
        Some((code.to_owned(), message.to_owned(), retryable)),
    )
}

/// RFC §13.2: a send that fails is tried again within the policy's
/// retries, an error that says it may not be retried sent once, the
/// policy's list matching the error's class or a base of it; a send's own
/// cancel is never tried again, as Python's retry catches no
/// `CancelledError` (`tasks/probes/step5-review/py_agent_errors.out`: one
/// send, `CancelledError`, retryable), its message the kit error's words
/// as every kit error's, where Python's `str()` of it is empty; the delay
/// grows by the exponent from the base, up to the most.
#[tokio::test(start_paused = true)]
async fn a_failed_send_is_tried_again_as_its_binding_s_policy_says() {
    let cases = [
        (
            Flaky::new(1, Failure::Plain),
            policy(1, None),
            (2, sent_ok()),
        ),
        (
            Flaky::new(3, Failure::Plain),
            policy(2, None),
            (3, failed("ChannelError", "flaky", true)),
        ),
        (
            Flaky::new(3, Failure::Refused),
            policy(2, None),
            (1, failed("E42", "rejected", false)),
        ),
        (
            Flaky::new(3, Failure::Plain),
            policy(2, Some(&["TimeoutError"])),
            (1, failed("ChannelError", "flaky", true)),
        ),
        (
            Flaky::new(1, Failure::Plain),
            policy(2, Some(&["Exception"])),
            (2, sent_ok()),
        ),
        (
            Flaky::new(3, Failure::Cancelled),
            policy(2, Some(&["BaseException"])),
            (
                1,
                failed("CancelledError", &RoomKitError::Cancelled.to_string(), true),
            ),
        ),
    ];
    for (ws, policy, (sends, read)) in cases {
        let (reads, _) = sent(&ws, 1, Duration::ZERO, retrying(policy)).await;
        assert_eq!((ws.sends(), reads), (sends, vec![read]));
    }
    let ws = Flaky::new(3, Failure::Plain);
    let grown = RetryPolicy {
        max_retries: 3,
        base_delay_seconds: 0.02,
        exponential_base: 2.0,
        max_delay_seconds: 0.03,
        ..RetryPolicy::default()
    };
    let (reads, _) = sent(&ws, 1, Duration::ZERO, retrying(grown)).await;
    assert_eq!((ws.gaps(), reads), (vec![20, 30, 30], vec![sent_ok()]));
}

/// RFC §13.1: five failed sends in a row open the channel's breaker, which
/// skips the sends after them, failed `DeliveryFailed`,
/// `circuit_breaker_open`; past its recovery a probe that succeeds closes
/// it. Each transition is a framework event.
#[tokio::test(start_paused = true)]
async fn five_failed_sends_open_the_channel_s_breaker() {
    let ws = Flaky::new(5, Failure::Plain);
    let (reads, events) = sent(&ws, 7, Duration::ZERO, AttachChannelOptions::default()).await;
    let mut expected = vec![failed("ChannelError", "flaky", true); 5];
    expected.extend(vec![
        failed("DeliveryFailed", "circuit_breaker_open", true);
        2
    ]);
    let opened =
        serde_json::from_value::<Metadata>(json!({ "channel_id": "ws", "state": "open" })).unwrap();
    assert_eq!(
        (ws.sends(), reads, events),
        (
            5,
            expected,
            vec![("circuit_breaker_opened".to_owned(), opened.clone())]
        )
    );
    let ws = Flaky::new(5, Failure::Plain);
    let (reads, events) = sent(
        &ws,
        6,
        Duration::from_secs(61),
        AttachChannelOptions::default(),
    )
    .await;
    let closed =
        serde_json::from_value::<Metadata>(json!({ "channel_id": "ws", "state": "closed" }))
            .unwrap();
    assert_eq!(
        (ws.sends(), reads.last().cloned(), events),
        (
            6,
            Some(sent_ok()),
            vec![
                ("circuit_breaker_opened".to_owned(), opened),
                ("circuit_breaker_closed".to_owned(), closed)
            ]
        )
    );
}

/// RFC §10.2 step 3d: a send waits for its token in the channel's bucket,
/// which holds a second of the binding's rate: at 20 a second three sends
/// go at once, at one a second each waits a second.
#[tokio::test(start_paused = true)]
async fn a_send_waits_for_its_token_under_its_binding_s_rate_limit() {
    let mut gaps = Vec::new();
    for per_second in [20.0, 1.0] {
        let ws = Flaky::new(0, Failure::Plain);
        let options = AttachChannelOptions {
            rate_limit: Some(RateLimit {
                max_per_second: Some(per_second),
                ..RateLimit::default()
            }),
            ..Default::default()
        };
        let (reads, _) = sent(&ws, 3, Duration::ZERO, options).await;
        assert_eq!(reads, vec![sent_ok(); 3]);
        gaps.push(ws.gaps());
    }
    assert_eq!(gaps, [vec![0, 0], vec![1000, 1000]]);
}

fn missing_recipient_result() -> ProviderResult {
    ProviderResult {
        error: Some("no_recipient".into()),
        metadata: serde_json::from_value(json!({"code":"no_recipient","retryable":false,"channel_id":"ws","recipient_key":"customer"})).unwrap(),
        ..ProviderResult::new(false)
    }
}

#[tokio::test(start_paused = true)]
async fn only_an_application_refusal_before_sending_spares_the_breaker() {
    let refused = RoomKitError::no_recipient("ws", "customer");
    assert!(
        matches!(&refused, RoomKitError::NoRecipient {channel_id, recipient_key, ..}
        if channel_id == "ws" && recipient_key == "customer")
    );
    assert_eq!(refused.to_string(), "no_recipient");
    assert_eq!(refused.code().as_deref(), Some("no_recipient"));
    assert_eq!(refused.retryable(), Some(false));
    assert_eq!(refused.provider_result(), Some(&missing_recipient_result()));

    for (failure, failed_sends, attempts, breaker_events) in [
        (Failure::NoRecipient, 6, 7, 0),
        (Failure::ProviderNoRecipient, 5, 5, 1),
    ] {
        let (captured, _capture) = warnings::Warnings::capture();
        let app = Flaky::new(6, failure);
        let (deliveries, events) =
            send_results(&app, 7, |_| Duration::ZERO, retrying(policy(3, None))).await;
        assert_eq!((app.sends(), events.len()), (attempts, breaker_events));
        let refused = captured.containing("Delivery to ws refused: the binding has no customer");
        assert_eq!(
            refused.len(),
            if matches!(failure, Failure::NoRecipient) {
                6
            } else {
                0
            }
        );
        assert!(refused.iter().all(|line| line.starts_with("WARN")));

        for failed in &deliveries[..failed_sends] {
            let error = failed.error.as_ref().unwrap();
            assert_eq!(
                (&*error.code, &*error.message, error.retryable),
                ("no_recipient", "no_recipient", false)
            );
            assert_eq!(
                failed.provider_result.as_ref(),
                Some(&missing_recipient_result())
            );
        }
        if matches!(failure, Failure::NoRecipient) {
            assert_eq!(deliveries[6].status, DeliveryResultStatus::Sent);
        } else {
            assert!(
                deliveries[5..]
                    .iter()
                    .all(|delivery| delivery.error.as_ref().unwrap().message
                        == "circuit_breaker_open")
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_missing_recipient_releases_the_probe_for_the_next_send() {
    let app = Flaky::new(6, Failure::MissingOnProbe);
    let (results, events) = send_results(
        &app,
        7,
        |n| {
            if n == 5 {
                Duration::from_secs(60)
            } else {
                Duration::ZERO
            }
        },
        AttachChannelOptions::default(),
    )
    .await;
    assert_eq!(
        results[5].provider_result.as_ref(),
        Some(&missing_recipient_result())
    );
    assert_eq!(results[6].status, DeliveryResultStatus::Sent);
    assert_eq!(app.sends(), 7);
    let transitions: Vec<_> = events.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(
        transitions,
        ["circuit_breaker_opened", "circuit_breaker_closed"]
    );
}
