//! Identity verification's lifetimes and timers (RFC §11.7.2, §11.7.5):
//! what the shared scenarios (roomkit-specs `conformance/scenarios/
//! verification/`) cannot say, a lifetime's length and a timer's silence,
//! each test translating the Python implementation's test of its name.
//! A verification ends at its expiry by a timer, real time here as in
//! Python's tests; the lifetimes are longer than theirs, a secret's hash
//! taking its production cost here (Python's tests lower it).

use std::future::ready;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelId, IdentityId, RoomEvent, RoomId, VerificationPolicy, VerificationRequest,
    VerificationStatus,
};
use roomkit::store::GetVerificationOptions;
use roomkit::{
    CompleteVerificationOptions, ConversationStore, CreateRoomOptions, HookExecution, HookOptions,
    HookTrigger, InMemoryStore, RequestVerificationOptions, RevokeVerificationOptions, RoomKit,
    RoomKitOptions, VerificationOfOptions,
};
use serde_json::Value;

/// How long a test waits for what a timer does before it fails.
const BOUND: Duration = Duration::from_secs(5);

fn alice() -> IdentityId {
    IdentityId::new("id-alice").unwrap()
}

fn sms() -> ChannelId {
    ChannelId::new("sms").unwrap()
}

fn room() -> RoomId {
    RoomId::new("r1").unwrap()
}

/// The hooks a kit fired on `ON_VERIFICATION_COMPLETED` and
/// `ON_VERIFICATION_ENDED`: each trigger with its payload's data.
type Fired = Arc<Mutex<Vec<(HookTrigger, serde_json::Map<String, Value>)>>>;

/// A kit over `store` with `policy`, its room `r1` created, recording the
/// verification hooks it fires (Python's `_kit`).
async fn kit_on(store: Arc<InMemoryStore>, policy: VerificationPolicy) -> (RoomKit, Fired) {
    let kit = RoomKit::new(RoomKitOptions {
        store,
        verification_policy: Some(policy),
        ..RoomKitOptions::default()
    });
    if kit.store().get_room(&room()).await.unwrap().is_none() {
        let create = CreateRoomOptions {
            room_id: Some(room()),
            ..CreateRoomOptions::default()
        };
        kit.create_room(create).await.unwrap();
    }
    let fired: Fired = Arc::default();
    for trigger in [
        HookTrigger::OnVerificationCompleted,
        HookTrigger::OnVerificationEnded,
    ] {
        let heard = Arc::clone(&fired);
        let options = HookOptions {
            execution: HookExecution::Async,
            ..HookOptions::default()
        };
        kit.hook(
            trigger,
            move |event: RoomEvent, _| {
                let data = serde_json::to_value(&event.content).unwrap()["data"].clone();
                let data = data.as_object().cloned().unwrap_or_default();
                heard.lock().unwrap().push((trigger, data));
                ready(())
            },
            options,
        )
        .unwrap();
    }
    (kit, fired)
}

async fn kit(policy: VerificationPolicy) -> (RoomKit, Fired) {
    kit_on(Arc::new(InMemoryStore::new()), policy).await
}

fn policy() -> VerificationPolicy {
    VerificationPolicy::default()
}

async fn request(
    kit: &RoomKit,
    options: RequestVerificationOptions,
) -> (VerificationRequest, String) {
    kit.request_verification(&alice(), &room(), &sms(), options)
        .await
        .unwrap()
}

/// Alice verified through a link request (Python's `_verified`).
async fn verified(kit: &RoomKit) -> roomkit::model::Verification {
    let (request, secret) = request(kit, RequestVerificationOptions::default()).await;
    kit.complete_verification(&request.id, &secret, CompleteVerificationOptions::default())
        .await
        .unwrap()
}

/// The ends the hooks heard: each one's reason and what ended.
fn ends(fired: &Fired) -> Vec<(String, String)> {
    let read = |data: &serde_json::Map<String, Value>, key: &str| {
        data.get(key)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    fired
        .lock()
        .unwrap()
        .iter()
        .filter(|(trigger, _)| *trigger == HookTrigger::OnVerificationEnded)
        .map(|(_, data)| (read(data, "reason"), read(data, "ended")))
        .collect()
}

/// Waits until `done` holds, polling with a deadline: a timer's end comes
/// in its own time.
async fn eventually(done: impl Fn() -> bool) {
    tokio::time::timeout(BOUND, async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the condition was not met in time");
}

// Python: tests/test_identity_verification.py::test_a_completed_request_verifies_the_identity
#[tokio::test]
async fn a_completed_request_verifies_the_identity() {
    let (kit, _) = kit(policy()).await;

    let verification = verified(&kit).await;

    let lifetime = verification
        .expires_at
        .unwrap()
        .duration_since(verification.verified_at.unwrap());
    assert_eq!(
        (verification.status, lifetime),
        (VerificationStatus::Verified, Some(Duration::from_mins(15)))
    );
    kit.close().await.unwrap();
}

// Python: tests/test_identity_verification.py::test_the_secret_is_returned_once_and_never_stored
#[tokio::test]
async fn the_secret_is_returned_once_and_never_stored() {
    let (kit, _) = kit(policy()).await;

    let (request, secret) = request(&kit, RequestVerificationOptions::default()).await;
    let stored = kit
        .store()
        .get_verification_request(&request.id)
        .await
        .unwrap()
        .unwrap();

    assert!(
        secret.len() >= 32,
        "a link secret carries at least 128 bits"
    );
    assert!(!serde_json::to_string(&stored).unwrap().contains(&secret));
    assert!(stored.secret_hash.starts_with("scrypt$"));
    kit.close().await.unwrap();
}

// Python: tests/test_identity_verification.py::test_a_code_request_sends_digits_and_lives_shorter
#[tokio::test]
async fn a_code_request_sends_digits_and_lives_shorter() {
    let (kit, _) = kit(policy()).await;
    let options = RequestVerificationOptions {
        method: "code".to_owned(),
        ..RequestVerificationOptions::default()
    };

    let (request, code) = request(&kit, options).await;

    assert!(
        code.len() == 6 && code.bytes().all(|byte| byte.is_ascii_digit()),
        "{code}"
    );
    assert_eq!(
        request.expires_at.duration_since(request.created_at),
        Some(Duration::from_mins(5))
    );
    let completed = kit
        .complete_verification(&request.id, &code, CompleteVerificationOptions::default())
        .await
        .unwrap();
    assert_eq!(completed.method.as_deref(), Some("code"));
    kit.close().await.unwrap();
}

// Python: tests/test_verification_lifecycle.py::test_a_revocation_fires_the_end_once_and_stops_the_timer
#[tokio::test]
async fn a_revocation_fires_the_end_once_and_stops_the_timer() {
    let (kit, fired) = kit(VerificationPolicy {
        ttl: Duration::from_millis(300),
        ..policy()
    })
    .await;
    verified(&kit).await;

    let revoke = RevokeVerificationOptions {
        reason: "advisor".to_owned(),
        ..RevokeVerificationOptions::default()
    };
    kit.revoke_verification(&alice(), revoke).await.unwrap();
    // A timer left armed would end it again by then.
    tokio::time::sleep(Duration::from_millis(350)).await;
    roomkit::testing::idle(&kit).await;

    assert_eq!(
        ends(&fired),
        [("advisor".to_owned(), "verification".to_owned())]
    );
    kit.close().await.unwrap();
}

// Python: tests/test_verification_lifecycle.py::test_a_verification_that_outlived_an_unanswered_request_still_ends
#[tokio::test]
async fn a_verification_that_outlived_an_unanswered_request_still_ends() {
    let (kit, fired) = kit(VerificationPolicy {
        ttl: Duration::from_millis(800),
        request_ttl: Duration::from_millis(250),
        ..policy()
    })
    .await;
    verified(&kit).await;
    let (again, secret) = request(&kit, RequestVerificationOptions::default()).await;
    eventually(|| ends(&fired).len() == 1).await;
    let late = kit
        .complete_verification(&again.id, &secret, CompleteVerificationOptions::default())
        .await;

    eventually(|| ends(&fired).len() == 2).await;

    assert!(late.is_err(), "too late: the request ended");
    assert_eq!(
        ends(&fired),
        [
            ("expired".to_owned(), "request".to_owned()),
            ("expired".to_owned(), "verification".to_owned()),
        ]
    );
    kit.close().await.unwrap();
}

// Python: tests/test_verification_lifecycle.py::test_a_starting_kit_rearms_the_timers_of_verifications_in_force
#[tokio::test]
async fn a_starting_kit_rearms_the_timers_of_verifications_in_force() {
    let store = Arc::new(InMemoryStore::new());
    let short = || VerificationPolicy {
        ttl: Duration::from_millis(500),
        ..policy()
    };
    let (first, _) = kit_on(Arc::clone(&store), short()).await;
    verified(&first).await;
    first.close().await.unwrap(); // its timer dies with it

    let (second, fired) = kit_on(store, short()).await;
    second.start().await.unwrap();
    eventually(|| !ends(&fired).is_empty()).await;

    assert_eq!(
        ends(&fired),
        [("expired".to_owned(), "verification".to_owned())]
    );
    second.close().await.unwrap();
}

// Python: tests/test_verification_lifecycle.py::test_a_closed_kit_ends_nothing_afterwards
#[tokio::test]
async fn a_closed_kit_ends_nothing_afterwards() {
    let (kit, fired) = kit(VerificationPolicy {
        ttl: Duration::from_millis(300),
        ..policy()
    })
    .await;
    verified(&kit).await;
    eventually(|| !fired.lock().unwrap().is_empty()).await; // the completion

    kit.close().await.unwrap();
    // Its timer, had it lived on, would have fired by then.
    tokio::time::sleep(Duration::from_millis(350)).await;

    let triggers: Vec<HookTrigger> = fired.lock().unwrap().iter().map(|(t, _)| *t).collect();
    let stored = kit
        .store()
        .get_verification(&alice(), GetVerificationOptions::default())
        .await
        .unwrap()
        .map(|verification| verification.status);
    // A timer that outlived the close would have written its end, which no
    // observer hears once the kit is shut down.
    assert_eq!(
        (triggers, stored),
        (
            vec![HookTrigger::OnVerificationCompleted],
            Some(VerificationStatus::Verified)
        )
    );
}

// Python: tests/test_verification_lifecycle.py::test_a_completed_request_ends_nothing_at_its_expiry
#[tokio::test]
async fn a_completed_request_ends_nothing_at_its_expiry() {
    let (kit, fired) = kit(VerificationPolicy {
        ttl: Duration::from_secs(5),
        request_ttl: Duration::from_millis(250),
        ..policy()
    })
    .await;
    verified(&kit).await;

    // Its timer, had it not been stopped, would have fired by then.
    tokio::time::sleep(Duration::from_millis(300)).await;
    roomkit::testing::idle(&kit).await;

    assert_eq!(ends(&fired), Vec::<(String, String)>::new());
    let state = kit
        .verification_of(&alice(), VerificationOfOptions::default())
        .await
        .unwrap();
    assert_eq!(state.status, VerificationStatus::Verified);
    kit.close().await.unwrap();
}
