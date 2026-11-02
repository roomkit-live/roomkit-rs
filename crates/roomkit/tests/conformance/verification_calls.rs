//! Identity verification's calls (roomkit-specs `conformance/README.md`,
//! "Calls"), made on the kit: `request_verification`, whose `as` label
//! names the request's id and `<label>.secret` its secret,
//! `complete_verification`, `record_failed_attempt`, `revoke_verification`,
//! `verification_of`, `check_verifications`, and `sender_verification`, an
//! action of a scripted hook on the event and the context the hook was
//! handed; and the kit's `verification_policy`, its lifetimes in
//! milliseconds.

use std::sync::Arc;
use std::time::Duration;

use roomkit::model::{ChannelId, IdentityId, RoomId, Timestamp, VerificationPolicy};
use roomkit::{
    CompleteVerificationOptions, RecordFailedAttemptOptions, RequestVerificationOptions,
    RevokeVerificationOptions, RoomKitOptions, VerificationOfOptions,
};
use serde_json::{Value, json};

use crate::calls::{Called, Unreadable, dump, field, finish, optional};
use crate::run::Run;
use crate::scripted::HOOKED;

/// The framework events of identity verification (RFC §8.2, §11.7),
/// recorded beside the §26.1 table's.
pub(crate) const VERIFICATION_EVENTS: [&str; 3] = [
    "verification_requested",
    "verification_completed",
    "verification_ended",
];

/// Each call's arguments, beside `call` and `organization_id`.
pub(crate) fn arguments(call: &str) -> Option<&'static [&'static str]> {
    Some(match call {
        "request_verification" => &[
            "identity_id",
            "room_id",
            "channel_id",
            "method",
            "window_start",
        ],
        "complete_verification" => &["request_id", "secret", "method"],
        "record_failed_attempt" => &["request_id"],
        "revoke_verification" => &["identity_id", "reason"],
        "verification_of" => &["identity_id"],
        "check_verifications" | "sender_verification" => &[],
        _ => return None,
    })
}

pub(crate) fn handles(call: &str) -> bool {
    arguments(call).is_some()
}

/// The kit's verification policy a scenario's `setup.kit` names, over
/// `options`.
pub(crate) fn kit_options<S>(mut options: RoomKitOptions<S>, kit: &Value) -> RoomKitOptions<S> {
    let Some(spec) = kit.get("verification_policy") else {
        return options;
    };
    let mut policy = VerificationPolicy::default();
    let millis = |name: &str| spec[name].as_u64().map(Duration::from_millis);
    let count = |name: &str| spec[name].as_u64().and_then(|n| u32::try_from(n).ok());
    if let Some(ttl) = millis("ttl_ms") {
        policy.ttl = ttl;
    }
    if let Some(ttl) = millis("request_ttl_ms") {
        policy.request_ttl = ttl;
    }
    if let Some(ttl) = millis("code_request_ttl_ms") {
        policy.code_request_ttl = ttl;
    }
    if let Some(age) = millis("release_max_age_ms") {
        policy.release_max_age = age;
    }
    if let Some(attempts) = count("max_attempts") {
        policy.max_attempts = attempts;
    }
    if let Some(length) = count("code_length") {
        policy.code_length = length;
    }
    if let Some(messages) = count("release_max_messages") {
        policy.release_max_messages = messages;
    }
    if let Some(release) = spec["release_on_verify"].as_bool() {
        policy.release_on_verify = release;
    }
    if let Some(hold) = spec["hold_unverified"].as_bool() {
        policy.hold_unverified = hold;
    }
    options.verification_policy = Some(policy);
    options
}

fn organization_id(spec: &Value) -> Option<String> {
    spec.get("organization_id")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

/// Makes the call `spec` names, its references already resolved; a
/// labelled `request_verification` labels its secret too.
pub(crate) async fn call(
    run: &Arc<Run>,
    name: &str,
    spec: &Value,
) -> Result<(Called, Option<String>), Unreadable> {
    let kit = run.kit();
    let organization_id = organization_id(spec);
    let called = match name {
        "request_verification" => {
            let identity_id: IdentityId = field(spec, "identity_id")?;
            let room_id: RoomId = field(spec, "room_id")?;
            let channel_id: ChannelId = field(spec, "channel_id")?;
            let mut options = RequestVerificationOptions {
                window_start: optional::<Timestamp>(spec, "window_start")?,
                organization_id,
                ..RequestVerificationOptions::default()
            };
            if let Some(method) = optional(spec, "method")? {
                options.method = method;
            }
            let outcome = kit
                .request_verification(&identity_id, &room_id, &channel_id, options)
                .await;
            if let (Ok((request, secret)), Some(label)) =
                (&outcome, spec.get("as").and_then(Value::as_str))
            {
                run.label(format!("{label}.secret"), secret.clone());
                let called = Ok(json!({ "request": dump(request), "secret": secret }));
                return Ok((called, Some(request.id.clone())));
            }
            finish(
                run,
                name,
                outcome,
                |(request, secret)| json!({ "request": dump(request), "secret": secret }),
            )
        }
        "complete_verification" => {
            let request_id: String = field(spec, "request_id")?;
            let secret: String = field(spec, "secret")?;
            let options = CompleteVerificationOptions {
                method: optional(spec, "method")?,
                organization_id,
            };
            let outcome = kit
                .complete_verification(&request_id, &secret, options)
                .await;
            finish(run, name, outcome, dump)
        }
        "record_failed_attempt" => {
            let request_id: String = field(spec, "request_id")?;
            let options = RecordFailedAttemptOptions { organization_id };
            let outcome = kit.record_failed_attempt(&request_id, options).await;
            finish(run, name, outcome, dump)
        }
        "revoke_verification" => {
            let identity_id: IdentityId = field(spec, "identity_id")?;
            let mut options = RevokeVerificationOptions {
                organization_id,
                ..RevokeVerificationOptions::default()
            };
            if let Some(reason) = optional(spec, "reason")? {
                options.reason = reason;
            }
            let outcome = kit.revoke_verification(&identity_id, options).await;
            finish(run, name, outcome, dump)
        }
        "verification_of" => {
            let identity_id: IdentityId = field(spec, "identity_id")?;
            let options = VerificationOfOptions { organization_id };
            let outcome = kit.verification_of(&identity_id, options).await;
            finish(run, name, outcome, dump)
        }
        "check_verifications" => {
            let outcome = kit.check_verifications().await;
            finish(run, name, outcome, dump)
        }
        "sender_verification" => {
            let Ok((event, context)) = HOOKED.try_with(Clone::clone) else {
                return Err("sender_verification outside a scripted hook's action".to_owned());
            };
            let outcome = kit.sender_verification(&event, &context).await;
            finish(run, name, outcome, |verification| {
                verification.as_ref().map_or(Value::Null, dump)
            })
        }
        other => return Err(format!("no call {other}")),
    };
    Ok((called, None))
}
