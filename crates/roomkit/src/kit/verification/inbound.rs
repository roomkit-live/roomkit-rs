//! What the inbound pipeline's step 5b asks the kit (RFC §10.1 step 5b,
//! §11.7.3, §11.7.4): the sender's verification as it stands, and the
//! attempt of a code reply, Python's `verification_of`, `_code_reply` and
//! `_attempt_code`. The driver carries both out for the core, which reads
//! and writes nothing itself.

use std::sync::Arc;

use roomkit_core::model::{ErrorRecord, IdentityId, RoomId, VerificationRequestStatus};
use roomkit_core::ops::{IdentityAnswer, Outcome};

use super::{CompleteVerificationOptions, METHOD_CODE, VerificationOfOptions};
use crate::error::RoomKitError;
use crate::kit::{Inner, RoomKit};
use crate::store::ConversationStore;
use crate::tasks::Kind;

/// The class of a failure step 5b's requests report: the kit's error, by
/// its message.
const VERIFICATION_ERROR: &str = "VerificationError";

/// The verification of `identity_id` in `organization_id` as it stands
/// now, the kit's `verification_of`; a store that fails it fails the
/// message.
pub(crate) async fn read<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    identity_id: &IdentityId,
    organization_id: Option<String>,
) -> Outcome {
    let kit = RoomKit {
        inner: Arc::clone(inner),
    };
    let options = VerificationOfOptions { organization_id };
    match kit.verification_of(identity_id, options).await {
        Ok(verification) => IdentityAnswer::Verification(Box::new(verification)).into(),
        Err(error) => Outcome::Failed(record(&error)),
    }
}

/// Attempts `code` against the request `request_id` when it is an open
/// code request of `room_id`, and answers whether it was (Python's
/// `_code_reply` and `_attempt_code`): a wrong code counts as an attempt
/// and is consumed all the same, the refusal kept from the message. A
/// store that fails it fails the message. A `deferred` call's attempt goes
/// on apart, as a call of the kit's whose failure is logged: the answer
/// comes once the reply is known for one (RFC §11.7.4).
pub(crate) async fn try_code<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    request_id: &str,
    room_id: &RoomId,
    code: &str,
    deferred: bool,
) -> Outcome {
    let kit = RoomKit {
        inner: Arc::clone(inner),
    };
    let request = match kit.store().get_verification_request(request_id).await {
        Ok(request) => request,
        Err(error) => return Outcome::Failed(error.to_record()),
    };
    let Some(request) = request.filter(|request| {
        request.method == METHOD_CODE
            && request.status == VerificationRequestStatus::Open
            && request.room_id == *room_id
    }) else {
        return IdentityAnswer::CodeTried(false).into();
    };
    let options = CompleteVerificationOptions {
        method: None,
        organization_id: request.organization_id.clone(),
    };
    if deferred {
        let code = code.to_owned();
        let attempt = async move {
            match kit.complete_verification(&request.id, &code, options).await {
                Ok(_) | Err(RoomKitError::VerificationRefused { .. }) => {}
                Err(error) => tracing::error!(
                    target: "roomkit::framework",
                    %error,
                    "A deferred code reply's attempt failed"
                ),
            }
        };
        // Within no work but its own: it outlives the call (Python's task in
        // a context of its own), and the kit's close gives it its grace.
        drop(inner.tasks.spawn_alone(Kind::Call, attempt));
        return IdentityAnswer::CodeTried(true).into();
    }
    match kit.complete_verification(&request.id, code, options).await {
        Ok(_) | Err(RoomKitError::VerificationRefused { .. }) => {
            IdentityAnswer::CodeTried(true).into()
        }
        Err(error) => Outcome::Failed(record(&error)),
    }
}

fn record(error: &RoomKitError) -> ErrorRecord {
    ErrorRecord::new(VERIFICATION_ERROR, error.to_string())
}
