use std::sync::{Arc, Mutex};

use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::json;

use super::*;
use crate::ProviderError;
use crate::image::{
    ImageGenerationError, ImageProgressCallback, notify_image_progress, to_data_uri,
};

/// A one-pixel PNG, Python's `PNG` of `test_image.py`.
fn png() -> Vec<u8> {
    let written = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==";
    STANDARD.decode(written).expect("the PNG is base64")
}

fn result() -> ImageResult {
    ImageResult::new(to_data_uri(&png(), "image/png"), "image/png").expect("a data URI")
}

// Python: tests/test_providers/test_image.py::test_image_result_decodes_to_the_original_bytes
#[test]
fn image_result_decodes_to_the_original_bytes() {
    assert_eq!(result().decoded(), Ok(png()));
}

// Python: tests/test_providers/test_image.py::test_image_result_refuses_anything_but_a_data_uri
#[test]
fn image_result_refuses_anything_but_a_data_uri() {
    for (data, refused) in [
        ("iVBORw0KGgo=", ImageResultError::NotADataUri),
        ("https://example.com/cat.png", ImageResultError::NotADataUri),
        ("data:image/png;base64,", ImageResultError::NoPayload),
        ("data:image/png,notbase64", ImageResultError::NotBase64),
    ] {
        assert_eq!(ImageResult::new(data, "image/png"), Err(refused), "{data}");
        let written = json!({ "data": data, "mime_type": "image/png" });
        assert!(
            serde_json::from_value::<ImageResult>(written).is_err(),
            "{data}"
        );
    }
}

// Python: tests/test_providers/test_image.py::test_image_result_decoded_reports_a_corrupt_payload
#[test]
fn image_result_decoded_reports_a_corrupt_payload() {
    let corrupt = ImageResult::new("data:image/png;base64,!!!not-base64!!!", "image/png");
    let refused = corrupt.expect("a data URI").decoded().unwrap_err();
    assert!(refused.to_string().contains("not valid base64"));
}

// Python: tests/test_providers/test_image.py::test_image_result_becomes_a_message_part
#[test]
fn image_result_becomes_a_message_part() {
    let result = result();
    let part = result.to_image_part();
    assert_eq!(part.url, result.data);
    assert_eq!(part.mime_type.as_deref(), Some("image/png"));
}

// Python: tests/test_providers/test_image.py::test_image_result_round_trips_through_a_message_part
#[test]
fn image_result_round_trips_through_a_message_part() {
    let back = ImageResult::new(result().to_image_part().url, "image/png");
    assert_eq!(back.expect("a data URI").decoded(), Ok(png()));
}

// Python: ImageAttempt(...).model_dump_json(), the attempt and its result
// as Python writes them
#[test]
fn an_attempt_reads_and_writes_as_python_writes_it() {
    let written = json!({
        "id": "a1", "index": 1, "status": "succeeded", "provider_request_id": "req_1",
        "usage": { "output_image_tokens": 1024 }, "raw_usage": {}, "effective_options": {},
        "metadata": {},
        "results": [{
            "data": "data:image/png;base64,iVBORw0KGgo=", "mime_type": "image/png",
            "revised_prompt": "a fox", "usage": {}, "attempt_id": null,
            "provider_request_id": null, "raw_usage": {}, "effective_options": {},
            "metadata": {}, "width": 1, "height": null,
        }],
        "error": null, "status_code": null,
    });
    let attempt: ImageAttempt = serde_json::from_value(written.clone()).expect("an attempt");
    assert_eq!(attempt.status, ImageAttemptStatus::Succeeded);
    assert_eq!(attempt.results[0].revised_prompt.as_deref(), Some("a fox"));
    assert_eq!(serde_json::to_value(&attempt).expect("written"), written);
}

// Python: ImageAttempt(id=<factory>): str(uuid4()), one per attempt
#[test]
fn each_attempt_is_given_its_own_id() {
    let (first, second) = (ImageAttempt::default(), ImageAttempt::default());
    assert_ne!(first.id, second.id);
    assert!(uuid::Uuid::parse_str(&first.id).is_ok(), "{}", first.id);
}

fn attempt(index: u32, status_code: Option<u16>, results: Vec<ImageResult>) -> ImageAttempt {
    ImageAttempt {
        index,
        status: ImageAttemptStatus::Failed,
        status_code,
        results,
        ..ImageAttempt::default()
    }
}

// Python: ImageGenerationError(message, provider=..., attempts=...): its
// results every attempt's, its status the first truthy one, never retryable
#[test]
fn a_generation_error_keeps_every_outcome_and_is_final() {
    let attempts = vec![
        attempt(0, None, vec![result()]),
        attempt(1, Some(0), vec![]),
        attempt(2, Some(500), vec![result(), result()]),
        attempt(3, Some(429), vec![]),
    ];
    let failed = ImageGenerationError::new("the vendor failed", "MockImageProvider", attempts);
    assert_eq!(failed.results.len(), 3);
    let error = ProviderError::from(failed.clone());
    assert!(!error.retryable);
    assert_eq!(error.status_code, Some(500));
    assert_eq!(error.provider, "MockImageProvider");
    let source = std::error::Error::source(&error).and_then(|source| source.downcast_ref());
    assert_eq!(source, Some(&failed));
}

fn recording(seen: Arc<Mutex<Vec<ImageAttempt>>>, fails: bool) -> ImageProgressCallback {
    Arc::new(move |attempt| {
        let seen = Arc::clone(&seen);
        Box::pin(async move {
            seen.lock().expect("not poisoned").push(attempt);
            if fails {
                return Err("storage unavailable".into());
            }
            Ok(())
        })
    })
}

// Python: tests/test_providers/test_image_advanced.py::test_callback_failure_keeps_the_vendor_result
#[tokio::test]
async fn callback_failure_keeps_the_vendor_result() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let callback = recording(Arc::clone(&seen), true);
    let succeeded = ImageAttempt {
        status: ImageAttemptStatus::Succeeded,
        results: vec![result()],
        ..ImageAttempt::default()
    };
    let failed = notify_image_progress(Some(&callback), &succeeded, "GeminiImageProvider")
        .await
        .unwrap_err();
    assert_eq!(
        failed.message,
        "Image progress callback failed: storage unavailable"
    );
    assert_eq!(failed.provider, "GeminiImageProvider");
    assert_eq!(failed.attempts, vec![succeeded.clone()]);
    assert_eq!(failed.results.len(), 1);
    let cause = std::error::Error::source(&failed).map(ToString::to_string);
    assert_eq!(cause.as_deref(), Some("storage unavailable"));
    assert!(!ProviderError::from(failed).retryable);
    assert_eq!(*seen.lock().expect("not poisoned"), vec![succeeded]);
}

// Python: notify_image_progress(None, ...) returns; a callback that
// succeeds is told of the attempt
#[tokio::test]
async fn progress_reaches_the_callback_when_there_is_one() {
    let attempt = ImageAttempt::default();
    assert_eq!(
        notify_image_progress(None, &attempt, "MockImageProvider").await,
        Ok(())
    );
    let seen = Arc::new(Mutex::new(Vec::new()));
    let callback = recording(Arc::clone(&seen), false);
    let told = notify_image_progress(Some(&callback), &attempt, "MockImageProvider").await;
    assert_eq!(told, Ok(()));
    assert_eq!(*seen.lock().expect("not poisoned"), vec![attempt]);
}

// Python: str(ImageGenerationError("boom", provider="gemini",
// attempts=[ImageAttempt(status_code=500)])) == "gemini (500): boom", and
// a message that names its provider reads as given
#[test]
fn a_generation_error_reads_as_the_provider_error_it_is() {
    let failed = ImageGenerationError::new("boom", "gemini", vec![attempt(0, Some(500), vec![])]);
    assert_eq!(failed.to_string(), "gemini (500): boom");
    let named = ImageGenerationError::new("gemini failed", "gemini", vec![]);
    assert_eq!(named.to_string(), "gemini failed");
}

#[derive(Debug)]
struct Unreachable;

impl fmt::Display for Unreachable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the vendor is unreachable")
    }
}

impl std::error::Error for Unreachable {}

// Python: raise ImageGenerationError(...) from exc keeps exc as the cause,
// past the ProviderError it is
#[test]
fn a_generation_error_keeps_what_it_was_raised_from() {
    use std::error::Error;
    let failed = ImageGenerationError::new("boom", "openai", vec![]).with_source(Unreachable);
    let error = ProviderError::from(failed);
    let generation = error.source().expect("the generation's failure");
    let cause = generation.source().expect("what it was raised from");
    assert_eq!(cause.to_string(), "the vendor is unreachable");
}
