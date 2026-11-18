//! Image parts embedded in text messages, read through the provider API.

use std::error::Error;

use roomkit_ai::{AiImagePart, image_part_base64, image_part_payload, image_part_uri};

#[test]
fn all_three_readers_agree_on_data_and_remote_uris() {
    let image = AiImagePart {
        url: "data:;base64,QUJD\nMTI".into(),
        mime_type: Some("image/png".into()),
    };
    assert_eq!(
        image_part_payload(&image, "demo").unwrap(),
        ("image/png".into(), b"ABC12".to_vec())
    );
    assert_eq!(
        image_part_base64(&image, "demo").unwrap(),
        ("image/png".into(), "QUJDMTI=".into())
    );
    assert_eq!(
        image_part_uri(&image, "demo").unwrap(),
        "data:image/png;base64,QUJDMTI="
    );
    let remote = AiImagePart {
        url: "https://example.test/image.png".into(),
        mime_type: None,
    };
    assert_eq!(image_part_uri(&remote, "demo").unwrap(), remote.url);
    assert_eq!(
        image_part_payload(&remote, "demo").unwrap_err().message,
        "invalid image part: expected a data: URI, got a https URL"
    );
    assert!(image_part_base64(&remote, "demo").is_err());
}

#[test]
fn all_readers_report_the_provider_and_the_decoding_cause() {
    let part = AiImagePart {
        url: "data:image/png;base64,not*base64".into(),
        mime_type: None,
    };
    for error in [
        image_part_uri(&part, "sample").unwrap_err(),
        image_part_base64(&part, "sample").unwrap_err(),
        image_part_payload(&part, "sample").unwrap_err(),
    ] {
        assert_eq!(error.provider, "sample");
        assert!(!error.retryable);
        assert!(!error.before_request);
        assert_eq!(error.context_overflow, None);
        assert_eq!(
            error.message,
            format!("invalid image part: {}", error.source().unwrap())
        );
    }
}
