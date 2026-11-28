//! The image provider contract (RFC §25.2) through the public API, as a
//! caller drives it: translated from Python's
//! `tests/test_providers/test_image.py`, and the base provider's defaults
//! Python's `ImageProvider` gives.

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "a test fails on a broken fixture"
)]

use std::future::Future;
use std::sync::Arc;

use base64::{Engine, engine::general_purpose::STANDARD};
use roomkit_ai::image::{
    GenerateOptions, GenerateWithOptionsOptions, ImageCapabilities, ImageCatalogEntry, ImageError,
    ImageModelInfo, ImageOptions, ImageProvider, ImageQuality, ImageResult, MockImageProvider,
    MockImageProviderOptions, to_data_uri,
};
use roomkit_ai::{AiImagePart, Date, ModelInfo};

/// A one-pixel PNG, Python's `PNG`.
fn png() -> Vec<u8> {
    let written = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==";
    STANDARD.decode(written).expect("the PNG is base64")
}

fn reference() -> AiImagePart {
    AiImagePart {
        url: to_data_uri(&png(), "image/png"),
        mime_type: Some("image/png".to_owned()),
    }
}

fn asked(n: u32) -> GenerateOptions {
    GenerateOptions {
        n,
        ..GenerateOptions::default()
    }
}

/// The refusal message of `refused`, which must be Python's `ValueError`.
fn value_error(refused: Result<Vec<ImageResult>, ImageError>) -> String {
    match refused {
        Err(ImageError::Value(refused)) => refused.to_string(),
        other => panic!("not a ValueError: {other:?}"),
    }
}

// Python: tests/test_providers/test_image.py::test_mock_returns_exactly_n_decodable_images
#[tokio::test]
async fn mock_returns_exactly_n_decodable_images() {
    let provider = MockImageProvider::default();
    let results = provider.generate("an origami fox", asked(3)).await.unwrap();
    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|result| result.decoded() == Ok(png())));
}

// Python: tests/test_providers/test_image.py::test_mock_records_its_calls
#[tokio::test]
async fn mock_records_its_calls() {
    let provider = MockImageProvider::default();
    let options = GenerateOptions {
        size: Some("1024x1024".to_owned()),
        reference_images: vec![reference()],
        ..GenerateOptions::default()
    };
    provider.generate("redraw it", options).await.unwrap();
    let call = (
        "redraw it".to_owned(),
        Some("1024x1024".to_owned()),
        1,
        vec![reference()],
    );
    assert_eq!(provider.calls(), vec![call]);
}

// Python: tests/test_providers/test_image.py::test_mock_cycles_through_its_images
#[tokio::test]
async fn mock_cycles_through_its_images() {
    let (first, second) = (png(), [png(), vec![0]].concat());
    let provider = MockImageProvider::new(MockImageProviderOptions {
        images: vec![first.clone(), second.clone()],
        ..MockImageProviderOptions::default()
    });
    let results = provider.generate("two", asked(3)).await.unwrap();
    let decoded: Vec<Vec<u8>> = results
        .iter()
        .map(|result| result.decoded().unwrap())
        .collect();
    assert_eq!(decoded, vec![first.clone(), second, first]);
}

// Python: tests/test_providers/test_image.py::test_mock_reports_disjoint_usage_counters
#[tokio::test]
async fn mock_reports_disjoint_usage_counters() {
    let provider = MockImageProvider::default();
    let options = GenerateOptions {
        reference_images: vec![reference()],
        ..GenerateOptions::default()
    };
    let results = provider.generate("a b c", options).await.unwrap();
    let usage = serde_json::to_value(&results[0].usage).unwrap();
    let expected = serde_json::json!({
        "input_tokens": 3, "input_image_tokens": 100, "output_tokens": 0, "output_image_tokens": 1024,
    });
    assert_eq!(usage, expected);
}

// Python: tests/test_providers/test_image.py::test_mock_refuses_editing_when_it_says_it_cannot
#[tokio::test]
async fn mock_refuses_editing_when_it_says_it_cannot() {
    let provider = MockImageProvider::new(MockImageProviderOptions {
        supports_editing: false,
        ..MockImageProviderOptions::default()
    });
    assert!(!provider.supports_editing());
    let options = GenerateOptions {
        reference_images: vec![reference()],
        ..GenerateOptions::default()
    };
    let refused = value_error(provider.generate("edit", options).await);
    assert!(refused.contains("does not support editing"), "{refused}");
    assert_eq!(provider.calls(), Vec::new());
}

// Python: tests/test_providers/test_image.py::test_mock_rejects_a_malformed_size
#[tokio::test]
async fn mock_rejects_a_malformed_size() {
    let provider = MockImageProvider::default();
    let options = GenerateOptions {
        size: Some("huge".to_owned()),
        ..GenerateOptions::default()
    };
    assert!(value_error(provider.generate("hi", options).await).contains("size"));
}

// Python: tests/test_providers/test_image.py::test_mock_rejects_n_below_one
#[tokio::test]
async fn mock_rejects_n_below_one() {
    let provider = MockImageProvider::default();
    assert!(value_error(provider.generate("hi", asked(0)).await).contains("at least 1"));
}

/// A provider no catalogue carries, as Python's `Bare`.
struct Bare;

impl ImageProvider for Bare {
    fn model_name(&self) -> &'static str {
        "nothing"
    }

    fn generate<'a>(
        &'a self,
        _prompt: &'a str,
        _options: GenerateOptions,
    ) -> impl Future<Output = Result<Vec<ImageResult>, ImageError>> + Send + 'a {
        std::future::ready(Ok(Vec::new()))
    }
}

// Python: tests/test_providers/test_image.py::test_base_catalog_is_empty_and_lookup_returns_none
#[test]
fn base_catalog_is_empty_and_lookup_returns_none() {
    assert_eq!(Bare::available_models(), Vec::new());
    assert!(Bare.catalog_entry().is_none());
    assert!(!Bare.supports_editing());
    assert_eq!(Bare.name(), "Bare");
}

// Python: ImageProvider.generate_with_options refuses controls, a mask or
// a callback it would drop, and is generate otherwise
#[tokio::test]
async fn the_base_refuses_what_it_would_drop() {
    let quality = ImageOptions {
        quality: Some(ImageQuality::High),
        ..ImageOptions::default()
    };
    let refused = [
        GenerateWithOptionsOptions {
            options: Some(quality),
            ..GenerateWithOptionsOptions::default()
        },
        GenerateWithOptionsOptions {
            mask: Some(reference()),
            ..GenerateWithOptionsOptions::default()
        },
    ];
    for options in refused {
        let said = value_error(Bare.generate_with_options("a fox", options).await);
        assert_eq!(said, "Bare does not support advanced image options");
    }
    let callback = GenerateWithOptionsOptions {
        on_progress: Some(Arc::new(|_| Box::pin(async { Ok(()) }))),
        ..GenerateWithOptionsOptions::default()
    };
    let said = value_error(Bare.generate_with_options("a fox", callback).await);
    assert_eq!(said, "Bare does not support image progress callbacks");
    let plain = GenerateWithOptionsOptions {
        options: Some(ImageOptions::default()),
        ..GenerateWithOptionsOptions::default()
    };
    assert_eq!(
        Bare.generate_with_options("a fox", plain).await.unwrap(),
        Vec::new()
    );
}

/// A provider whose catalogue holds an entry of each kind.
struct Cataloged(&'static str);

impl ImageProvider for Cataloged {
    fn model_name(&self) -> &'static str {
        self.0
    }

    fn available_models() -> Vec<ImageCatalogEntry> {
        let verified = Date::new(2026, 11, 28).unwrap();
        let mut drawn = ImageModelInfo::new("gpt-image-2", ImageCapabilities::new(verified));
        drawn.display_name = Some("GPT Image 2".to_owned());
        drawn.aliases = vec!["gpt-image-2-2026-04-21".to_owned()];
        let billed = ModelInfo::new("grok-imagine-image");
        vec![drawn.into(), billed.into()]
    }

    fn generate<'a>(
        &'a self,
        _prompt: &'a str,
        _options: GenerateOptions,
    ) -> impl Future<Output = Result<Vec<ImageResult>, ImageError>> + Send + 'a {
        std::future::ready(Ok(Vec::new()))
    }
}

// Python: tests/test_providers/test_image.py::test_catalog_entry_resolves_the_active_model,
// and image_model_entry: an id, an alias of a verified entry, never an
// alias Python's plain ModelInfo cannot have
#[test]
fn catalog_entry_resolves_the_active_model() {
    let entry = Cataloged("gpt-image-2")
        .catalog_entry()
        .expect("in the catalogue");
    let ImageCatalogEntry::Image(drawn) = &entry else {
        panic!("not an image entry: {entry:?}");
    };
    assert_eq!(drawn.display_name.as_deref(), Some("GPT Image 2"));
    let by_alias = Cataloged("gpt-image-2-2026-04-21").catalog_entry();
    assert_eq!(
        by_alias.as_ref().map(ImageCatalogEntry::id),
        Some("gpt-image-2")
    );
    let billed = Cataloged("grok-imagine-image")
        .catalog_entry()
        .expect("in the catalogue");
    assert!(billed.image().is_none(), "{billed:?}");
    assert!(Cataloged("dall-e-3").catalog_entry().is_none());
}

// Python: a catalogue entry is written as Python's model_dump writes it,
// an ImageModelInfo with its `image`, a ModelInfo without
#[test]
fn a_catalogue_entry_reads_back_as_the_kind_it_is() {
    for entry in Cataloged::available_models() {
        let written = serde_json::to_value(&entry).unwrap();
        assert_eq!(
            written.get("image").is_some(),
            entry.image().is_some(),
            "{written}"
        );
        let read: ImageCatalogEntry = serde_json::from_value(written).unwrap();
        assert_eq!(read, entry);
    }
}
