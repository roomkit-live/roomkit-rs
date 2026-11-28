//! Python's refusals of image requests, replayed: each case of
//! `image/request_cases.json` (scripts/build-image-request-cases.py) is a
//! request to a model of Python's catalogs, and what Python's
//! `ImageCapabilities.validate_request` answers it. Rust answers each the
//! same: accepted, refused with Python's message, or refused for options
//! Python refuses when they are built (here, when they are read or
//! checked).

use roomkit_ai::image::{ImageCapabilities, ImageOptions};
use serde::Deserialize;
use serde_json::Value;

/// What Rust's check says of options Python refuses when they are built.
const OUT_OF_BOUNDS: [&str; 3] = [
    "output_compression must be between 0 and 100",
    "partial_images must be between 0 and 3",
    "previous_interaction_id must not be empty",
];

#[derive(Deserialize)]
struct Cases {
    profiles: Vec<Profile>,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Profile {
    model: String,
    image: ImageCapabilities,
}

#[derive(Deserialize)]
struct Case {
    profile: usize,
    options: Value,
    size: Option<String>,
    n: u32,
    references: usize,
    mask: bool,
    refused: Option<String>,
}

/// What a case answers: `None` accepted, `Some("built")` refused for its
/// options, else Python's message.
fn answer(profile: &Profile, case: &Case) -> Option<String> {
    let Ok(options) = serde_json::from_value::<ImageOptions>(case.options.clone()) else {
        return Some("built".to_owned());
    };
    let size = case.size.as_deref();
    let checked =
        profile
            .image
            .validate_request(&options, size, case.n, case.references, case.mask);
    checked.err().map(|refused| refused.to_string())
}

#[test]
fn every_image_request_is_refused_or_accepted_as_python_does() {
    let cases: Cases = serde_json::from_str(include_str!("image/request_cases.json"))
        .expect("the cases are Python's JSON");
    assert!(cases.cases.len() > 1000, "{}", cases.cases.len());
    let mut differs = Vec::new();
    for (at, case) in cases.cases.iter().enumerate() {
        let profile = &cases.profiles[case.profile];
        let answered = answer(profile, case);
        let same = match (&case.refused, &answered) {
            // Python refuses such options when they are built; Rust when
            // it reads them, or first when it checks them, in its own words.
            (Some(built), Some(answered)) if built == "built" => {
                answered == "built" || OUT_OF_BOUNDS.contains(&answered.as_str())
            }
            (expected, answered) => expected == answered,
        };
        if !same {
            differs.push(format!(
                "case {at} on {}: Python {:?}, Rust {answered:?}",
                profile.model, case.refused
            ));
        }
    }
    assert!(
        differs.is_empty(),
        "{} differ:\n{}",
        differs.len(),
        differs.join("\n")
    );
}
