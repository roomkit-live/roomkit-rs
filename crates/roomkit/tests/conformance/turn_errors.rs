//! Synthetic application exceptions and observations of their native and wire identities.

use roomkit::turns::ErrorClasses;
use roomkit::turns::wire::ErrorRecord;
use roomkit::{ChannelError, RoomKitError};
use roomkit_ai::{ProviderError, ProviderErrorOptions};
use serde_json::{Value, json};

const DETAIL: &str = "fixture:DetailError";
const HOST: &str = "fixture:HostProviderError";
const PROVIDER: &str = "roomkit.providers.ai.base:ProviderError";

#[derive(Debug)]
struct LocalDetail(&'static str);

#[derive(Debug, thiserror::Error)]
#[error("{}", .0.as_ref().map_or("detail unavailable", |detail| detail.0))]
struct DetailError(Option<LocalDetail>);

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct HostProviderError(ProviderError);

fn provider_record(error: &ProviderError) -> ErrorRecord {
    let mut record = ErrorRecord::new(PROVIDER, error.to_string());
    record.args = vec![json!(error.message)];
    record
        .attributes
        .insert("provider".into(), json!(error.provider));
    record
        .attributes
        .insert("status_code".into(), json!(error.status_code));
    record
}

fn decode_provider(record: &ErrorRecord) -> Option<ProviderError> {
    Some(ProviderError::new(
        record.args.first()?.as_str()?,
        ProviderErrorOptions {
            provider: record.attributes.get("provider")?.as_str()?.to_owned(),
            status_code: serde_json::from_value(record.attributes.get("status_code")?.clone())
                .ok()?,
            ..Default::default()
        },
    ))
}

#[derive(Clone, serde::Deserialize)]
pub(crate) enum FixtureClass {
    DetailError,
    HostProviderError,
}

pub(crate) fn classes(names: &[FixtureClass]) -> ErrorClasses {
    let mut classes = ErrorClasses::default();
    classes.register(PROVIDER, decode_provider, provider_record);
    for kind in names {
        match kind {
            // The process-local object is never encoded or reconstructible.
            FixtureClass::DetailError => classes.register(
                DETAIL,
                |_| Some(DetailError(None)),
                |e: &DetailError| ErrorRecord::new(DETAIL, e.to_string()),
            ),
            FixtureClass::HostProviderError => classes.register(
                HOST,
                |row| decode_provider(row).map(HostProviderError),
                |error: &HostProviderError| {
                    let mut row = provider_record(&error.0);
                    row.bases = vec![PROVIDER.into()];
                    row
                },
            ),
        }
    }
    classes
}

pub(super) fn failure(kind: &str) -> ChannelError {
    match kind {
        "DetailError" => Box::new(DetailError(Some(LocalDetail("detail from worker")))),
        "HostProviderError" => Box::new(HostProviderError(ProviderError::new(
            "provider unavailable",
            ProviderErrorOptions {
                provider: "fixture".into(),
                status_code: Some(429),
                ..Default::default()
            },
        ))),
        _ => Box::new(RoomKitError::Value(format!("unknown fixture class {kind}"))),
    }
}

fn short(kind: &str) -> &str {
    kind.rsplit(':').next().unwrap_or(kind)
}

pub(crate) fn inspect(error: &RoomKitError) -> Value {
    let RoomKitError::RemoteTurn {
        detail: Some(detail),
        ..
    } = error
    else {
        return json!({"message":error.to_string()});
    };
    let record = detail.record();
    let native = detail.native_error();
    let reads_as = native.and_then(|error| {
        if error.is::<DetailError>() {
            Some("DetailError")
        } else if error.is::<HostProviderError>() {
            Some("HostProviderError")
        } else if error.is::<ProviderError>() {
            Some("ProviderError")
        } else {
            None
        }
    });
    // A native instance represents the whole failure only when it reproduces
    // both the class and its displayed message. Otherwise the remote envelope
    // supplies the missing part while the native value identifies the class.
    let generic = reads_as != Some(short(&record.kind))
        || native.is_none_or(|native| native.to_string() != error.to_string());
    json!({"kind":short(&record.kind),"generic":generic,"reads_as":reads_as,
        "message":error.to_string(),"bases":record.bases.iter().map(|s| short(s)).collect::<Vec<_>>(),
        "attributes":record.attributes})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_declared_class_is_observed_only_when_the_native_downcast_succeeds() {
        let registry = classes(&[FixtureClass::DetailError]);
        let original = DetailError(Some(LocalDetail("the worker's detail")));
        let record = registry.record(&original);
        let declared = inspect(&registry.rebuild(&record));
        let undeclared = inspect(&ErrorClasses::default().rebuild(&record));
        assert_eq!(declared["message"], "the worker's detail");
        assert_eq!(declared["generic"], true);
        assert_eq!(declared["reads_as"], "DetailError");
        assert_eq!(undeclared["kind"], declared["kind"]);
        assert_eq!(undeclared["message"], declared["message"]);
        assert_eq!(undeclared["reads_as"], Value::Null);
    }
}
