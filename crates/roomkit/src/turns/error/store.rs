//! Store causes cross as bounded records, never reconstructed driver objects.

use std::error::Error;
use std::fmt;

use roomkit_core::model::ErrorRecord;

use super::{CAUSES_KEPT, CHANNEL_ERROR, ClassedError, RoomKitError, WireError, from_record};

#[derive(Debug)]
struct Cause {
    record: ErrorRecord,
    next: Option<Box<Self>>,
}

impl fmt::Display for Cause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.record.message)
    }
}

impl Error for Cause {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.next.as_deref().map(|next| -> &dyn Error { next })
    }
}

fn record(error: &(dyn Error + 'static)) -> ErrorRecord {
    if let Some(cause) = error.downcast_ref::<Cause>() {
        return cause.record.clone();
    }
    if let Some(record) = crate::error::stored_record(error) {
        return record.clone();
    }
    let classed = error.downcast_ref::<ClassedError>();
    ErrorRecord {
        attributes: Box::new(
            classed
                .map(ClassedError::attributes)
                .cloned()
                .unwrap_or_default(),
        ),
        ..ErrorRecord::new(
            classed.map_or(CHANNEL_ERROR, ClassedError::class),
            error.to_string(),
        )
    }
}

pub(super) fn causes(error: &(dyn Error + 'static)) -> Vec<WireError> {
    let mut causes = Vec::new();
    let mut next = Some(error);
    while let Some(error) = next {
        if causes.len() == CAUSES_KEPT {
            break;
        }
        causes.push(from_record(&record(error)));
        next = error.source();
    }
    causes
}

pub(super) fn rebuild(error: &WireError) -> Option<RoomKitError> {
    let mut next = None;
    for cause in error.causes.iter().take(CAUSES_KEPT).rev() {
        next = Some(Box::new(Cause {
            record: super::to_record(cause),
            next,
        }));
    }
    // An old or incomplete record has no driver's cause to reconstruct.
    // Keep the codec's ordinary RemoteTurn fallback instead of inventing one.
    Some(RoomKitError::Store(crate::store::StoreFailure::new(*next?)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::turns::error::{kit_error, one, rebuild_kit_error};
    use serde_json::json;

    #[test]
    fn a_store_cause_roundtrips_its_class_message_and_attributes() {
        let cause = ClassedError::new("OSError", std::io::Error::other("disk unavailable"))
            .with_attributes(json!({"errno": 28}).as_object().unwrap().clone());
        let original = RoomKitError::Store(crate::store::StoreFailure::new(cause));
        let crossed = kit_error(&original);
        assert_eq!(crossed.causes.len(), 1);
        assert_eq!(crossed.causes[0].kind, "OSError");
        assert_eq!(crossed.causes[0].message, "disk unavailable");
        assert_eq!(crossed.causes[0].attributes["errno"], 28);
        let rebuilt = rebuild_kit_error(&crossed);
        assert!(matches!(rebuilt, RoomKitError::Store(_)));
        assert_eq!(rebuilt.to_string(), "the store failed");
        assert_eq!(rebuilt.source().unwrap().to_string(), "disk unavailable");
        assert_eq!(kit_error(&rebuilt), crossed);
    }

    #[test]
    fn a_store_record_without_a_cause_uses_the_existing_fallback() {
        let mut crossed = one("RoomKitError", "the store failed".into());
        crossed.attributes.insert("variant".into(), json!("Store"));
        assert!(
            matches!(rebuild_kit_error(&crossed), RoomKitError::RemoteTurn { message, .. } if message == "the store failed")
        );
    }

    #[test]
    fn store_cause_reconstruction_and_reencoding_keep_the_existing_bound() {
        let mut crossed = one("RoomKitError", "the store failed".into());
        crossed.attributes.insert("variant".into(), json!("Store"));
        crossed.causes = (0..CAUSES_KEPT + 5)
            .map(|index| one("OSError", format!("cause {index}")))
            .collect();
        let rebuilt = rebuild_kit_error(&crossed);
        assert_eq!(kit_error(&rebuilt).causes, crossed.causes[..CAUSES_KEPT]);
        let mut count = 0;
        let mut cause = rebuilt.source();
        while let Some(next) = cause {
            count += 1;
            cause = next.source();
        }
        assert_eq!(count, CAUSES_KEPT);
    }
}
