//! Class and attributes carried by the kit's public text errors.

use roomkit_core::model::{ErrorRecord, Metadata};
use serde_json::Value;
use std::error::Error;

use super::{
    ChannelRefusalError, TaskCutShortError, TaskTurnFailedError, ToolTimeoutError,
    TurnCutShortError,
};

fn with_fields<const N: usize>(
    kind: &str,
    error: &dyn Error,
    fields: [(&str, Value); N],
) -> ErrorRecord {
    let attributes = fields
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect::<Metadata>();
    ErrorRecord {
        attributes: Box::new(attributes),
        ..ErrorRecord::new(kind, error.to_string())
    }
}

impl TaskCutShortError {
    pub(crate) fn record(&self) -> ErrorRecord {
        with_fields(
            "TaskCutShortError",
            self,
            [
                ("reason", Value::from(self.reason.clone())),
                ("narration", Value::from(self.narration.clone())),
            ],
        )
    }
}

impl TaskTurnFailedError {
    pub(crate) fn cause_record(&self) -> ErrorRecord {
        let error = self.error.as_ref();
        if let Some(known) = error.downcast_ref::<crate::RoomKitError>() {
            return ErrorRecord::new(known.class_name(), known.to_string());
        }
        crate::turns::record_of(error)
    }
}

/// Copy the fields while the driver still has the original application error.
pub(crate) fn text_record(error: &(dyn Error + 'static)) -> Option<ErrorRecord> {
    if let Some(cut) = error.downcast_ref::<TaskCutShortError>() {
        return Some(cut.record());
    }
    if let Some(cut) = error.downcast_ref::<TurnCutShortError>() {
        return Some(with_fields(
            "TurnCutShortError",
            cut,
            [("reason", Value::from(cut.reason.clone()))],
        ));
    }
    if let Some(failure) = error.downcast_ref::<TaskTurnFailedError>() {
        return Some(with_fields(
            "TaskTurnFailedError",
            failure,
            [
                ("reason", Value::from(failure.reason.clone())),
                ("narration", Value::from(failure.narration.clone())),
            ],
        ));
    }
    if let Some(timeout) = error.downcast_ref::<ToolTimeoutError>() {
        return Some(with_fields(
            "ToolTimeoutError",
            timeout,
            [
                ("name", Value::from(timeout.name.clone())),
                ("timeout", Value::from(timeout.timeout)),
            ],
        ));
    }
    let refusal = error.downcast_ref::<ChannelRefusalError>()?;
    Some(with_fields(
        "ChannelRefusalError",
        refusal,
        [("message", Value::from(refusal.message.clone()))],
    ))
}
