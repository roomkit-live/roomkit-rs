//! Calls of the application callbacks registered on this turn's owner.

use roomkit_core::model::Metadata;
use serde_json::Value;

use super::TurnHost;
use crate::context::ExecutionContext;
use crate::error::{RoomKitError, repr};
use crate::turns::declared::BUILTINS;

impl TurnHost {
    /// Call a verb the owner declared with [`crate::RoomKit::declare_host_verb`]
    /// (RFC §10.6.5). `None` supplies an empty argument object.
    ///
    /// The owner's record changes are applied before either its result or
    /// error returns. After this turn, only explicitly admitted callbacks
    /// run, for the same room and channel with fresh context.
    ///
    /// # Errors
    ///
    /// Built-in names must use their own methods. Unknown or inadmissible
    /// names are refused by the owner. Callback errors use the same existing
    /// error mapping as other host verbs: known kit errors are rebuilt,
    /// otherwise [`RoomKitError::RemoteTurn`] preserves their kind and message.
    pub async fn call(
        &self,
        verb: &str,
        arguments: Option<Metadata>,
    ) -> Result<Value, RoomKitError> {
        if BUILTINS.contains(&verb) {
            return Err(RoomKitError::Value(format!(
                "{} is a verb of RoomKit's: send it with TurnHost.{verb}",
                repr(verb)
            )));
        }
        let floor = ExecutionContext::current().turn_depth().unwrap_or(0);
        self.shared
            .link
            .verb(verb, arguments.unwrap_or_default(), floor)
            .await
    }
}
