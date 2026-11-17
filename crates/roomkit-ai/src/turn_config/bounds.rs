//! Copy the channel's tool bounds for one resolved turn (RFC §21.6).

use super::AiChannelTurnConfig;
use crate::AiError;
use crate::tool_loop::Bounds;
use crate::turn_settings::VALUE_ERROR;
use roomkit::{ChannelError, ClassedError};

pub(crate) fn for_turn(
    channel: &Bounds,
    config: Option<&AiChannelTurnConfig>,
) -> Result<Bounds, ChannelError> {
    let mut bounds = channel.clone();
    let Some(given) = config.and_then(|config| config.tool_timeouts.as_ref()) else {
        return Ok(bounds);
    };
    for (name, bound) in given {
        if bound.is_some_and(|bound| bound.is_zero()) {
            return Err(Box::new(ClassedError::new(
                VALUE_ERROR,
                AiError::ToolTimeoutOf { name: name.clone() },
            )));
        }
    }
    bounds.tool_timeouts.extend(given.clone());
    Ok(bounds)
}
