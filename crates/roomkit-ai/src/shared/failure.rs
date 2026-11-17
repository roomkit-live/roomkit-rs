//! Repeated scripted failures keep the original error as their cause.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use roomkit::{ChannelError, ClassedError};

#[derive(Debug)]
struct Retained(Arc<dyn Error + Send + Sync>);

impl fmt::Display for Retained {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Error for Retained {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}

pub(crate) fn retained(error: &Arc<dyn Error + Send + Sync>) -> ChannelError {
    let cause = Retained(Arc::clone(error));
    if let Some(classed) = error.downcast_ref::<ClassedError>() {
        Box::new(
            ClassedError::new(classed.class(), cause).with_attributes(classed.attributes().clone()),
        )
    } else {
        Box::new(cause)
    }
}
