//! An original recoverable publication failure, retained without erasing its cause.

use std::error::Error;
use std::ops::Deref;

/// What a realtime backend that could not be reached or failed reports as its cause:
/// a publisher's error, an I/O error, or a message (`"connection
/// refused".into()`).
///
/// It reads as the error it holds: it dereferences to it, which is
/// [`crate::RoomKitError::Realtime`]'s source, shows it, and gives it back
/// ([`into_inner`](Self::into_inner)). It equals only itself: a cause
/// cannot be compared. It is not an error of its own, so the source chain
/// goes from `RoomKitError::Realtime` to the backend error, with nothing between.
pub struct RealtimeFailure(Box<dyn Error + Send + Sync>);

impl RealtimeFailure {
    /// The failure `cause` stands for: the error the realtime backend met, or a
    /// message.
    pub fn new(cause: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        Self(cause.into())
    }

    /// The error it holds.
    pub fn into_inner(self) -> Box<dyn Error + Send + Sync> {
        self.0
    }
}

/// Anything a boxed error is made from: an error, or a message.
impl<E: Into<Box<dyn Error + Send + Sync>>> From<E> for RealtimeFailure {
    fn from(cause: E) -> Self {
        Self::new(cause)
    }
}

impl Deref for RealtimeFailure {
    type Target = dyn Error + Send + Sync + 'static;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl AsRef<dyn Error + Send + Sync + 'static> for RealtimeFailure {
    fn as_ref(&self) -> &(dyn Error + Send + Sync + 'static) {
        &*self.0
    }
}

/// Shows the error it holds, as that error shows itself.
impl std::fmt::Display for RealtimeFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, formatter)
    }
}

/// Shows the error it holds, as that error shows itself.
impl std::fmt::Debug for RealtimeFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, formatter)
    }
}

/// One failure equals itself only.
impl PartialEq for RealtimeFailure {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

impl Eq for RealtimeFailure {}
