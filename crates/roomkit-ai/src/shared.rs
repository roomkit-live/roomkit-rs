//! Private storage for the small, explicitly shared speaking inputs.

pub(crate) mod failure;
pub(crate) mod ordered;

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

#[derive(Clone)]
pub(crate) struct Shared<T>(Arc<Mutex<T>>);

impl<T> Shared<T> {
    pub(crate) fn new(value: T) -> Self {
        Self(Arc::new(Mutex::new(value)))
    }

    pub(crate) fn update<R>(&self, edit: impl FnOnce(&mut T) -> R) -> R {
        edit(&mut self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }

    pub(crate) fn same_object(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl<T: Clone> Shared<T> {
    pub(crate) fn snapshot(&self) -> T {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl<T: Clone + PartialEq> PartialEq for Shared<T> {
    fn eq(&self, other: &Self) -> bool {
        self.same_object(other) || self.snapshot() == other.snapshot()
    }
}

impl<T: Clone + fmt::Debug> fmt::Debug for Shared<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.snapshot().fmt(formatter)
    }
}

macro_rules! shared_input {
    ($(#[$attribute:meta])* $name:ident, $value:ty) => {
        $(#[$attribute])*
        #[derive(Clone, Debug, PartialEq)]
        pub struct $name(crate::shared::Shared<$value>);

        impl $name {
            /// Takes ownership of an input without copying or validating it.
            pub fn new(value: $value) -> Self {
                Self(crate::shared::Shared::new(value))
            }

            /// An owned snapshot. Cloning the handle instead keeps the same object.
            pub fn snapshot(&self) -> $value {
                self.0.snapshot()
            }

            /// Edits this object atomically. The callback must be short and
            /// synchronous, and must not access this handle or its clones again.
            pub fn update<R>(&self, edit: impl FnOnce(&mut $value) -> R) -> R {
                self.0.update(edit)
            }

            /// Whether both handles refer to the same object, regardless of its value.
            pub fn same_object(&self, other: &Self) -> bool {
                self.0.same_object(&other.0)
            }
        }

        impl From<$value> for $name {
            fn from(value: $value) -> Self {
                Self::new(value)
            }
        }
    };
}

pub(crate) use shared_input;
