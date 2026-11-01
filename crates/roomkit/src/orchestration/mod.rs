//! Multi-agent coordination: Python's `roomkit.orchestration`. For now its
//! status bus (RFC §19.8), the shared log every agent posts its status to
//! and any code can follow: the entry, its level, and the in-memory backend
//! a bus uses by default; [`backend`] holds what another backend
//! implements.

pub mod backend;
mod memory;
mod status;

pub use self::memory::{InMemoryStatusBackend, InMemoryStatusBackendOptions};
pub use self::status::{
    HasCompletedOptions, PostAsyncOptions, PostOptions, RecentOptions, StatusBus, StatusBusOptions,
    StatusEntry, StatusLevel,
};
