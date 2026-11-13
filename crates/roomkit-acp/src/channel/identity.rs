//! The agent's public identity attributes (RFC §6.4).

use super::AcpChannel;
use crate::AcpTransport;

impl<T: AcpTransport> AcpChannel<T> {
    /// Its display name: Python's `name`.
    pub fn name(&self) -> Option<&str> {
        self.shared.name.as_deref()
    }

    /// Its role: Python's `role`.
    pub fn role(&self) -> Option<&str> {
        self.shared.role.as_deref()
    }

    /// What it does: Python's `description`.
    pub fn description(&self) -> Option<&str> {
        self.shared.description.as_deref()
    }
}
