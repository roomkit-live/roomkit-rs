//! The organization a call acts for (RFC §17.2). The kit's own calls act
//! for none and read rooms unscoped; an [`OrganizationKit`]'s act for its
//! organization, whose rooms only they reach. Each call is written once,
//! on [`Scope`], and both fronts hand it their organization.
//!
//! [`OrganizationKit`]: super::OrganizationKit

use super::RoomKit;

/// A kit, and the organization its call acts for: `None` reads rooms
/// unscoped, and a room the call creates belongs to no organization.
#[derive(Debug)]
pub(super) struct Scope<'kit, S> {
    pub(super) kit: &'kit RoomKit<S>,
    organization_id: Option<&'kit str>,
}

impl<'kit, S> Scope<'kit, S> {
    /// The calls of `kit`, made for `organization_id`.
    pub(super) fn new(kit: &'kit RoomKit<S>, organization_id: Option<&'kit str>) -> Self {
        Self {
            kit,
            organization_id,
        }
    }

    /// The organization the call acts for, as an operation takes it.
    pub(super) fn organization_id(&self) -> Option<String> {
        self.organization_id.map(str::to_owned)
    }
}

impl<S> RoomKit<S> {
    /// The kit's own calls, which act for no organization.
    pub(super) fn unscoped(&self) -> Scope<'_, S> {
        Scope::new(self, None)
    }
}
