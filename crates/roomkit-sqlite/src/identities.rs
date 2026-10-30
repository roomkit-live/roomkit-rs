//! Identities and the addresses they use (RFC §5.6, §14.1, §17.2), in the
//! `identities` table and its index of addresses, `identity_addresses`,
//! which scopes each address by organization, none being the empty string,
//! as the Python implementation writes it.

use roomkit::StoreError;
use roomkit::model::{ChannelType, Identity, IdentityId};
use rusqlite::{Connection, OptionalExtension, params};

use crate::error::Failure;
use crate::records::{self, Kind};

/// The organization column's value for `organization`.
fn scope(organization: Option<&str>) -> &str {
    organization.unwrap_or_default()
}

/// The identity `identity_id`, with its row's JSON.
fn stored(
    connection: &Connection,
    identity_id: &IdentityId,
) -> Result<Option<(Identity, String)>, Failure> {
    let data: Option<String> = connection
        .query_row(
            "SELECT data FROM identities WHERE id = ?",
            [identity_id.as_str()],
            |row| row.get(0),
        )
        .optional()?;
    data.map(|data| Ok((records::read_identity(&data)?, data)))
        .transpose()
}

/// The identity `identity_id`.
pub(crate) fn get(
    connection: &Connection,
    identity_id: &IdentityId,
) -> Result<Option<Identity>, Failure> {
    Ok(stored(connection, identity_id)?.map(|(identity, _)| identity))
}

/// The identity of `organization` that uses `address` on channels of
/// `channel_type`.
pub(crate) fn resolve(
    connection: &Connection,
    channel_type: &ChannelType,
    address: &str,
    organization: Option<&str>,
) -> Result<Option<Identity>, Failure> {
    let Some(holder) = holder(connection, channel_type, address, organization)? else {
        return Ok(None);
    };
    Ok(get(connection, &holder)?
        .filter(|identity| identity.organization_id.as_deref() == organization))
}

/// The identity of `organization` the index files `address` under.
fn holder(
    connection: &Connection,
    channel_type: &ChannelType,
    address: &str,
    organization: Option<&str>,
) -> Result<Option<IdentityId>, Failure> {
    let holder: Option<String> = connection
        .query_row(
            "SELECT identity_id FROM identity_addresses
             WHERE channel_type = ? AND address = ? AND organization_id = ?",
            [channel_type.as_str(), address, scope(organization)],
            |row| row.get(0),
        )
        .optional()?;
    Ok(holder.and_then(|holder| IdentityId::new(holder).ok()))
}

/// Refuses `address` when an identity of `organization` other than `owner`
/// uses it (RFC §14.1, gap G-060).
fn check_free(
    connection: &Connection,
    channel_type: &ChannelType,
    address: &str,
    organization: Option<&str>,
    owner: &IdentityId,
) -> Result<(), Failure> {
    match holder(connection, channel_type, address, organization)? {
        Some(holder) if &holder != owner => Err(StoreError::AddressLinked {
            channel_type: channel_type.clone(),
        }
        .into()),
        Some(_) | None => Ok(()),
    }
}

/// Files `address` under the identity in the index.
fn index(
    connection: &Connection,
    identity_id: &IdentityId,
    channel_type: &ChannelType,
    address: &str,
    organization: Option<&str>,
) -> Result<(), Failure> {
    connection.execute(
        "INSERT OR REPLACE INTO identity_addresses(channel_type, address, identity_id, organization_id)
         VALUES(?, ?, ?, ?)",
        params![channel_type.as_str(), address, identity_id.as_str(), scope(organization)],
    )?;
    Ok(())
}

/// Stores a new identity; an id taken is refused
/// [`StoreError::IdentityExists`], an address another identity of its
/// organization uses [`StoreError::AddressLinked`].
pub(crate) fn create(connection: &Connection, identity: &Identity) -> Result<(), Failure> {
    if stored(connection, &identity.id)?.is_some() {
        return Err(StoreError::IdentityExists(identity.id.clone()).into());
    }
    let organization = identity.organization_id.as_deref();
    for (channel_type, addresses) in &identity.channel_addresses {
        for address in addresses {
            check_free(
                connection,
                channel_type,
                address,
                organization,
                &identity.id,
            )?;
        }
    }
    connection.execute(
        "INSERT INTO identities(id, data) VALUES(?, ?)",
        params![
            identity.id.as_str(),
            records::written(identity, Kind::Identity, None)?,
        ],
    )?;
    for (channel_type, addresses) in &identity.channel_addresses {
        for address in addresses {
            index(
                connection,
                &identity.id,
                channel_type,
                address,
                organization,
            )?;
        }
    }
    Ok(())
}

/// The identity `identity_id` of `organization`, refused
/// [`StoreError::IdentityNotFound`] otherwise.
fn of_organization(
    connection: &Connection,
    identity_id: &IdentityId,
    organization: Option<&str>,
) -> Result<(Identity, String), Failure> {
    stored(connection, identity_id)?
        .filter(|(identity, _)| identity.organization_id.as_deref() == organization)
        .ok_or_else(|| StoreError::IdentityNotFound(identity_id.clone()).into())
}

/// Writes `identity` over its row.
fn write(connection: &Connection, identity: &Identity, stored: &str) -> Result<(), Failure> {
    connection.execute(
        "UPDATE identities SET data = ? WHERE id = ?",
        params![
            records::written(identity, Kind::Identity, Some(stored))?,
            identity.id.as_str(),
        ],
    )?;
    Ok(())
}

/// Links `address` to the identity `identity_id` of `organization`.
pub(crate) fn link(
    connection: &Connection,
    identity_id: &IdentityId,
    channel_type: &ChannelType,
    address: &str,
    organization: Option<&str>,
) -> Result<(), Failure> {
    let (mut identity, data) = of_organization(connection, identity_id, organization)?;
    check_free(connection, channel_type, address, organization, identity_id)?;
    identity.link(channel_type.clone(), address);
    write(connection, &identity, &data)?;
    index(connection, identity_id, channel_type, address, organization)
}

/// Removes `address` from the identity `identity_id` of `organization`.
pub(crate) fn unlink(
    connection: &Connection,
    identity_id: &IdentityId,
    channel_type: &ChannelType,
    address: &str,
    organization: Option<&str>,
) -> Result<(), Failure> {
    let (mut identity, data) = of_organization(connection, identity_id, organization)?;
    identity.unlink(channel_type, address);
    write(connection, &identity, &data)?;
    connection.execute(
        "DELETE FROM identity_addresses
         WHERE channel_type = ? AND address = ? AND identity_id = ? AND organization_id = ?",
        params![
            channel_type.as_str(),
            address,
            identity_id.as_str(),
            scope(organization)
        ],
    )?;
    Ok(())
}
