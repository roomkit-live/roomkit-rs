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
        .filter(|identity| scope(identity.organization_id.as_deref()) == scope(organization)))
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

/// Stores `identity`, replacing the one stored under its id, as Python's
/// store does (`INSERT OR REPLACE`); an address another identity of its
/// organization uses is refused [`StoreError::AddressLinked`].
pub(crate) fn create(connection: &Connection, identity: &Identity) -> Result<(), Failure> {
    let held = stored(connection, &identity.id)?;
    let organization = identity.organization_id.as_deref();
    // Within its organization only (RFC §5.6, §17.2): an identity of another is
    // never overwritten, as Python's store refuses it.
    if let Some((stored, _)) = &held
        && scope(stored.organization_id.as_deref()) != scope(organization)
    {
        return Err(StoreError::IdentityNotFound(identity.id.clone()).into());
    }
    let held = held.map(|(_, data)| data);
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
    let written = records::written(identity, Kind::Identity, held.as_deref())?;
    connection.execute(
        "INSERT OR REPLACE INTO identities(id, data) VALUES(?, ?)",
        params![identity.id.as_str(), written],
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
    drop_addresses_no_longer_held(connection, &identity.id, &written, scope(organization))
}

/// Removes what a replaced identity no longer holds, so that it stops resolving
/// to it, a row of another scope for an address it keeps among them (RFC §17.2),
/// as Python's store does. `written` is its JSON, which holds the addresses under
/// a key the model does not read too.
fn drop_addresses_no_longer_held(
    connection: &Connection,
    identity_id: &IdentityId,
    written: &str,
    organization: &str,
) -> Result<(), Failure> {
    let written: serde_json::Value = serde_json::from_str(written)?;
    let declared = |channel_type: &str, address: &str| {
        written
            .pointer("/channel_addresses")
            .and_then(|by_type| by_type.get(channel_type))
            .and_then(serde_json::Value::as_array)
            .is_some_and(|held| held.iter().any(|value| value.as_str() == Some(address)))
    };
    let mut statement = connection.prepare(
        "SELECT channel_type, address, organization_id FROM identity_addresses
         WHERE identity_id = ?",
    )?;
    let rows = statement
        .query_map([identity_id.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    for (channel_type, address, held_in) in rows {
        if held_in != organization || !declared(&channel_type, &address) {
            connection.execute(
                "DELETE FROM identity_addresses
                 WHERE channel_type = ? AND address = ? AND organization_id = ?",
                [&channel_type, &address, &held_in],
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
        .filter(|(identity, _)| scope(identity.organization_id.as_deref()) == scope(organization))
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
         WHERE channel_type = ? AND address = ? AND identity_id = ?",
        params![channel_type.as_str(), address, identity_id.as_str()],
    )?;
    Ok(())
}
