//! Identities and the addresses they are reached at (RFC §5.6, §17.2), with
//! the Python implementation's SQL: an address is unique within an
//! organization, `''` the unscoped tenant.

use deadpool_postgres::{GenericClient, Object, Transaction};
use roomkit::StoreError;
use roomkit::model::{ChannelType, Identity, IdentityId};
use serde_json::Value;

use crate::error::Failure;
use crate::rows::{self, IDENTITY_COLUMNS};
use crate::values::{execute, query_opt};

/// Files `address` under `identity_id`, refusing one another identity of the
/// organization holds (RFC §5.6). The first statement decides who waits, so two
/// concurrent links of one address cannot both win; the no-op update on a conflict
/// is what makes `RETURNING` give the holder.
async fn file_address(
    transaction: &Transaction<'_>,
    channel_type: &str,
    address: &str,
    identity_id: &IdentityId,
    organization: &str,
) -> Result<(), Failure> {
    let holder = query_opt(
        transaction,
        "INSERT INTO identity_addresses (channel_type, address, identity_id, organization_id) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (channel_type, address, organization_id) \
         DO UPDATE SET identity_id = identity_addresses.identity_id \
         RETURNING identity_id",
        &[
            &channel_type,
            &address,
            &identity_id.as_str(),
            &organization,
        ],
    )
    .await?
    .ok_or(Failure::OutOfRange("an address's holder"))?;
    let holder: &str = holder.try_get(0)?;
    if holder == identity_id.as_str() {
        return Ok(());
    }
    take_a_row_no_read_serves(
        transaction,
        channel_type,
        address,
        identity_id,
        organization,
        holder,
    )
    .await
}

/// Takes the row `holder` holds, locked by the filing, if no read serves it: its
/// holder is of another organization, which only an older release wrote. Else
/// refuses. The holder's organization is read by a statement of its own, which sees
/// an identity that committed while the filing waited on its row: a subquery of the
/// filing would miss it, and the second link would win too.
async fn take_a_row_no_read_serves(
    transaction: &Transaction<'_>,
    channel_type: &str,
    address: &str,
    identity_id: &IdentityId,
    organization: &str,
    holder: &str,
) -> Result<(), Failure> {
    let held_in = query_opt(
        transaction,
        "SELECT COALESCE(organization_id, '') FROM identities WHERE id = $1",
        &[&holder],
    )
    .await?;
    if let Some(held_in) = held_in
        && held_in.try_get::<_, &str>(0)? == organization
    {
        return Err(StoreError::AddressLinked {
            channel_type: ChannelType::new(channel_type)
                .map_err(|_| Failure::OutOfRange("a channel type"))?,
        }
        .into());
    }
    execute(
        transaction,
        "UPDATE identity_addresses SET identity_id = $4 \
         WHERE channel_type = $1 AND address = $2 AND organization_id = $3",
        &[
            &channel_type,
            &address,
            &organization,
            &identity_id.as_str(),
        ],
    )
    .await?;
    Ok(())
}

/// Puts back in `addresses` those `stored` keeps under keys that are no
/// channel type's name, which the model never read.
fn keep_unread_addresses(stored: &Value, addresses: &mut Value) {
    let (Value::Object(stored), Value::Object(addresses)) = (stored, addresses) else {
        return;
    };
    for (channel_type, kept) in stored {
        if ChannelType::new(channel_type.as_str()).is_err() {
            addresses.insert(channel_type.clone(), kept.clone());
        }
    }
}

/// Stores an identity, in place of the one its id names within its
/// organization, as the other stores do, in one transaction: an identity of
/// another organization is never overwritten (RFC §5.6, §17.2), refused
/// [`StoreError::IdentityNotFound`] by the write itself, so that a creation
/// racing another's reads it once that commits. It files the addresses it
/// declares under its own organization, then removes the rest of the identity's
/// rows, a row of another scope for an address it keeps among them, so that what
/// it no longer holds stops resolving to it; in that order, so that a replacement
/// refused for an address it takes from another has locked nothing that other is
/// removing.
pub(crate) async fn create(client: &mut Object, identity: &Identity) -> Result<(), Failure> {
    let transaction = client.transaction().await?;
    let mut addresses = serde_json::to_value(&identity.channel_addresses)?;
    let stored = query_opt(
        &transaction,
        "SELECT channel_addresses FROM identities WHERE id = $1 FOR UPDATE",
        &[&identity.id.as_str()],
    )
    .await?;
    if let Some(stored) = stored {
        keep_unread_addresses(&stored.try_get(0)?, &mut addresses);
    }
    let written = execute(
        &transaction,
        "INSERT INTO identities \
         (id, organization_id, display_name, email, phone, channel_addresses, \
          external_id, external_ids, metadata) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
         ON CONFLICT (id) DO UPDATE SET \
          organization_id=$2, display_name=$3, email=$4, phone=$5, \
          channel_addresses=$6, external_id=$7, external_ids=$8, metadata=$9 \
         WHERE COALESCE(identities.organization_id, '') = COALESCE($2, '')",
        &[
            &identity.id.as_str(),
            &identity.organization_id,
            &identity.display_name,
            &identity.email,
            &identity.phone,
            &addresses,
            &identity.external_id,
            &serde_json::to_value(&identity.external_ids)?,
            &Value::Object(identity.metadata.clone()),
        ],
    )
    .await?;
    if written == 0 {
        return Err(StoreError::IdentityNotFound(identity.id.clone()).into());
    }
    let organization = identity.organization_id.as_deref().unwrap_or_default();
    let mut declared = declared_addresses(&addresses);
    // In one order whoever files them, so that two replacements taking each
    // other's addresses never wait on each other's rows. A row no read serves is the
    // exception: taken over, it stays locked while the removal below waits, and two
    // writers on such rows can still deadlock (RMK-1394).
    declared.sort_unstable();
    for (channel_type, address) in &declared {
        file_address(
            &transaction,
            channel_type,
            address,
            &identity.id,
            organization,
        )
        .await?;
    }
    let (channel_types, kept): (Vec<&str>, Vec<&str>) = declared.iter().copied().unzip();
    execute(
        &transaction,
        "DELETE FROM identity_addresses WHERE identity_id = $1 \
         AND (organization_id <> $4 OR NOT EXISTS ( \
          SELECT 1 FROM unnest($2::text[], $3::text[]) AS kept(channel_type, address) \
          WHERE kept.channel_type = identity_addresses.channel_type \
          AND kept.address = identity_addresses.address))",
        &[&identity.id.as_str(), &channel_types, &kept, &organization],
    )
    .await?;
    transaction.commit().await?;
    Ok(())
}

/// Each channel type and address of `addresses`, those under a key the model
/// does not read among them.
fn declared_addresses(addresses: &Value) -> Vec<(&str, &str)> {
    let Value::Object(by_type) = addresses else {
        return Vec::new();
    };
    by_type
        .iter()
        .flat_map(|(channel_type, held)| {
            held.as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(move |address| (channel_type.as_str(), address))
        })
        .collect()
}

/// The identity `identity_id`.
pub(crate) async fn get<C: GenericClient>(
    client: &C,
    identity_id: &IdentityId,
) -> Result<Option<Identity>, Failure> {
    let row = query_opt(
        client,
        &format!("SELECT {IDENTITY_COLUMNS} FROM identities WHERE id = $1"),
        &[&identity_id.as_str()],
    )
    .await?;
    row.as_ref().map(rows::identity).transpose()
}

/// The identity holding `address` in `organization`, in its own organization
/// only (RFC §17.2), whatever a row left by an upgrade or written directly says.
pub(crate) async fn resolve<C: GenericClient>(
    client: &C,
    channel_type: &ChannelType,
    address: &str,
    organization: Option<&str>,
) -> Result<Option<Identity>, Failure> {
    let row = query_opt(
        client,
        "SELECT i.id, i.organization_id, i.display_name, i.email, i.phone, \
                i.channel_addresses, i.external_id, i.external_ids, i.metadata \
         FROM identity_addresses ia JOIN identities i ON i.id = ia.identity_id \
         WHERE ia.channel_type = $1 AND ia.address = $2 AND ia.organization_id = $3 \
         AND COALESCE(i.organization_id, '') = ia.organization_id",
        &[
            &channel_type.as_str(),
            &address,
            &organization.unwrap_or_default(),
        ],
    )
    .await?;
    row.as_ref().map(rows::identity).transpose()
}

/// The addresses of the identity an address is linked to or unlinked from,
/// its row locked for the transaction. An organization links its own
/// identities only (RFC §5.6): an unknown identity, or one of another
/// organization, is not found.
async fn locked_addresses(
    transaction: &Transaction<'_>,
    identity_id: &IdentityId,
    organization: Option<&str>,
) -> Result<Value, Failure> {
    let row = query_opt(
        transaction,
        "SELECT organization_id, channel_addresses FROM identities WHERE id = $1 FOR UPDATE",
        &[&identity_id.as_str()],
    )
    .await?;
    let not_found = || StoreError::IdentityNotFound(identity_id.clone());
    let row = row.ok_or_else(not_found)?;
    let held: Option<&str> = row.try_get("organization_id")?;
    if held.unwrap_or_default() != organization.unwrap_or_default() {
        return Err(not_found().into());
    }
    Ok(row.try_get("channel_addresses")?)
}

/// Writes the identity's addresses.
async fn write_addresses(
    transaction: &Transaction<'_>,
    identity_id: &IdentityId,
    addresses: &Value,
) -> Result<(), Failure> {
    execute(
        transaction,
        "UPDATE identities SET channel_addresses = $2 WHERE id = $1",
        &[&identity_id.as_str(), addresses],
    )
    .await?;
    Ok(())
}

/// Links `address` to the identity, in one transaction.
pub(crate) async fn link(
    client: &mut Object,
    identity_id: &IdentityId,
    channel_type: &ChannelType,
    address: &str,
    organization: Option<&str>,
) -> Result<(), Failure> {
    let transaction = client.transaction().await?;
    let mut addresses = locked_addresses(&transaction, identity_id, organization).await?;
    file_address(
        &transaction,
        channel_type.as_str(),
        address,
        identity_id,
        organization.unwrap_or_default(),
    )
    .await?;
    let Value::Object(by_type) = &mut addresses else {
        return Err(Failure::OutOfRange("an identity's addresses"));
    };
    let current = by_type
        .entry(channel_type.as_str().to_owned())
        .or_insert_with(|| Value::Array(Vec::new()));
    let Value::Array(current) = current else {
        return Err(Failure::OutOfRange("an identity's addresses"));
    };
    let linked = Value::String(address.to_owned());
    if !current.contains(&linked) {
        current.push(linked);
        write_addresses(&transaction, identity_id, &addresses).await?;
    }
    transaction.commit().await?;
    Ok(())
}

/// `addresses` without `address` under `channel_type`, a type left with no
/// address dropped, as Python's `without_address`.
fn without_address(addresses: &Value, channel_type: &str, address: &str) -> Value {
    let mut remaining = addresses.clone();
    if let Value::Object(by_type) = &mut remaining
        && let Some(Value::Array(held)) = by_type.get_mut(channel_type)
    {
        held.retain(|value| value.as_str() != Some(address));
        if held.is_empty() {
            by_type.shift_remove(channel_type);
        }
    }
    remaining
}

/// Unlinks `address` from the identity, in one transaction: its own rows only, in
/// every scope, so that an address another identity holds stays theirs and a row an
/// upgrade left elsewhere goes with the link.
pub(crate) async fn unlink(
    client: &mut Object,
    identity_id: &IdentityId,
    channel_type: &ChannelType,
    address: &str,
    organization: Option<&str>,
) -> Result<(), Failure> {
    let transaction = client.transaction().await?;
    let addresses = locked_addresses(&transaction, identity_id, organization).await?;
    execute(
        &transaction,
        "DELETE FROM identity_addresses WHERE channel_type = $1 AND address = $2 \
         AND identity_id = $3",
        &[&channel_type.as_str(), &address, &identity_id.as_str()],
    )
    .await?;
    let remaining = without_address(&addresses, channel_type.as_str(), address);
    if remaining != addresses {
        write_addresses(&transaction, identity_id, &remaining).await?;
    }
    transaction.commit().await?;
    Ok(())
}
