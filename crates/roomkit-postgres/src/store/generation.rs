//! A pool and the cancellation transport chosen when it was opened.

use std::sync::Arc;

use deadpool_postgres::{Object, Pool};
use roomkit::StoreError;

use super::ACQUIRE_TIMEOUT;
use crate::PostgresCancelQuery;
use crate::connect::Tls;
use crate::error::NoConnection;

/// A store clone may close and reopen the pool with different TLS trust.
/// Keep a checkout and its cancellation capability on the same generation.
#[derive(Clone)]
pub(super) struct PoolGeneration {
    pub(super) pool: Pool,
    pub(super) tls: Option<Tls>,
    cancel_query: Option<PostgresCancelQuery>,
}

impl PoolGeneration {
    pub(super) fn owned(pool: Pool, tls: Option<Tls>) -> Self {
        let cancel_tls = tls.clone();
        let cancel_query: PostgresCancelQuery = Arc::new(move |token| {
            let tls = cancel_tls.clone();
            Box::pin(async move {
                match tls {
                    Some(tls) => token.cancel_query(tls).await,
                    None => token.cancel_query(tokio_postgres::NoTls).await,
                }
                .map_err(StoreError::unavailable)
            })
        });
        Self {
            pool,
            tls,
            cancel_query: Some(cancel_query),
        }
    }

    pub(super) fn external(pool: Pool, cancel_query: Option<PostgresCancelQuery>) -> Self {
        Self {
            pool,
            tls: None,
            cancel_query,
        }
    }

    pub(super) fn for_scope(self) -> Result<(Self, PostgresCancelQuery), StoreError> {
        let cancel = self.cancel_query.clone().ok_or_else(|| {
            StoreError::Value(
                "PostgresStore.connection() requires cancel_query for an external pool".to_owned(),
            )
        })?;
        Ok((self, cancel))
    }

    pub(super) async fn checkout(&self) -> Result<Object, StoreError> {
        match tokio::time::timeout(ACQUIRE_TIMEOUT, self.pool.get()).await {
            Ok(client) => client.map_err(StoreError::unavailable),
            Err(_) => Err(StoreError::unavailable(NoConnection::TimedOut)),
        }
    }
}
