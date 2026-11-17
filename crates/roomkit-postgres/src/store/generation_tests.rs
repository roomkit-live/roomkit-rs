//! A cloned store can close and reopen while a scope is acquiring its connection.

use std::path::{Path, PathBuf};

use uuid::Uuid;

use super::*;

fn fixture_pem(variable: &str) -> PathBuf {
    PathBuf::from(std::env::var(variable).unwrap()).with_extension("pem")
}

fn dsn(port: &str, schema: &str, root: &Path) -> String {
    format!(
        "postgresql://postgres:fixture@localhost:{port}/postgres?search_path={schema}&sslmode=verify-full&sslrootcert={}",
        root.display()
    )
}

#[tokio::test]
async fn a_captured_pool_cannot_lend_a_reopened_stores_connection() {
    let Ok(port) = std::env::var("TLS_POOL_PORT") else {
        return;
    };
    let schema = format!("rk_generation_{}", Uuid::new_v4().simple());
    let trusted = fixture_pem("TLS_POOL_CA");
    let admin_dsn = dsn(&port, &schema, &trusted);
    let (admin, _) = pool::open(Some(&admin_dsn), 1, 1).await.unwrap();
    let admin = admin.get().await.unwrap();
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();

    let root = std::env::temp_dir().join(format!("{schema}.pem"));
    std::fs::copy(fixture_pem("TLS_POOL_BAD_CA"), &root).unwrap();
    let store = PostgresStore::new(PostgresStoreOptions {
        dsn: Some(dsn(&port, &schema, &root)),
        ..Default::default()
    });
    // With zero warm connections, init keeps its pool before preparing the schema.
    let options = InitOptions {
        min_size: 0,
        max_size: 1,
    };
    store.init(options).await.unwrap_err();
    let (previous, previous_cancel) = store.initialized_pool().unwrap().for_scope().unwrap();
    let clone = store.clone();
    clone.close().await;
    std::fs::copy(trusted, &root).unwrap();
    clone.init(options).await.unwrap();
    std::fs::remove_file(root).unwrap();

    // Resume the interrupted acquisition. It cannot acquire from the replacement.
    let error = previous.checkout().await.unwrap_err();
    assert!(matches!(error, StoreError::Unavailable(cause)
        if matches!(cause.downcast_ref::<deadpool_postgres::PoolError>(),
                    Some(deadpool_postgres::PoolError::Closed))));
    let (current, current_cancel) = store.initialized_pool().unwrap().for_scope().unwrap();
    let connection = current.checkout().await.unwrap();
    let token = connection.cancel_token();
    previous_cancel(token.clone()).await.unwrap_err();
    current_cancel(token).await.unwrap();

    drop(connection);
    store.close().await;
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
}
