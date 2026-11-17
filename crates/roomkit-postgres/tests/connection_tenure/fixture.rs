use std::sync::{Arc, Mutex};

use deadpool_postgres::{Hook, Manager, Pool};
use roomkit::model::{Room, RoomId};
use roomkit::{ConversationStore, StoreError};
use roomkit_postgres::{InitOptions, PostgresCancelQuery, PostgresStore, PostgresStoreOptions};
use tokio_postgres::NoTls;

use super::support::TempSchema;

pub(super) fn room() -> RoomId {
    RoomId::new("r1").unwrap()
}

#[derive(Default)]
pub(super) struct Checkouts(Mutex<Vec<i32>>);

impl Checkouts {
    pub(super) fn pids(&self) -> Vec<i32> {
        self.0.lock().unwrap().clone()
    }
    pub(super) fn reset(&self) {
        self.0.lock().unwrap().clear();
    }
}

fn count(counter: &Arc<Checkouts>) -> Hook {
    let counter = Arc::clone(counter);
    Hook::async_fn(move |client, _| {
        let counter = Arc::clone(&counter);
        Box::pin(async move {
            let pid = client
                .query_one("SELECT pg_backend_pid()", &[])
                .await
                .map_err(deadpool_postgres::HookError::Backend)?
                .get(0);
            counter.0.lock().unwrap().push(pid);
            Ok(())
        })
    })
}

pub(super) async fn backend_pid(pool: &Pool) -> i32 {
    pool.get()
        .await
        .unwrap()
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0)
}

pub(super) fn cancel() -> PostgresCancelQuery {
    Arc::new(|token| {
        Box::pin(async move {
            token
                .cancel_query(NoTls)
                .await
                .map_err(StoreError::unavailable)
        })
    })
}

pub(super) async fn counted(schema: &TempSchema) -> (PostgresStore, Pool, Arc<Checkouts>) {
    counted_with(schema, cancel()).await
}

pub(super) async fn counted_with(
    schema: &TempSchema,
    cancel: PostgresCancelQuery,
) -> (PostgresStore, Pool, Arc<Checkouts>) {
    let counter = Arc::new(Checkouts::default());
    let pool = Pool::builder(Manager::new(schema.dsn().parse().unwrap(), NoTls))
        .max_size(3)
        .post_create(count(&counter))
        .post_recycle(count(&counter))
        .build()
        .unwrap();
    let store = PostgresStore::new(PostgresStoreOptions {
        pool: Some(pool.clone()),
        cancel_query: Some(cancel),
        ..PostgresStoreOptions::default()
    });
    store.init(InitOptions::default()).await.unwrap();
    store.create_room(Room::new(room())).await.unwrap();
    counter.reset();
    (store, pool, counter)
}
