//! Verified TLS on the real retained scope, for owned and external pools.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod fixture;

use roomkit::model::{Room, RoomId, RoomStatus};
use roomkit::{ConversationStore, StoreError};
use serde_json::json;

use fixture::{Fixture, available, blocked, client_state};

#[tokio::test]
async fn the_owned_pool_reuses_its_verified_tls_connection_after_a_cut() {
    same_connection(true).await;
}

#[tokio::test]
async fn an_external_pool_reuses_its_exact_tls_connector_after_a_cut() {
    same_connection(false).await;
}

async fn same_connection(owned: bool) {
    if !available() {
        return;
    }
    let fixture = Fixture::new(if owned { "owned" } else { "external" }, owned, false).await;
    let room = RoomId::new("r1").unwrap();
    fixture
        .store
        .create_room(Room::new(room.clone()))
        .await
        .unwrap();
    fixture
        .holder
        .batch_execute("BEGIN; SELECT 1 FROM rooms WHERE id = 'r1' FOR UPDATE")
        .await
        .unwrap();
    let pid = fixture.worker().await;
    assert!(
        fixture
            .observer
            .query_one("SELECT ssl FROM pg_stat_ssl WHERE pid = $1", &[&pid])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    fixture
        .store
        .connection(async {
            let mut cut = Room::new(room.clone());
            cut.metadata.insert("cut".to_owned(), json!(true));
            let mut write = Box::pin(fixture.store.update_room_if(
                cut,
                RoomStatus::Active,
                None,
                None,
            ));
            tokio::select! {
                result = &mut write => panic!("write escaped its lock: {result:?}"),
                () = blocked(&fixture.observer, pid) => {},
            }
            drop(write);
            assert!(
                fixture
                    .store
                    .get_room(&room)
                    .await?
                    .unwrap()
                    .metadata
                    .is_empty()
            );
            assert_eq!(fixture.worker().await, pid);
            assert_eq!(
                client_state(&fixture.observer, pid).await,
                ("idle".to_owned(), true)
            );
            fixture.holder.batch_execute("COMMIT").await.unwrap();
            let mut next = Room::new(room.clone());
            next.metadata.insert("next".to_owned(), json!(true));
            fixture.store.update_room(next).await?;
            Ok::<_, StoreError>(())
        })
        .await
        .unwrap();
    assert_eq!(fixture.worker().await, pid);
    assert_eq!(
        fixture
            .store
            .get_room(&room)
            .await
            .unwrap()
            .unwrap()
            .metadata,
        [("next".to_owned(), json!(true))].into_iter().collect()
    );
    fixture.assert_connector_calls(if owned { 0 } else { 2 });
    fixture.close().await;
}

#[tokio::test]
async fn a_wrong_cancel_certificate_is_refused_without_a_fallback() {
    if !available() {
        return;
    }
    let fixture = Fixture::new("bad_ca", false, true).await;
    let room = RoomId::new("r1").unwrap();
    fixture
        .store
        .create_room(Room::new(room.clone()))
        .await
        .unwrap();
    fixture
        .holder
        .batch_execute("BEGIN; SELECT 1 FROM rooms WHERE id = 'r1' FOR UPDATE")
        .await
        .unwrap();
    let pid = fixture.worker().await;
    let failure = fixture
        .store
        .connection(async {
            let mut cut = Room::new(room.clone());
            cut.metadata.insert("cut".to_owned(), json!(true));
            let mut write = Box::pin(fixture.store.update_room(cut));
            tokio::select! {
                result = &mut write => panic!("write escaped its lock: {result:?}"),
                () = blocked(&fixture.observer, pid) => {},
            }
            drop(write);
            Ok::<_, StoreError>(())
        })
        .await
        .unwrap_err();
    let mut text = failure.to_string();
    let mut cause = std::error::Error::source(&failure);
    while let Some(error) = cause {
        text.push_str(": ");
        text.push_str(&error.to_string());
        cause = error.source();
    }
    assert!(text.contains("certificate"), "{text}");
    fixture.assert_failed_connector_calls();
    assert_eq!(fixture.pool.as_ref().unwrap().status().size, 0);
    // This fixture created this exact backend. A detached connection can keep
    // waiting on its server lock until PostgreSQL notices the disconnection.
    fixture.observer.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid = $1 AND application_name = $2", &[&pid, &fixture.name]).await.unwrap();
    fixture.holder.batch_execute("COMMIT").await.unwrap();
    assert!(
        fixture
            .store
            .get_room(&room)
            .await
            .unwrap()
            .unwrap()
            .metadata
            .is_empty()
    );
    fixture.close().await;
}
