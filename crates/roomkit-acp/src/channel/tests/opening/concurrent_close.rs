//! A closer on another worker cannot erase the setter's returned values.

use std::sync::Arc;
use tokio::sync::Notify;

use super::*;

// Python: test_acp_session_setup::TestOneOpeningAtATime::test_close_session_waits_for_an_opening_then_closes_it.
// The shared close/open scenario also checks the setter's returned values.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_close_session_waits_for_an_opening_then_closes_it_on_several_workers() {
    tokio::time::timeout(PATIENCE, check()).await.unwrap();
}

async fn closing_after_poll(channel: AcpChannel<Piped>, entered: Arc<Notify>) -> bool {
    let room = RoomId::new("r1").unwrap();
    let mut closing = pin!(channel.close_session(&room));
    pending(closing.as_mut()).await;
    entered.notify_one();
    closing.await
}

async fn check() {
    let (channel, mut agent) = connected().await;
    let setting_channel = channel.clone();
    let setter = tokio::spawn(async move {
        setting_channel
            .set_config_option(&RoomId::new("r1").unwrap(), "model", json!("sonnet"))
            .await
    });
    let opening = agent.read().await;
    assert_eq!(opening["method"], "session/new");
    let entered = Arc::new(Notify::new());
    let closer = tokio::spawn(closing_after_poll(channel.clone(), Arc::clone(&entered)));
    entered.notified().await;
    assert!(
        !closer.is_finished(),
        "close waits for the unanswered opening"
    );
    opened(&mut agent, &opening).await;
    setting(&mut agent, "sonnet").await;
    closing(&mut agent).await;
    let returned = setter.await.unwrap().unwrap();
    assert_eq!(
        returned,
        Metadata::from_iter([("model".to_owned(), json!("sonnet"))])
    );
    assert!(closer.await.unwrap());
    let room = RoomId::new("r1").unwrap();
    assert!(channel.session_id(&room).is_none());
    assert!(channel.session_config(&room).is_empty());
    assert!(channel.room_state(&room).unwrap().is_empty());
    channel.close().await;
}
