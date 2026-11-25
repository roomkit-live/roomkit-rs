//! Duplicate references in Redis streams, without a Redis connection.
use std::time::Duration;

use super::{Message, Routes};

fn entry(kind: &str, reference: &str) -> Message {
    Message::new(kind, "worker", String::new(), "", reference)
}

#[tokio::test(start_paused = true)]
async fn questions_cannot_evict_a_publication_that_the_worker_can_still_retry() {
    let mut routes = Routes::default();
    let work = entry("work", "publication");
    assert_eq!(routes.first(&work), Ok(()));
    tokio::time::advance(Duration::from_secs(1)).await;
    for seq in 0..100_001 {
        assert_eq!(routes.first(&entry("request", &seq.to_string())), Ok(()));
    }
    assert_eq!(routes.first(&work), Err(None));
    assert_eq!(routes.first(&entry("request", "90001")), Err(None));
    assert_eq!(routes.first(&entry("request", "90000")), Ok(()));
}

#[tokio::test(start_paused = true)]
async fn questions_expire_at_sixty_seconds_but_publications_survive_the_retry_window() {
    let mut routes = Routes::default();
    let question = entry("request", "question");
    let work = entry("work", "publication");
    assert_eq!(routes.first(&question), Ok(()));
    assert_eq!(routes.first(&work), Ok(()));
    tokio::time::advance(Duration::from_secs(60)).await;
    assert_eq!(routes.first(&entry("owner", "boundary")), Ok(()));
    assert_eq!(routes.first(&question), Err(None));
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(routes.first(&entry("channel", "after-boundary")), Ok(()));
    assert_eq!(routes.first(&question), Ok(()));
    tokio::time::advance(Duration::from_secs(239)).await;
    assert_eq!(routes.first(&entry("work", "other")), Ok(()));
    assert_eq!(routes.first(&work), Err(None));
    tokio::time::advance(Duration::from_secs(300)).await;
    assert_eq!(routes.first(&entry("work", "boundary")), Ok(()));
    assert_eq!(routes.first(&work), Err(None));
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(routes.first(&entry("work", "sweep")), Ok(()));
    assert_eq!(routes.first(&work), Ok(()));
}

#[test]
fn a_publication_and_a_question_can_share_a_reference_without_sharing_an_answer() {
    let mut routes = Routes::default();
    let work = entry("work", "common");
    let question = entry("owner", "common");
    assert_eq!(routes.first(&work), Ok(()));
    assert_eq!(routes.first(&question), Ok(()));
    assert_eq!(routes.first(&question), Err(None));
    routes.answered(&question, "prior answer");
    assert_eq!(routes.first(&question), Err(Some("prior answer".into())));
    assert_eq!(routes.first(&work), Err(None));
    let another_sender = Message::new("owner", "another", String::new(), "", "common");
    assert_eq!(routes.first(&another_sender), Ok(()));
}
