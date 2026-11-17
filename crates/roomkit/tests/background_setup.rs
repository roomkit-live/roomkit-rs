//! The standalone setup an application can construct before registering an agent.

use roomkit::RoomKitError;
use roomkit::tasks::BackgroundSetup;

#[test]
fn a_setup_preserves_its_worker_text() {
    for worker in ["", " leading", "a,b"] {
        let setup = BackgroundSetup::new(Some(worker.to_owned()), "post").unwrap();
        assert_eq!(setup.worker(), Some(worker));
        assert_eq!(setup.hand_back(), "post");
    }
}

#[test]
fn an_unknown_hand_back_is_rejected_at_construction() {
    assert!(matches!(
        BackgroundSetup::new(None, "bad"),
        Err(RoomKitError::Value(message)) if message == "hand_back must be one of ('relay', 'post'), not 'bad'"
    ));
}
