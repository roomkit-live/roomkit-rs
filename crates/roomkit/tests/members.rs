//! A room's members on the tokio runtime (RFC §5.5): what a scenario
//! cannot say, the warning that names both channels when a record homed on
//! one is handed to another, and a re-join's `joined_at`. The conformance
//! scenarios `members/*` pin what each call writes, records and fires.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
#[path = "support/warnings.rs"]
mod warnings;

use pretty_assertions::assert_eq;
use roomkit::model::{ChannelId, IdentityId, ParticipantId, ParticipantStatus, RoomId};
use roomkit::{
    AddMemberOptions, CreateRoomOptions, EnsureParticipantOptions, IsMemberOptions,
    RemoveMemberOptions, RoomKit, RoomKitOptions,
};

use crate::warnings::Warnings;

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn u1() -> ParticipantId {
    ParticipantId::new("u1").unwrap()
}

/// A kit with room `r1` and `u1` a member through `ws:u1:r1`, identified
/// as `u1`, as the Python tests set it up.
async fn kit_with_u1() -> (RoomKit, RoomId) {
    let kit = RoomKit::new(RoomKitOptions::default());
    let room = RoomId::new("r1").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    })
    .await
    .unwrap();
    kit.add_member(&room, &channel("ws:u1:r1"), &u1(), identified())
        .await
        .unwrap();
    (kit, room)
}

fn identified() -> AddMemberOptions {
    AddMemberOptions {
        identity_id: Some(IdentityId::new("u1").unwrap()),
        ..AddMemberOptions::default()
    }
}

// Python: tests/test_participant_membership.py::TestChannelReuse::test_ensure_participant_warns_naming_both_channels
#[tokio::test]
async fn ensure_participant_warns_naming_both_channels() {
    let (kit, room) = kit_with_u1().await;
    let (warnings, capturing) = Warnings::capture();
    kit.ensure_participant(
        &room,
        &channel("conference:r1"),
        &u1(),
        EnsureParticipantOptions::default(),
    )
    .await
    .unwrap();
    drop(capturing);
    kit.close().await.unwrap();
    let named = warnings.containing("is recorded on channel");
    assert_eq!(
        (
            named.len(),
            named
                .iter()
                .all(|line| line.contains("ws:u1:r1") && line.contains("conference:r1")),
        ),
        (1, true)
    );
}

// Python: tests/test_participant_membership.py::TestChannelReuse::test_ensure_participant_on_the_same_channel_is_quiet
#[tokio::test]
async fn ensure_participant_on_the_same_channel_is_quiet() {
    let (kit, room) = kit_with_u1().await;
    let (warnings, capturing) = Warnings::capture();
    let got = kit
        .ensure_participant(
            &room,
            &channel("ws:u1:r1"),
            &u1(),
            EnsureParticipantOptions::default(),
        )
        .await
        .unwrap();
    drop(capturing);
    kit.close().await.unwrap();
    assert_eq!(
        (
            warnings.containing("Participant u1").len(),
            got.connected_via
        ),
        (0, vec![channel("ws:u1:r1")])
    );
}

// Python: tests/test_participant_membership.py::TestChannelReuse::test_add_member_moves_the_primary_but_keeps_the_one_it_replaces
#[tokio::test]
async fn add_member_moves_the_primary_but_keeps_the_one_it_replaces() {
    let (kit, room) = kit_with_u1().await;
    kit.remove_member(&room, &u1(), RemoveMemberOptions::default())
        .await
        .unwrap();
    let (warnings, capturing) = Warnings::capture();
    let rejoined = kit
        .add_member(&room, &channel("conference:r1"), &u1(), identified())
        .await
        .unwrap();
    drop(capturing);
    kit.close().await.unwrap();
    let named = warnings.containing("is joining through channel");
    assert_eq!(
        (
            rejoined.channel_id.clone(),
            rejoined.connected_via.clone(),
            named.len(),
            named
                .iter()
                .all(|line| line.contains("ws:u1:r1") && line.contains("conference:r1")),
        ),
        (
            channel("conference:r1"),
            vec![channel("conference:r1"), channel("ws:u1:r1")],
            1,
            true
        )
    );
}

// Python: tests/test_participant_membership.py::TestMembership::test_rejoin_flips_left_back_to_active
#[tokio::test]
async fn rejoin_flips_left_back_to_active() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let room = RoomId::new("r1").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    })
    .await
    .unwrap();
    let identity = IdentityId::new("u1").unwrap();
    let first = kit
        .add_member(&room, &channel("ws:u1:r1"), &u1(), identified())
        .await
        .unwrap();
    kit.remove_member(&room, &u1(), RemoveMemberOptions::default())
        .await
        .unwrap();
    let left = kit
        .is_member(&room, &identity, IsMemberOptions::default())
        .await
        .unwrap();
    let rejoined = kit
        .add_member(&room, &channel("ws:u1:r1"), &u1(), identified())
        .await
        .unwrap();
    let back = kit
        .is_member(&room, &identity, IsMemberOptions::default())
        .await
        .unwrap();
    kit.close().await.unwrap();
    assert_eq!(
        (
            left,
            rejoined.status,
            rejoined.joined_at == first.joined_at,
            back
        ),
        (false, ParticipantStatus::Active, true, true)
    );
}
