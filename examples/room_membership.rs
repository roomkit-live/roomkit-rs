//! Room membership (RFC §5.5, §10.3 validation rule 3). The host records
//! who takes part in a room as its participants, each with a role: Alice
//! owns the team's room, Bob is an agent, Carol a member, all on the team's
//! WebSocket. Every hook and channel reads the roster in its context. Bob
//! deletes Carol's spam as an administrator. Carol leaves, and keeps her
//! record with the status `left`; Bob is banned, keeps his record with the
//! status `banned`, and loses the authority his role gave him: his next
//! admin delete is refused.
//!
//! roomkit-rs has no member operations yet (the RFC's `add_member` and
//! `remove_member`, with `ON_PARTICIPANT_JOINED` and `ON_PARTICIPANT_LEFT`):
//! the host writes the participants through the store.
//!
//! ```sh
//! cargo run --example room_membership
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;

use roomkit::model::{
    ChannelId, ChannelType, DeleteContent, DeleteType, EventContent, EventId, EventType,
    HookResult, InboundMessage, Participant, ParticipantId, ParticipantRole, ParticipantStatus,
    RoomId, TextContent,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, InboundResult,
    ProcessInboundOptions, RoomKit, StoreError, is_deleted,
};

use shared::{Checks, Clock, Log};

/// The team's room on its WebSocket, and the roster its hook reads.
struct Team {
    kit: RoomKit,
    clock: Clock,
    room: RoomId,
    ws: ChannelId,
    /// The active members a `BEFORE_BROADCAST` hook read, for each message.
    rosters: Log<Vec<String>>,
}

impl Team {
    async fn open() -> Result<Self, Box<dyn Error>> {
        let clock = Clock::new();
        let kit = clock.kit();
        let ws = RecordingTransport::new(ChannelId::new("ws")?, ChannelType::WEBSOCKET);
        kit.register_channel(ws.clone())?;
        let rosters = Log::new();
        let read = rosters.clone();
        // Every hook and channel reads the room's participants in its context.
        kit.before_broadcast("roster", move |_, context| {
            let active = context
                .participants
                .iter()
                .filter(|member| member.status == ParticipantStatus::Active)
                .filter_map(|member| member.display_name.clone())
                .collect();
            read.push(active);
            async { Ok(HookResult::allow()) }
        })?;
        let room = RoomId::new("team")?;
        kit.create_room(CreateRoomOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        })
        .await?;
        kit.attach_channel(&room, ws.id(), AttachChannelOptions::default())
            .await?;
        Ok(Self {
            kit,
            clock,
            room,
            ws: ws.id().clone(),
            rosters,
        })
    }

    /// The host records `id` as a participant of the room, joining on the
    /// WebSocket with `role`.
    async fn join(
        &self,
        id: &str,
        name: &str,
        role: ParticipantRole,
    ) -> Result<Participant, Box<dyn Error>> {
        let member = Participant {
            display_name: Some(name.to_owned()),
            role,
            joined_at: self.clock.now(),
            ..Participant::new(ParticipantId::new(id)?, self.room.clone(), self.ws.clone())
        };
        self.kit.store().create_participant(member.clone()).await?;
        Ok(member)
    }

    /// The host sets `member`'s status, keeping the record.
    async fn set_status(
        &self,
        member: &Participant,
        status: ParticipantStatus,
    ) -> Result<(), Box<dyn Error>> {
        let changed = Participant {
            status,
            ..member.clone()
        };
        self.kit.store().update_participant(changed).await?;
        Ok(())
    }

    /// `who` writes `text` on the WebSocket; returns the event's id.
    async fn says(&self, who: &str, text: &str) -> Result<EventId, Box<dyn Error>> {
        println!("  {who} writes {text:?}.");
        let message = InboundMessage::new(self.ws.clone(), who, TextContent::new(text));
        let result = self
            .kit
            .process_inbound(
                message,
                ProcessInboundOptions {
                    room_id: Some(self.room.clone()),
                    ..Default::default()
                },
            )
            .await?;
        Ok(result.event.ok_or("the message was refused")?.id)
    }

    /// `who` deletes `target` as an administrator (RFC §10.3).
    async fn deletes_as_admin(
        &self,
        who: &str,
        target: &EventId,
    ) -> Result<InboundResult, Box<dyn Error>> {
        println!("  {who} deletes {target} as an administrator.");
        let mut delete = DeleteContent {
            delete_type: DeleteType::Admin,
            ..DeleteContent::new(target.clone())
        };
        delete.reason = Some("moderation".to_owned());
        let content = EventContent::Delete(delete);
        let message = InboundMessage {
            event_type: EventType::Delete,
            ..InboundMessage::new(self.ws.clone(), who, content)
        };
        Ok(self
            .kit
            .process_inbound(
                message,
                ProcessInboundOptions {
                    room_id: Some(self.room.clone()),
                    ..Default::default()
                },
            )
            .await?)
    }

    /// Whether the stored event `id` is marked deleted.
    async fn is_deleted(&self, id: &EventId) -> Result<bool, Box<dyn Error>> {
        let event = self.kit.store().get_event(id).await?;
        Ok(event.as_ref().is_some_and(is_deleted))
    }
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let team = Team::open().await?;
    let mut checks = Checks::default();

    println!("The host records Alice, the owner, Bob, an agent, and Carol, a member.");
    let alice = team.join("alice", "Alice", ParticipantRole::Owner).await?;
    let bob = team.join("bob", "Bob", ParticipantRole::Agent).await?;
    let carol = team.join("carol", "Carol", ParticipantRole::Member).await?;
    let again = team.join("alice", "Alice", ParticipantRole::Owner).await;
    checks.expect(
        "a participant is one record per room and id: Alice cannot join twice",
        again.is_err_and(|error| {
            matches!(
                error.downcast_ref::<StoreError>(),
                Some(StoreError::ParticipantExists { .. })
            )
        }),
    );

    let hello = team.says("carol", "Hi team").await?;
    let spam = team.says("carol", "Cheap watches at example.com").await?;
    let removed = team.deletes_as_admin("bob", &spam).await?;
    checks.expect(
        "Bob, an agent, deletes Carol's spam as an administrator",
        !removed.blocked && team.is_deleted(&spam).await?,
    );

    println!("Carol leaves; the host marks her record left.");
    team.set_status(&carol, ParticipantStatus::Left).await?;
    let agenda = team.says("alice", "Bob, please post the agenda.").await?;

    println!("Bob is banned; the host marks his record banned.");
    team.set_status(&bob, ParticipantStatus::Banned).await?;
    let refused = team.deletes_as_admin("bob", &agenda).await?;
    checks.expect(
        "banned, Bob holds no administrator's authority: his delete is refused not_authorized",
        refused.blocked
            && refused.reason.as_deref() == Some("not_authorized")
            && !team.is_deleted(&agenda).await?,
    );

    let rosters = team.rosters.entries();
    println!("Active members each message's hook read: {rosters:?}");
    checks.expect(
        "every hook reads the room's participants: Carol is gone from the roster once she left",
        rosters
            .first()
            .is_some_and(|roster| roster == &["Alice", "Bob", "Carol"])
            && rosters
                .last()
                .is_some_and(|roster| roster == &["Alice", "Bob"]),
    );

    let members = team.kit.store().list_participants(&team.room).await?;
    let statuses: Vec<(String, ParticipantStatus)> = members
        .iter()
        .map(|member| (member.id.to_string(), member.status))
        .collect();
    println!("Participants: {statuses:?}");
    checks.expect(
        "leaving and a ban keep the records, in the order they joined",
        statuses
            == [
                (alice.id.to_string(), ParticipantStatus::Active),
                (bob.id.to_string(), ParticipantStatus::Banned),
                (carol.id.to_string(), ParticipantStatus::Left),
            ],
    );
    checks.expect(
        "Carol's message stays in the room after she left",
        !team.is_deleted(&hello).await?,
    );
    shared::print_timeline(&shared::timeline(&team.kit, &team.room).await?);
    team.kit.close().await?;
    Ok(checks.finish())
}
