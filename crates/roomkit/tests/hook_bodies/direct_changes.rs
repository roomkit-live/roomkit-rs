//! A refused direct change makes no write and starts no regenerated turn.

use super::*;
use pretty_assertions::assert_eq;
use roomkit::RegenerateResponseOptions;
use roomkit::model::Metadata;

#[derive(Clone, Copy, Debug)]
pub(super) enum Change {
    Update,
    Delete,
    Regenerate,
}

impl Change {
    async fn apply(self, kit: &RoomKit, event: &RoomEvent) -> Result<(), RoomKitError> {
        match self {
            Self::Update => kit
                .update_event(
                    &room_id(),
                    &event.id,
                    UpdateEventOptions {
                        metadata: Some(Metadata::from_iter([("changed".to_owned(), true.into())])),
                        ..Default::default()
                    },
                )
                .await
                .map(|_| ()),
            Self::Delete => kit
                .delete_event(&room_id(), &event.id, DeleteEventOptions::default())
                .await
                .map(|_| ()),
            Self::Regenerate => kit
                .regenerate_response(&room_id(), RegenerateResponseOptions::default())
                .await
                .map(|_| ()),
        }
    }
}

fn install_bulk(kit: &RoomKit, spare: EventId, change: Change, refused: Kept<RoomKitError>) {
    let weak = kit.downgrade();
    kit.hook(
        HookTrigger::AfterBroadcast,
        move |event: RoomEvent, _| {
            let (kit, refused, spare) =
                (weak.upgrade().unwrap(), Arc::clone(&refused), spare.clone());
            async move {
                if text_of(&event.content) != "hi" {
                    return;
                }
                for n in 0..30 {
                    kit.update_event(
                        &room_id(),
                        &spare,
                        UpdateEventOptions {
                            metadata: Some(Metadata::from_iter([("n".to_owned(), n.into())])),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                }
                if let Err(error) = change.apply(&kit, &event).await {
                    push(&refused, error);
                }
            }
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "bulk".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
}

pub(super) async fn check(change: Change) {
    let kit = room(3).await;
    let agent = Answering::new("ai");
    kit.register_channel(agent.clone()).unwrap();
    kit.attach_channel(&room_id(), &channel("ai"), AttachChannelOptions::default())
        .await
        .unwrap();
    let spare = kit
        .send_event(
            &room_id(),
            &channel("sms"),
            TextContent::new("spare"),
            SendEventOptions {
                addressed_to: Some(Vec::new()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let refused = kept();
    install_bulk(&kit, spare.id.clone(), change, Arc::clone(&refused));
    let hi = kit
        .send_event(
            &room_id(),
            &channel("sms"),
            TextContent::new("hi"),
            SendEventOptions {
                metadata: Metadata::from_iter([("kept".to_owned(), true.into())]),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), roomkit::testing::idle(&kit))
        .await
        .unwrap();
    let stored = kit.store().get_event(&hi.id).await.unwrap().unwrap();
    assert_eq!(
        taken(&refused),
        vec![RoomKitError::ReentryLoopCap(room_id())],
        "{change:?}"
    );
    assert_eq!(stored.metadata, hi.metadata, "{change:?}");
    assert_eq!(text_of(&stored.content), "hi", "{change:?}");
    assert_eq!(len(&agent.asked), 1, "{change:?}");
    let spare = kit.store().get_event(&spare.id).await.unwrap().unwrap();
    assert_eq!(
        spare.metadata,
        Metadata::from_iter([("n".to_owned(), 29.into())])
    );
    kit.close().await.unwrap();
}
