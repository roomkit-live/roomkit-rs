//! Submit each turn to the worker a host reserved (RFC §10.6.7).
//! Run `cargo run --example turns_submit_to_executor`.
//!
//! RoomKit places a room's turns itself. A host that places them by rules of
//! its own reserves a worker first, then hands the turn to that worker and no
//! other with `TurnBackend::submit_to`. `SlotBackend` is such a host's
//! backend: it wraps the turn backend the kit is given, reserves the worker
//! with the most free slots, and reads what a refusal lets it do next:
//!
//! - `no_executor`: the worker did not take the turn and nothing was handed,
//!   so the host reserves another;
//! - any other reason comes after the hand-over: the worker may have begun the
//!   turn, which is never submitted elsewhere.

use std::collections::BTreeMap;
use std::error::Error;
use std::future::ready;
use std::process::ExitCode;
use std::sync::{Arc, Mutex, PoisonError};

use roomkit::model::{
    ChannelCategory, ChannelId, ChannelType, InboundMessage, RoomId, TextContent,
};
use roomkit::store::ListEventsOptions;
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::turns::wire::{Control, Frame, TurnRequest};
use roomkit::turns::{
    ChannelControl, ChannelResult, ExecutorInfo, ExecutorInfoOptions, InMemoryTurnBackend,
    RoomOwner, ServedExecutor, TurnBackend, TurnBackendHandle, TurnFuture, TurnLink,
};
use roomkit::{
    AgentIdentity, AttachChannelOptions, ChannelDeclaration, ConversationStore, CreateRoomOptions,
    ProcessInboundOptions, RoomKit, RoomKitError, RoomKitOptions, TurnExecutor,
    TurnExecutorOptions,
};
use shared::{Checks, Script, ScriptedAgent, text_of};

/// The free slots of each worker the host knows, and what it did, in order.
#[derive(Default)]
struct Slots {
    free: Mutex<BTreeMap<String, usize>>,
    journal: Mutex<Vec<String>>,
}

impl Slots {
    /// Takes a slot of the worker with the most free ones.
    fn reserve(&self) -> Option<String> {
        let mut free = self.free.lock().unwrap_or_else(PoisonError::into_inner);
        let (worker, slots) = free.iter_mut().max_by_key(|(_, slots)| **slots)?;
        if *slots == 0 {
            return None;
        }
        *slots -= 1;
        Some(worker.clone())
    }

    fn give_back(&self, worker: &str) {
        if let Some(slots) = self
            .free
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_mut(worker)
        {
            *slots += 1;
        }
    }

    fn forget(&self, worker: &str) {
        self.free
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(worker);
    }

    fn add(&self, worker: &str, slots: usize) {
        self.free
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(worker.to_owned(), slots);
    }

    fn note(&self, line: String) {
        self.journal
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(line);
    }
}

/// A turn's link that gives its worker's slot back once the turn is let go.
struct SlotLink {
    link: Arc<dyn TurnLink>,
    slots: Arc<Slots>,
    worker: String,
}

impl TurnLink for SlotLink {
    fn request(&self) -> &TurnRequest {
        self.link.request()
    }
    fn taken(&self) -> bool {
        self.link.taken()
    }
    fn receive(&self) -> TurnFuture<'_, Result<(u64, Frame), RoomKitError>> {
        self.link.receive()
    }
    fn send(&self, message: Control) -> TurnFuture<'_, Result<(), RoomKitError>> {
        self.link.send(message)
    }
    fn release(&self) {
        self.link.release();
        self.slots.give_back(&self.worker);
    }
    fn beat_heard(&self) -> bool {
        self.link.beat_heard()
    }
}

/// The host's turn backend: each turn goes to the worker whose slot the host
/// reserved, by a rule RoomKit knows nothing of.
struct SlotBackend {
    inner: InMemoryTurnBackend,
    slots: Arc<Slots>,
}

impl TurnBackend for SlotBackend {
    fn submit(
        &self,
        request: TurnRequest,
    ) -> TurnFuture<'_, Result<Arc<dyn TurnLink>, RoomKitError>> {
        Box::pin(async move {
            loop {
                let Some(worker) = self.slots.reserve() else {
                    return Err(RoomKitError::TurnLink {
                        reason: "no_executor".into(),
                        message: Some("No worker has a free slot".into()),
                    });
                };
                let executor = ExecutorInfo::new(worker.as_str(), ExecutorInfoOptions::default());
                // A clone shares the request's mark: `handed` reads this attempt.
                match self.inner.submit_to(request.clone(), executor).await {
                    Ok(link) => {
                        self.slots.note(format!("reserved {worker}"));
                        let held: Arc<dyn TurnLink> = Arc::new(SlotLink {
                            link,
                            slots: Arc::clone(&self.slots),
                            worker,
                        });
                        return Ok(held);
                    }
                    Err(RoomKitError::TurnLink { reason, .. })
                        if reason == "no_executor" && !request.handed() =>
                    {
                        self.slots
                            .note(format!("{worker} refused before the hand-over"));
                        self.slots.forget(&worker);
                    }
                    Err(error) => {
                        // Handed: the worker may have begun it. Never elsewhere.
                        self.slots.give_back(&worker);
                        return Err(error);
                    }
                }
            }
        })
    }

    // The rest is the wrapped backend's.

    fn serve(&self, executor: Arc<dyn ServedExecutor>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        self.inner.serve(executor)
    }
    fn control(
        &self,
        message: ChannelControl,
    ) -> TurnFuture<'_, Result<Option<ChannelResult>, RoomKitError>> {
        self.inner.control(message)
    }
    fn attend(&self, owner: Arc<dyn RoomOwner>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        self.inner.attend(owner)
    }
    fn executors(&self) -> Option<TurnFuture<'_, Result<Vec<ExecutorInfo>, RoomKitError>>> {
        TurnBackend::executors(&self.inner)
    }
    fn close(&self) -> TurnFuture<'_, Result<(), RoomKitError>> {
        self.inner.close()
    }
}

fn worker(id: &str, answers: Script) -> Result<Arc<TurnExecutor>, RoomKitError> {
    TurnExecutor::new(
        move |channel| ready(Ok(Some(ScriptedAgent::new(channel, answers)))),
        TurnExecutorOptions {
            executor_id: Some(id.into()),
            hosts: Some(["coder".into()].into()),
            ..Default::default()
        },
    )
    .map(Arc::new)
}

async fn ask(
    kit: &RoomKit,
    room: &RoomId,
    chat: &ChannelId,
    coder: &ChannelId,
    text: &str,
) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
    let result = kit
        .process_inbound(
            InboundMessage::new(chat.clone(), "ana", TextContent::new(text)),
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            },
        )
        .await?;
    if result.error.is_some() {
        return Ok(None);
    }
    let timeline = kit
        .store()
        .list_events(room, ListEventsOptions::default())
        .await?;
    Ok(timeline
        .iter()
        .rfind(|event| event.source.channel_id == *coder)
        .map(|event| text_of(&event.content)))
}

async fn run() -> Result<ExitCode, Box<dyn Error + Send + Sync>> {
    let slots = Arc::new(Slots::default());
    slots.add("small", 1);
    let backend = TurnBackendHandle::new(Arc::new(SlotBackend {
        inner: InMemoryTurnBackend::new(),
        slots: Arc::clone(&slots),
    }));
    let small = worker(
        "small",
        &[("First.", "coder on small."), ("Third.", "coder on small.")],
    )?;
    let big = worker("big", &[("Second.", "coder on big.")])?;
    backend.serve(Arc::<TurnExecutor>::clone(&small)).await?;
    let kit = RoomKit::new(RoomKitOptions {
        turn_backend: Some(backend.clone()),
        ..Default::default()
    });
    let chat: ChannelId = "chat".parse()?;
    let coder: ChannelId = "coder".parse()?;
    let room: RoomId = "room".parse()?;
    kit.register_channel(RecordingTransport::new(
        chat.clone(),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    ))?;
    kit.register_channel(ChannelDeclaration {
        identity: Some(AgentIdentity::default()),
        ..ChannelDeclaration::new(coder.clone())
    })?;
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await?;
    kit.attach_channel(&room, &chat, AttachChannelOptions::default())
        .await?;
    let intelligence = AttachChannelOptions {
        category: Some(ChannelCategory::Intelligence),
        ..Default::default()
    };
    kit.attach_channel(&room, &coder, intelligence).await?;

    let mut checks = Checks::default();
    let first = ask(&kit, &room, &chat, &coder, "First.").await?;
    checks.expect(
        "small alone runs the room's first turn",
        first.as_deref() == Some("coder on small."),
    );

    backend.serve(Arc::<TurnExecutor>::clone(&big)).await?;
    slots.add("big", 4);
    let second = ask(&kit, &room, &chat, &coder, "Second.").await?;
    checks.expect(
        "the turn runs on big, reserved, though small held the room",
        second.as_deref() == Some("coder on big."),
    );

    big.close().await;
    let third = ask(&kit, &room, &chat, &coder, "Third.").await?;
    checks.expect(
        "big closed: refused before the hand-over, the turn runs on small",
        third.as_deref() == Some("coder on small."),
    );
    let journal = slots
        .journal
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    checks.expect(
        "the host reserved small, big, then big refused and small again",
        journal
            == [
                "reserved small",
                "reserved big",
                "big refused before the hand-over",
                "reserved small",
            ],
    );
    kit.close().await?;
    small.close().await;
    Ok(checks.finish())
}

fn main() -> Result<ExitCode, Box<dyn Error + Send + Sync>> {
    tokio::runtime::Runtime::new()?.block_on(run())
}
