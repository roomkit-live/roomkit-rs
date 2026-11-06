//! A discussion served by several processes where a scenario cannot reach
//! (RFC §19.7.5 rule 16): each "process" is a kit of its own over one store
//! and one lock manager, as processes share a database and its locks.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::*;
use roomkit::orchestration::DiscussionOptions;
use roomkit::store::{self, ConversationStore as _};
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions, idle,
};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CreateRoomOptions, Discussion,
    InMemoryLockManager, InMemoryLockManagerOptions, InMemoryStore, InstallStrategyOptions,
    Orchestration, ProcessInboundOptions, RoomKit, RoomKitOptions, RoomLockManager,
};

const AGENTS: [&str; 3] = ["investigator", "sre", "comms"];

fn id(name: &str) -> ChannelId {
    ChannelId::new(name).unwrap()
}

fn room() -> RoomId {
    RoomId::new("r1").unwrap()
}

/// Answers once a while has passed, and logs, across processes, `+name` as
/// its turn starts and `-name` as it answers: Python's `_Slow`.
#[derive(Clone)]
struct Slow {
    echo: EchoAgent,
    who: String,
    log: Arc<Mutex<Vec<String>>>,
    delay: Duration,
}

impl Channel for Slow {
    fn channel_id(&self) -> ChannelId {
        Channel::channel_id(&self.echo)
    }

    fn channel_type(&self) -> ChannelType {
        Channel::channel_type(&self.echo)
    }

    fn category(&self) -> ChannelCategory {
        Channel::category(&self.echo)
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        Channel::handle_inbound(&self.echo, message, context)
    }

    fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        Channel::deliver(&self.echo, event, binding, context)
    }

    fn on_event(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let this = self.clone();
        async move {
            this.log.lock().unwrap().push(format!("+{}", this.who));
            tokio::time::sleep(this.delay).await;
            this.log.lock().unwrap().push(format!("-{}", this.who));
            Channel::on_event(&this.echo, event, binding, context).await
        }
    }
}

/// Processes serving one room: one store, one lock manager, one log of the
/// agents' turns: Python's `_Cluster`.
struct Cluster {
    store: Arc<InMemoryStore>,
    locks: Arc<dyn RoomLockManager>,
    log: Arc<Mutex<Vec<String>>>,
    kits: Vec<RoomKit>,
}

impl Cluster {
    fn new() -> Self {
        Self {
            store: Arc::new(InMemoryStore::default()),
            locks: Arc::new(InMemoryLockManager::new(
                InMemoryLockManagerOptions::default(),
            )),
            log: Arc::default(),
            kits: Vec::new(),
        }
    }

    /// A process holding `ops` and the three agents, each answering after
    /// `delay`.
    fn kit(&mut self, delay: Duration) -> RoomKit {
        let kit = RoomKit::new(RoomKitOptions {
            store: Arc::clone(&self.store),
            lock_manager: Some(Arc::clone(&self.locks)),
            ..RoomKitOptions::default()
        });
        kit.register_channel(RecordingTransport::new(
            id("ops"),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        ))
        .unwrap();
        for agent in AGENTS {
            kit.register_channel(Slow {
                echo: EchoAgent::new(
                    id(agent),
                    format!("{agent} answers"),
                    EchoAgentOptions::default(),
                ),
                who: agent.to_owned(),
                log: Arc::clone(&self.log),
                delay,
            })
            .unwrap();
        }
        self.kits.push(kit.clone());
        kit
    }

    fn discussion() -> Arc<Discussion> {
        let agents = AGENTS.map(|agent| id(agent).into()).to_vec();
        Arc::new(Discussion::new(agents, DiscussionOptions::default()).unwrap())
    }

    /// A process that creates `r1`, binds `ops` and installs the
    /// discussion.
    async fn installed(&mut self, delay: Duration) -> RoomKit {
        let kit = self.kit(delay);
        let created = CreateRoomOptions {
            room_id: Some(room()),
            ..Default::default()
        };
        kit.create_room(created).await.unwrap();
        kit.attach_channel(&room(), &id("ops"), AttachChannelOptions::default())
            .await
            .unwrap();
        let options = InstallStrategyOptions::default();
        kit.install_strategy(&room(), Self::discussion(), options)
            .await
            .unwrap();
        kit
    }

    async fn answers(&self) -> Vec<String> {
        let options = store::ListEventsOptions::default();
        let events = self.store.list_events(&room(), options).await.unwrap();
        events
            .into_iter()
            .map(|event| event.source.channel_id.to_string())
            .filter(|channel| AGENTS.contains(&channel.as_str()))
            .collect()
    }

    /// The most turns that ran at once, read from the log.
    fn overlap(&self) -> usize {
        let (mut running, mut most) = (0usize, 0usize);
        for line in self.log.lock().unwrap().iter() {
            if line.starts_with('+') {
                running += 1;
            } else {
                running = running.saturating_sub(1);
            }
            most = most.max(running);
        }
        most
    }

    /// Until `done` holds of the answers, each process's drivers roused;
    /// fails past 10 s.
    async fn until(&self, done: impl Fn(&[String]) -> bool) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !done(&self.answers().await) {
                for kit in &self.kits {
                    idle(kit).await;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }

    async fn close(self) {
        for kit in self.kits {
            kit.close().await.unwrap();
        }
    }
}

async fn say(kit: &RoomKit, text: &str) {
    let message = InboundMessage::new(id("ops"), "ops", TextContent::new(text));
    let options = ProcessInboundOptions {
        room_id: Some(room()),
        ..ProcessInboundOptions::default()
    };
    kit.process_inbound(message, options).await.unwrap();
}

// Python: tests/orchestration/test_discussion_processes.py::test_two_processes_never_give_two_turns_at_once
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_processes_never_give_two_turns_at_once() {
    let mut cluster = Cluster::new();
    // Turns long enough to overlap, were two processes to give one each.
    let delay = Duration::from_millis(100);
    let a = cluster.installed(delay).await;
    let b = cluster.kit(delay);
    let options = InstallStrategyOptions::default();
    b.install_strategy(&room(), Cluster::discussion(), options)
        .await
        .unwrap();

    say(&a, "@investigator what do the logs say?").await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    say(&b, "@sre and the metrics?").await;
    cluster
        .until(|answers| {
            let mut answers = answers.to_vec();
            answers.sort();
            answers == ["investigator", "sre"]
        })
        .await;

    assert_eq!(cluster.overlap(), 1);
    cluster.close().await;
}

// Python: tests/orchestration/test_discussion_processes.py::test_a_direct_install_in_a_process_that_follows_takes_the_turns
#[tokio::test]
async fn a_direct_install_in_a_process_that_follows_takes_the_turns() {
    let mut cluster = Cluster::new();
    let a = cluster.installed(Duration::ZERO).await;
    a.close().await.unwrap();
    let b = cluster.kit(Duration::ZERO);
    // Followed: queued, nobody gives it.
    say(&b, "@investigator are you there?").await;
    idle(&b).await;
    assert_eq!(cluster.answers().await, Vec::<String>::new());

    let discussion = Cluster::discussion();
    Orchestration::install(discussion.as_ref(), &b, &room())
        .await
        .unwrap();
    cluster.until(|answers| answers == ["investigator"]).await;

    cluster.close().await;
}
