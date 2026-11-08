//! A discussion served by several processes where a scenario cannot reach
//! (RFC §19.7.5 rule 16): each "process" is a kit of its own over one store
//! and one lock manager, as processes share a database and its locks.
//!
//! What another process stores reaches a turn running here only as its
//! driver reads the queue again, once a second, and a lease lasts 15
//! seconds: the tests of that run on tokio's paused clock, which the kits'
//! clock follows ([`TokioClock`]), as Python's patch the poll and the lease.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::any::TypeId;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::hook::AllowOptions;
use roomkit::model::*;
use roomkit::orchestration::DiscussionOptions;
use roomkit::store::{self, ConversationStore as _};
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, Environment, RecordingTransport, RecordingTransportOptions, idle,
    kit_with_environment,
};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CreateRoomOptions, Discussion, HookOptions,
    HookTrigger, InMemoryLockManager, InMemoryLockManagerOptions, InMemoryStore,
    InstallStrategyOptions, ListenOnlyOptions, Orchestration, ProcessInboundOptions, RoomKit,
    RoomKitOptions, RoomLockManager, RoomStrategyIsCurrentOptions, RoomStrategyOptions,
    SpeakQueueOptions, UninstallStrategyOptions,
};
use roomkit_core::ops::SystemEnvironment;
use roomkit_core::rules::transcoding::text_of;

const AGENTS: [&str; 3] = ["investigator", "sre", "comms"];
const KEPT: [&str; 2] = ["investigator", "sre"];

fn id(name: &str) -> ChannelId {
    ChannelId::new(name).unwrap()
}

fn room() -> RoomId {
    RoomId::new("r1").unwrap()
}

/// The kits' clock on tokio's, which a paused test advances: the lease
/// expires as its time passes. Ids are the system's, unique across the
/// processes.
#[derive(Clone)]
struct TokioClock {
    start: tokio::time::Instant,
    base: Timestamp,
}

impl TokioClock {
    fn new() -> Self {
        Self {
            start: tokio::time::Instant::now(),
            base: SystemEnvironment.now(),
        }
    }
}

impl Environment for TokioClock {
    fn now(&self) -> Timestamp {
        self.base.checked_add(self.start.elapsed()).unwrap()
    }

    fn new_event_id(&self) -> EventId {
        SystemEnvironment.new_event_id()
    }

    fn new_observation_id(&self) -> ObservationId {
        SystemEnvironment.new_observation_id()
    }

    fn new_room_id(&self) -> RoomId {
        SystemEnvironment.new_room_id()
    }
}

/// Answers once a while has passed, and logs, across processes, `+name` as
/// its turn starts, `-name` as it answers, and `xname` once its turn is cut
/// before it answered: Python's `_Slow`. `who@process` goes to `starts`, to
/// tell which process ran the turn.
#[derive(Clone)]
struct Slow {
    echo: EchoAgent,
    who: String,
    process: usize,
    log: Arc<Mutex<Vec<String>>>,
    starts: Arc<Mutex<Vec<String>>>,
    delay: Duration,
}

/// Logs `xname` when the turn it guards ends before its answer.
struct Cut {
    who: String,
    log: Arc<Mutex<Vec<String>>>,
    answered: bool,
}

impl Drop for Cut {
    fn drop(&mut self) {
        if !self.answered {
            self.log.lock().unwrap().push(format!("x{}", self.who));
        }
    }
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
            this.starts
                .lock()
                .unwrap()
                .push(format!("{}@{}", this.who, this.process));
            let mut cut = Cut {
                who: this.who.clone(),
                log: Arc::clone(&this.log),
                answered: false,
            };
            tokio::time::sleep(this.delay).await;
            cut.answered = true;
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
    starts: Arc<Mutex<Vec<String>>>,
    kits: Vec<RoomKit>,
    /// The kits' clock, on a paused test's.
    clock: Option<TokioClock>,
}

impl Cluster {
    fn new() -> Self {
        Self {
            store: Arc::new(InMemoryStore::default()),
            locks: Arc::new(InMemoryLockManager::new(
                InMemoryLockManagerOptions::default(),
            )),
            log: Arc::default(),
            starts: Arc::default(),
            kits: Vec::new(),
            clock: None,
        }
    }

    /// Processes whose clock is tokio's, for a test on its paused clock.
    fn paused() -> Self {
        Self {
            clock: Some(TokioClock::new()),
            ..Self::new()
        }
    }

    /// A process holding `ops` and the three agents, each answering after
    /// `delay`.
    fn kit(&mut self, delay: Duration) -> RoomKit {
        let options = RoomKitOptions {
            store: Arc::clone(&self.store),
            lock_manager: Some(Arc::clone(&self.locks)),
            ..RoomKitOptions::default()
        };
        let kit = match &self.clock {
            Some(clock) => kit_with_environment(options, clock.clone()),
            None => RoomKit::new(options),
        };
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
                process: self.kits.len(),
                log: Arc::clone(&self.log),
                starts: Arc::clone(&self.starts),
                delay,
            })
            .unwrap();
        }
        self.kits.push(kit.clone());
        kit
    }

    fn discussion() -> Arc<Discussion> {
        Self::discussion_of(&AGENTS)
    }

    fn discussion_of(agents: &[&str]) -> Arc<Discussion> {
        let agents = agents.iter().map(|agent| id(agent).into()).collect();
        Arc::new(Discussion::new(agents, DiscussionOptions::default()).unwrap())
    }

    /// A process that installs the discussion of all three agents in `r1`,
    /// which another process created.
    async fn installing(&mut self, delay: Duration) -> RoomKit {
        let kit = self.kit(delay);
        let options = InstallStrategyOptions::default();
        kit.install_strategy(&room(), Self::discussion(), options)
            .await
            .unwrap();
        kit
    }

    /// Until the log holds `line`, each process's drivers roused; fails
    /// past 10 s.
    async fn logged(&self, line: &str) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !self.log.lock().unwrap().iter().any(|logged| logged == line) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }

    /// Until the turn of `agent` has run to its end, cut or not, and every
    /// process is idle: Python's `_turn_over`.
    async fn turn_over(&self, agent: &str) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let ended = self
                    .log
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|line| *line == format!("-{agent}") || *line == format!("x{agent}"));
                if ended {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            for kit in &self.kits {
                idle(kit).await;
            }
        })
        .await
        .unwrap();
    }

    /// That the turn of `agent` was cut before it answered, its generation
    /// ended at once: Python's `_Slow` ends its wait on the cut.
    fn assert_cut(&self, agent: &str) {
        self.assert_cut_in(agent, "");
    }

    /// [`assert_cut`](Self::assert_cut), `case` named on a failure.
    fn assert_cut_in(&self, agent: &str, case: &str) {
        let log = self.log.lock().unwrap();
        let cut = log.contains(&format!("x{agent}")) && !log.contains(&format!("-{agent}"));
        assert!(cut, "{case} {log:?}");
    }

    /// The process that started the turn of `agent`, by its index.
    fn started(&self, agent: &str) -> usize {
        let starts = self.starts.lock().unwrap();
        let line = starts
            .iter()
            .rev()
            .find(|line| line.starts_with(&format!("{agent}@")))
            .unwrap();
        line.rsplit('@').next().unwrap().parse().unwrap()
    }

    /// The room's metadata as the store holds it.
    async fn metadata(&self) -> Metadata {
        let room = self.store.get_room(&room()).await.unwrap().unwrap();
        room.metadata
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
        self.until_within(done, Duration::from_secs(10)).await;
    }

    /// [`until`](Self::until), failing past `within`.
    async fn until_within(&self, done: impl Fn(&[String]) -> bool, within: Duration) {
        tokio::time::timeout(within, async {
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

/// The agent whose turn `kit` reads as running, as last read.
fn speaking(kit: &RoomKit) -> Option<ChannelId> {
    kit.speak_queue(&room(), SpeakQueueOptions::default())
        .and_then(|queue| queue.speaking)
}

/// Until `kit` reads no turn running, each process's drivers roused;
/// fails past `within`.
async fn until_quiet(cluster: &Cluster, kit: &RoomKit, within: Duration) {
    tokio::time::timeout(within, async {
        while speaking(kit).is_some() {
            for each in &cluster.kits {
                idle(each).await;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

// Python: tests/orchestration/test_discussion_processes.py::test_listen_only_in_one_process_cuts_the_turn_running_in_another
#[tokio::test(start_paused = true)]
async fn listen_only_in_one_process_cuts_the_turn_running_in_another() {
    let mut cluster = Cluster::paused();
    let a = cluster.installed(Duration::from_secs(2)).await;
    let follower = cluster.kit(Duration::from_secs(2));
    say(&follower, "@investigator take your time").await;
    cluster.logged("+investigator").await;

    follower
        .listen_only(&room(), &[id("investigator")], ListenOnlyOptions::default())
        .await
        .unwrap();

    until_quiet(&cluster, &a, Duration::from_secs(3)).await;
    cluster.turn_over("investigator").await;
    assert_eq!(cluster.answers().await, Vec::<String>::new());
    cluster.assert_cut("investigator");
    cluster.close().await;
}

// Python: tests/orchestration/test_discussion_processes.py::test_removing_the_agent_whose_turn_runs_cuts_it_in_the_process_giving_it
#[tokio::test(start_paused = true)]
async fn removing_the_agent_whose_turn_runs_cuts_it_in_the_process_giving_it() {
    let mut cluster = Cluster::paused();
    let a = cluster.installed(Duration::from_secs(2)).await;
    let b = cluster.installing(Duration::from_secs(2)).await;
    say(&a, "@comms take your time").await;
    cluster.logged("+comms").await;

    let options = InstallStrategyOptions::default();
    b.install_strategy(&room(), Cluster::discussion_of(&KEPT), options)
        .await
        .unwrap();

    until_quiet(&cluster, &a, Duration::from_secs(3)).await;
    cluster.turn_over("comms").await;
    assert_eq!(cluster.answers().await, Vec::<String>::new());
    cluster.assert_cut("comms");
    cluster.close().await;
}

/// Runs `action` on `kit` under the room's lock, as a host serialising its
/// own room changes: from a hook the kit calls under that lock, since a Rust
/// host cannot hold the lock manager's lock around a kit call (the names
/// table, `InMemoryLockManager`'s reentrance). It returns within a second,
/// whatever the turn running (RFC §19.7): Python's `_uninstall(locked=True)`.
async fn under_the_lock<F, Fut>(kit: &RoomKit, action: F)
where
    F: FnOnce(RoomKit) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let once = Arc::new(Mutex::new(Some(action)));
    let host = kit.clone();
    let options = HookOptions {
        name: "host".to_owned(),
        ..HookOptions::default()
    };
    let hook = move |event: RoomEvent, _context: RoomContext| {
        let action = if text_of(&event.content) == "host" {
            once.lock().unwrap().take()
        } else {
            None
        };
        let host = host.clone();
        async move {
            if let Some(action) = action {
                action(host).await;
            }
            HookResult::allow(AllowOptions::default())
        }
    };
    kit.hook(HookTrigger::BeforeBroadcast, hook, options)
        .unwrap();
    // Addressed to nobody: it asks no agent for a turn.
    let mut message = InboundMessage::new(id("ops"), "ops", TextContent::new("host"));
    message.addressed_to = Some(Vec::new());
    let options = ProcessInboundOptions {
        room_id: Some(room()),
        ..ProcessInboundOptions::default()
    };
    tokio::time::timeout(
        Duration::from_secs(1),
        kit.process_inbound(message, options),
    )
    .await
    .unwrap()
    .unwrap();
}

/// Uninstalls the discussion of `r1` from `kit`, under the room's lock when
/// `locked`, within a second: Python's `_uninstall`.
async fn uninstall(kit: &RoomKit, locked: bool) {
    if !locked {
        let room_id = room();
        let uninstalling = kit.uninstall_strategy(&room_id, UninstallStrategyOptions::default());
        let uninstalled = tokio::time::timeout(Duration::from_secs(1), uninstalling).await;
        assert!(uninstalled.unwrap().unwrap());
        return;
    }
    under_the_lock(kit, |host| async move {
        let room_id = room();
        let uninstalled = host.uninstall_strategy(&room_id, UninstallStrategyOptions::default());
        assert!(uninstalled.await.unwrap());
    })
    .await;
}

/// Who uninstalls the discussion while a turn runs, in
/// `uninstalling_while_a_turn_runs_cuts_it_and_records_nothing`.
#[derive(Clone, Copy, Debug)]
enum Uninstaller {
    GivingTheTurn,
    OtherInstaller,
    Follower,
}

// Python: tests/orchestration/test_discussion_processes.py::test_uninstalling_while_a_turn_runs_cuts_it_and_records_nothing
#[tokio::test(start_paused = true)]
async fn uninstalling_while_a_turn_runs_cuts_it_and_records_nothing() {
    for locked in [false, true] {
        for who in [
            Uninstaller::GivingTheTurn,
            Uninstaller::OtherInstaller,
            Uninstaller::Follower,
        ] {
            uninstalling_while_a_turn_runs(who, locked).await;
        }
    }
}

async fn uninstalling_while_a_turn_runs(who: Uninstaller, locked: bool) {
    let case = format!("{who:?} locked={locked}");
    let mut cluster = Cluster::paused();
    let a = cluster.installed(Duration::from_secs(2)).await;
    let b = cluster.installing(Duration::from_secs(2)).await;
    let follower = cluster.kit(Duration::from_secs(2));
    say(&follower, "@investigator take your time").await;
    cluster.logged("+investigator").await;
    let giving = cluster.started("investigator");
    let (giving, other) = if giving == 0 { (&a, &b) } else { (&b, &a) };
    let kit = match who {
        Uninstaller::GivingTheTurn => giving,
        Uninstaller::OtherInstaller => other,
        Uninstaller::Follower => &follower,
    };

    uninstall(kit, locked).await;
    cluster.turn_over("investigator").await;

    let metadata = cluster.metadata().await;
    for key in ["_speak_queue", "_discussion"] {
        let stored = metadata.get(key);
        assert!(
            stored.is_none_or(serde_json::Value::is_null),
            "{case}: {key} {stored:?}"
        );
    }
    // Cut wherever it ran.
    assert_eq!(cluster.answers().await, Vec::<String>::new(), "{case}");
    cluster.assert_cut_in("investigator", &case);
    // Each installer drops it once its driver reads the room.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let gone = [&a, &b].iter().all(|kit| {
                kit.room_strategy(&room(), RoomStrategyOptions::default())
                    .is_none()
            });
            if gone {
                break;
            }
            for kit in &cluster.kits {
                idle(kit).await;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    cluster.close().await;
}

// Python: tests/orchestration/test_discussion_processes.py::test_installing_under_the_room_lock_returns_mid_turn,
// unlocked: a Rust host cannot hold the room's lock around a kit call (the
// names table, InMemoryLockManager's reentrance), and an install from a
// hook under the lock waits while a turn runs (RMK-1580). Over the
// discussion, the speaking agent left out, its turn is cut.
#[tokio::test(start_paused = true)]
async fn installing_under_the_room_lock_returns_mid_turn() {
    let mut cluster = Cluster::paused();
    let a = cluster.installed(Duration::from_secs(2)).await;
    say(&a, "@investigator take your time").await;
    cluster.logged("+investigator").await;

    let options = InstallStrategyOptions::default();
    let discussion = Cluster::discussion_of(&["sre", "comms"]);
    let room_id = room();
    let installing = a.install_strategy(&room_id, discussion, options);
    let installed = tokio::time::timeout(Duration::from_secs(1), installing).await;
    installed.unwrap().unwrap();
    cluster.turn_over("investigator").await;

    assert_eq!(cluster.answers().await, Vec::<String>::new());
    cluster.assert_cut("investigator");
    let metadata = cluster.metadata().await;
    let agents = &metadata["_discussion"]["agents"];
    assert_eq!(agents, &serde_json::json!(["sre", "comms"]));
    cluster.close().await;
}

// Python: tests/orchestration/test_discussion_processes.py::test_a_turn_longer_than_the_lease_keeps_it:
// the process giving a turn renews its lease while the turn runs, so no
// other process gives a turn meanwhile.
#[tokio::test(start_paused = true)]
async fn a_turn_longer_than_the_lease_keeps_it() {
    let mut cluster = Cluster::paused();
    // Past the lease's 15 seconds.
    let delay = Duration::from_secs(20);
    let a = cluster.installed(delay).await;
    let b = cluster.installing(delay).await;
    say(&a, "@investigator what do the logs say?").await;
    cluster.logged("+investigator").await;

    say(&b, "@sre and the metrics?").await;
    let both = |answers: &[String]| {
        let mut answers = answers.to_vec();
        answers.sort();
        answers == ["investigator", "sre"]
    };
    cluster.until_within(both, Duration::from_secs(60)).await;

    assert_eq!(cluster.overlap(), 1);
    cluster.close().await;
}

// Python: tests/orchestration/test_discussion_processes.py::test_an_uninstall_elsewhere_is_taken_at_once_even_under_the_lock_mid_turn
#[tokio::test(start_paused = true)]
async fn an_uninstall_elsewhere_is_taken_at_once_even_under_the_lock_mid_turn() {
    let mut cluster = Cluster::paused();
    let a = cluster.installed(Duration::from_secs(2)).await;
    let b = cluster.installing(Duration::from_secs(2)).await;
    say(&a, "@investigator take your time").await;
    cluster.logged("+investigator").await;
    let uninstalled = b
        .uninstall_strategy(&room(), UninstallStrategyOptions::default())
        .await
        .unwrap();
    assert!(uninstalled);

    // Taken by the host's call under the room's lock, before A's driver
    // reads the room.
    under_the_lock(&a, |host| async move {
        let options = RoomStrategyIsCurrentOptions::default();
        let room_id = room();
        let kind = TypeId::of::<Discussion>();
        let current = host.room_strategy_is_current(&room_id, kind, options);
        assert!(!current.await.unwrap());
    })
    .await;
    assert!(
        a.room_strategy(&room(), RoomStrategyOptions::default())
            .is_none()
    );

    // Nothing of A's cut turn comes back.
    cluster.turn_over("investigator").await;
    cluster.assert_cut("investigator");
    let metadata = cluster.metadata().await;
    for key in ["_speak_queue", "_discussion"] {
        assert!(metadata.get(key).is_none_or(serde_json::Value::is_null));
    }
    assert_eq!(cluster.answers().await, Vec::<String>::new());
    cluster.close().await;
}
