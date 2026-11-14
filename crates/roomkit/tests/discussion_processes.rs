//! A discussion served by several processes where a scenario cannot reach
//! (RFC §19.7.5 rule 16): each "process" is a kit of its own over one store
//! and one lock manager, as processes share a database and its locks.
//!
//! What another process stores reaches a turn running here only as its
//! driver reads the queue again, once a second, and a lease lasts 15
//! seconds: the tests of that run on tokio's paused clock, which the kits'
//! clock follows ([`LeaseClock`]), as Python's patch the poll and the lease.
//! Each process reaches the store and the lock manager through handles of
//! its own, which a test holds as a stalled database or lock would
//! ([`Holds`]), as Python's patch one process's reads and edits.

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
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions, idle,
    kit_with_environment,
};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CreateRoomOptions, Discussion, HookOptions,
    HookTrigger, InMemoryLockManager, InMemoryLockManagerOptions, InMemoryStore,
    InstallStrategyOptions, ListenOnlyOptions, Orchestration, ProcessInboundOptions, RoomKit,
    RoomKitOptions, RoomLockManager, RoomStrategyIsCurrentOptions, RoomStrategyOptions,
    SpeakQueueChange, SpeakQueueEvent, SpeakQueueOptions, UninstallStrategyOptions,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions, AiContext, AiProvider, AiResponse, ProviderError};
#[path = "support/clock.rs"]
mod clock;
#[path = "discussion_processes/held.rs"]
mod held;
use held::{HeldLocks, HeldStore, Holds, LeaseClock};
use roomkit_core::rules::transcoding::text_of;

/// A process of the cluster: a kit on its own handle on the store.
type Kit = RoomKit<HeldStore>;

const AGENTS: [&str; 3] = ["investigator", "sre", "comms"];
const KEPT: [&str; 2] = ["investigator", "sre"];

fn id(name: &str) -> ChannelId {
    ChannelId::new(name).unwrap()
}

fn room() -> RoomId {
    RoomId::new("r1").unwrap()
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
    kits: Vec<Kit>,
    /// The kits' clock, on a paused test's.
    clock: Option<LeaseClock>,
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
            clock: Some(LeaseClock::new()),
            ..Self::new()
        }
    }

    /// The options of a process's kit: its own handles on the store and the
    /// lock manager.
    fn options(&self) -> RoomKitOptions<HeldStore> {
        let holds = Arc::new(Holds::default());
        let locks: Arc<dyn RoomLockManager> = Arc::new(HeldLocks::new(&self.locks, &holds));
        RoomKitOptions {
            lock_manager: Some(locks),
            ..RoomKitOptions::with_store(HeldStore::new(&self.store, &holds))
        }
    }

    /// A process holding `ops` and the three agents, each answering after
    /// `delay`.
    fn kit(&mut self, delay: Duration) -> Kit {
        let options = self.options();
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
    async fn installing(&mut self, delay: Duration) -> Kit {
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
        self.logged_times(line, 1, Duration::from_secs(10)).await;
    }

    /// Until the log holds `line` `times` times; fails past `within`.
    async fn logged_times(&self, line: &str, times: usize, within: Duration) {
        let logged = tokio::time::timeout(within, async {
            while self.times(line) < times {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        let log = self.log.lock().unwrap();
        assert!(logged.is_ok(), "{line} x{times}: {log:?}");
    }

    /// How many times the log holds `line`.
    fn times(&self, line: &str) -> usize {
        let log = self.log.lock().unwrap();
        log.iter().filter(|logged| *logged == line).count()
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
    async fn installed(&mut self, delay: Duration) -> Kit {
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

async fn say(kit: &Kit, text: &str) {
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
fn speaking(kit: &Kit) -> Option<ChannelId> {
    kit.speak_queue(&room(), SpeakQueueOptions::default())
        .and_then(|queue| queue.speaking)
}

/// Until `kit` reads no turn running, each process's drivers roused;
/// fails past `within`.
async fn until_quiet(cluster: &Cluster, kit: &Kit, within: Duration) {
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

// Python: tests/orchestration/test_discussion_processes.py::test_listen_only_in_one_process_cuts_the_turn_running_in_another[free]
// and [stalled]: a renewal of the lease stalled in the process giving the
// turn holds back no cut; the lease outlasts the turn.
#[tokio::test(start_paused = true)]
async fn listen_only_in_one_process_cuts_the_turn_running_in_another() {
    for stalled in [false, true] {
        listen_only_cuts_the_turn_in_another(stalled).await;
    }
}

async fn listen_only_cuts_the_turn_in_another(stalled: bool) {
    let case = if stalled { "stalled" } else { "free" };
    let mut cluster = Cluster::paused();
    let a = cluster.installed(Duration::from_secs(2)).await;
    let follower = cluster.kit(Duration::from_secs(2));
    say(&follower, "@investigator take your time").await;
    cluster.logged("+investigator").await;
    if stalled {
        a.store().holds.locks.hold();
    }

    follower
        .listen_only(&room(), &[id("investigator")], ListenOnlyOptions::default())
        .await
        .unwrap();

    cluster
        .logged_times("xinvestigator", 1, Duration::from_secs(3))
        .await;
    a.store().holds.release();
    until_quiet(&cluster, &a, Duration::from_secs(3)).await;
    cluster.turn_over("investigator").await;
    assert_eq!(cluster.answers().await, Vec::<String>::new(), "{case}");
    cluster.assert_cut_in("investigator", case);
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
async fn under_the_lock<F, Fut>(kit: &Kit, action: F)
where
    F: FnOnce(Kit) -> Fut + Send + 'static,
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
async fn uninstall(kit: &Kit, locked: bool) {
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

/// How often the driver reads the stored queue, in seconds (rule 16):
/// Python's `POLL_SECONDS`.
const POLL_SECONDS: f64 = 1.0;

/// The speak queue as the store holds it now.
async fn stored_queue(cluster: &Cluster) -> serde_json::Value {
    let metadata = cluster.metadata().await;
    metadata.get("_speak_queue").cloned().unwrap_or_default()
}

/// The process holding the room's lease, as the store holds it.
async fn lease_holder(cluster: &Cluster) -> Option<String> {
    let queue = stored_queue(cluster).await;
    let holder = queue
        .get("lease_holder")
        .and_then(serde_json::Value::as_str);
    holder.map(str::to_owned)
}

/// Until the store holds the lease another process's than `holder`'s;
/// fails past 30 s. Only the store is read: a process whose reads a test
/// holds is not waited for.
async fn until_taken_from(cluster: &Cluster, holder: &str) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while lease_holder(cluster)
            .await
            .is_none_or(|holding| holding == holder)
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

/// The process holding the room's lease once one took it: the first
/// process to install the discussion, when the test installs it in the next
/// one only then. Fails past 10 s.
async fn first_holder(cluster: &Cluster) -> String {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(holder) = lease_holder(cluster).await {
                return holder;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap()
}

/// The agents whose turn `kit` tells ended, in the order told: Python's
/// `_turns_ended`.
fn turns_ended(kit: &Kit) -> Arc<Mutex<Vec<String>>> {
    let ended: Arc<Mutex<Vec<String>>> = Arc::default();
    let told = Arc::clone(&ended);
    let hook = move |event: SpeakQueueEvent, _context: RoomContext| {
        if matches!(event.change, SpeakQueueChange::TurnEnded) {
            let agents = event.channel_ids.iter().map(ToString::to_string);
            told.lock().unwrap().extend(agents);
        }
        async {}
    };
    kit.hook(HookTrigger::OnSpeakQueue, hook, HookOptions::default())
        .unwrap();
    ended
}

// Python: tests/orchestration/test_discussion_processes.py::test_a_turn_whose_lease_ends_unrenewed_is_cut_before_another_process_takes_it[held]
// and [failing]: A cannot renew its lease while investigator's turn runs,
// its room lock held or its writes of the queue failing. The turn is cut
// once the lease ends, then B takes the lease and gives investigator a
// turn again. One turn at a time, only B's commits, and the end of A's
// turn, recorded once A can, leaves B's as the one speaking (rules 6, 7
// and 16).
#[tokio::test(start_paused = true)]
async fn a_turn_whose_lease_ends_unrenewed_is_cut_before_another_process_takes_it() {
    for failing in [false, true] {
        a_turn_whose_lease_ends_unrenewed(failing).await;
    }
}

async fn a_turn_whose_lease_ends_unrenewed(failing: bool) {
    let case = if failing { "failing" } else { "held" };
    let mut cluster = Cluster::paused();
    // Past the lease's 15 seconds, and the read a taker waits past them.
    let delay = Duration::from_secs(60);
    let a = cluster.installed(delay).await;
    // A holds the lease before B installs: A gives the turn.
    let a_holds = first_holder(&cluster).await;
    let b = cluster.installing(delay).await;
    let ended = turns_ended(&a);
    say(&a, "@investigator what do the logs say?").await;
    cluster.logged("+investigator").await;
    let holds = &a.store().holds;
    if failing {
        holds.fail_queue_writes();
    } else {
        holds.locks.hold();
    }

    let queue = stored_queue(&cluster).await;
    let expires = queue
        .get("lease_expires")
        .and_then(serde_json::Value::as_f64);
    let expires = expires.unwrap();

    say(&b, "@investigator anything new?").await;
    let past_the_lease = Duration::from_secs(30);
    // When A's turn is cut, by the lease's clock; none, B's turn first.
    let clock = cluster.clock.clone().unwrap();
    let cut_at = tokio::time::timeout(past_the_lease, async {
        loop {
            if cluster.times("xinvestigator") > 0 {
                return Some(clock.seconds());
            }
            if cluster.times("+investigator") > 1 {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    cluster
        .logged_times("+investigator", 2, past_the_lease)
        .await;
    let log = cluster.log.lock().unwrap().clone();
    holds.release();
    let told = tokio::time::timeout(delay * 2, async {
        while *ended.lock().unwrap() != ["investigator"] {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    let told_ended = ended.lock().unwrap().clone();
    let log_now = cluster.log.lock().unwrap().clone();
    assert!(
        told.is_ok(),
        "{case}: ended {told_ended:?}, log {log_now:?}"
    );
    let queue = stored_queue(&cluster).await;
    // B's turn runs to its end, its answer stored.
    cluster.logged_times("-investigator", 1, delay * 2).await;
    for kit in &cluster.kits {
        idle(kit).await;
    }

    assert_eq!(
        log,
        ["+investigator", "xinvestigator", "+investigator"],
        "{case}"
    );
    // Cut before another process may take the lease, one read past its end,
    // not at that same moment (rule 7).
    assert!(
        cut_at.is_some_and(|at| at < expires + POLL_SECONDS),
        "{case}: cut at {cut_at:?}, the lease ends at {expires}"
    );
    assert_eq!(cluster.started("investigator"), 1, "{case}");
    let read = |key: &str| queue.get(key).and_then(serde_json::Value::as_str);
    assert!(
        read("lease_holder").is_some_and(|holder| holder != a_holds),
        "{case}"
    );
    assert_eq!(read("speaking"), Some("investigator"), "{case}");
    assert_eq!(cluster.answers().await, ["investigator"], "{case}");
    cluster.close().await;
}

// Python: tests/orchestration/test_discussion_processes.py::test_a_turn_whose_lease_another_process_took_is_cut:
// B uninstalls the discussion and installs it again while A's turn runs,
// A's reads held meanwhile: A next reads the lease B's, and cuts the turn
// before B gives one (rules 1 and 16). The turn would end before A's
// lease: only B's taking it cuts the turn.
#[tokio::test(start_paused = true)]
async fn a_turn_whose_lease_another_process_took_is_cut() {
    let mut cluster = Cluster::paused();
    let a = cluster.installed(Duration::from_secs(5)).await;
    let b = cluster.kit(Duration::from_millis(20));
    say(&a, "@investigator what do the logs say?").await;
    cluster.logged("+investigator").await;
    let a_holds = lease_holder(&cluster).await.unwrap();
    a.store().holds.reads.hold();
    let uninstalled = b
        .uninstall_strategy(&room(), UninstallStrategyOptions::default())
        .await
        .unwrap();
    assert!(uninstalled);
    let options = InstallStrategyOptions::default();
    b.install_strategy(&room(), Cluster::discussion(), options)
        .await
        .unwrap();
    until_taken_from(&cluster, &a_holds).await;

    a.store().holds.release();
    cluster.turn_over("investigator").await;
    say(&b, "@sre and the metrics?").await;
    cluster
        .until(|answers| answers.iter().any(|agent| agent == "sre"))
        .await;

    let log = cluster.log.lock().unwrap().clone();
    assert_eq!(log, ["+investigator", "xinvestigator", "+sre", "-sre"]);
    assert_eq!(cluster.answers().await, ["sre"]);
    cluster.close().await;
}

// Python: tests/orchestration/test_discussion_processes.py::test_an_expired_lease_is_taken_over_one_read_past_its_end:
// A's renewal stalls during its turn. B, which looks once A's lease has
// expired, leaves it to A for one read more, the one in which A cuts its
// turn when it could not renew; then B takes it (rules 7 and 16). The
// lease's clock stops at each of B's looks, as Python's.
#[tokio::test(start_paused = true)]
async fn an_expired_lease_is_taken_over_one_read_past_its_end() {
    let mut cluster = Cluster::paused();
    let a = cluster.installed(Duration::from_secs(60)).await;
    // A holds the lease before B installs: A gives the turn.
    let a_holds = first_holder(&cluster).await;
    let b = cluster.installing(Duration::from_secs(60)).await;
    say(&a, "@investigator what do the logs say?").await;
    cluster.logged("+investigator").await;
    let holds = &a.store().holds;
    holds.locks.hold();
    // A's renewal waits: the lease is as stored.
    tokio::time::timeout(Duration::from_secs(15), holds.locks.entered())
        .await
        .unwrap();
    let queue = stored_queue(&cluster).await;
    let expires = queue["lease_expires"].as_f64().unwrap();
    assert_eq!(queue["lease_holder"], a_holds.as_str());
    let clock = cluster.clock.clone().unwrap();

    clock.stop_at(expires + POLL_SECONDS / 2.0);
    idle(&b).await;
    let within_a_read = lease_holder(&cluster).await;

    clock.stop_at(expires + 1.5 * POLL_SECONDS);
    idle(&b).await;
    until_taken_from(&cluster, &a_holds).await;
    holds.release();
    assert_eq!(within_a_read, Some(a_holds));
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

/// Says what its script says, logging `+name` as it generates, `-name` as
/// it answers, and `xname` when its generation is dropped before. The
/// cluster's first investigator turn waits for `ends` (five seconds at
/// most) and, as a provider that does not stream, meets a `Cancel` only
/// once it answered: Python's `_Held`.
struct Held {
    who: String,
    log: Arc<Mutex<Vec<String>>>,
    ends: tokio::sync::watch::Receiver<bool>,
    script: ScriptedProvider,
}

impl AiProvider for Held {
    fn model_name(&self) -> &'static str {
        "held"
    }

    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        let first = {
            let mut log = self.log.lock().unwrap();
            let first = !log.contains(&format!("+{}", self.who));
            log.push(format!("+{}", self.who));
            first
        };
        let mut cut = Cut {
            who: self.who.clone(),
            log: Arc::clone(&self.log),
            answered: false,
        };
        if first && self.who == "investigator" {
            let mut ends = self.ends.clone();
            let ended = ends.wait_for(|ended| *ended);
            drop(tokio::time::timeout(Duration::from_secs(5), ended).await);
        }
        cut.answered = true;
        self.log.lock().unwrap().push(format!("-{}", self.who));
        self.script.generate(context).await
    }
}

/// Processes whose agents are AI channels on [`Held`] providers:
/// investigator asks `@sre` in every answer. Python's `_Holding` and
/// `_handing_over`.
struct Holding {
    cluster: Cluster,
    first_turn_ends: tokio::sync::watch::Sender<bool>,
}

impl Holding {
    fn new() -> Self {
        Self {
            cluster: Cluster::paused(),
            first_turn_ends: tokio::sync::watch::channel(false).0,
        }
    }

    /// A process holding `ops` and investigator and sre.
    fn kit(&mut self) -> Kit {
        let options = self.cluster.options();
        let clock = self.cluster.clock.clone().unwrap();
        let kit = kit_with_environment(options, clock);
        kit.register_channel(RecordingTransport::new(
            id("ops"),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        ))
        .unwrap();
        for agent in KEPT {
            let body = if agent == "investigator" {
                "@sre please check".to_owned()
            } else {
                format!("{agent} here")
            };
            let rounds = (0..5).map(|_| ScriptedRound::answering(body.clone()));
            let provider = Held {
                who: agent.to_owned(),
                log: Arc::clone(&self.cluster.log),
                ends: self.first_turn_ends.subscribe(),
                script: ScriptedProvider::new(rounds),
            };
            let channel = AiChannel::new(id(agent), provider, AiChannelOptions::default()).unwrap();
            kit.register_channel(channel).unwrap();
        }
        self.cluster.kits.push(kit.clone());
        kit
    }

    /// A process that creates `r1`, binds `ops` and installs the discussion
    /// of investigator and sre.
    async fn created(&mut self) -> Kit {
        let kit = self.kit();
        let created = CreateRoomOptions {
            room_id: Some(room()),
            ..Default::default()
        };
        kit.create_room(created).await.unwrap();
        kit.attach_channel(&room(), &id("ops"), AttachChannelOptions::default())
            .await
            .unwrap();
        let options = InstallStrategyOptions::default();
        kit.install_strategy(&room(), Cluster::discussion_of(&KEPT), options)
            .await
            .unwrap();
        kit
    }

    /// A gives investigator's long turn; B uninstalls, and A's watch reads
    /// it, which cuts the turn: Python's `_cut_by_an_uninstall_elsewhere`.
    async fn cut_by_an_uninstall_elsewhere(&self, a: &Kit, b: &Kit) {
        say(a, "@investigator take your time").await;
        self.cluster.logged("+investigator").await;
        let uninstalled = b
            .uninstall_strategy(&room(), UninstallStrategyOptions::default())
            .await
            .unwrap();
        assert!(uninstalled);
        // The watch reads the room once a second: past two of its reads.
        tokio::time::sleep(Duration::from_millis(2500)).await;
    }

    /// Until `kit` no longer holds the discussion, each process's drivers
    /// roused.
    async fn left(&self, kit: &Kit) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while kit
                .room_strategy(&room(), RoomStrategyOptions::default())
                .is_some()
            {
                for each in &self.cluster.kits {
                    idle(each).await;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }

    /// investigator's turn ends recorded: the `@sre` it says gives sre a
    /// turn. Python's `_sre_answers_investigator`.
    async fn sre_answers_investigator(&self, kit: &Kit) {
        let mark = self.cluster.log.lock().unwrap().len();
        say(kit, "@investigator go on").await;
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let answered = self
                    .cluster
                    .log
                    .lock()
                    .unwrap()
                    .iter()
                    .skip(mark)
                    .any(|line| line == "+sre");
                if answered {
                    break;
                }
                for each in &self.cluster.kits {
                    idle(each).await;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
}

/// That the investigator's turn the watch cut answered nothing: through
/// the `Cancel` its loop met once its provider answered, when its process
/// left the discussion at its driver's look, or its generation dropped,
/// when its host's call left it first. Only the later turn's answer is
/// stored.
async fn assert_one_investigator_answer(holding: &Holding) {
    for kit in &holding.cluster.kits {
        idle(kit).await;
    }
    let answers = holding.cluster.answers().await;
    let investigator = answers.iter().filter(|agent| *agent == "investigator");
    assert_eq!(investigator.count(), 1, "{answers:?}");
}

// Python: tests/orchestration/test_discussion_reinstall.py::test_a_process_that_saw_an_uninstall_leaves_the_discussion_installed_again
#[tokio::test(start_paused = true)]
async fn a_process_that_saw_an_uninstall_leaves_the_discussion_installed_again() {
    for host_asks in [false, true] {
        let mut holding = Holding::new();
        let a = holding.created().await;
        let b = holding.kit();
        holding.cut_by_an_uninstall_elsewhere(&a, &b).await;
        // Before A looks again.
        let options = InstallStrategyOptions::default();
        b.install_strategy(&room(), Cluster::discussion_of(&KEPT), options)
            .await
            .unwrap();

        if host_asks {
            let options = RoomStrategyIsCurrentOptions::default();
            let kind = TypeId::of::<Discussion>();
            let current = a.room_strategy_is_current(&room(), kind, options).await;
            assert!(!current.unwrap(), "host_asks");
        }
        holding.first_turn_ends.send_replace(true);

        holding.left(&a).await;
        // Past A's cut turn.
        holding.cluster.turn_over("investigator").await;
        holding.sre_answers_investigator(&b).await;
        assert_one_investigator_answer(&holding).await;
        holding.cluster.close().await;
    }
}

// Python: tests/orchestration/test_discussion_reinstall.py::test_installing_again_over_a_discussion_seen_uninstalled_starts_it_afresh
#[tokio::test(start_paused = true)]
async fn installing_again_over_a_discussion_seen_uninstalled_starts_it_afresh() {
    let mut holding = Holding::new();
    let a = holding.created().await;
    let b = holding.kit();
    holding.cut_by_an_uninstall_elsewhere(&a, &b).await;
    // A's host still wants the discussion: it installs before A looks again.
    let options = InstallStrategyOptions::default();
    a.install_strategy(&room(), Cluster::discussion_of(&KEPT), options)
        .await
        .unwrap();

    let options = RoomStrategyIsCurrentOptions::default();
    let kind = TypeId::of::<Discussion>();
    let current = a.room_strategy_is_current(&room(), kind, options).await;
    assert!(current.unwrap());
    holding.first_turn_ends.send_replace(true);
    holding.cluster.turn_over("investigator").await;
    holding.sre_answers_investigator(&a).await;
    assert_one_investigator_answer(&holding).await;
    holding.cluster.close().await;
}
