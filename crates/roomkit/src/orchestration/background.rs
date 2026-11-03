//! A strategy's background run: work a tool call started, its outcome
//! handed back to whoever made the call (RFC §19.7.3, §23.3 step 8):
//! Python's `orchestration/_background.py`.
//!
//! One sequence for a supervisor's background workers: run the work, free
//! the room, hand the outcome back to the channel whose call started it,
//! then post the run's one terminal entry on the status bus. The kit
//! tracks every run until it ends, and its close cuts one at once: a run
//! cut frees its room and posts its terminal entry, failed, `cancelled`,
//! with nothing handed back; a run the close finds before its first step
//! never runs its work.

use roomkit_core::model::{ChannelId, Metadata, RoomId};

use crate::channel::BoxFuture;
use crate::error::RoomKitError;
use crate::kit::{RoomKit, WeakRoomKit};
use crate::orchestration::StatusLevel;
use crate::store::ConversationStore;
use crate::tasks::handback::{hand_back, not_handed_back};
use crate::tasks::{Kind, Marks, Phase};

/// What a run's caller reads when its work raised: the framework's words,
/// never the error's message (RFC §9.3), and nothing to fence. Python's
/// `background_failure_text`.
pub(crate) fn background_failure_text(what: &str) -> String {
    format!(
        "[Your background {what} failed: The delegated work could not be completed. Tell the \
         user.]"
    )
}

/// What a run's caller reads of its outcome.
pub(crate) type Told<T> = Box<dyn FnOnce(Option<&T>) -> String + Send>;

/// What posts a run's terminal entry on the status bus.
pub(crate) type PostEntry = Box<dyn Fn(StatusLevel, &str) + Send + Sync>;

/// One background run of a strategy, and how its outcome reads: Python's
/// `BackgroundRun`.
pub(crate) struct BackgroundRun<T> {
    /// The room of the call that started the run.
    pub(crate) room_id: RoomId,
    /// The channel whose call started it: who is told.
    pub(crate) notify: ChannelId,
    /// The run itself; it fails with an error when it fails. Dropped when
    /// the kit's close cuts the run.
    pub(crate) work: BoxFuture<'static, Result<T, RoomKitError>>,
    /// What `notify` reads of the outcome: the work's result, or `None`
    /// when it failed (never the error's message, RFC §9.3).
    pub(crate) told: Told<T>,
    /// The terminal entry of work that returned, once handed back.
    pub(crate) ended: fn(&T) -> (StatusLevel, &'static str),
    /// Posts the run's terminal entry on the status bus.
    pub(crate) post: PostEntry,
    /// Frees the room, told whether the work returned: the model's turn on
    /// the outcome may start a new run.
    pub(crate) release: Box<dyn FnOnce(bool) + Send>,
}

/// Starts `run` as a task the kit tracks until it ends, its outcome handed
/// back at `chain_depth`, the depth of the turn whose call started it
/// (RFC §23.3): Python's `start_background_run(kit, run_in_background(kit,
/// run))`.
///
/// # Errors
///
/// [`RoomKitError::KitClosed`] once the kit's close began: the run never
/// starts, nothing posted or released.
pub(crate) fn start_background_run<S, T>(
    kit: &RoomKit<S>,
    run: BackgroundRun<T>,
    chain_depth: u32,
) -> Result<(), RoomKitError>
where
    S: ConversationStore,
    T: Send + 'static,
{
    kit.inner.open()?;
    let weak = kit.downgrade();
    kit.inner
        .tasks
        // A run of its own, apart from the call that started it, which
        // ends before it.
        .spawn_alone(Kind::Turn, run_in_background(weak, run, chain_depth))
        .map(drop)
        .ok_or(RoomKitError::KitClosed)
}

/// Runs `run`, frees its room, hands its outcome back, posts its terminal
/// entry: Python's `run_in_background`. The kit's close cuts it wherever
/// it is, its work or its hand-back under way, unless the close is called
/// from within the run itself (its hand-back's own turn).
async fn run_in_background<S, T>(kit: WeakRoomKit<S>, run: BackgroundRun<T>, chain_depth: u32)
where
    S: ConversationStore,
    T: Send + 'static,
{
    let BackgroundRun {
        room_id,
        notify,
        work,
        told,
        ended,
        post,
        release,
    } = run;
    let mut ending = Ending {
        post,
        release: Some(release),
        posted: false,
    };
    let Some(kit) = kit.upgrade() else {
        return;
    };
    // The run's own unit: a close called from within it spares it.
    let marks = Marks::current();
    let ran = tokio::select! {
        biased;
        () = cut_by_close(&kit, &marks) => None,
        ran = work => Some(ran),
    };
    let outcome = match ran {
        // Cut: posted cancelled and freed as the guard drops.
        None => return,
        Some(Ok(outcome)) => Some(outcome),
        Some(Err(error)) => {
            // For the logs and the status bus, never a model (RFC §9.3).
            tracing::error!(target: "roomkit::orchestration", %error, "Background run in room {room_id} failed");
            ending.post(StatusLevel::Failed, &error.to_string());
            None
        }
    };
    ending.release(outcome.is_some());
    let text = told(outcome.as_ref());
    let handing = hand_back(&kit, &room_id, &notify, text, chain_depth, Metadata::new());
    let handed = tokio::select! {
        biased;
        () = cut_by_close(&kit, &marks) => None,
        handed = handing => Some(handed),
    };
    let missed = match handed {
        None => Some("cancelled".to_owned()),
        Some(Err(error)) => {
            tracing::error!(target: "roomkit::orchestration", %error, "Handing back a background run in room {room_id} failed");
            Some(error.to_string())
        }
        Some(Ok(delivered)) => not_handed_back(delivered.as_ref()),
    };
    match (outcome, missed) {
        // Its work failed: posted so already.
        (None, _) => {}
        (Some(_), Some(missed)) => ending.post(StatusLevel::Failed, &missed),
        (Some(outcome), None) => {
            let (level, detail) = ended(&outcome);
            ending.post(level, detail);
        }
    }
}

/// Resolves once the kit's close began, at once on a kit already closing,
/// unless the close was called from within `marks`, the run's own work,
/// which it spares (RFC §12.10.4): Python's close cuts every background
/// run but those it is called under.
async fn cut_by_close<S: ConversationStore>(kit: &RoomKit<S>, marks: &Marks) {
    let mut phases = kit.inner.tasks.phases();
    // A dropped sender is a kit gone: cut too.
    let closing = phases
        .wait_for(|phase| *phase >= Phase::Closing)
        .await
        .map(drop);
    if closing.is_ok() && kit.inner.tasks.spares(marks) {
        std::future::pending::<()>().await;
    }
}

/// What ends a run however it ends: its room freed once, its terminal entry
/// posted once. A run dropped before either, cut by the close or aborted
/// with the kit's left work, frees its room and posts `cancelled`, as
/// Python's `finally` and its cancelled run do.
struct Ending {
    post: PostEntry,
    release: Option<Box<dyn FnOnce(bool) + Send>>,
    posted: bool,
}

impl Ending {
    fn release(&mut self, returned: bool) {
        if let Some(release) = self.release.take() {
            release(returned);
        }
    }

    fn post(&mut self, level: StatusLevel, detail: &str) {
        self.posted = true;
        (self.post)(level, detail);
    }
}

impl Drop for Ending {
    fn drop(&mut self) {
        if !self.posted {
            self.post(StatusLevel::Failed, "cancelled");
        }
        self.release(false);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, PoisonError};

    use roomkit_core::model::{ChannelId, RoomId};

    use super::{BackgroundRun, background_failure_text, start_background_run};
    use crate::error::RoomKitError;
    use crate::kit::{RoomKit, RoomKitOptions};
    use crate::orchestration::StatusLevel;

    /// What a test run did: its work, its releases, its posts.
    #[derive(Clone, Default)]
    struct Seen(Arc<Mutex<Vec<String>>>);

    impl Seen {
        fn note(&self, what: String) {
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(what);
        }

        fn read(&self) -> Vec<String> {
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        }
    }

    /// A run whose work, release and posts note what they do in `seen`.
    fn run(seen: &Seen) -> BackgroundRun<()> {
        let (work, post, release) = (seen.clone(), seen.clone(), seen.clone());
        BackgroundRun {
            room_id: RoomId::new("r").unwrap_or_else(|error| panic!("{error}")),
            notify: ChannelId::new("boss").unwrap_or_else(|error| panic!("{error}")),
            work: Box::pin(async move {
                work.note("worked".to_owned());
                Ok(())
            }),
            told: Box::new(|_| String::new()),
            ended: |()| (StatusLevel::Completed, "completed"),
            post: Box::new(move |level, detail| post.note(format!("{level:?} {detail}"))),
            release: Box::new(move |returned| release.note(format!("released {returned}"))),
        }
    }

    // Python: tests/orchestration/test_background_close.py::test_a_run_asked_for_while_the_kit_closes_never_starts[per-worker]
    // (orchestration/_background.py start_background_run: check_open)
    #[tokio::test]
    async fn a_run_asked_for_once_the_kit_closes_never_starts() {
        let kit = RoomKit::new(RoomKitOptions::default());
        // The close begun, not over: the kit still spawns the work it spares.
        kit.inner.tasks.close();
        let seen = Seen::default();

        let started = start_background_run(&kit, run(&seen), 0);
        tokio::task::yield_now().await;

        assert!(matches!(started, Err(RoomKitError::KitClosed)));
        assert!(seen.read().is_empty(), "{:?}", seen.read());
    }

    // Python: tests/orchestration/test_background_close.py::test_a_run_held_as_the_kit_closes_ends_cancelled_without_its_work[per-worker]
    #[tokio::test]
    async fn a_run_the_close_finds_before_its_first_step_ends_cancelled_without_its_work() {
        let kit = RoomKit::new(RoomKitOptions::default());
        let seen = Seen::default();

        start_background_run(&kit, run(&seen), 0).unwrap_or_else(|error| panic!("{error}"));
        kit.inner.tasks.close();
        crate::testing::idle(&kit).await;

        assert_eq!(seen.read(), ["Failed cancelled", "released false"]);
    }

    // Python: orchestration/_background.py background_failure_text, as
    // _inject_per_worker.py _worker_told reads it for a run that raised.
    #[test]
    fn a_failed_run_is_told_in_the_framework_s_words() {
        assert_eq!(
            background_failure_text("task for researcher"),
            "[Your background task for researcher failed: The delegated work could not be \
             completed. Tell the user.]"
        );
    }
}
