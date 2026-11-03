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
use crate::tasks::{Kind, Phase};

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

/// How a run's work ended.
enum Worked<T> {
    /// It returned.
    Returned(T),
    /// It failed: its error was logged and posted.
    Failed,
    /// The kit's close cut it: posted cancelled, nothing handed back.
    Cut,
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
        .spawn(Kind::Turn, run_in_background(weak, run, chain_depth))
        .map(drop)
        .ok_or(RoomKitError::KitClosed)
}

/// Runs `run`, frees its room, hands its outcome back, posts its terminal
/// entry: Python's `run_in_background`.
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
    let Some(kit) = kit.upgrade() else {
        return;
    };
    let outcome = match worked(&kit, &room_id, work, &*post).await {
        Worked::Returned(outcome) => Some(outcome),
        Worked::Failed => None,
        Worked::Cut => {
            release(false);
            return;
        }
    };
    release(outcome.is_some());
    let text = told(outcome.as_ref());
    // A close that begins while the outcome is handed back cuts it, as
    // Python's cancels the hand-back under way.
    let closing = kit.inner.tasks.is_closing();
    let handed = hand_back(&kit, &room_id, &notify, text, chain_depth, Metadata::new()).await;
    let cut = !closing && kit.inner.tasks.is_closing();
    let missed = match &handed {
        _ if cut => Some("cancelled".to_owned()),
        Err(error) => {
            tracing::error!(target: "roomkit::orchestration", %error, "Handing back a background run in room {room_id} failed");
            Some(error.to_string())
        }
        Ok(delivered) => not_handed_back(delivered.as_ref()),
    };
    match (outcome, missed) {
        // Its work failed: posted so already.
        (None, _) => {}
        (Some(_), Some(missed)) => post(StatusLevel::Failed, &missed),
        (Some(outcome), None) => {
            let (level, detail) = ended(&outcome);
            post(level, detail);
        }
    }
}

/// The run's work, cut when the kit's close begins: a failure is logged and
/// posted with its message (for the logs and the status bus, never a
/// model); a cut is posted `cancelled`. On a kit already closing (a run
/// held just before its close) the work never runs. Python's `_work`.
async fn worked<S, T>(
    kit: &RoomKit<S>,
    room_id: &RoomId,
    work: BoxFuture<'static, Result<T, RoomKitError>>,
    post: &(dyn Fn(StatusLevel, &str) + Send + Sync),
) -> Worked<T>
where
    S: ConversationStore,
{
    let mut phases = kit.inner.tasks.phases();
    let closed = async move {
        // A dropped sender is a kit gone: cut too.
        drop(phases.wait_for(|phase| *phase >= Phase::Closing).await);
    };
    let ran = tokio::select! {
        biased;
        () = closed => None,
        ran = work => Some(ran),
    };
    match ran {
        None => {
            post(StatusLevel::Failed, "cancelled");
            Worked::Cut
        }
        Some(Ok(outcome)) => Worked::Returned(outcome),
        Some(Err(error)) => {
            tracing::error!(target: "roomkit::orchestration", %error, "Background run in room {room_id} failed");
            post(StatusLevel::Failed, &error.to_string());
            Worked::Failed
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use roomkit_core::model::{ChannelId, RoomId};

    use super::{BackgroundRun, background_failure_text, start_background_run};
    use crate::error::RoomKitError;
    use crate::kit::{RoomKit, RoomKitOptions};

    // Python: tests/orchestration/test_background_close.py::test_a_run_asked_for_while_the_kit_closes_never_starts[per-worker]
    // (orchestration/_background.py start_background_run: check_open)
    #[tokio::test]
    async fn a_run_asked_for_once_the_kit_closes_never_starts() {
        let kit = RoomKit::new(RoomKitOptions::default());
        kit.close().await.unwrap_or_else(|error| panic!("{error}"));
        let (worked, released) = (
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        );
        let (work, release) = (Arc::clone(&worked), Arc::clone(&released));
        let run = BackgroundRun::<()> {
            room_id: RoomId::new("r").unwrap_or_else(|error| panic!("{error}")),
            notify: ChannelId::new("boss").unwrap_or_else(|error| panic!("{error}")),
            work: Box::pin(async move {
                work.store(true, Ordering::SeqCst);
                Ok(())
            }),
            told: Box::new(|_| String::new()),
            ended: |()| (crate::orchestration::StatusLevel::Completed, "completed"),
            post: Box::new(|_, _| {}),
            release: Box::new(move |_| release.store(true, Ordering::SeqCst)),
        };

        let started = start_background_run(&kit, run, 0);
        tokio::task::yield_now().await;

        assert!(matches!(started, Err(RoomKitError::KitClosed)));
        assert!(!worked.load(Ordering::SeqCst));
        assert!(!released.load(Ordering::SeqCst));
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
