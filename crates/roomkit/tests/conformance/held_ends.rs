//! Turns whose end is owed once cut (README: a channel's `holds_end`,
//! `expect.turn_ends`).
//!
//! The kit is built over an in-memory turn backend that hands the owner each
//! turn of such a channel through a link that, once the owner sent the
//! turn's executor a `close` or a `cancel` (RFC §10.6.8), holds the turn's
//! `end` at the channel's gate: the executor applies the cut as it does any,
//! and its end reaches the owner only once a step opens the gate. Each turn
//! so cut is recorded with whether the owner, still holding the turn when
//! the gate opened, read its end. The scenario's `turn_timeouts` are the
//! backend's, as Python's runner gives the kit's backend those bounds.
//!
//! A kit given a transport takes its turns through it (the kit's
//! application path, `RoomKitOptions::turn_backend`) rather than its own
//! in-memory turns: the owner's side, its cancel bound and its account of
//! an end owed are the same on both paths, and Python's kit takes its turns
//! through an in-memory backend too. Every scenario that holds a turn's end
//! or bounds its turns, outside `executors/`, takes that path. A run that
//! carries its turns over a transport of its own, the Redis executor pass,
//! holds no end: a scenario that asks it to is refused as a setup problem.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use roomkit::turns::TurnBackendHandle;
use roomkit::turns::wire::{Control, Frame, TurnRequest};
use roomkit::turns::{
    ChannelControl, ChannelResult, ExecutorInfo, ExecutorsListener, InMemoryTurnBackend, RoomOwner,
    ServedExecutor, TurnBackend, TurnFuture, TurnLink,
};
use roomkit::{RoomKitError, RoomKitOptions, TurnTimeouts};
use serde_json::{Value, json};

use crate::identity::RunSlot;
use crate::run::lock;

/// The in-memory backend a scenario's kit is built with when it holds a
/// turn's end or bounds its turns.
pub(crate) struct HeldEnds {
    inner: InMemoryTurnBackend,
    /// The gate each channel with `holds_end` holds its cut turns' end at.
    gates: BTreeMap<String, String>,
    timeouts: Option<TurnTimeouts>,
    run: RunSlot,
}

impl HeldEnds {
    /// The backend `setup` asks for, none when it neither holds a turn's
    /// end nor bounds a turn, or when the run carries its turns over a
    /// transport of its own (`own_transport`), where holding an end is a
    /// setup problem.
    pub(crate) fn of(
        setup: &Value,
        run: &RunSlot,
        own_transport: bool,
    ) -> Result<Option<Self>, String> {
        let gates: BTreeMap<String, String> = setup["channels"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|channel| {
                let gate = channel["holds_end"].as_str()?;
                Some((channel["id"].as_str()?.to_owned(), gate.to_owned()))
            })
            .collect();
        if own_transport {
            return match gates.keys().next() {
                Some(channel) => Err(format!(
                    "{channel}: holds_end is not held over this run's own transport"
                )),
                None => Ok(None),
            };
        }
        let timeouts = timeouts(&setup["kit"]["turn_timeouts"])?;
        if gates.is_empty() && timeouts.is_none() {
            return Ok(None);
        }
        Ok(Some(Self {
            inner: InMemoryTurnBackend::new(),
            gates,
            timeouts,
            run: run.clone(),
        }))
    }
}

/// `options`, their kit's turns carried by `held` unless the run configured
/// a transport of its own, which carries them as it is.
pub(crate) fn carry<S>(
    mut options: RoomKitOptions<S>,
    held: Option<HeldEnds>,
) -> RoomKitOptions<S> {
    if options.turn_backend.is_none()
        && let Some(held) = held
    {
        options.turn_backend = Some(TurnBackendHandle::new(Arc::new(held)));
    }
    options
}

/// The bounds `turn_timeouts` names over the defaults, none when absent.
fn timeouts(spec: &Value) -> Result<Option<TurnTimeouts>, String> {
    let Some(bounds) = spec.as_object() else {
        return Ok(None);
    };
    let mut timeouts = TurnTimeouts::default();
    for (name, value) in bounds {
        let bound = value
            .as_u64()
            .map(std::time::Duration::from_millis)
            .ok_or_else(|| format!("setup.kit.turn_timeouts.{name}: not milliseconds"))?;
        match name.as_str() {
            "accept_ms" => timeouts.accept = bound,
            "heartbeat_interval_ms" => timeouts.heartbeat_interval = bound,
            "heartbeat_timeout_ms" => timeouts.heartbeat_timeout = bound,
            "cancel_ms" => timeouts.cancel = bound,
            other => return Err(format!("setup.kit.turn_timeouts: unknown {other}")),
        }
    }
    Ok(Some(timeouts))
}

impl TurnBackend for HeldEnds {
    fn submit(
        &self,
        request: TurnRequest,
    ) -> TurnFuture<'_, Result<Arc<dyn TurnLink>, RoomKitError>> {
        Box::pin(async move {
            let gate = self.gates.get(request.channel_id.as_str()).cloned();
            let link = self.inner.submit(request).await?;
            let Some(gate) = gate else {
                return Ok(link);
            };
            let held: Arc<dyn TurnLink> = Arc::new(HeldEndLink {
                link,
                gate,
                run: self.run.clone(),
                cut: Mutex::new(None),
                released: AtomicBool::new(false),
            });
            Ok(held)
        })
    }

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

    fn claim_presence<'a>(
        &'a self,
        channel_id: &'a str,
        room_id: &'a str,
        online: Option<bool>,
    ) -> Option<TurnFuture<'a, Result<bool, RoomKitError>>> {
        TurnBackend::claim_presence(&self.inner, channel_id, room_id, online)
    }

    fn watch_executors(&self, listener: ExecutorsListener) {
        self.inner.watch_executors(listener);
    }

    fn executors(&self) -> Option<TurnFuture<'_, Result<Vec<ExecutorInfo>, RoomKitError>>> {
        TurnBackend::executors(&self.inner)
    }

    fn executors_now(&self) -> Option<TurnFuture<'_, Result<Vec<ExecutorInfo>, RoomKitError>>> {
        TurnBackend::executors_now(&self.inner)
    }

    fn timeouts(&self) -> Option<TurnTimeouts> {
        self.timeouts.or_else(|| TurnBackend::timeouts(&self.inner))
    }

    fn close(&self) -> TurnFuture<'_, Result<(), RoomKitError>> {
        self.inner.close()
    }
}

/// The owner's link to one turn, its end held at `gate` once cut.
struct HeldEndLink {
    link: Arc<dyn TurnLink>,
    gate: String,
    run: RunSlot,
    /// The turn's place in its channel's `turn_ends`, once cut.
    cut: Mutex<Option<usize>>,
    released: AtomicBool,
}

impl HeldEndLink {
    /// Records the turn cut by `how`, its end not read yet: its place.
    fn record_cut(&self, how: &str) -> Option<usize> {
        let run = self.run.run()?;
        let request = self.link.request();
        let mut observed = lock(&run.observed);
        let ends = observed
            .turn_ends
            .entry(request.channel_id.to_string())
            .or_default();
        ends.push(json!({"room_id": request.room_id.as_str(), "cut": how, "read": false}));
        Some(ends.len() - 1)
    }

    /// Records that the owner read the end of the turn at `place`.
    fn record_read(&self, place: usize) {
        let Some(run) = self.run.run() else {
            return;
        };
        let channel = self.link.request().channel_id.to_string();
        if let Some(entry) = lock(&run.observed)
            .turn_ends
            .get_mut(&channel)
            .and_then(|ends| ends.get_mut(place))
        {
            entry["read"] = json!(true);
        }
    }
}

impl TurnLink for HeldEndLink {
    fn request(&self) -> &TurnRequest {
        self.link.request()
    }

    fn taken(&self) -> bool {
        self.link.taken()
    }

    fn receive(&self) -> TurnFuture<'_, Result<(u64, Frame), RoomKitError>> {
        Box::pin(async move {
            let (seq, frame) = self.link.receive().await?;
            let cut = *lock(&self.cut);
            if let (Frame::End, Some(place)) = (&frame, cut) {
                if let Some(run) = self.run.run() {
                    run.gates.hold(&self.gate).await;
                }
                // An owner that let the turn go drops this end unread.
                if !self.released.load(Ordering::SeqCst) {
                    self.record_read(place);
                }
            }
            Ok((seq, frame))
        })
    }

    fn send(&self, message: Control) -> TurnFuture<'_, Result<(), RoomKitError>> {
        let how = if matches!(message, Control::Close) {
            Some("close")
        } else if matches!(message, Control::Cancel { .. }) {
            Some("cancel")
        } else {
            None
        };
        if let Some(how) = how {
            let mut cut = lock(&self.cut);
            if cut.is_none() {
                *cut = self.record_cut(how);
            }
        }
        self.link.send(message)
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.link.release();
    }

    fn beat_heard(&self) -> bool {
        self.link.beat_heard()
    }
}
