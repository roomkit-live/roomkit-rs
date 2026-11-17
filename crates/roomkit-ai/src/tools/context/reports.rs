//! Per-occurrence report rights retained by public turn callbacks.

use std::collections::HashMap;
use std::future::Future;

use super::current;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

/// A callback owns this book, never a different turn's current context.
pub(super) struct Reports {
    pub(super) channel: String,
    owner: Arc<()>,
    calls: Mutex<Calls>,
}

#[derive(Default)]
struct Calls {
    entries: Vec<Arc<Occurrence>>,
    holders: HashMap<String, Arc<Occurrence>>,
}

#[derive(Debug)]
pub(crate) struct Occurrence {
    id: String,
    owner: Arc<()>,
    reported: AtomicBool,
    duplicate: bool,
}

impl Occurrence {
    pub(crate) fn detached(id: &str) -> Arc<Self> {
        Self::new(id, Arc::new(()), false)
    }

    fn new(id: &str, owner: Arc<()>, duplicate: bool) -> Arc<Self> {
        Arc::new(Self {
            id: id.to_owned(),
            owner,
            reported: AtomicBool::new(false),
            duplicate,
        })
    }

    pub(crate) fn claim(&self) -> bool {
        !self.reported.swap(true, Ordering::AcqRel)
    }

    pub(crate) fn reported(&self) -> bool {
        self.reported.load(Ordering::Acquire)
    }

    pub(crate) fn duplicate(&self) -> bool {
        self.duplicate
    }

    pub(crate) fn callback(self: &Arc<Self>) -> Arc<dyn Fn() -> bool + Send + Sync> {
        let occurrence = Arc::clone(self);
        Arc::new(move || occurrence.claim())
    }
}

impl Reports {
    pub(super) fn new(channel: &str) -> Self {
        Self {
            channel: channel.to_owned(),
            owner: Arc::new(()),
            calls: Mutex::default(),
        }
    }

    pub(super) fn announce(&self, id: &str) -> Arc<Occurrence> {
        let mut calls = self.calls.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = Occurrence::new(id, Arc::clone(&self.owner), calls.holders.contains_key(id));
        calls
            .holders
            .entry(id.to_owned())
            .or_insert_with(|| Arc::clone(&entry));
        calls.entries.push(Arc::clone(&entry));
        entry
    }

    pub(super) fn next_round(&self) {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .holders
            .clear();
    }

    pub(super) fn entry(
        &self,
        id: &str,
        reporting: Option<&Arc<Occurrence>>,
    ) -> Option<Arc<Occurrence>> {
        if let Some(current) = reporting
            && current.id == id
            && Arc::ptr_eq(&current.owner, &self.owner)
        {
            return Some(Arc::clone(current));
        }
        let calls = self.calls.lock().unwrap_or_else(PoisonError::into_inner);
        calls
            .holders
            .get(id)
            .or_else(|| calls.entries.iter().rev().find(|entry| entry.id == id))
            .cloned()
    }
}

/// Returns the announced call's once-only report right in this channel's turn.
/// `None` outside an AI-loop context, for another channel, or for an
/// unknown id. An explicit `tool_turn_context` alone announces no call.
///
/// Clones share the right. A retained callback resolves its id in its captured
/// turn at invocation: a later round may reuse that id, a different turn cannot.
/// A successful report consumes the right; an unreported failed standalone call
/// remains claimable after its turn. Claiming does not change the model result.
pub fn turn_report_claim(
    call_id: &str,
    channel_id: &str,
) -> Option<Arc<dyn Fn() -> bool + Send + Sync>> {
    let scope = current()?;
    let reports = scope.reports.as_ref()?;
    if reports.channel != channel_id {
        return None;
    }
    reports.entry(call_id, scope.reporting.as_ref())?;
    let reports = Arc::clone(reports);
    let id = call_id.to_owned();
    Some(Arc::new(move || {
        let scope = current();
        let reporting = scope.as_ref().and_then(|scope| scope.reporting.as_ref());
        reports
            .entry(&id, reporting)
            .is_some_and(|entry| entry.claim())
    }))
}

pub(crate) fn announce_tool_call(id: &str) -> Arc<Occurrence> {
    current()
        .and_then(|scope| scope.reports.clone())
        .map_or_else(|| Occurrence::detached(id), |reports| reports.announce(id))
}

pub(crate) fn next_report_round() {
    if let Some(reports) = current().and_then(|scope| scope.reports.clone()) {
        reports.next_round();
    }
}

pub(crate) fn within_reporting<F: Future>(
    entry: Arc<Occurrence>,
    work: F,
) -> impl Future<Output = F::Output> {
    // Box before building the scope future, keeping nested provider reports
    // from embedding their complete generation future on the stack.
    let work = Box::pin(work);
    async move {
        let Some(parent) = current() else {
            return work.await;
        };
        let mut scope = (*parent).clone();
        scope.reporting = Some(entry);
        roomkit::within_tool_call_scope(Arc::new(scope), work).await
    }
}
