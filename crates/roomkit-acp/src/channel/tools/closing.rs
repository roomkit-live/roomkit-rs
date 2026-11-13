//! Closing markers stay readable until the turn's terminal item is read.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use roomkit::ResponseItem;

use super::Tools;

pub(in crate::channel) struct Closing {
    tools: Arc<Tools>,
    rows: VecDeque<ResponseItem>,
    closed: bool,
    reports: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl Closing {
    pub(in crate::channel) fn new(tools: Arc<Tools>) -> Self {
        Self {
            tools,
            rows: VecDeque::new(),
            closed: false,
            reports: None,
        }
    }

    pub(in crate::channel) fn count(&self) -> usize {
        self.tools.count()
    }

    pub(in crate::channel) fn start_reports(&self) {
        self.tools.start_reports();
    }

    pub(in crate::channel) fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<ResponseItem>> {
        if !self.closed {
            self.closed = true;
            let (rows, reports) = self.tools.finish();
            self.rows = rows.into();
            self.reports = Some(Box::pin(async move {
                for pending in reports {
                    let _gone = pending.await;
                }
            }));
        }
        if let Some(reports) = &mut self.reports {
            if reports.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            self.reports = None;
        }
        Poll::Ready(self.rows.pop_front())
    }
}

impl Drop for Closing {
    fn drop(&mut self) {
        // The reader is gone; publish and report without producing stored rows.
        let _announced = self.tools.finish();
    }
}
