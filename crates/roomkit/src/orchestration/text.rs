//! The status bus's recent entries as text for an agent's context (RFC
//! §19.8, §6.4): Python's `StatusBus.recent_text`.

use super::{RecentOptions, StatusBus};
use crate::tasks::text::{identifier, quoted};

/// How much of an entry's detail a line quotes: Python's `DETAIL_LIMIT`.
const DETAIL_LIMIT: usize = 200;

/// What [`StatusBus::recent_text`] takes: Python's `recent_text(n=10)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecentTextOptions {
    /// Python `n=10`: how many of the newest entries.
    pub n: usize,
}

impl Default for RecentTextOptions {
    fn default() -> Self {
        Self { n: 10 }
    }
}

impl StatusBus {
    /// The newest `options.n` entries as text for an agent's context, one
    /// line each, oldest first: `[HH:MM:SS] <agent>: <action> → <status>`,
    /// then ` | ` and the detail quoted when there is one; the agent and
    /// the action written as identifiers, the detail quoted so that it can
    /// close nothing (RFC §6.4). `No activity yet.` for a bus with no
    /// entry. Python's `recent_text(n=10)`.
    pub async fn recent_text(&self, options: RecentTextOptions) -> String {
        let recent = RecentOptions {
            n: options.n,
            ..RecentOptions::default()
        };
        let entries = self.recent(recent).await;
        if entries.is_empty() {
            return "No activity yet.".to_owned();
        }
        let lines: Vec<String> = entries
            .iter()
            .map(|entry| {
                // The entry's time of day, as Python's `ts[11:19]`.
                let at: String = entry.ts.chars().skip(11).take(8).collect();
                let mut line = format!(
                    "[{at}] {}: {} → {}",
                    identifier(&entry.agent_id, "agent"),
                    identifier(&entry.action, "action"),
                    entry.status
                );
                if !entry.detail.is_empty() {
                    line.push_str(" | ");
                    line.push_str(&quoted(&entry.detail, DETAIL_LIMIT));
                }
                line
            })
            .collect();
        lines.join("\n")
    }
}
