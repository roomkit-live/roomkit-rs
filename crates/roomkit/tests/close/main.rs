//! The kit's close (RFC §12.10.4 as RMK-1030 simplified it, RMK-1015,
//! RMK-1017), as the Python implementation's `tests/test_kit_close.py`,
//! `TestOneShutdown` (`tests/test_framework.py`) and
//! `tests/test_kit_close_doors.py` prove it:
//!
//! - `admission`: once `close()` started, every call but a read is refused,
//!   whoever makes it;
//! - `at_once`: what has not started is cut as the close starts, a queued
//!   delivery, a call waiting for its room's lock, and every model turn;
//! - `cut`: the running work gets one grace, then is cut, and each call the
//!   close cuts reads `kit_closed`, with what it delivered;
//! - `readers`: the reader of a cut turn's stream stores what its target
//!   received, and holds the close no longer;
//! - `answers`: a call whose run the close aborts reads the answers its
//!   cascade stored;
//! - `shutdown`: every `close` joins the kit's one shutdown, a caller that
//!   stops waiting stopping only its own wait; it never runs twice, and its
//!   end answers every caller, a channel whose close failed or a shutdown
//!   cut before it ended included;
//! - `doors`: a `close` from the kit's own work neither waits for that work
//!   nor cuts it, whether it started the shutdown or joined one under way,
//!   and the work stores and delivers nothing once the kit is sealed;
//! - `turn_doors`: a turn the close cuts, as its caller and the log read
//!   it, and a tool closing the kit from its own turn;
//! - `stuck`: what waits behind the work a close is called from is cut at
//!   once, and a spared delivery's turns are cut;
//! - `tasks`: the task, delegation and feedback calls, refused but for
//!   their reads and `cancel_task`.
//!
//! The kit's own work here is what roomkit-rs has of Python's doors: a
//! SYNC hook's body, an ASYNC hook a delivery fires, a transport's
//! delivery, an ASYNC observer run after the fact, an agent's tool call, a
//! tool's observer and a task spawned with `keep_context`. Python's other
//! doors, a voice, video or realtime session's end, a delegation's
//! `on_complete`, a discussion's announcement and a held write, are
//! features roomkit-rs does not have yet.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod admission;
mod answers;
mod at_once;
mod cut;
mod doors;
mod held;
mod readers;
mod shutdown;
mod store;
mod stuck;
mod support;
mod tasks;
mod turn_doors;
#[path = "../support/warnings.rs"]
mod warnings;
