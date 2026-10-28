//! The kit's one shutdown, and a close called from the kit's own work (RFC
//! §12.10.4 as RMK-1015 amended it; RMK-1017), as the Python
//! implementation's `TestOneShutdown` (`tests/test_framework.py`) and
//! `tests/test_kit_close_doors.py` prove them:
//!
//! - `shutdown`: every `close` joins the kit's one shutdown, a caller that
//!   stops waiting stopping only its own wait; it never runs twice, and its
//!   end answers every caller, a channel whose close failed or a shutdown
//!   cut before it ended included;
//! - `doors`: a `close` from the kit's own work neither waits for that work
//!   nor cuts it, whether it started the shutdown or joined one under way,
//!   and the work stores and delivers nothing once the kit is sealed;
//! - `stuck`: what waits behind spared work is dropped at once, and a turn
//!   the close cuts is waited for only until its reader's read is
//!   committed.
//!
//! The kit's own work here is what roomkit-rs has of Python's doors: a
//! SYNC hook's body, an ASYNC hook a delivery fires, a transport's
//! delivery, an ASYNC observer run after the fact, an agent's tool call, a
//! tool's observer and a task spawned with `keep_context`. Python's other
//! doors, a voice, video or realtime session's end, a delegation's
//! `on_complete`, a discussion's announcement and a held write, are
//! features roomkit-rs does not have yet.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod doors;
mod shutdown;
mod stuck;
mod support;
