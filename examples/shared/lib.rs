//! What the RoomKit examples share, so that each one reads as the roomkit
//! calls it shows.
//!
//! - [`kit`] and [`Clock`]: a kit whose clock moves only when told and whose
//!   ids are numbered in order, so that every run prints the same;
//! - [`Log`] and [`framework_events`]: what the example's hooks and
//!   channels note, and what the kit reports, while it runs;
//! - [`timeline`] and [`print_timeline`]: the room an example left;
//! - [`ScriptedAgent`]: an agent standing in for a model, answering from a
//!   script and keeping what it read;
//! - [`SmsNumber`]: a business's SMS number, sending to the number each
//!   room's binding names;
//! - [`OpenAiEndpoint`], [`FakeOpenAi`] and [`Reply`]: the endpoint an
//!   OpenAI agent calls, the environment's or a local fake of the OpenAI
//!   wire, and what the fake answers, texts or calls;
//! - [`WebSocketChannel`]: a WebSocket transport, its connections each
//!   following a room;
//! - [`server`] and [`bakery`]: the bakery's agent, its prompt and its
//!   `lookup_order` tool, served over WebSocket with its rooms in SQLite,
//!   which the server examples run;
//! - [`recording`]: a recording transport named by a string;
//! - [`TempDatabase`]: a database file the example leaves nothing of;
//! - [`mcp`]: a shop's MCP server over stdio, which the MCP example runs
//!   as its own process, and what is left of the processes it started;
//! - [`Checks`]: the checks that make an example verify what it shows, and
//!   its exit code.

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

mod agents;
pub mod bakery;
mod checks;
mod clock;
mod files;
pub mod mcp;
mod observe;
mod openai;
mod print;
mod recording;
pub mod server;
mod sms;
mod websocket;

pub use self::agents::{Script, ScriptedAgent, Turn};
pub use self::checks::Checks;
pub use self::clock::{Clock, kit, kit_with};
pub use self::files::TempDatabase;
pub use self::observe::{Log, framework_events};
pub use self::openai::{FakeOpenAi, OpenAiEndpoint, Reply};
pub use self::print::{every_row, print_timeline, timeline};
pub use self::recording::recording;
pub use self::sms::SmsNumber;
pub use self::websocket::{ConnectionId, Registration, WebSocketChannel, row_fields};
