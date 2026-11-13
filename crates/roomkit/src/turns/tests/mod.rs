//! The turn backend's tests: what crosses and how, end to end through a
//! kit, with the faults a transport could make.

mod cross_read;
mod cut_reads;
mod executor_close;
mod liveness;
mod never_started;
mod records;
mod support;
mod trace;
mod verb_depth;
mod window;
