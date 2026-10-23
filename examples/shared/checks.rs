//! The checks an example makes on what it ran.

use std::process::ExitCode;

/// The checks an example makes on what it ran, printed as they are made.
/// The example returns [`finish`](Self::finish) as its exit code, so a
/// failed check fails the run.
#[derive(Debug, Default)]
pub struct Checks {
    failed: usize,
}

impl Checks {
    /// Prints `PASS` or `FAIL` for `what`.
    pub fn expect(&mut self, what: &str, holds: bool) {
        if holds {
            println!("PASS {what}");
        } else {
            println!("FAIL {what}");
            self.failed += 1;
        }
    }

    /// The process's exit code: success when every check passed.
    pub fn finish(self) -> ExitCode {
        if self.failed == 0 {
            println!("All checks passed.");
            ExitCode::SUCCESS
        } else {
            println!("{} check(s) failed.", self.failed);
            ExitCode::FAILURE
        }
    }
}
