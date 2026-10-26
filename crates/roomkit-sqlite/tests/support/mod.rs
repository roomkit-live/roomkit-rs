//! A database file a test opens, removed with its journal files once the
//! test is done with it.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// The files a test made so far in this process.
static MADE: AtomicU64 = AtomicU64::new(0);

/// A path for a new database file, under the target's directory for tests'
/// files, removed when dropped.
#[derive(Debug)]
pub(crate) struct TempFile {
    path: PathBuf,
}

impl TempFile {
    /// A path no other test of any process uses, `name` telling which test
    /// made it.
    pub(crate) fn new(name: &str) -> Self {
        let directory = Path::new(env!("CARGO_TARGET_TMPDIR")).join("roomkit-sqlite");
        std::fs::create_dir_all(&directory).unwrap();
        let made = MADE.fetch_add(1, Ordering::Relaxed);
        Self {
            path: directory.join(format!("{name}-{}-{made}.db", std::process::id())),
        }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let mut path = self.path.clone().into_os_string();
            path.push(suffix);
            // A file the test never made, a journal among them, is none to
            // remove.
            std::fs::remove_file(path).ok();
        }
    }
}
