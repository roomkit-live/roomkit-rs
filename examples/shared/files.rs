//! A database file an example writes and leaves nothing of.

use std::path::{Path, PathBuf};
use std::{env, fs, process};

/// The files SQLite keeps beside a database in the write-ahead log's mode,
/// and while a transaction rolls back.
const COMPANIONS: [&str; 3] = ["-wal", "-shm", "-journal"];

/// The path of a database file in the system's temporary directory, named
/// after the example and its process. Any file left there by an earlier
/// run of the same id is removed first, and the file and its companions
/// are removed when it is dropped.
#[derive(Debug)]
pub struct TempDatabase {
    path: PathBuf,
}

impl TempDatabase {
    /// A database path for the example `name`.
    pub fn new(name: &str) -> Self {
        let path = env::temp_dir().join(format!("roomkit-{name}-{}.db", process::id()));
        let database = Self { path };
        database.remove();
        database
    }

    /// The file's path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn remove(&self) {
        // A file that is not there is what removing it is for.
        let _absent = fs::remove_file(&self.path);
        for suffix in COMPANIONS {
            let mut companion = self.path.clone().into_os_string();
            companion.push(suffix);
            let _absent = fs::remove_file(companion);
        }
    }
}

impl Drop for TempDatabase {
    fn drop(&mut self) {
        self.remove();
    }
}
