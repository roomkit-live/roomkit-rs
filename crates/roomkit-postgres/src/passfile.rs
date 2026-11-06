//! A password from a password file, `~/.pgpass`, as asyncpg 0.32 reads it
//! (`_read_password_file` and `_read_password_from_pgpass` in its
//! `connect_utils.py`; HITL fa9dd934).

use std::collections::BTreeSet;
use std::io::ErrorKind;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

use roomkit::StoreError;

/// What a password is looked up for: the hosts and ports a connection tries,
/// its database and its user.
#[derive(Debug, Clone)]
pub(crate) struct Lookup {
    pub(crate) file: PathBuf,
    pub(crate) hosts: Vec<(String, u16)>,
    /// The database, else the user's name, which the server takes for it.
    pub(crate) database: Option<String>,
    pub(crate) user: Option<String>,
}

/// The file `named` (a string's `passfile`, else `PGPASSFILE`), else
/// `~/.pgpass`: `None` without a home directory. A name asyncpg takes for a
/// path is one here, an empty one the current directory.
pub(crate) fn file(named: Option<String>) -> Option<PathBuf> {
    match named {
        Some(named) if named.is_empty() => Some(PathBuf::from(".")),
        Some(named) => Some(PathBuf::from(named)),
        None => std::env::home_dir().map(|home| home.join(".pgpass")),
    }
}

/// Warns `text` once in the process, as Python's default filter shows a
/// warning once for its message.
pub(crate) fn warn_once(text: &str) {
    static WARNED: Mutex<BTreeSet<String>> = Mutex::new(BTreeSet::new());
    let first = WARNED
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(text.to_owned());
    if first {
        tracing::warn!(target: "roomkit.store", "{text}");
    }
}

/// What a password file gave: the password of the line that matched, and
/// the warning of a file left unread, which the caller warns of on its own
/// thread.
#[derive(Debug, Default)]
pub(crate) struct Found {
    pub(crate) password: Option<String>,
    pub(crate) warning: Option<String>,
}

/// Whether others than the owner have any access to the file `mode` is the
/// mode of.
#[cfg(unix)]
fn others_can_access(metadata: &std::fs::Metadata) -> bool {
    metadata.permissions().mode() & 0o077 != 0
}

/// No mode bits to read here, as asyncpg reads none on Windows.
#[cfg(not(unix))]
fn others_can_access(_: &std::fs::Metadata) -> bool {
    false
}

/// Each line of `text` that is not blank nor a comment, cut into its fields
/// at the colons no backslash escapes, at most five, the escapes kept as
/// asyncpg keeps them (HITL 8142aaf2).
fn entries(text: &str) -> Vec<Vec<String>> {
    // Python's universal newlines: `\r` ends a line too.
    text.split(['\n', '\r'])
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let line = line.replace("\\\\", "\n");
            let mut fields = Vec::new();
            let mut field = String::new();
            let mut escaped = false;
            for c in line.chars() {
                if c == ':' && !escaped && fields.len() < 4 {
                    fields.push(std::mem::take(&mut field));
                } else {
                    field.push(c);
                }
                escaped = c == '\\';
            }
            fields.push(field);
            fields
                .into_iter()
                .map(|field| field.replace('\n', "\\\\"))
                .collect()
        })
        .collect()
}

/// The entries of the password file at `path`, none if it is missing or
/// cannot be read, or, with a warning, if it is not a plain file or others
/// can read it.
fn read(path: &Path) -> Result<(Vec<Vec<String>>, Option<String>), StoreError> {
    let shown = format!("'{}'", path.display());
    let Ok(metadata) = std::fs::metadata(path) else {
        return Ok((Vec::new(), None));
    };
    if !metadata.is_file() {
        return Ok((
            Vec::new(),
            Some(format!("password file {shown} is not a plain file")),
        ));
    }
    if others_can_access(&metadata) {
        return Ok((
            Vec::new(),
            Some(format!(
                "password file {shown} has group or world access; permissions should be u=rw (0600) or less"
            )),
        ));
    }
    match std::fs::read_to_string(path) {
        Ok(text) => Ok((entries(&text), None)),
        Err(error) if error.kind() == ErrorKind::InvalidData => Err(StoreError::Value(format!(
            "password file {shown} is not UTF-8"
        ))),
        Err(_) => Ok((Vec::new(), None)),
    }
}

/// The password the first line of `lookup`'s file that matches gives, its
/// hosts in their order, `*` matching any value; asyncpg's refusal of a line
/// of fewer than five fields met before one matches. A user nobody named
/// matches `*` alone.
pub(crate) fn password(lookup: &Lookup) -> Result<Found, StoreError> {
    let (entries, warning) = read(&lookup.file)?;
    let database = lookup.database.as_deref().or(lookup.user.as_deref());
    for (host, port) in &lookup.hosts {
        let port = port.to_string();
        for entry in &entries {
            let [entry_host, entry_port, entry_database, entry_user, password] = entry.as_slice()
            else {
                return Err(StoreError::Value(format!(
                    "password file '{}': a line has {} fields where five are expected",
                    lookup.file.display(),
                    entry.len()
                )));
            };
            let matches = |field: &str, value: Option<&str>| field == "*" || Some(field) == value;
            if matches(entry_host, Some(host))
                && matches(entry_port, Some(&port))
                && matches(entry_database, database)
                && matches(entry_user, lookup.user.as_deref())
            {
                return Ok(Found {
                    password: Some(password.clone()),
                    warning,
                });
            }
        }
    }
    Ok(Found {
        password: None,
        warning,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Python: asyncpg cuts a line at its colons no backslash escapes, five
    /// fields at most, and keeps the escapes as written.
    #[test]
    fn a_line_is_cut_at_its_unescaped_colons() {
        assert_eq!(
            entries("  # a comment\n\nh:5432:db:u:pa:ss  \r\nh\\:x:1:\\\\:u:p\ro:1:d:u:q\n"),
            [
                vec!["h", "5432", "db", "u", "pa:ss"],
                vec!["h\\:x", "1", "\\\\", "u", "p"],
                vec!["o", "1", "d", "u", "q"],
            ]
        );
    }

    /// A file of `text` in a directory of the test's, owner-only.
    fn file_of(text: &str) -> (PathBuf, impl Drop) {
        struct Removed(PathBuf);
        impl Drop for Removed {
            fn drop(&mut self) {
                std::fs::remove_dir_all(&self.0).ok();
            }
        }
        static MADE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let made = MADE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let directory =
            std::env::temp_dir().join(format!("rk-passfile-{}-{made}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let file = directory.join("pgpass");
        std::fs::write(&file, text).unwrap();
        #[cfg(unix)]
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
        (file, Removed(directory))
    }

    fn lookup(file: PathBuf, hosts: &[(&str, u16)], database: Option<&str>) -> Lookup {
        Lookup {
            file,
            hosts: hosts
                .iter()
                .map(|(host, port)| ((*host).to_owned(), *port))
                .collect(),
            database: database.map(str::to_owned),
            user: Some("ann".to_owned()),
        }
    }

    /// Python (`_read_password_from_pgpass`): the hosts in their order, each
    /// against every line in its order; a database nobody named is the
    /// user's; `*` matches any value.
    #[test]
    fn the_first_line_for_the_first_host_gives_the_password() {
        let (file, _removed) =
            file_of("b:2:*:ann:for-b\na:1:ann:ann:for-a-as-ann\na:1:*:*:for-a\n");
        let found = password(&lookup(file.clone(), &[("a", 1), ("b", 2)], None)).unwrap();
        assert_eq!(found.password.as_deref(), Some("for-a-as-ann"));
        let named = password(&lookup(file.clone(), &[("a", 1)], Some("other"))).unwrap();
        assert_eq!(named.password.as_deref(), Some("for-a"));
        let none = password(&lookup(file, &[("c", 3)], None)).unwrap();
        assert_eq!((none.password, none.warning), (None, None));
    }

    /// Python: a line of fewer than five fields raises once the lookup meets
    /// it, and not if a line before it matched.
    #[test]
    fn a_short_line_met_before_a_match_is_refused() {
        let (file, _removed) = file_of("a:1:*:ann:first\nshort:line\n");
        let found = password(&lookup(file.clone(), &[("a", 1)], None)).unwrap();
        assert_eq!(found.password.as_deref(), Some("first"));
        let refused = password(&lookup(file, &[("b", 2)], None));
        assert!(matches!(refused, Err(StoreError::Value(_))), "{refused:?}");
    }
}
