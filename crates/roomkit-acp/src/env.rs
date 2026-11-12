//! The environment a spawned agent gets, and the paths it is given.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use crate::error::AcpError;

/// The variables of the host's environment a spawned agent inherits, as the
/// Python ACP SDK's `default_environment` gives them (the MCP practice).
#[cfg(not(windows))]
const INHERITED: &[&str] = &["HOME", "LOGNAME", "PATH", "SHELL", "TERM", "USER"];

#[cfg(windows)]
const INHERITED: &[&str] = &[
    "APPDATA",
    "HOMEDRIVE",
    "HOMEPATH",
    "LOCALAPPDATA",
    "PATH",
    "PATHEXT",
    "PROCESSOR_ARCHITECTURE",
    "SYSTEMDRIVE",
    "SYSTEMROOT",
    "TEMP",
    "USERNAME",
    "USERPROFILE",
];

/// The host's inherited variables that are set, but a value that reads as
/// a shell function, which the SDK skips.
pub(crate) fn default_environment() -> Vec<(OsString, OsString)> {
    INHERITED
        .iter()
        .filter_map(|&key| std::env::var_os(key).map(|value| (OsString::from(key), value)))
        .filter(|(_, value)| !value.as_encoded_bytes().starts_with(b"()"))
        .collect()
}

/// The variables added to the default environment, Python's
/// `_resolve_spawn_env`: the `inherit_env` names `environ` sets (an unset
/// name skipped), then `env`'s entries over them; none when there is
/// nothing to add. Read at each spawn, so that a reconnect takes a rotated
/// value (a new SSH agent's socket).
pub(crate) fn resolve_spawn_env(
    inherit_env: &[String],
    env: Option<&BTreeMap<String, String>>,
    environ: impl Fn(&str) -> Option<OsString>,
) -> Option<BTreeMap<OsString, OsString>> {
    let mut merged: BTreeMap<OsString, OsString> = inherit_env
        .iter()
        .filter_map(|name| environ(name).map(|value| (OsString::from(name), value)))
        .collect();
    if let Some(env) = env {
        merged.extend(
            env.iter()
                .map(|(key, value)| (OsString::from(key), OsString::from(value))),
        );
    }
    (!merged.is_empty()).then_some(merged)
}

/// `value` as an absolute path, `~` and `~/` read with `HOME` as Python's
/// `expanduser` reads them: Python's `_absolute_path`, refusing a relative
/// path with its `ValueError`. Another user's `~name` reads as relative.
pub(crate) fn absolute_path(value: &Path, field_name: &str) -> Result<PathBuf, AcpError> {
    let expanded = expand_user(value);
    if expanded.is_absolute() {
        Ok(normalize_path(&expanded))
    } else {
        Err(AcpError::Value(format!(
            "{field_name} must be an absolute path"
        )))
    }
}

/// Match `pathlib.Path` lexically; the agent's paths need not exist locally.
fn normalize_path(value: &Path) -> PathBuf {
    #[cfg(unix)]
    {
        let bytes = value.as_os_str().as_encoded_bytes();
        if bytes.starts_with(b"//") && !bytes.starts_with(b"///") {
            // POSIX preserves exactly two leading slashes, unlike Rust's components.
            let mut normalized = PathBuf::from("//");
            normalized.extend(value.components().skip(1));
            return normalized;
        }
    }
    value.components().collect()
}

/// Validate every declared root without resolving it on the client's filesystem.
pub(crate) fn absolute_paths(
    values: Option<Vec<PathBuf>>,
    field_name: &str,
) -> Result<Vec<PathBuf>, AcpError> {
    values
        .unwrap_or_default()
        .iter()
        .map(|value| absolute_path(value, field_name))
        .collect()
}

fn expand_user(value: &Path) -> PathBuf {
    let mut components = value.components();
    let Some(std::path::Component::Normal(first)) = components.next() else {
        return value.to_path_buf();
    };
    if first != "~" {
        return value.to_path_buf();
    }
    match std::env::var_os("HOME") {
        Some(home) => {
            let mut expanded = PathBuf::from(home);
            expanded.extend(components);
            expanded
        }
        None => value.to_path_buf(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn environ(name: &str) -> Option<OsString> {
        match name {
            "SSH_AUTH_SOCK" => Some("/run/agent.sock".into()),
            "LANG" => Some("fr_CA.UTF-8".into()),
            _ => None,
        }
    }

    fn map(entries: &[(&str, &str)]) -> BTreeMap<OsString, OsString> {
        entries
            .iter()
            .map(|(key, value)| (OsString::from(key), OsString::from(value)))
            .collect()
    }

    // Python: tests/test_channels/test_acp.py::TestACPChannel::test_resolve_spawn_env
    #[test]
    fn the_spawn_env_reads_the_named_variables_then_env() {
        // Named variables are read from the parent environment; unset names skip.
        assert_eq!(
            resolve_spawn_env(&["SSH_AUTH_SOCK".into(), "MISSING".into()], None, environ),
            Some(map(&[("SSH_AUTH_SOCK", "/run/agent.sock")]))
        );
        // Explicit env entries win over inherited ones.
        let env = BTreeMap::from([
            ("SSH_AUTH_SOCK".to_owned(), "/custom.sock".to_owned()),
            ("X".to_owned(), "1".to_owned()),
        ]);
        assert_eq!(
            resolve_spawn_env(&["SSH_AUTH_SOCK".into()], Some(&env), environ),
            Some(map(&[("SSH_AUTH_SOCK", "/custom.sock"), ("X", "1")]))
        );
        // Nothing to add: none, so the default environment stays as it is.
        assert_eq!(resolve_spawn_env(&[], None, environ), None);
        assert_eq!(resolve_spawn_env(&["MISSING".into()], None, environ), None);
    }

    #[test]
    fn a_relative_path_is_refused_and_home_is_read() {
        assert!(matches!(
            absolute_path(Path::new("relative"), "cwd"),
            Err(AcpError::Value(message)) if message == "cwd must be an absolute path"
        ));
        assert_eq!(
            absolute_path(Path::new("/srv/project"), "cwd").unwrap(),
            PathBuf::from("/srv/project")
        );
        if let Some(home) = std::env::var_os("HOME") {
            assert_eq!(
                absolute_path(Path::new("~/project"), "cwd").unwrap(),
                PathBuf::from(home).join("project")
            );
        }
    }
}
