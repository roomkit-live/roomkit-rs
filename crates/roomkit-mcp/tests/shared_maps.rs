//! Shared handles preserve Python dictionaries while allowing atomic updates.

use roomkit_mcp::{McpEnvironment, McpHeaders};
use std::ffi::OsStr;
use std::sync::{Arc, Barrier};

#[test]
fn headers_keep_exact_keys_order_and_detached_snapshots() {
    let headers = McpHeaders::new([
        ("x-token".into(), "first".into()),
        ("X-Token".into(), "second".into()),
        ("x-token".into(), "replacement".into()),
    ]);
    let mut detached = headers.get();
    let alias = headers.clone();
    assert_eq!(
        alias.insert("x-token".into(), "third".into()).as_deref(),
        Some("replacement")
    );
    assert_eq!(
        headers.get(),
        vec![
            ("x-token".into(), "third".into()),
            ("X-Token".into(), "second".into())
        ]
    );
    assert_eq!(alias.remove("x-token").as_deref(), Some("third"));
    assert_eq!(alias.insert("x-token".into(), "last".into()), None);
    detached.clear();
    assert_eq!(
        headers.get(),
        vec![
            ("X-Token".into(), "second".into()),
            ("x-token".into(), "last".into())
        ]
    );
    alias.replace([("new".into(), "only".into())]);
    assert_eq!(headers.get(), vec![("new".into(), "only".into())]);
    assert_eq!(alias.remove("missing"), None);
}

#[test]
fn environment_aliases_share_atomic_changes_but_reads_are_owned() {
    let environment = McpEnvironment::new([("KEY".into(), "first".into())].into());
    let mut detached = environment.get();
    let alias = environment.clone();
    assert_eq!(
        alias.insert("KEY".into(), "second".into()).as_deref(),
        Some(OsStr::new("first"))
    );
    detached.clear();
    assert_eq!(
        environment
            .get()
            .get(OsStr::new("KEY"))
            .map(std::ffi::OsString::as_os_str),
        Some(OsStr::new("second"))
    );
    alias.replace([("OTHER".into(), "new".into())].into());
    assert_eq!(
        environment.remove(OsStr::new("OTHER")).as_deref(),
        Some(OsStr::new("new"))
    );
    assert_eq!(alias.get().len(), 0);
    assert_eq!(alias.remove(OsStr::new("missing")), None);
}

#[test]
fn concurrent_insertions_do_not_lose_distinct_keys() {
    let headers = McpHeaders::default();
    let environment = McpEnvironment::default();
    let ready = Arc::new(Barrier::new(2));
    std::thread::scope(|scope| {
        for name in ["one", "two"] {
            let (headers, environment, ready) =
                (headers.clone(), environment.clone(), Arc::clone(&ready));
            scope.spawn(move || {
                ready.wait();
                headers.insert(name.into(), name.into());
                environment.insert(name.into(), name.into());
            });
        }
    });
    let mut actual = headers.get();
    actual.sort();
    assert_eq!(
        actual,
        vec![("one".into(), "one".into()), ("two".into(), "two".into())]
    );
    assert_eq!(
        environment.get(),
        [("one".into(), "one".into()), ("two".into(), "two".into())].into()
    );
}
