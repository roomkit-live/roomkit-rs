//! The result file a pass writes (`crate::junit`): each scenario of a small
//! set with its outcome, on one thread and on four workers; where it goes,
//! and the revision it names.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Value, json};

use crate::junit::{self, Output, Revision};
use crate::scenarios::{self, FOUR_WORKERS, ONE_THREAD, Pass};

/// A scenario that creates the room `r1` and expects to read it back as
/// `expected`: it passes with `r1` and fails with any other id.
fn reads_room(id: &str, expected: &str) -> Value {
    json!({
        "id": id,
        "setup": {"rooms": [{"id": "r1"}]},
        "steps": [{"call": "get_room", "room_id": "r1", "expect": {"result": {"id": expected}}}]
    })
}

/// A new directory's path, created by the caller.
fn new_directory() -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    std::env::temp_dir().join(format!(
        "roomkit-conformance-junit-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ))
}

/// A new directory holding `scenarios`, each at the path its id names.
fn suite_of(scenarios: &[Value]) -> PathBuf {
    let root = new_directory();
    for scenario in scenarios {
        let path = root.join(format!("{}.json", scenario["id"].as_str().unwrap()));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, scenario.to_string()).unwrap();
    }
    root
}

/// The `<testcase>` lines of `file`'s report.
fn cases_of(file: &Path) -> Vec<String> {
    std::fs::read_to_string(file)
        .unwrap()
        .lines()
        .filter(|line| line.starts_with("<testcase"))
        .map(str::to_owned)
        .collect()
}

fn each_scenario_is_written_with_its_outcome(pass: &Pass) {
    if !crate::backend_available() {
        return;
    }
    let root = suite_of(&[
        reads_room("reads-its-room", "r1"),
        reads_room("reads-another-room", "r2"),
        reads_room("listed-as-failing", "r2"),
        json!({"id": "executors/runs-apart"}),
    ]);
    let run = scenarios::run(&root, None, pass, &[("listed-as-failing", "§0: forced")]);
    let file = root.with_extension("results").join("report.xml");
    let revision = Revision {
        commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
        clean: true,
    };
    let output = Output { file, revision };
    junit::write(&output, crate::BACKEND, pass.runtime, &run.cases);
    let file = output.file;
    let report = std::fs::read_to_string(&file).unwrap();
    let cases = cases_of(&file);
    std::fs::remove_dir_all(&root).unwrap();
    std::fs::remove_dir_all(file.parent().unwrap()).unwrap();

    let class = format!("conformance.{}.{}", crate::BACKEND, pass.runtime);
    let case = |name: &str| format!(r#"<testcase classname="{class}" name="{name}""#);
    assert_eq!(cases.len(), 4, "{report}");
    assert_eq!(
        cases[0],
        format!(
            r#"{}><skipped message="§10.6.7: an executor apart needs a public turn backend"/></testcase>"#,
            case("executors/runs-apart")
        )
    );
    let known = format!(
        r#"{}><failure message="known failure: §0: forced&#10;"#,
        case("listed-as-failing")
    );
    assert!(cases[1].starts_with(&known), "{}", cases[1]);
    let forced = format!(r#"{}><failure message=""#, case("reads-another-room"));
    assert!(cases[2].starts_with(&forced), "{}", cases[2]);
    assert!(cases[2].ends_with(r#""/></testcase>"#), "{}", cases[2]);
    assert_eq!(cases[3], format!("{}/>", case("reads-its-room")));
    assert!(
        report.contains(&format!(
            r#"<testsuite name="{class}" tests="4" failures="2" errors="0" skipped="1">"#
        )),
        "{report}"
    );
    assert!(
        report.contains(
            r#"<properties><property name="commit" value="0123456789abcdef0123456789abcdef01234567"/><property name="clean" value="true"/></properties>"#
        ),
        "{report}"
    );
    assert_eq!(run.report.len(), 1, "{:?}", run.report);
}

#[test]
fn each_scenario_is_written_with_its_outcome_on_one_thread() {
    each_scenario_is_written_with_its_outcome(&ONE_THREAD);
}

#[test]
fn each_scenario_is_written_with_its_outcome_on_four_workers() {
    each_scenario_is_written_with_its_outcome(&FOUR_WORKERS);
}

#[test]
fn a_result_file_is_claimed_only_when_a_directory_is_named() {
    let root = new_directory();
    assert_eq!(
        junit::claim_in(None, &root, "memory", "current_thread"),
        None
    );
    assert_eq!(
        junit::claim_in(Some("".into()), &root, "memory", "current_thread"),
        None
    );

    let stale = root.join("results").join("memory-current_thread.xml");
    std::fs::create_dir_all(stale.parent().unwrap()).unwrap();
    std::fs::write(&stale, "an earlier run").unwrap();
    let claimed = junit::claim_in(Some("results".into()), &root, "memory", "current_thread");
    let removed = !stale.exists();
    let elsewhere = new_directory();
    let absolute = junit::claim_in(
        Some(elsewhere.clone().into()),
        &root,
        "sqlite",
        "current_thread",
    );
    std::fs::remove_dir_all(&root).unwrap();

    assert_eq!(claimed, Some(stale));
    assert!(removed, "the earlier run's file is still there");
    assert_eq!(absolute, Some(elsewhere.join("sqlite-current_thread.xml")));
}

#[test]
fn a_directory_git_cannot_read_names_no_commit() {
    let directory = new_directory();
    std::fs::create_dir_all(&directory).unwrap();
    let revision = junit::revision(&directory);
    std::fs::remove_dir_all(&directory).unwrap();
    assert_eq!(
        revision,
        Revision {
            commit: "unknown".to_owned(),
            clean: false
        }
    );
}
