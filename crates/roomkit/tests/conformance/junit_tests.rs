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
        json!({"id": "unavailable-feature", "requires_features": ["voice", "protocol_trace"],
               "steps": [{"call": "unsupported_media_step"}]}),
        reads_room("reads-another-room", "r2"),
        reads_room("listed-as-failing", "r2"),
        json!({"id": "another-area/runs-apart", "setup": {"executors": []}}),
    ]);
    let run = scenarios::run(
        &root,
        None,
        pass,
        &[("listed-as-failing", "§0: forced")],
        None,
    );
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
    assert_eq!(cases.len(), 5, "{report}");
    assert_eq!(
        cases[0],
        format!(
            r#"{}><skipped message="§10.6.7: this pass has no Redis turn executor"/></testcase>"#,
            case("another-area/runs-apart")
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
    assert_eq!(
        cases[4],
        format!(
            r#"{}><skipped message="missing scenario features: voice, protocol_trace"/></testcase>"#,
            case("unavailable-feature")
        )
    );
    assert!(
        report.contains(&format!(
            r#"<testsuite name="{class}" tests="5" failures="2" errors="0" skipped="2">"#
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
    assert_eq!(run.failing, ["listed-as-failing", "reads-another-room"]);
}

#[test]
fn each_scenario_is_written_with_its_outcome_on_one_thread() {
    each_scenario_is_written_with_its_outcome(&ONE_THREAD);
}

#[test]
fn each_scenario_is_written_with_its_outcome_on_four_workers() {
    each_scenario_is_written_with_its_outcome(&FOUR_WORKERS);
}

/// A new scenario anywhere in the corpus uses the distributed fixture as
/// soon as it declares executors, even when no workers are initially present.
#[test]
fn a_distributed_scenario_outside_executors_reports_its_observed_failure() {
    let id = "another-area/new-distributed-case";
    let root = suite_of(&[json!({"id": id, "setup": {"executors": []}})]);
    let run = scenarios::run(
        &root,
        None,
        &ONE_THREAD,
        &[],
        Some(|scenario, _| Some(vec![format!("unexpected operation in {}", scenario["id"])])),
    );
    std::fs::remove_dir_all(root).unwrap();
    assert_eq!(run.failing, [id]);
    assert!(run.report[0].contains("unexpected operation"));
}

/// A pass that runs the scenarios with executors, across the Redis turn
/// executor, writes a file and a class of its own,
/// `<backend>+redis-executors`; a pass that skips them writes the backend's.
#[test]
fn a_pass_running_the_executor_scenarios_is_named_apart() {
    let runs: scenarios::ExecutorPass = |_, _| Some(Vec::new());
    let apart = format!("{}+redis-executors", crate::BACKEND);
    let skipped =
        r#"><skipped message="§10.6.7: this pass has no Redis turn executor"/></testcase>"#;
    for (executors, backend, outcome) in [
        (None, crate::BACKEND, skipped),
        (Some(runs), apart.as_str(), "/>"),
    ] {
        let root = suite_of(&[json!({"id": "area/runs-apart", "setup": {"executors": []}})]);
        let named = scenarios::result_backend(crate::BACKEND, executors);
        let file =
            junit::claim_in(Some("results".into()), &root, &named, ONE_THREAD.runtime).unwrap();
        let run = scenarios::run(&root, None, &ONE_THREAD, &[], executors);
        let revision = Revision {
            commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
            clean: true,
        };
        let output = Output { file, revision };
        junit::write(&output, &named, ONE_THREAD.runtime, &run.cases);
        let cases = cases_of(&output.file);
        std::fs::remove_dir_all(&root).unwrap();

        let class = format!("conformance.{backend}.current_thread");
        assert_eq!(
            output.file,
            root.join("results")
                .join(format!("{backend}-current_thread.xml"))
        );
        assert_eq!(
            cases,
            [format!(
                r#"<testcase classname="{class}" name="area/runs-apart"{outcome}"#
            )]
        );
    }
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
    let revision = junit::revision(&new_directory());
    assert_eq!(
        revision,
        Revision {
            commit: "unknown".to_owned(),
            clean: false
        }
    );
}

#[test]
fn a_run_names_its_commit_and_whether_tracked_files_differ() {
    let directory = new_directory();
    std::fs::create_dir_all(&directory).unwrap();
    let git = |arguments: &[&str]| {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(&directory)
            .args(arguments)
            .output()
            .unwrap();
        assert!(output.status.success(), "git {arguments:?}: {output:?}");
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    };
    git(&["init", "-q"]);
    std::fs::write(directory.join("tracked"), "one").unwrap();
    git(&["add", "tracked"]);
    git(&[
        "-c",
        "user.name=Conformance",
        "-c",
        "user.email=test@example.invalid",
        "-c",
        "commit.gpgsign=false",
        "-c",
        "core.hooksPath=/dev/null",
        "commit",
        "-qm",
        "fixture",
    ]);
    let commit = git(&["rev-parse", "HEAD"]);
    let clean = junit::revision(&directory);
    std::fs::write(directory.join("untracked"), "new").unwrap();
    let untracked = junit::revision(&directory);
    std::fs::write(directory.join("tracked"), "two").unwrap();
    let changed = junit::revision(&directory);
    std::fs::remove_dir_all(&directory).unwrap();

    let at = |clean| Revision {
        commit: commit.clone(),
        clean,
    };
    assert_eq!(clean, at(true));
    assert_eq!(untracked, at(true), "an untracked file is no change");
    assert_eq!(changed, at(false));
}
