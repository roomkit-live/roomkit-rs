//! One result per scenario, as an XML test report (the `JUnit` format) that
//! roomkit-specs records as an execution receipt
//! (`conformance/TRACEABILITY.md`, "Execution receipts"). When
//! `ROOMKIT_CONFORMANCE_JUNIT` names a directory, a relative one read from
//! the repository's root, each pass writes `<backend>-<runtime>.xml` there:
//! one test case per selected scenario, named by its id, under the class
//! `conformance.<backend>.<runtime>`, and the commit the run was at, with
//! whether tracked files differed from it. A pass that runs the scenarios
//! with executors, across the Redis turn executor (`REDIS_URL` set), names
//! its backend `<backend>+redis-executors`
//! (`crate::scenarios::result_backend`). Without the variable nothing is
//! written.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

/// The variable naming the directory the result files go to.
const VARIABLE: &str = "ROOMKIT_CONFORMANCE_JUNIT";

/// A scenario's outcome in a pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Outcome {
    Passed,
    /// It did not give what the RFC says, or could not run: why.
    Failed(String),
    /// It did not run: why.
    Skipped(String),
}

/// A selected scenario and its outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Case {
    pub(crate) id: String,
    pub(crate) outcome: Outcome,
}

/// Where a pass writes its results, and the revision it ran at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Output {
    pub(crate) file: PathBuf,
    pub(crate) revision: Revision,
}

/// The commit a run is at, as git reads it in the repository, `unknown`
/// when git cannot tell, and whether the tracked files are the commit's:
/// roomkit-specs records a run only at the commit it ran at, clean.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Revision {
    pub(crate) commit: String,
    pub(crate) clean: bool,
}

/// Where the pass of `runtime` on `backend` writes its results when
/// `ROOMKIT_CONFORMANCE_JUNIT` asks for it, and the revision it runs at.
pub(crate) fn claim(backend: &str, runtime: &str) -> Option<Output> {
    // Cargo runs a test from its crate's directory.
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let file = claim_in(std::env::var_os(VARIABLE), &root, backend, runtime)?;
    Some(Output {
        file,
        revision: revision(&root),
    })
}

/// The file of the pass of `runtime` on `backend` in `directory`, a
/// relative one read from `root`, none without a directory; the file an
/// earlier run left there is removed, so a pass that stops before writing
/// leaves none.
pub(crate) fn claim_in(
    directory: Option<OsString>,
    root: &Path,
    backend: &str,
    runtime: &str,
) -> Option<PathBuf> {
    let directory = directory.filter(|directory| !directory.is_empty())?;
    let file = root
        .join(directory)
        .join(format!("{backend}-{runtime}.xml"));
    if let Err(error) = std::fs::remove_file(&file) {
        assert!(
            error.kind() == std::io::ErrorKind::NotFound,
            "cannot remove {}: {error}",
            file.display()
        );
    }
    Some(file)
}

/// The revision of the repository at `root`, as git reports it.
pub(crate) fn revision(root: &Path) -> Revision {
    let git = |arguments: &[&str]| {
        std::process::Command::new("git")
            .arg("-C")
            .arg(root)
            .args(arguments)
            .output()
            .ok()
            .filter(|output| output.status.success())
            .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
    };
    let commit = git(&["rev-parse", "HEAD"]);
    let changes = git(&["status", "--porcelain", "--untracked-files=no"]);
    match (commit, changes) {
        (Some(commit), Some(changes)) => Revision {
            commit,
            clean: changes.is_empty(),
        },
        _ => Revision {
            commit: "unknown".to_owned(),
            clean: false,
        },
    }
}

/// Writes `cases`, the pass of `runtime` on `backend`, to `output`.
pub(crate) fn write(output: &Output, backend: &str, runtime: &str, cases: &[Case]) {
    let file = &output.file;
    if let Some(directory) = file.parent() {
        std::fs::create_dir_all(directory)
            .map_err(|error| format!("cannot create {}: {error}", directory.display()))
            .unwrap();
    }
    std::fs::write(file, render(backend, runtime, &output.revision, cases))
        .map_err(|error| format!("cannot write {}: {error}", file.display()))
        .unwrap();
}

/// The report of `cases`, the pass of `runtime` on `backend` at `revision`:
/// one line per case, in the order the pass ran them.
pub(crate) fn render(backend: &str, runtime: &str, revision: &Revision, cases: &[Case]) -> String {
    let class = format!("conformance.{backend}.{runtime}");
    let count =
        |kind: fn(&Outcome) -> bool| cases.iter().filter(|case| kind(&case.outcome)).count();
    let failures = count(|outcome| matches!(outcome, Outcome::Failed(_)));
    let skipped = count(|outcome| matches!(outcome, Outcome::Skipped(_)));
    let mut report = format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
         <testsuites name=\"cargo tests\">\n\
         <testsuite name=\"{class}\" tests=\"{}\" failures=\"{failures}\" errors=\"0\" \
         skipped=\"{skipped}\">\n\
         <properties><property name=\"commit\" value=\"{}\"/>\
         <property name=\"clean\" value=\"{}\"/></properties>\n",
        cases.len(),
        escape(&revision.commit),
        revision.clean
    );
    for case in cases {
        let opening = format!(
            r#"<testcase classname="{class}" name="{}""#,
            escape(&case.id)
        );
        let line = match &case.outcome {
            Outcome::Passed => format!("{opening}/>"),
            Outcome::Failed(why) => {
                format!(
                    r#"{opening}><failure message="{}"/></testcase>"#,
                    message(why)
                )
            }
            Outcome::Skipped(why) => {
                format!(
                    r#"{opening}><skipped message="{}"/></testcase>"#,
                    message(why)
                )
            }
        };
        report.push_str(&line);
        report.push('\n');
    }
    report.push_str("</testsuite>\n</testsuites>\n");
    report
}

/// The characters of a message the report keeps: what broke, not the whole
/// diff, which the suite's own output prints.
const MESSAGE_LENGTH: usize = 1000;

/// `why`, escaped, cut after [`MESSAGE_LENGTH`] characters.
fn message(why: &str) -> String {
    let mut kept: String = why.chars().take(MESSAGE_LENGTH).collect();
    if why.chars().nth(MESSAGE_LENGTH).is_some() {
        kept.push('…');
    }
    escape(&kept)
}

/// `text` as an XML attribute value: markup and line breaks escaped, and
/// the control characters XML 1.0 cannot hold replaced.
fn escape(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\n' => escaped.push_str("&#10;"),
            '\r' => escaped.push_str("&#13;"),
            '\t' => escaped.push_str("&#9;"),
            '\u{0}'..='\u{1f}' | '\u{fffe}' | '\u{ffff}' => escaped.push('\u{fffd}'),
            other => escaped.push(other),
        }
    }
    escaped
}
