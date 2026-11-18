//! One result per scenario, as an XML test report (the `JUnit` format) that
//! roomkit-specs records as an execution receipt
//! (`conformance/TRACEABILITY.md`, "Execution receipts"). When
//! `ROOMKIT_CONFORMANCE_JUNIT` names a directory, a relative one read from
//! the repository's root, each pass writes `<backend>-<runtime>.xml` there:
//! one test case per selected scenario, named by its id, under the class
//! `conformance.<backend>.<runtime>`. Without the variable nothing is
//! written.

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

/// The file the pass of `runtime` on `backend` writes when
/// `ROOMKIT_CONFORMANCE_JUNIT` asks for one, with the file an earlier run
/// left there removed: a pass that stops before writing leaves none.
pub(crate) fn claim(backend: &str, runtime: &str) -> Option<PathBuf> {
    let directory = std::env::var_os(VARIABLE).filter(|directory| !directory.is_empty())?;
    // Cargo runs a test from its crate's directory; an absolute path stays.
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
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

/// Writes `cases`, the pass of `runtime` on `backend`, to `file`.
pub(crate) fn write(file: &Path, backend: &str, runtime: &str, cases: &[Case]) {
    if let Some(directory) = file.parent() {
        std::fs::create_dir_all(directory).unwrap();
    }
    std::fs::write(file, render(backend, runtime, cases)).unwrap();
}

/// The report of `cases`, the pass of `runtime` on `backend`: one line per
/// case, in the order the pass ran them.
pub(crate) fn render(backend: &str, runtime: &str, cases: &[Case]) -> String {
    let class = format!("conformance.{backend}.{runtime}");
    let count =
        |kind: fn(&Outcome) -> bool| cases.iter().filter(|case| kind(&case.outcome)).count();
    let failures = count(|outcome| matches!(outcome, Outcome::Failed(_)));
    let skipped = count(|outcome| matches!(outcome, Outcome::Skipped(_)));
    let mut report = format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
         <testsuites name=\"cargo tests\">\n\
         <testsuite name=\"{class}\" tests=\"{}\" failures=\"{failures}\" errors=\"0\" \
         skipped=\"{skipped}\">\n",
        cases.len()
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
