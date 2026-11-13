//! The wire pinned to the Python implementation's codec (step 4, milestone
//! 7): every message its in-memory backend carried for `turns_probe`'s
//! case 1, a streamed answer whose tool sends an event through its turn, is
//! parsed by this crate's wire and written back with nothing lost.
//!
//! The file is `tests/fixtures/turns/python-case1.jsonl`, written by
//! `tests/fixtures/turns/make_fixture.py` (`make turns-fixture`) at the
//! roomkit commit `tests/fixtures/turns/ROOMKIT_COMMIT` names. The other
//! direction, Python parsing what this crate writes, is `read_rust.py`'s,
//! on `rust-case1.jsonl` (crates/roomkit/tests/turns).

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::PathBuf;

use pretty_assertions::assert_eq;
use roomkit_core::model::Timestamp;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::turns::wire::{ControlEnvelope, FrameEnvelope, TurnRequest};

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/turns")
}

/// Each line of the fixture: its family and its message.
fn lines() -> Vec<(String, Value)> {
    std::fs::read_to_string(fixtures().join("python-case1.jsonl"))
        .unwrap()
        .lines()
        .map(|line| {
            let line: Value = serde_json::from_str(line).unwrap();
            let family = line["direction"].as_str().unwrap().to_owned();
            (family, line["message"].clone())
        })
        .collect()
}

/// `message` of `family`, parsed by this crate's wire and written back.
fn rewritten(family: &str, message: &Value) -> Value {
    match family {
        "request" => {
            let parsed: TurnRequest = serde_json::from_value(message.clone()).unwrap();
            serde_json::to_value(parsed).unwrap()
        }
        "frame" => {
            let parsed: FrameEnvelope = serde_json::from_value(message.clone()).unwrap();
            serde_json::to_value(parsed).unwrap()
        }
        "control" => {
            let parsed: ControlEnvelope = serde_json::from_value(message.clone()).unwrap();
            serde_json::to_value(parsed).unwrap()
        }
        other => panic!("no family {other}"),
    }
}

/// What the rewrite of `before` lost, and the members it added: an instant
/// compares as the instant it writes, a number as its value (the SQLite
/// cross-read's rule).
fn differences(
    path: &str,
    before: &Value,
    after: &Value,
    lost: &mut Vec<String>,
    added: &mut BTreeSet<String>,
) {
    match (before, after) {
        (Value::Object(before), Value::Object(after)) => {
            for (member, value) in before {
                let path = format!("{path}.{member}");
                match after.get(member) {
                    Some(rewritten) => differences(&path, value, rewritten, lost, added),
                    None => lost.push(format!("{path} lost: {value}")),
                }
            }
            for member in after.keys().filter(|member| !before.contains_key(*member)) {
                added.insert(format!("{path}.{member}"));
            }
        }
        (Value::Array(before), Value::Array(after)) if before.len() == after.len() => {
            for (value, rewritten) in before.iter().zip(after) {
                differences(&format!("{path}[]"), value, rewritten, lost, added);
            }
        }
        (Value::Number(before), Value::Number(after)) => {
            let (before, after) = (before.as_f64().unwrap(), after.as_f64().unwrap());
            if (before - after).abs() > 1e-6 {
                lost.push(format!("{path} changed: {before} to {after}"));
            }
        }
        (Value::String(before), Value::String(after)) if before != after => {
            let instants = (before.parse::<Timestamp>(), after.parse::<Timestamp>());
            if !matches!(instants, (Ok(before), Ok(after)) if before == after) {
                lost.push(format!("{path} changed: {before:?} to {after:?}"));
            }
        }
        _ if before != after => lost.push(format!("{path} changed: {before} to {after}")),
        _ => {}
    }
}

/// The fixture is the one `make turns-fixture` wrote, at a roomkit commit.
#[test]
fn the_fixture_is_the_one_its_generator_wrote() {
    let commit = std::fs::read_to_string(fixtures().join("ROOMKIT_COMMIT")).unwrap();
    let recorded = std::fs::read_to_string(fixtures().join("FIXTURE_SHA256")).unwrap();
    let file = std::fs::read(fixtures().join("python-case1.jsonl")).unwrap();
    let digest = Sha256::digest(file)
        .iter()
        .fold(String::new(), |mut text, byte| {
            let _ = write!(text, "{byte:02x}");
            text
        });
    assert_eq!(
        (
            commit.trim().len() == 40 && commit.trim().bytes().all(|byte| byte.is_ascii_hexdigit()),
            digest.as_str()
        ),
        (true, recorded.trim()),
        "the fixture differs from its generation: run `make turns-fixture`"
    );
}

/// RFC §1.4, §10.6.3: every message Python's in-memory backend carried for
/// the turn is read, and writing it back loses none of its members. This
/// historical fixture predates the optional history boundary: its absence
/// reads as unknown, written as null. That is the only added member.
#[test]
fn every_message_python_carried_reads_and_rewrites_with_nothing_lost() {
    let (mut lost, mut added) = (Vec::new(), BTreeSet::new());
    let lines = lines();
    for (line, (family, message)) in lines.iter().enumerate() {
        let after = rewritten(family, message);
        if family == "request" {
            assert_eq!(
                after.pointer("/context/history_start_index"),
                Some(&Value::Null)
            );
        }
        differences(
            &format!("{line}:{family}"),
            message,
            &after,
            &mut lost,
            &mut added,
        );
    }
    let member = |path: &str| {
        path.split_once('.')
            .map_or(path.to_owned(), |(_, member)| member.to_owned())
    };
    let lost: BTreeSet<String> = lost.iter().map(|path| member(path)).collect();
    let added: BTreeSet<String> = added.iter().map(|path| member(path)).collect();
    assert_eq!(
        (lines.len(), lost, added),
        (
            29,
            BTreeSet::new(),
            BTreeSet::from(["context.history_start_index".to_owned()])
        )
    );
}

/// Python: tests/test_context_history_boundary.py::test_the_optional_boundary_survives_serialization_and_rejects_negative_indices
/// An explicitly supplied boundary survives the same request codec; this
/// extends the historical request, not its claim of Python provenance.
#[test]
fn an_explicit_history_boundary_survives_the_request_codec() {
    let (_, request) = lines()
        .into_iter()
        .find(|(family, _)| family == "request")
        .unwrap();
    for boundary in [None, Some(0), Some(7)] {
        let mut message = request.clone();
        message["context"]["history_start_index"] = serde_json::json!(boundary);
        let parsed: TurnRequest = serde_json::from_value(message.clone()).unwrap();
        assert_eq!(parsed.context.history_start_index, boundary);
        let after = serde_json::to_value(parsed).unwrap();
        let (mut lost, mut added) = (Vec::new(), BTreeSet::new());
        differences("request", &message, &after, &mut lost, &mut added);
        assert_eq!((lost, added), (Vec::new(), BTreeSet::new()));
    }
}
