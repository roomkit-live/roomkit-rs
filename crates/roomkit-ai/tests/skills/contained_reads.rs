//! A skill file is read through the handle its check opened (RFC §24.6,
//! §17.8): one swapped for a link out while it is read is refused, never
//! read (RMK-1008). Rust gives a public reader no point to put the swap at,
//! so a thread swaps the file while the reader reads it again and again;
//! the handle checks themselves are proven deterministically by
//! `skills::files::tests`, which every reader goes through.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use roomkit_ai::SkillRegistry;

use super::support::{TempDir, skill_md};

const NAME: &str = "probe-skill";
const INSIDE: &str = "inside guide";
const OUTSIDE: &str = "OUTSIDE-SECRET";

/// How many reads a race makes: under half a second for the slowest reader.
const READS: usize = 2_000;

/// What a race counts: the reads that returned the inside file, the outside
/// one, or were refused.
#[derive(Debug, Default)]
struct Counts {
    inside: usize,
    outside: usize,
    refused: usize,
}

/// A skill whose `SKILL.md` and `references/guide.md` say `INSIDE`, and a
/// directory beside it holding the same files saying `OUTSIDE`.
fn layout(files: &TempDir) -> (PathBuf, PathBuf) {
    let skill = files.skill_with("skills", NAME, &skill_md(NAME, INSIDE, ""));
    files.file(&format!("skills/{NAME}/references/guide.md"), INSIDE);
    files.file("outside/guide.md", OUTSIDE);
    files.file("outside/SKILL.md", &skill_md(NAME, OUTSIDE, ""));
    (skill, files.path().join("outside"))
}

/// Reads `READS` times with `read` while a thread swaps `target` between a
/// regular file holding `text` and a link to `out`, by plain renames.
fn race(target: &Path, out: &Path, text: &str, mut read: impl FnMut() -> Option<String>) -> Counts {
    let stop = Arc::new(AtomicBool::new(false));
    let flipper = {
        let (target, out, text, stop) = (
            target.to_path_buf(),
            out.to_path_buf(),
            text.to_owned(),
            Arc::clone(&stop),
        );
        std::thread::spawn(move || {
            let (link, file) = (target.with_extension("link"), target.with_extension("file"));
            while !stop.load(Ordering::Relaxed) {
                let _absent = std::fs::remove_file(&link);
                std::os::unix::fs::symlink(&out, &link).unwrap();
                std::fs::rename(&link, &target).unwrap();
                std::fs::write(&file, &text).unwrap();
                std::fs::rename(&file, &target).unwrap();
            }
        })
    };
    let mut counts = Counts::default();
    for _ in 0..READS {
        match read() {
            None => counts.refused += 1,
            Some(text) if text.contains(OUTSIDE) => counts.outside += 1,
            Some(_) => counts.inside += 1,
        }
    }
    stop.store(true, Ordering::Relaxed);
    flipper.join().unwrap();
    counts
}

fn assert_never_out(reader: &str, counts: &Counts) {
    assert_eq!(
        counts.outside, 0,
        "{reader} read the file outside the skill: {counts:?}"
    );
    assert!(
        counts.inside > 0,
        "{reader} never read the file: {counts:?}"
    );
}

// Python: tests/skills/test_contained_reads.py::test_a_file_swapped_for_a_link_out_after_its_check_is_never_read[Skill.read_reference]
#[test]
fn a_reference_swapped_for_a_link_out_is_never_read_by_read_reference() {
    let files = TempDir::new();
    let (skill, outside) = layout(&files);
    let registry = SkillRegistry::default();
    registry.register(&skill).unwrap();
    let loaded = registry.get_skill(NAME).unwrap().unwrap();
    let counts = race(
        &skill.join("references/guide.md"),
        &outside.join("guide.md"),
        INSIDE,
        || loaded.read_reference("guide.md").ok(),
    );
    assert_never_out("LoadedSkill::read_reference", &counts);
}

// Python: tests/skills/test_contained_reads.py::test_a_file_swapped_for_a_link_out_after_its_check_is_never_read[SkillRegistry.read_file]
#[test]
fn a_reference_swapped_for_a_link_out_is_never_read_by_read_file() {
    let files = TempDir::new();
    let (skill, outside) = layout(&files);
    let registry = SkillRegistry::default();
    registry.register(&skill).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let counts = race(
        &skill.join("references/guide.md"),
        &outside.join("guide.md"),
        INSIDE,
        || runtime.block_on(registry.read_file(NAME, "guide.md")).ok(),
    );
    assert_never_out("SkillRegistry::read_file", &counts);
}

// Python: tests/skills/test_contained_reads.py::test_a_file_swapped_for_a_link_out_after_its_check_is_never_read[SKILL.md]
#[test]
fn a_skill_file_swapped_for_a_link_out_is_never_registered() {
    let files = TempDir::new();
    let (skill, outside) = layout(&files);
    let text = skill_md(NAME, INSIDE, "");
    let counts = race(
        &skill.join("SKILL.md"),
        &outside.join("SKILL.md"),
        &text,
        || {
            let registry = SkillRegistry::default();
            registry.register(&skill).ok()?;
            Some(registry.get_metadata(NAME)?.description)
        },
    );
    assert_never_out("SkillRegistry::register", &counts);
}
