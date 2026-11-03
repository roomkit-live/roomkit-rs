//! A worker's output as any strategy hands it a model (RFC §6.4, §19.7):
//! Python's `roomkit.tasks.handback.worker_block`.

use roomkit_core::rules::transcript::fence;

use crate::tasks::text::{bounded_text, one_line};

/// The characters of a worker's label: a role, a description or an id.
const LABEL_CHARS: usize = 120;

/// One agent's output, fenced as a worker's under its `label`: it can
/// neither close its block nor pass itself off as another agent's.
/// Python's `worker_block(label, output)`.
pub(crate) fn worker_block(label: &str, output: &str) -> String {
    let heading = bounded_text(&one_line(label), LABEL_CHARS);
    let output = if output.is_empty() {
        "(no output)"
    } else {
        output
    };
    format!("[{heading}]\n{}", fence("worker_output", output))
}

#[cfg(test)]
mod tests {
    use super::worker_block;

    // Python: roomkit.tasks.handback.worker_block, its outputs for these
    // inputs (no Python test pins them; the Loop scenarios' prompts do).
    #[test]
    fn a_worker_s_output_is_fenced_under_its_label_an_empty_one_named_so() {
        assert_eq!(
            worker_block("Feedback from Editor", "Needs work."),
            "[Feedback from Editor]\n<worker_output>\nNeeds work.\n</worker_output>"
        );
        assert_eq!(
            worker_block("Line one\nline two", ""),
            "[Line one line two]\n<worker_output>\n(no output)\n</worker_output>"
        );
    }
}
