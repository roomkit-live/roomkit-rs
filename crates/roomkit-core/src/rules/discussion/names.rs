//! The names a message addresses, and an agent's answer that stays silent
//! (RFC §19.7.5): Python's `_names.py`.
//!
//! A name is `@` followed by an agent's handle, or a person's name; `@all`
//! names every agent. It starts the text or follows a character that is
//! neither a letter, a digit, `_` nor `@` (so `ops@example.com` names no
//! one), it is the longest run of identifier characters that follows
//! (letters of any script, digits, `_ . -`), a final `.` excepted, and it
//! matches ignoring case. Names are read in what the speaker wrote: never in
//! a fenced code block or a quoted line.

use crate::rules::transcript::identifier;
use crate::rules::unicode::{casefold, is_alnum, nfc};

/// The name that names every agent.
const EVERYONE: &str = "all";

/// The longest name a person is addressed by (a name joins every turn's
/// notes): a longer one is not addressable, never cut short, since a cut
/// name could be another person's whole one.
pub const NAME_LIMIT: usize = 32;

/// The marks that open a fenced code block.
const FENCES: [&str; 2] = ["```", "~~~"];

/// What a message names: agents (by channel id) and people, in order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Names {
    /// The agents it names, each once.
    pub agents: Vec<String>,
    /// The people it names, each once.
    pub people: Vec<String>,
}

/// Whether Python's `\w` reads `c`: what `str.isalnum()` accepts, and `_`.
fn is_word(c: char) -> bool {
    c == '_' || is_alnum(c)
}

/// Whether a name holds `c`: Python's `[\w.\-]`.
fn holds(c: char) -> bool {
    is_word(c) || c == '.' || c == '-'
}

/// A person's name kept to the characters a name holds (`Alice Martin` gives
/// `AliceMartin`, `Hélène` stays), the form a message names them by.
#[must_use]
pub fn name_key(name: &str) -> String {
    nfc(name).chars().filter(|c| holds(*c)).collect()
}

/// Whether `value` can name an agent: a name's characters, as a message
/// names it (NFC), at most [`NAME_LIMIT`] of them, no final `.` (a name drops
/// it) and not `all`, which names every agent. A transcript labels the
/// agent's turns with it unchanged (`@` and its [`identifier`]), so a handle
/// that would change there (`dev-`, `a--b`) is none.
#[must_use]
pub fn is_handle(value: &str) -> bool {
    !value.is_empty()
        && value.chars().count() <= NAME_LIMIT
        && value.chars().all(holds)
        && nfc(value) == value
        && identifier(value, "") == value
        && !value.ends_with('.')
        && casefold(value) != EVERYONE
}

/// Whether two names read as one, ignoring case.
#[must_use]
pub fn same_name(one: &str, other: &str) -> bool {
    casefold(one) == casefold(other)
}

/// The agents (by channel id) and people `text` names, in order, the
/// `speaker` (a channel id) left out; `agents` gives each agent's handle and
/// channel id, in the agents' order. Where a name reads as both an agent's
/// handle and a person's name, it is the agent's. A name that is neither
/// addresses nobody.
#[must_use]
pub fn read_names(
    text: &str,
    agents: &[(String, String)],
    people: &[String],
    speaker: Option<&str>,
) -> Names {
    let by_agent: Vec<(String, &str)> = agents
        .iter()
        .map(|(handle, agent)| (casefold(handle), agent.as_str()))
        .collect();
    let by_person: Vec<(String, &str)> = people
        .iter()
        .filter_map(|person| {
            let key = name_key(person);
            (!key.is_empty()).then(|| (casefold(&key), person.as_str()))
        })
        .collect();
    let mut named = Names::default();
    for raw in names_in(&spoken(&nfc(text))) {
        let key = casefold(raw.strip_suffix('.').unwrap_or(&raw));
        if key == EVERYONE {
            for (_, agent) in agents {
                name_agent(&mut named, agent, speaker);
            }
        } else if let Some((_, agent)) = last_with(&by_agent, &key) {
            name_agent(&mut named, agent, speaker);
        } else if let Some((_, person)) = last_with(&by_person, &key) {
            let person = *person;
            if Some(person) != speaker && !named.people.iter().any(|known| known == person) {
                named.people.push(person.to_owned());
            }
        }
    }
    named
}

/// The last entry of `pairs` under `key`: of two names reading alike, a
/// Python dict keeps the later.
fn last_with<'a, 'b>(pairs: &'a [(String, &'b str)], key: &str) -> Option<&'a (String, &'b str)> {
    pairs.iter().rev().find(|(known, _)| known == key)
}

/// Adds `agent` to what is named, unless it speaks or is named already.
fn name_agent(named: &mut Names, agent: &str, speaker: Option<&str>) {
    if Some(agent) != speaker && !named.agents.iter().any(|known| known == agent) {
        named.agents.push(agent.to_owned());
    }
}

/// The names `text` holds, as Python's `(?<![\w@])@([\w.\-]+)` finds them.
fn names_in(text: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut previous: Option<char> = None;
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        previous = if c == '@' && !previous.is_some_and(|before| before == '@' || is_word(before)) {
            let mut name = String::new();
            let mut last = c;
            while let Some(next) = chars.next_if(|next| holds(*next)) {
                name.push(next);
                last = next;
            }
            if !name.is_empty() {
                found.push(name);
            }
            Some(last)
        } else {
            Some(c)
        };
    }
    found
}

/// `text` without its fenced code blocks and quoted lines: what the speaker
/// says, not what it shows or repeats. One pass over the lines: the text is
/// a participant's, and nothing it holds may make the reading stall the room.
fn spoken(text: &str) -> String {
    let mut kept: Vec<&str> = Vec::new();
    let mut fence: Option<&str> = None;
    for line in lines(text) {
        let start = line.trim_start_matches([' ', '\t']);
        if let Some(open) = fence {
            if start.starts_with(open) {
                fence = None;
            }
            continue;
        }
        if let Some(opening) = FENCES.into_iter().find(|mark| start.starts_with(mark)) {
            fence = Some(opening);
        } else if !start.starts_with('>') {
            kept.push(line);
        }
    }
    kept.join("\n")
}

/// `text`'s lines, split at every line break Python's `str.splitlines`
/// knows, a final break ending the last line rather than opening one.
fn lines(text: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut rest = text;
    while let Some((at, c)) = rest.char_indices().find(|(_, c)| is_line_break(*c)) {
        let (line, tail) = rest.split_at(at);
        lines.push(line);
        let after = tail.get(c.len_utf8()..).unwrap_or_default();
        rest = if c == '\r' {
            after.strip_prefix('\n').unwrap_or(after)
        } else {
            after
        };
    }
    if !rest.is_empty() {
        lines.push(rest);
    }
    lines
}

/// Whether Python's `str.splitlines` ends a line at `c`.
fn is_line_break(c: char) -> bool {
    matches!(
        c,
        '\n' | '\r'
            | '\u{0b}'
            | '\u{0c}'
            | '\u{1c}'
            | '\u{1d}'
            | '\u{1e}'
            | '\u{85}'
            | '\u{2028}'
            | '\u{2029}'
    )
}

/// The closing bracket of an opening one a token may be written between.
fn closing(opening: char) -> Option<char> {
    match opening {
        '(' => Some(')'),
        '[' => Some(']'),
        '{' => Some('}'),
        '<' => Some('>'),
        _ => None,
    }
}

/// Whether Python's `str.strip()` strips `c`: Unicode's white space, and the
/// four information separators.
fn is_python_space(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

/// `text` as Python's `text.strip().lower()` reads it.
fn lowered(text: &str) -> String {
    text.trim_matches(is_python_space).to_lowercase()
}

/// The answer of an agent that stays silent, and the forms it is read in.
///
/// The token is read ignoring case, surrounding whitespace, a final period
/// and the brackets around it when it has some: `(silent)`, `Silent.`,
/// `SILENT`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SilentToken {
    token: String,
}

impl Default for SilentToken {
    fn default() -> Self {
        Self::new("(silent)")
    }
}

impl SilentToken {
    /// The token `token`.
    #[must_use]
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }

    /// The forms the token is read in: itself, and what its brackets hold.
    fn forms(&self) -> Vec<String> {
        let token = lowered(&self.token);
        let mut chars = token.chars();
        let bracketed = match (chars.next(), chars.next_back()) {
            (Some(first), Some(last)) => token.chars().count() > 2 && closing(first) == Some(last),
            _ => false,
        };
        if !bracketed {
            return vec![token];
        }
        let inner = chars.as_str().trim_matches(is_python_space).to_owned();
        vec![token, inner]
    }

    /// Whether `text` is the token: says nothing. Empty text says nothing
    /// too.
    #[must_use]
    pub fn is_silent(&self, text: &str) -> bool {
        let said = lowered(text);
        let said = said
            .strip_suffix('.')
            .unwrap_or(&said)
            .trim_end_matches(is_python_space);
        said.is_empty() || self.forms().iter().any(|form| form == said)
    }

    /// Whether `text`, the start of a streamed answer, may still become the
    /// token: what is held back until it cannot.
    #[must_use]
    pub fn may_become(&self, text: &str) -> bool {
        let said = lowered(text);
        if said.is_empty() {
            return true;
        }
        self.forms()
            .iter()
            .any(|form| form.starts_with(&said) || said == *form || said == format!("{form}."))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use pretty_assertions::assert_eq;

    use super::*;

    /// Each agent's handle and channel id: here, its channel id is its
    /// handle.
    fn agents() -> Vec<(String, String)> {
        ["investigator", "sre", "dev", "comms"]
            .map(|agent| (agent.to_owned(), agent.to_owned()))
            .to_vec()
    }

    /// Agents keyed by record id, which cannot be a name, addressed by their
    /// handles.
    fn keyed() -> Vec<(String, String)> {
        vec![
            ("Jev".to_owned(), "agent:3f2a".to_owned()),
            ("Counsel".to_owned(), "agent:9b1c".to_owned()),
        ]
    }

    fn dev() -> Vec<(String, String)> {
        vec![("dev".to_owned(), "dev".to_owned())]
    }

    fn named(agents: &[&str], people: &[&str]) -> Names {
        Names {
            agents: agents.iter().map(|agent| (*agent).to_owned()).collect(),
            people: people.iter().map(|person| (*person).to_owned()).collect(),
        }
    }

    fn people(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    // Python: tests/test_discussion_names.py::test_names_are_read_in_order_ignoring_case_and_the_speaker
    #[test]
    fn names_are_read_in_order_ignoring_case_and_the_speaker() {
        let text = "@SRE please check deploys, then @comms; cc @sre again";
        assert_eq!(
            read_names(text, &agents(), &[], Some("investigator")),
            named(&["sre", "comms"], &[])
        );
        assert_eq!(
            read_names("@sre over to you", &agents(), &[], Some("sre")),
            Names::default()
        );
    }

    // Python: tests/test_discussion_names.py::test_a_name_needs_a_boundary_before_it_and_drops_a_final_dot
    #[test]
    fn a_name_needs_a_boundary_before_it_and_drops_a_final_dot() {
        let read = |text: &str| read_names(text, &agents(), &[], None);
        assert_eq!(read("mail ops@example.com"), Names::default());
        assert_eq!(read("thanks @dev."), named(&["dev"], &[]));
        assert_eq!(read("@dev.team is not dev"), Names::default());
        assert_eq!(read("(@comms)"), named(&["comms"], &[]));
        // An `@` right after another opens no name; what the lookbehind reads
        // is the text's character, the first name's last one included.
        assert_eq!(read("@@dev"), Names::default());
        assert_eq!(read("@dev@comms"), named(&["dev"], &[]));
        assert_eq!(read("@dev.@comms"), named(&["dev", "comms"], &[]));
    }

    // Python: tests/test_discussion_names.py::test_all_names_every_agent_and_unknown_names_nobody
    #[test]
    fn all_names_every_agent_and_unknown_names_nobody() {
        assert_eq!(
            read_names("@all heads up", &agents(), &[], Some("sre")),
            named(&["investigator", "dev", "comms"], &[])
        );
        assert_eq!(
            read_names("@nobody here", &agents(), &[], None),
            Names::default()
        );
    }

    // Python: tests/test_discussion_names.py::test_names_in_code_or_quotes_are_not_read
    #[test]
    fn names_in_code_or_quotes_are_not_read() {
        let text = "see\n```\n@dev run this\n```\n> @sre said this\nok @comms";
        assert_eq!(
            read_names(text, &agents(), &[], None),
            named(&["comms"], &[])
        );
        // Every line break Python's `splitlines` knows ends a quoted line.
        let quoted = "> @sre\u{2028}@dev";
        assert_eq!(
            read_names(quoted, &agents(), &[], None),
            named(&["dev"], &[])
        );
    }

    // Python: tests/test_discussion_names.py::test_a_hostile_text_is_read_in_linear_time
    #[test]
    fn a_hostile_text_is_read_in_linear_time() {
        let hostile = format!(
            "{}{}{}@sre",
            "```\n".repeat(20_000),
            "\n".repeat(20_000),
            "> \n".repeat(20_000)
        );
        let flood = format!("~~~ x\n{}", "@sre ".repeat(50_000));
        let started = Instant::now();
        drop(read_names(&hostile, &agents(), &[], None));
        drop(read_names(&flood, &agents(), &[], None));
        assert!(started.elapsed().as_secs_f64() < 1.0);
    }

    // Python: tests/test_discussion_names.py::test_an_unclosed_fence_hides_what_follows
    #[test]
    fn an_unclosed_fence_hides_what_follows() {
        assert_eq!(
            read_names(
                "@dev before\n```\n@sre inside, never closed",
                &agents(),
                &[],
                None
            ),
            named(&["dev"], &[])
        );
    }

    // Python: tests/test_discussion_names.py::test_people_are_named_too_and_an_agent_wins_a_shared_name
    #[test]
    fn people_are_named_too_and_an_agent_wins_a_shared_name() {
        let listed = people(&["ops", "Alice Martin", "sre"]);
        assert_eq!(
            read_names("@ops and @AliceMartin, ask @sre", &agents(), &listed, None),
            named(&["sre"], &["ops", "Alice Martin"])
        );
        assert_eq!(name_key("Alice Martin"), "AliceMartin");
    }

    // Python: tests/test_discussion_names.py::test_the_silent_token_is_read_leniently
    #[test]
    fn the_silent_token_is_read_leniently() {
        for text in ["(silent)", " (Silent). ", "SILENT", "silent.", ""] {
            assert!(SilentToken::default().is_silent(text), "{text:?}");
        }
    }

    // Python: tests/test_discussion_names.py::test_text_that_says_something_is_not_silent
    #[test]
    fn text_that_says_something_is_not_silent() {
        for text in ["I stay (silent) for now", "silently", "pass"] {
            assert!(!SilentToken::default().is_silent(text), "{text:?}");
        }
    }

    // Python: tests/test_discussion_names.py::test_the_start_of_a_streamed_answer_is_held_while_it_may_become_the_token
    #[test]
    fn the_start_of_a_streamed_answer_is_held_while_it_may_become_the_token() {
        let token = SilentToken::default();
        for start in [
            "",
            " ",
            "(",
            "(sil",
            "(silent",
            "(silent)",
            "(silent).",
            "Sil",
        ] {
            assert!(token.may_become(start), "{start:?}");
        }
        for start in ["(silent) but", "Hello", "(s!"] {
            assert!(!token.may_become(start), "{start:?}");
        }
    }

    // Python: tests/test_discussion_names.py::test_a_custom_token_without_brackets
    #[test]
    fn a_custom_token_without_brackets() {
        let token = SilentToken::new("PASS");
        assert!(token.is_silent("pass.") && token.may_become("pa"));
        assert!(!token.is_silent("(pass)"));
    }

    // Python: tests/test_discussion_names.py::test_names_hold_letters_of_any_script_and_are_never_cut
    #[test]
    fn names_hold_letters_of_any_script_and_are_never_cut() {
        assert_eq!(
            read_names(
                "@Hélène, which region? café@dev",
                &dev(),
                &people(&["Hélène"]),
                None
            ),
            named(&[], &["Hélène"])
        );
        assert_eq!(name_key("Zoë O'Brien-Smith"), "ZoëOBrien-Smith");
        // Cut short, a name could be another person's whole one.
        assert_eq!(name_key(&"x".repeat(500)).len(), 500);
        assert!(500 > NAME_LIMIT);
    }

    // Python: tests/test_discussion_names.py::test_only_one_final_period_is_dropped
    #[test]
    fn only_one_final_period_is_dropped() {
        assert_eq!(
            read_names("thanks @dev.", &dev(), &[], None).agents,
            ["dev"]
        );
        assert!(
            read_names("thanks @dev..", &dev(), &[], None)
                .agents
                .is_empty()
        );
    }

    // Python: tests/test_discussion_names.py::test_a_handle_names_its_agent_by_channel_id_and_the_channel_id_names_nobody
    #[test]
    fn a_handle_names_its_agent_by_channel_id_and_the_channel_id_names_nobody() {
        assert_eq!(
            read_names(
                "@counsel, check clause 4 with @Jev",
                &keyed(),
                &[],
                Some("agent:3f2a")
            ),
            named(&["agent:9b1c"], &[])
        );
        // The channel id is no name: "@agent:9b1c" reads as "@agent".
        assert_eq!(
            read_names("@agent:9b1c over to you", &keyed(), &[], None),
            Names::default()
        );
        assert_eq!(
            read_names("@all heads up", &keyed(), &[], Some("agent:9b1c")),
            named(&["agent:3f2a"], &[])
        );
    }

    // Python: tests/test_discussion_names.py::test_a_person_named_like_a_handle_is_the_agents_name
    #[test]
    fn a_person_named_like_a_handle_is_the_agents_name() {
        assert_eq!(
            read_names(
                "@Jev and @Alice",
                &keyed(),
                &people(&["Jev", "Alice"]),
                None
            ),
            named(&["agent:3f2a"], &["Alice"])
        );
    }

    // Python: tests/test_discussion_names.py::test_a_handle_is_a_name
    #[test]
    fn a_handle_is_a_name() {
        let longest = "x".repeat(NAME_LIMIT);
        for value in ["dev", "Jev", "Hélène", "sre-2", "a.b", longest.as_str()] {
            assert!(is_handle(value), "{value:?}");
        }
    }

    // Python: tests/test_discussion_names.py::test_what_cannot_name_an_agent_is_no_handle
    #[test]
    fn what_cannot_name_an_agent_is_no_handle() {
        let long = "x".repeat(NAME_LIMIT + 1);
        for value in [
            "",
            "agent:3f2a",
            "two words",
            "dev.",
            "all",
            "ALL",
            long.as_str(),
            "dev\n",
            // A transcript would label these otherwise: "@dev", "@dev",
            // "@a-b", "@dev".
            "dev-",
            "-dev",
            "a--b",
            "dev\u{2475}",
        ] {
            assert!(!is_handle(value), "{value:?}");
        }
    }

    // What Python's own `_names` answers on the corners its tests leave out:
    // `str.casefold` and not `lower`, `\w` without the marks, each line break
    // of `splitlines`.
    #[test]
    fn names_read_as_python_s_str_and_re_read_them() {
        let agents: Vec<(String, String)> =
            [("STRASSE", "s"), ("\u{17f}am", "sam"), ("dev", "dev")]
                .map(|(handle, agent)| (handle.to_owned(), agent.to_owned()))
                .to_vec();
        let listed = people(&["ki", "\u{915}", "\u{c9}tienne"]);
        for (text, agents_named, people_named) in [
            ("@stra\u{df}e go", vec!["s"], vec![]),
            ("@SAM hi", vec!["sam"], vec![]),
            ("@\u{17f}am hi", vec!["sam"], vec![]),
            ("@Dev\r\n> @sam\r\n@straSSe", vec!["dev", "s"], vec![]),
            ("a\u{b}> @dev\u{b}@sam", vec!["sam"], vec![]),
            ("@dev\u{200c}x @sam", vec!["dev", "sam"], vec![]),
            // A vowel sign is a mark: it ends the name.
            ("@\u{915}\u{93f}", vec![], vec!["\u{915}"]),
            ("x@dev @\u{e9}tienne", vec![], vec!["\u{c9}tienne"]),
            ("@dev\u{301} @sam", vec!["dev", "sam"], vec![]),
        ] {
            assert_eq!(
                read_names(text, &agents, &listed, None),
                named(&agents_named, &people_named),
                "{text:?}"
            );
        }
        for (value, handle) in [
            ("\u{17f}s", true),
            ("aLL", false),
            ("\u{aa}ll", true),
            ("a.b.c", true),
            ("\u{663}", true),
            ("dev\u{301}", false),
            ("\u{e9}", true),
        ] {
            assert_eq!(is_handle(value), handle, "{value:?}");
        }
        assert!(SilentToken::new("[ Pass ]").is_silent("pass"));
        assert!(SilentToken::new("[ Pass ]").may_become("[ pass ]"));
        assert!(SilentToken::new("()").is_silent("()"));
        assert!(SilentToken::new("(x)").is_silent("x."));
        assert!(SilentToken::new("x").may_become("x."));
        assert!(!SilentToken::new("x").may_become("x ."));
    }

    // Python: tests/test_discussion_names.py::test_a_decomposed_name_is_no_handle_since_a_message_names_it_composed
    #[test]
    fn a_decomposed_name_is_no_handle_since_a_message_names_it_composed() {
        assert!(is_handle("Hélène") && !is_handle("He\u{301}le\u{300}ne"));
    }
}
