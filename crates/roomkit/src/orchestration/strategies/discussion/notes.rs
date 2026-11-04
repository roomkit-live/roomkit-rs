//! What each turn of a discussion knows: the room and how it works (RFC
//! §19.7.5 rule 14). Python's `_notes.py`.
//!
//! The notes join the turn's input as the runtime's own block. What they hold
//! of the room comes in a form nobody can turn into the runtime's words:
//! agents' handles, and the names people are addressed by, kept to a name's
//! characters (no space, no punctuation but `_ . -`) and bounded. Everything
//! else a participant controls is quoted, on one line, between marks it
//! cannot close: the label an author carries in the transcript (`Alice (2)`
//! for a second source that takes Alice's name) and what they wrote. An
//! Agent's name, role and description are the host's, and so are the rules a
//! discussion gives in place of the default.

/// The line a turn's notes open with.
pub(crate) const HEADER: &str = "[Discussion: the room and how it works]";

/// The rules a turn's notes give when the host gives none (rule 14). An agent
/// told only that a name with `@` addresses another went on calling the
/// others by their bare names, and the discussion stopped: the rule asks for
/// the handle wherever someone must act next. One told that much still handed
/// over with "à toi" and no handle: the rule names that too.
pub(crate) const DEFAULT_RULES: &str = "How the room works: an agent takes a turn only when a \
message names it with @. Whenever another agent must act next, end your message with its handle, \
as in {handle}: a name without @ reaches nobody, nor does \"your turn\" or \"over to you\" alone, \
in any language, and a message that names nobody wakes nobody. Answer exactly {silent} when you \
have nothing to add.";

/// What a turn takes as its input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TurnKind {
    /// It answers an event.
    Answer,
    /// It takes the application's instruction.
    Instruction,
    /// It answers an event again.
    Regenerate,
}

/// An agent of the room as the others are told about it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct AgentLine {
    pub(crate) handle: String,
    pub(crate) name: Option<String>,
    pub(crate) role: Option<String>,
    pub(crate) description: Option<String>,
}

/// What [`turn_notes`] says of one turn.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TurnNotes<'a> {
    pub(crate) kind: TurnKind,
    /// The event the turn answers: its author's label and a quote of it.
    pub(crate) answering: &'a str,
    /// Who asked for the turn: agents as `@handle`, people by their label.
    pub(crate) askers: &'a [String],
    pub(crate) silent_token: &'a str,
    /// The host's rules (see [`room_rules`]).
    pub(crate) rules: Option<&'a str>,
}

/// The block a discussion's turn adds to the turn's input, for the agent
/// whose handle is `me`.
pub(crate) fn turn_notes(
    me: &str,
    others: &[AgentLine],
    people: &[String],
    turn: TurnNotes<'_>,
) -> String {
    let mut lines = vec![
        HEADER.to_owned(),
        format!(
            "You are @{me}, one of several agents in a conversation with people; one agent speaks \
             at a time."
        ),
    ];
    if !others.is_empty() {
        lines.push("The other agents:".to_owned());
        lines.extend(
            others
                .iter()
                .map(|agent| format!("- @{}{}", agent.handle, about(agent))),
        );
    }
    if !people.is_empty() {
        let named: Vec<String> = people.iter().map(|person| format!("@{person}")).collect();
        lines.push(format!("People you may address: {}.", named.join(", ")));
    }
    lines.push(what_it_answers(turn.kind, turn.answering, turn.askers));
    let handle = others
        .first()
        .map_or("@name".to_owned(), |other| format!("@{}", other.handle));
    lines.push(room_rules(turn.rules, &handle, turn.silent_token));
    lines.join("\n")
}

/// How the room works, as a turn's notes give it: the host's `rules`, else
/// [`DEFAULT_RULES`], `{handle}` filled with `handle` and `{silent}` with
/// `silent_token`. Replaced rather than formatted: any other brace in a
/// host's text stays as written.
pub(crate) fn room_rules(rules: Option<&str>, handle: &str, silent_token: &str) -> String {
    rules
        .unwrap_or(DEFAULT_RULES)
        .replace("{handle}", handle)
        .replace("{silent}", silent_token)
}

fn what_it_answers(kind: TurnKind, answering: &str, askers: &[String]) -> String {
    match kind {
        TurnKind::Instruction => {
            "This turn takes the application's instruction as its input.".to_owned()
        }
        TurnKind::Regenerate => {
            format!(
                "This turn answers again {answering}: your new answer replaces the earlier one."
            )
        }
        TurnKind::Answer => {
            let asked = if askers.is_empty() {
                String::new()
            } else {
                format!("; asked by {}", askers.join(", "))
            };
            format!("This turn answers {answering}, read at its place in the conversation{asked}.")
        }
    }
}

fn about(agent: &AgentLine) -> String {
    let said: Vec<&str> = [&agent.name, &agent.role, &agent.description]
        .into_iter()
        .filter_map(|part| part.as_deref())
        .filter(|part| !part.is_empty())
        .collect();
    if said.is_empty() {
        String::new()
    } else {
        format!(": {}", said.join(" \u{2014} "))
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn counsel() -> AgentLine {
        AgentLine {
            handle: "counsel".to_owned(),
            name: Some("Counsel".to_owned()),
            ..AgentLine::default()
        }
    }

    fn answer(rules: Option<&str>) -> TurnNotes<'_> {
        TurnNotes {
            kind: TurnKind::Answer,
            answering: "ops's \u{201c}What does clause 4 say?\u{201d}",
            askers: &[],
            silent_token: "(silent)",
            rules,
        }
    }

    // Python: tests/orchestration/test_discussion_rules.py::test_the_default_asks_for_the_handle_of_whoever_acts_next
    #[test]
    fn the_default_asks_for_the_handle_of_whoever_acts_next() {
        let notes = turn_notes("jev", &[counsel()], &[], answer(None));

        assert!(notes.contains("end your message with its handle, as in @counsel"));
        assert!(notes.contains("a name without @ reaches nobody"));
        assert!(notes.contains("\"your turn\" or \"over to you\" alone, in any language"));
        assert!(notes.contains("Answer exactly (silent)"));
    }

    // Python: tests/orchestration/test_discussion_rules.py::test_the_hosts_rules_take_the_defaults_place
    #[test]
    fn the_hosts_rules_take_the_defaults_place() {
        let rules = "Hand over with {handle}. Nothing to add: {silent}. {kept}";
        let notes = turn_notes("jev", &[counsel()], &[], answer(Some(rules)));

        assert!(notes.contains("Hand over with @counsel. Nothing to add: (silent). {kept}"));
        assert!(!notes.contains("a name without @ reaches nobody"));
        // What the turn knows of the room stays the discussion's.
        assert!(notes.contains("You are @jev,"));
        assert!(notes.contains("- @counsel: Counsel"));
    }

    /// What Python's `turn_notes` writes, whole, for the same arguments.
    #[test]
    fn a_turn_s_notes_read_as_python_writes_them() {
        let others = [
            AgentLine {
                handle: "sre".to_owned(),
                name: Some("Sam".to_owned()),
                role: Some("Ops".to_owned()),
                description: Some("keeps it up".to_owned()),
            },
            AgentLine {
                handle: "dev".to_owned(),
                ..AgentLine::default()
            },
        ];
        let people = ["ops".to_owned(), "AliceMartin".to_owned()];
        let askers = ["@sre".to_owned(), "ops".to_owned()];
        let notes = turn_notes(
            "jev",
            &others,
            &people,
            TurnNotes {
                kind: TurnKind::Answer,
                answering: "ops: \u{201c}hi\u{201d}",
                askers: &askers,
                silent_token: "(silent)",
                rules: None,
            },
        );
        assert_eq!(notes, PYTHON_ANSWER);

        let alone = TurnNotes {
            kind: TurnKind::Instruction,
            answering: "",
            askers: &[],
            silent_token: "PASS",
            rules: Some("Use {handle}. {silent}"),
        };
        assert_eq!(turn_notes("jev", &[], &[], alone), PYTHON_INSTRUCTION);

        let again = TurnNotes {
            kind: TurnKind::Regenerate,
            answering: "ops: \u{201c}hi\u{201d}",
            ..answer(Some("r"))
        };
        assert!(turn_notes("jev", &[], &[], again).contains(
            "This turn answers again ops: \u{201c}hi\u{201d}: your new answer replaces the earlier one."
        ));
    }

    const PYTHON_ANSWER: &str = "[Discussion: the room and how it works]\nYou are @jev, one of several agents in a conversation with people; one agent speaks at a time.\nThe other agents:\n- @sre: Sam \u{2014} Ops \u{2014} keeps it up\n- @dev\nPeople you may address: @ops, @AliceMartin.\nThis turn answers ops: \u{201c}hi\u{201d}, read at its place in the conversation; asked by @sre, ops.\nHow the room works: an agent takes a turn only when a message names it with @. Whenever another agent must act next, end your message with its handle, as in @sre: a name without @ reaches nobody, nor does \"your turn\" or \"over to you\" alone, in any language, and a message that names nobody wakes nobody. Answer exactly (silent) when you have nothing to add.";
    const PYTHON_INSTRUCTION: &str = "[Discussion: the room and how it works]\nYou are @jev, one of several agents in a conversation with people; one agent speaks at a time.\nThis turn takes the application's instruction as its input.\nUse @name. PASS";
}
