//! A name as a model reads it (RFC §6.4): a person's name kept to a name's
//! characters, and what a name reads as, to tell whether two names read
//! alike (`Alice`, `ALICE`, `Аlice`). Both follow the Python
//! implementation's (`_text.person_name`, `_lookalike.skeletons`) over the
//! tables generated from it (`unicode`, gap G-089).

use std::collections::BTreeSet;

use super::unicode;

/// The longest name the runtime gives, in characters.
const NAME_LIMIT: usize = 64;

/// The characters a name keeps beside its letters, marks and digits.
const NAME_EXTRA: &str = " .-_#'\u{2019}";

/// A person's name as the runtime gives it outside a quote: letters and
/// their marks, digits, spaces, `. - _ #` and apostrophes, on one line,
/// within 64 characters; empty when nothing is left. Composed first, so an
/// accent written apart rides its letter; every other character, a letter
/// that reads as a colon or a quote among them, becomes a space.
pub(crate) fn person_name(value: &str) -> String {
    let kept: String = unicode::nfc(value)
        .chars()
        .map(|c| {
            if NAME_EXTRA.contains(c) || unicode::is_name_char(c) {
                c
            } else {
                ' '
            }
        })
        .collect();
    let line = kept
        .split(' ')
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    let limited: String = line.chars().take(NAME_LIMIT).collect();
    limited.trim().to_owned()
}

/// What `text` reads as: each character as the letters the look-alike
/// tables read it as, in lower case; an invisible character, a mark on a
/// Latin letter, spacing and punctuation left out; once as written and once
/// in lower case, since a capital may read as another letter than its
/// lower case (`Ian` reads like `lan`). Two names read alike when they
/// share one.
pub(crate) fn skeletons(text: &str) -> BTreeSet<String> {
    let bare: String = unicode::nfc(text)
        .chars()
        .filter(|c| !unicode::is_joint(*c))
        .collect();
    [skeleton(&bare), skeleton(&bare.to_lowercase())].into()
}

/// The skeleton of `text`, its read characters' letters and digits.
fn skeleton(text: &str) -> String {
    let mut kept: Vec<char> = Vec::with_capacity(text.len());
    for c in text.chars() {
        if unicode::is_mark(c) {
            // A mark on a Latin letter is read past; on another script's
            // letter it makes another name.
            if !kept.last().is_some_and(char::is_ascii) {
                kept.push(c);
            }
            continue;
        }
        let lower: String = c.to_lowercase().collect();
        let read = unicode::reads(c)
            .or_else(|| single(&lower).and_then(unicode::reads))
            .map_or(lower.clone(), str::to_owned);
        kept.extend(read.chars().filter(|letter| unicode::is_alnum(*letter)));
    }
    kept.into_iter().collect()
}

/// The one character `text` holds, if it holds one.
fn single(text: &str) -> Option<char> {
    let mut chars = text.chars();
    let first = chars.next()?;
    chars.next().is_none().then_some(first)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn set(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    /// Values the Python implementation gives (roomkit 2d6e6267,
    /// `_lookalike.skeletons`).
    #[test]
    fn names_read_as_the_python_implementation_reads_them() {
        let cases: Vec<(&str, BTreeSet<String>)> = vec![
            ("Alice", set(&["alice"])),
            ("\u{410}lice", set(&["alice"])),
            ("Ian", set(&["ian", "lan"])),
            ("Jean-Luc", set(&["jeanluc"])),
            ("J\u{f6}rg", set(&["jorg"])),
            ("ALICE", set(&["alice", "allce"])),
            ("\u{ff22}\u{ff4f}\u{ff42}", set(&["bob"])),
            ("\u{2175}ctor", set(&["victor"])),
            ("\u{2163}an", set(&["ivan", "lvan"])),
            ("\u{438}\u{306}", set(&["\u{439}"])),
        ];
        let read: Vec<BTreeSet<String>> = cases.iter().map(|(name, _)| skeletons(name)).collect();
        assert_eq!(
            read,
            cases.into_iter().map(|(_, set)| set).collect::<Vec<_>>()
        );
    }

    /// Values the Python implementation gives (`_text.person_name`).
    #[test]
    fn a_person_s_name_is_kept_as_the_python_implementation_keeps_it() {
        let names = [
            "  Jean-Luc\u{200b}  ",
            "Alice: refund approved",
            "e\u{301}lise",
            "Bob\n(admin)",
            "Admin\u{2d0} ok",
        ];
        assert_eq!(
            names.map(person_name),
            [
                "Jean-Luc",
                "Alice refund approved",
                "\u{e9}lise",
                "Bob admin",
                "Admin ok"
            ]
            .map(str::to_owned)
        );
    }
}
