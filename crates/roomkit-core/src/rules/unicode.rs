//! The Unicode properties a name is read with, as the Python implementation
//! reads them (RFC §6.4, §10.1 step 12, gap G-089): canonical composition
//! (NFC), the combining marks, the characters `str.isalnum()` accepts, the
//! characters a person's name keeps, those a name's letters are read past,
//! what each character reads as in a name and in a phrase, the opening
//! and closing punctuation, and the characters `str.isprintable()` accepts,
//! which Python's `repr` of a `str` writes as they are, and what
//! `str.casefold()` writes.
//! The tables are generated from roomkit and the Unicode data of its
//! Python (`unicode_data`), so that both implementations read a name alike,
//! whatever Unicode version Rust's standard library carries.

use std::cmp::Ordering;
use std::fmt::Write as _;

use super::unicode_data::{
    ALNUM, CASEFOLDS, CLOSING, COMBINING_CLASSES, COMPOSITIONS, DECOMPOSITIONS, JOINT, MARKS,
    NAME_CHARS, OPENING, PHRASE_GAPS, PHRASE_READS, PRINTABLE, READS,
};

/// Whether `ranges`, inclusive and sorted, holds `c`.
fn within(ranges: &[(char, char)], c: char) -> bool {
    ranges
        .binary_search_by(|&(start, end)| {
            if end < c {
                Ordering::Less
            } else if start > c {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        })
        .is_ok()
}

/// Whether `c` is a combining mark (general category M).
pub(crate) fn is_mark(c: char) -> bool {
    within(MARKS, c)
}

/// Whether Python's `str.isalnum()` accepts `c`.
pub fn is_alnum(c: char) -> bool {
    within(ALNUM, c)
}

/// Whether a person's name keeps `c`: a letter, a mark or a digit that
/// reads as one, never one that reads as a colon or a quote.
pub(crate) fn is_name_char(c: char) -> bool {
    within(NAME_CHARS, c)
}

/// Whether a name's letters are read past `c`: an invisible, control or
/// line-break character, a combining mark of the Latin range.
pub(crate) fn is_joint(c: char) -> bool {
    within(JOINT, c)
}

/// Whether `c` is opening punctuation (general category Ps): a bracket,
/// `[` and its look-alike `［` among them.
pub(crate) fn is_opening(c: char) -> bool {
    within(OPENING, c)
}

/// Whether `c` is closing punctuation (general category Pe).
pub(crate) fn is_closing(c: char) -> bool {
    within(CLOSING, c)
}

/// What `c` reads as, when the look-alike tables read it as something.
pub(crate) fn reads(c: char) -> Option<&'static str> {
    READS
        .binary_search_by(|&(form, _)| form.cmp(&c))
        .ok()
        .and_then(|found| READS.get(found))
        .map(|&(_, read)| read)
}

/// What `c` reads as in a phrase, as the Python implementation's pattern
/// of a runtime mark reads it (`_lookalike.char_class`) and as a name reads
/// it: each letter, digit or run of letters it may stand for, separated by
/// spaces (`I` reads as `i` or `l`, `𝐈` too, `ⅵ` as `vi`, `|` as `l`);
/// `None` for a character that reads as no letter.
pub(crate) fn phrase_reads(c: char) -> Option<&'static str> {
    PHRASE_READS
        .binary_search_by(|&(form, _)| form.cmp(&c))
        .ok()
        .and_then(|found| PHRASE_READS.get(found))
        .map(|&(_, read)| read)
}

/// Whether `c`, which reads as a letter in a phrase, may also sit between
/// its words as markup (`|`).
pub(crate) fn is_phrase_gap(c: char) -> bool {
    within(PHRASE_GAPS, c)
}

/// Whether Python's `str.isprintable()` accepts `c`: not a control, a
/// format, a separator other than the space, a private-use, a surrogate or
/// an unassigned character.
fn is_printable(c: char) -> bool {
    within(PRINTABLE, c)
}

/// `text` as Python's `repr` of a `str` writes it, which the Python
/// implementation quotes a name with (`{name!r}`) in a record or an error's
/// text: in single quotes, double ones when it holds a single quote and no
/// double one; its backslashes and its quote escaped, `\t`, `\n` and `\r`
/// as such, and every other character `str.isprintable()` refuses as
/// `\xNN` under U+0100, `\uNNNN` under U+10000, `\UNNNNNNNN` past it (a
/// no-break space as `\xa0`, a right-to-left override as `\u202e`).
#[doc(hidden)]
pub fn python_repr(text: &str) -> String {
    let quote = if text.contains('\'') && !text.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut written = String::with_capacity(text.len() + 2);
    written.push(quote);
    for character in text.chars() {
        match character {
            '\\' => written.push_str("\\\\"),
            '\t' => written.push_str("\\t"),
            '\n' => written.push_str("\\n"),
            '\r' => written.push_str("\\r"),
            _ if character == quote => {
                written.push('\\');
                written.push(character);
            }
            _ if is_printable(character) => written.push(character),
            _ => {
                let code = u32::from(character);
                // Writing to a `String` does not fail.
                let _ = if code < 0x100 {
                    write!(written, "\\x{code:02x}")
                } else if code < 0x1_0000 {
                    write!(written, "\\u{code:04x}")
                } else {
                    write!(written, "\\U{code:08x}")
                };
            }
        }
    }
    written.push(quote);
    written
}

/// The canonical combining class of `c`, 0 for a starter.
fn combining_class(c: char) -> u8 {
    COMBINING_CLASSES
        .binary_search_by(|&(mark, _)| mark.cmp(&c))
        .ok()
        .and_then(|found| COMBINING_CLASSES.get(found))
        .map_or(0, |&(_, class)| class)
}

/// The first and last Hangul syllables, the leading and vowel jamo, and the
/// counts the syllables are composed from (Unicode §3.12).
const S_BASE: u32 = 0xAC00;
const L_BASE: u32 = 0x1100;
const V_BASE: u32 = 0x1161;
const T_BASE: u32 = 0x11A7;
const L_COUNT: u32 = 19;
const V_COUNT: u32 = 21;
const T_COUNT: u32 = 28;
const N_COUNT: u32 = V_COUNT * T_COUNT;
const S_COUNT: u32 = L_COUNT * N_COUNT;

/// `c`'s full canonical decomposition, pushed onto `out`.
fn decompose_into(c: char, out: &mut Vec<char>) {
    let code = u32::from(c);
    if let Some(index) = code.checked_sub(S_BASE).filter(|index| *index < S_COUNT) {
        let jamo = [
            Some(L_BASE + index / N_COUNT),
            Some(V_BASE + (index % N_COUNT) / T_COUNT),
            Some(index % T_COUNT).filter(|t| *t > 0).map(|t| T_BASE + t),
        ];
        out.extend(jamo.into_iter().flatten().filter_map(char::from_u32));
        return;
    }
    match DECOMPOSITIONS.binary_search_by(|&(composite, _)| composite.cmp(&c)) {
        Ok(found) => {
            if let Some(&(_, decomposed)) = DECOMPOSITIONS.get(found) {
                out.extend(decomposed.chars());
            }
        }
        Err(_) => out.push(c),
    }
}

/// What `first` and `second` compose into, if they are a primary composite
/// or a Hangul syllable's parts.
fn compose(first: char, second: char) -> Option<char> {
    let (starter, next) = (u32::from(first), u32::from(second));
    let leading = starter.wrapping_sub(L_BASE);
    let vowel = next.wrapping_sub(V_BASE);
    if leading < L_COUNT && vowel < V_COUNT {
        return char::from_u32(S_BASE + (leading * V_COUNT + vowel) * T_COUNT);
    }
    let syllable = starter.wrapping_sub(S_BASE);
    let trailing = next.wrapping_sub(T_BASE);
    if syllable < S_COUNT && syllable % T_COUNT == 0 && trailing > 0 && trailing < T_COUNT {
        return char::from_u32(starter + trailing);
    }
    COMPOSITIONS
        .binary_search_by(|&(x, y, _)| (x, y).cmp(&(first, second)))
        .ok()
        .and_then(|found| COMPOSITIONS.get(found))
        .map(|&(_, _, composite)| composite)
}

/// `text` as Python's `str.casefold()` writes it, with which two names read
/// as one ignoring case (`Straße` and `STRASSE`).
pub fn casefold(text: &str) -> String {
    let mut folded = String::with_capacity(text.len());
    for c in text.chars() {
        match CASEFOLDS.binary_search_by(|&(form, _)| form.cmp(&c)) {
            Ok(found) => folded.push_str(CASEFOLDS.get(found).map_or("", |&(_, read)| read)),
            Err(_) => folded.push(c),
        }
    }
    folded
}

/// `text` in Normalization Form C: decomposed, its marks put in canonical
/// order, then composed again (Unicode Standard Annex #15).
pub(crate) fn nfc(text: &str) -> String {
    let mut chars = Vec::with_capacity(text.len());
    for c in text.chars() {
        decompose_into(c, &mut chars);
    }
    canonical_order(&mut chars);
    compose_all(&chars).into_iter().collect()
}

/// Sorts each run of combining marks by their class, stably.
fn canonical_order(chars: &mut [char]) {
    let mut start = 0;
    while start < chars.len() {
        let run = chars
            .get(start..)
            .unwrap_or_default()
            .iter()
            .take_while(|c| combining_class(**c) > 0)
            .count();
        if let Some(marks) = chars.get_mut(start..start + run) {
            marks.sort_by_key(|c| combining_class(*c));
        }
        start += run.max(1);
    }
}

/// The canonical composition of `chars`, decomposed and in canonical order:
/// each character joins the last starter when nothing between them blocks
/// it, a mark of the same class or higher, and the pair composes.
fn compose_all(chars: &[char]) -> Vec<char> {
    let mut composed: Vec<char> = Vec::with_capacity(chars.len());
    // The last starter, by its position in `composed`, and the class of the
    // last character kept since it, `None` when there is none.
    let mut starter: Option<usize> = None;
    let mut since: Option<u8> = None;
    for &c in chars {
        let class = combining_class(c);
        let blocked = since.is_some_and(|last| last == 0 || last >= class);
        if !blocked && let Some(at) = starter {
            let made = composed.get(at).and_then(|&first| compose(first, c));
            if let (Some(made), Some(slot)) = (made, composed.get_mut(at)) {
                *slot = made;
                continue;
            }
        }
        if class == 0 {
            starter = Some(composed.len());
            since = None;
        } else {
            since = Some(class);
        }
        composed.push(c);
    }
    composed
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    /// Values Python 3.13 gives (`str.casefold`).
    #[test]
    fn casefold_folds_as_python_does() {
        let cases = [
            ("Stra\u{df}e", "strasse"),
            ("STRASSE", "strasse"),
            ("\u{17f}am", "sam"),
            ("\u{fb01}n", "fin"),
            ("\u{3a3}\u{3a3}", "\u{3c3}\u{3c3}"),
            ("\u{3c2}", "\u{3c3}"),
            ("\u{c9}tienne", "\u{e9}tienne"),
            ("\u{130}", "i\u{307}"),
            ("agent:1", "agent:1"),
        ];
        for (text, folded) in cases {
            assert_eq!(casefold(text), folded, "{text:?}");
        }
    }

    /// Values Python 3.13 gives (`unicodedata.normalize("NFC", ...)`).
    #[test]
    fn nfc_composes_as_python_does() {
        let cases = [
            ("e\u{301}", "\u{e9}"),
            ("\u{438}\u{306}", "\u{439}"),
            ("\u{1100}\u{1161}\u{11a8}", "\u{ac01}"),
            ("a\u{323}\u{302}", "\u{1ead}"),
            ("a\u{302}\u{323}", "\u{1ead}"),
            ("\u{2126}", "\u{3a9}"),
            ("A\u{30a}\u{301}", "\u{1fa}"),
            ("\u{e9}", "\u{e9}"),
            ("q\u{307}\u{323}", "q\u{323}\u{307}"),
            ("Alice", "Alice"),
        ];
        let normalized: Vec<String> = cases.iter().map(|(text, _)| nfc(text)).collect();
        let expected: Vec<String> = cases.iter().map(|(_, nfc)| (*nfc).to_owned()).collect();
        assert_eq!(normalized, expected);
    }

    /// Values Python 3.13 gives (`print(repr(...))`).
    #[test]
    fn a_text_is_written_as_python_s_repr_writes_it() {
        let cases = [
            ("Bob\u{a0}Smith", "'Bob\\xa0Smith'"),
            ("a\u{200b}b", "'a\\u200bb'"),
            ("x\u{202e}y", "'x\\u202ey'"),
            ("l1\u{2028}l2", "'l1\\u2028l2'"),
            ("c1\u{85}", "'c1\\x85'"),
            ("\u{ad}", "'\\xad'"),
            ("pu\u{f0000}", "'pu\\U000f0000'"),
            ("t\u{e0001}", "'t\\U000e0001'"),
            (
                "caf\u{e9} \u{4e2d} \u{1f600}",
                "'caf\u{e9} \u{4e2d} \u{1f600}'",
            ),
            ("it's", "\"it's\""),
            ("say \"hi\" it's", "'say \"hi\" it\\'s'"),
            ("\u{0}\u{1f}\u{7f}\\", "'\\x00\\x1f\\x7f\\\\'"),
            ("tab\tline\nend\r", "'tab\\tline\\nend\\r'"),
        ];
        let written: Vec<String> = cases.iter().map(|(text, _)| python_repr(text)).collect();
        let expected: Vec<String> = cases.iter().map(|(_, repr)| (*repr).to_owned()).collect();
        assert_eq!(written, expected);
    }

    #[test]
    fn the_tables_answer_python_s_properties() {
        assert_eq!(
            (
                is_mark('\u{301}'),
                is_alnum('\u{e9}'),
                is_alnum('-'),
                is_name_char(':'),
                is_name_char('\u{410}'),
                is_joint('\u{200b}'),
                reads('\u{410}'),
            ),
            (true, true, false, false, true, true, Some("a"))
        );
    }
}
