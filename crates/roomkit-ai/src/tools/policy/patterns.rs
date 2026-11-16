/// Whether `pattern`, where `*` matches any characters and `?` one, matches
/// the whole of `name`.
pub(crate) fn glob_matches(pattern: &str, name: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let name: Vec<char> = name.chars().collect();
    // The positions after the last `*` and the name's character it stands
    // for so far, to go back to when what follows fails.
    let (mut p, mut n) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while n < name.len() {
        match pattern.get(p) {
            Some('*') => {
                star = Some((p + 1, n));
                p += 1;
            }
            Some(&wanted) if wanted == '?' || Some(&wanted) == name.get(n) => {
                p += 1;
                n += 1;
            }
            Some(_) | None => match star {
                Some((after, taken)) => {
                    p = after;
                    n = taken + 1;
                    star = Some((after, taken + 1));
                }
                None => return false,
            },
        }
    }
    pattern
        .get(p..)
        .unwrap_or_default()
        .iter()
        .all(|&c| c == '*')
}

#[cfg(test)]
mod tests {
    use super::glob_matches;

    #[test]
    fn a_glob_matches_the_whole_name() {
        let cases = [
            ("admin_*", "admin_wipe", true),
            ("admin_*", "my_admin_wipe", false),
            ("*_order", "lookup_order", true),
            ("get_?", "get_x", true),
            ("get_?", "get_xy", false),
            ("*", "", true),
            ("a*b*c", "aXbYc", true),
            ("a*b*c", "aXbY", false),
            ("lookup", "lookup", true),
        ];
        for (pattern, name, matched) in cases {
            assert_eq!(glob_matches(pattern, name), matched, "{pattern} {name}");
        }
    }
}
