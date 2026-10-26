//! Which tools an agent may call (RFC §21.1 `ToolPolicy`).

/// Glob patterns over tool names (`*` any characters, `?` one), applied to
/// every tool the agent declares or runs (RFC §21.1):
///
/// 1. a name `deny` matches is denied, whatever `allow` says;
/// 2. with `allow` given, a name it does not match is denied;
/// 3. any other name is allowed.
///
/// A denied tool is never declared to the model, and a call to it is
/// refused before its arguments are read: `Tool 'x' is not permitted by the
/// agent's tool policy.`
///
/// ```
/// use roomkit_ai::ToolPolicy;
///
/// let policy = ToolPolicy::new().allow("search_*").allow("get_?").deny("search_admin*");
/// assert!(policy.permits("search_orders"));
/// assert!(policy.permits("get_x"));
/// assert!(!policy.permits("search_admin_users"));
/// assert!(!policy.permits("delete_orders"));
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ToolPolicy {
    allow: Vec<String>,
    deny: Vec<String>,
}

impl ToolPolicy {
    /// A policy that allows every tool.
    pub fn new() -> Self {
        Self::default()
    }

    /// The same policy, allowing the names `pattern` matches: once one
    /// pattern is allowed, only allowed names are.
    #[must_use]
    pub fn allow(mut self, pattern: impl Into<String>) -> Self {
        self.allow.push(pattern.into());
        self
    }

    /// The same policy, denying the names `pattern` matches.
    #[must_use]
    pub fn deny(mut self, pattern: impl Into<String>) -> Self {
        self.deny.push(pattern.into());
        self
    }

    /// Whether the agent may call the tool `name`.
    pub fn permits(&self, name: &str) -> bool {
        let matches = |pattern: &String| glob_matches(pattern, name);
        !self.deny.iter().any(matches) && (self.allow.is_empty() || self.allow.iter().any(matches))
    }
}

/// Whether `pattern`, where `*` matches any characters and `?` one, matches
/// the whole of `name`.
fn glob_matches(pattern: &str, name: &str) -> bool {
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
    use super::*;

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

    #[test]
    fn deny_wins_over_allow() {
        let policy = ToolPolicy::new().allow("*").deny("admin_*");
        assert!(!policy.permits("admin_wipe"));
    }
}
