//! Which tools an agent may call (RFC §21.1 `ToolPolicy`).

/// Glob patterns over tool names (`*` any characters, `?` one), applied to
/// every tool the agent declares or runs (RFC §21.1): Python's
/// `ToolPolicy(allow=[], deny=[])`.
///
/// 1. With neither list, every tool is allowed.
/// 2. A name `deny` matches is denied, whatever `allow` says.
/// 3. With `allow` given, a name it does not match is denied.
/// 4. Any other name is allowed.
///
/// A denied tool is never declared to the model, and a call to it is
/// refused before its arguments are read: `Tool 'x' is not permitted by the
/// agent's tool policy.`
///
/// ```
/// use roomkit_ai::ToolPolicy;
///
/// let policy = ToolPolicy {
///     allow: vec!["search_*".into(), "get_?".into()],
///     deny: vec!["search_admin*".into()],
/// };
/// assert!(policy.is_allowed("search_orders"));
/// assert!(policy.is_allowed("get_x"));
/// assert!(!policy.is_allowed("search_admin_users"));
/// assert!(!policy.is_allowed("delete_orders"));
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ToolPolicy {
    /// The names allowed: with one pattern given, only the names they
    /// match are.
    pub allow: Vec<String>,
    /// The names denied, whatever `allow` says.
    pub deny: Vec<String>,
}

impl ToolPolicy {
    /// Whether the agent may call the tool `tool_name`.
    pub fn is_allowed(&self, tool_name: &str) -> bool {
        let matches = |pattern: &String| glob_matches(pattern, tool_name);
        !self.deny.iter().any(matches) && (self.allow.is_empty() || self.allow.iter().any(matches))
    }
}

/// The server an MCP alias `mcp__<server>__<tool>` names, `None` for a name
/// that is not an alias (RFC §21.2): the label a labelled MCP provider
/// serves aliases under. For the MCP crate.
#[doc(hidden)]
pub fn alias_server(name: &str) -> Option<&str> {
    let (server, _tool) = name.strip_prefix("mcp__")?.split_once("__")?;
    Some(server)
}

/// The tool a call to `name` runs: an MCP alias `mcp__<server>__<tool>`
/// runs `<tool>`, any other name runs as itself. For the MCP crate.
#[doc(hidden)]
pub fn served_tool_name(name: &str) -> &str {
    name.strip_prefix("mcp__")
        .and_then(|rest| rest.split_once("__"))
        .map_or(name, |(_server, tool)| tool)
}

/// Whether `policy` admits a call to `name` under every name it is judged
/// by: the name called, and the tool an MCP alias runs, so that no policy
/// is passed by calling a tool under its alias (RFC §21.1).
pub(crate) fn call_admitted(policy: &ToolPolicy, name: &str) -> bool {
    policy.is_allowed(name) && policy.is_allowed(served_tool_name(name))
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
        let policy = ToolPolicy {
            allow: vec!["*".into()],
            deny: vec!["admin_*".into()],
        };
        assert!(!policy.is_allowed("admin_wipe"));
    }
}
