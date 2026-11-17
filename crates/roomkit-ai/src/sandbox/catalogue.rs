//! Reference declarations; only the host executor supplies their behavior.
use roomkit::model::Metadata;
use std::sync::LazyLock;

/// Prefix of names a sandbox executor declares.
pub const SANDBOX_TOOL_PREFIX: &str = "sandbox_";

/// The prompt describes the host environment without claiming isolation.
pub const SANDBOX_PREAMBLE: &str = "You have access to a development environment with file reading, search, and git capabilities, run by the host's sandbox executor with token-optimized output. Do not assume it is isolated from the host machine. Prefer specific tools (sandbox_read, sandbox_grep, sandbox_git) over sandbox_bash for common operations — they produce more structured, compact output.";

/// Ordered reference schemas. Clone the catalogue to customize an executor's tools.
pub static SANDBOX_TOOL_SCHEMAS: LazyLock<Vec<Metadata>> = LazyLock::new(|| {
    vec![
        Metadata::from_iter([
            ("name".into(), serde_json::json!("sandbox_read")),
            (
                "description".into(),
                serde_json::json!(
                    "Read file contents from the sandbox. Returns numbered lines. Use offset and limit to read specific portions of large files."
                ),
            ),
            (
                "parameters".into(),
                serde_json::json!({"type": "object", "properties": {"path": {"type": "string", "description": "Path to the file to read."}, "offset": {"type": "integer", "description": "Line number to start reading from (0-based)."}, "limit": {"type": "integer", "description": "Maximum number of lines to read."}}, "required": ["path"]}),
            ),
        ]),
        Metadata::from_iter([
            ("name".into(), serde_json::json!("sandbox_write")),
            (
                "description".into(),
                serde_json::json!(
                    "Write content to a file in the sandbox. Creates the file if it doesn't exist, or overwrites it if it does."
                ),
            ),
            (
                "parameters".into(),
                serde_json::json!({"type": "object", "properties": {"path": {"type": "string", "description": "Path to the file to write."}, "content": {"type": "string", "description": "The full content to write to the file."}}, "required": ["path", "content"]}),
            ),
        ]),
        Metadata::from_iter([
            ("name".into(), serde_json::json!("sandbox_edit")),
            (
                "description".into(),
                serde_json::json!(
                    "Edit a file in the sandbox by replacing an exact string match. Use sandbox_read first to see the current content."
                ),
            ),
            (
                "parameters".into(),
                serde_json::json!({"type": "object", "properties": {"path": {"type": "string", "description": "Path to the file to edit."}, "old_string": {"type": "string", "description": "The exact text to find and replace (must be unique in the file)."}, "new_string": {"type": "string", "description": "The replacement text."}}, "required": ["path", "old_string", "new_string"]}),
            ),
        ]),
        Metadata::from_iter([
            ("name".into(), serde_json::json!("sandbox_ls")),
            (
                "description".into(),
                serde_json::json!("List directory contents with compact, token-optimized output."),
            ),
            (
                "parameters".into(),
                serde_json::json!({"type": "object", "properties": {"path": {"type": "string", "description": "Directory path to list. Defaults to working directory."}}}),
            ),
        ]),
        Metadata::from_iter([
            ("name".into(), serde_json::json!("sandbox_grep")),
            (
                "description".into(),
                serde_json::json!("Search file contents using regex patterns with compact output."),
            ),
            (
                "parameters".into(),
                serde_json::json!({"type": "object", "properties": {"pattern": {"type": "string", "description": "Regex pattern to search for."}, "path": {"type": "string", "description": "File or directory to search in."}, "type": {"type": "string", "description": "File type filter (e.g. 'py', 'js', 'rs')."}}, "required": ["pattern"]}),
            ),
        ]),
        Metadata::from_iter([
            ("name".into(), serde_json::json!("sandbox_find")),
            (
                "description".into(),
                serde_json::json!("Find files by name pattern with compact tree output."),
            ),
            (
                "parameters".into(),
                serde_json::json!({"type": "object", "properties": {"path": {"type": "string", "description": "Directory to search in."}, "name": {"type": "string", "description": "Filename pattern to match."}, "type": {"type": "string", "description": "File type filter: 'f' for files, 'd' for directories."}}}),
            ),
        ]),
        Metadata::from_iter([
            ("name".into(), serde_json::json!("sandbox_git")),
            (
                "description".into(),
                serde_json::json!(
                    "Run any git command with compact, token-optimized output. Supports all git subcommands: status, diff, log, show, clone, checkout, fetch, pull, push, blame, branch, stash, etc."
                ),
            ),
            (
                "parameters".into(),
                serde_json::json!({"type": "object", "properties": {"args": {"type": "string", "description": "Git arguments as a single string. Examples: 'status', 'diff HEAD~3', 'log --oneline -10', 'clone https://github.com/org/repo.git', 'checkout -b feature', 'blame src/main.py'."}}, "required": ["args"]}),
            ),
        ]),
        Metadata::from_iter([
            ("name".into(), serde_json::json!("sandbox_diff")),
            (
                "description".into(),
                serde_json::json!("Compare two files with ultra-condensed diff output."),
            ),
            (
                "parameters".into(),
                serde_json::json!({"type": "object", "properties": {"file_a": {"type": "string", "description": "Path to the first file."}, "file_b": {"type": "string", "description": "Path to the second file."}}, "required": ["file_a", "file_b"]}),
            ),
        ]),
        Metadata::from_iter([
            ("name".into(), serde_json::json!("sandbox_delete")),
            (
                "description".into(),
                serde_json::json!("Delete a file or empty directory in the sandbox."),
            ),
            (
                "parameters".into(),
                serde_json::json!({"type": "object", "properties": {"path": {"type": "string", "description": "Path to the file or empty directory to delete."}}, "required": ["path"]}),
            ),
        ]),
        Metadata::from_iter([
            ("name".into(), serde_json::json!("sandbox_bash")),
            (
                "description".into(),
                serde_json::json!(
                    "Execute a bash command in the sandbox. Output is token-optimized. Prefer specific tools (sandbox_read, sandbox_grep, sandbox_git) over bash equivalents for common operations."
                ),
            ),
            (
                "parameters".into(),
                serde_json::json!({"type": "object", "properties": {"command": {"type": "string", "description": "The bash command to execute."}, "timeout": {"type": "integer", "description": "Timeout in seconds. Default: 30."}}, "required": ["command"]}),
            ),
        ]),
    ]
});
