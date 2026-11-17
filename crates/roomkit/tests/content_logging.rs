//! Isolated public environment/setter tests, without a runtime or other threads.

use roomkit::{content_logging_enabled, set_content_logging};

fn child(expected: bool) {
    assert_eq!(content_logging_enabled(), expected);
    assert_eq!(content_logging_enabled(), expected);
    set_content_logging(!expected);
    assert_eq!(content_logging_enabled(), !expected);
    set_content_logging(expected);
    assert_eq!(content_logging_enabled(), expected);
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(expected) = std::env::args().nth(1) {
        child(expected == "true");
        return Ok(());
    }
    for (value, expected) in [
        (None, false),
        (Some(""), false),
        (Some("0"), false),
        (Some("false"), false),
        (Some("1"), true),
        (Some("true"), true),
        (Some("yes"), true),
        (Some("on"), true),
        (Some("  TrUe  "), true),
        (Some("enabled"), false),
        (Some("\u{1c}\u{1d}TrUe\u{1e}\u{1f}"), true),
        (Some("tr\u{1c}ue"), false),
    ] {
        let mut command = std::process::Command::new(std::env::current_exe()?);
        command
            .arg(expected.to_string())
            .env_remove("ROOMKIT_LOG_CONTENT");
        if let Some(value) = value {
            command.env("ROOMKIT_LOG_CONTENT", value);
        }
        let output = command.output()?;
        assert!(
            output.status.success(),
            "{value:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}
