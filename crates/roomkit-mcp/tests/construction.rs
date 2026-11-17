//! Public constructor validation follows Python's source order (RFC §21.2).

use roomkit_mcp::{FromCommandOptions, McpError, McpToolProvider};

// Python: tests/test_mcp_constructor.py::test_factories_check_the_label_before_the_other_options
#[test]
fn validation_checks_the_label_before_a_command() {
    let provider = McpToolProvider::from_command(
        "",
        FromCommandOptions {
            label: Some("bad__label".into()),
            ..Default::default()
        },
    );
    assert_label_refusal(&provider);
}

#[cfg(feature = "http")]
#[test]
fn validation_checks_the_label_before_a_url_or_headers() {
    use roomkit_mcp::FromUrlOptions;

    for (url, headers) in [
        ("invalid", std::collections::BTreeMap::default()),
        (
            "http://example.test",
            [("Accept".to_owned(), "invalid".to_owned())].into(),
        ),
    ] {
        let provider = McpToolProvider::from_url(
            url,
            FromUrlOptions {
                headers,
                label: Some("bad__label".into()),
                ..Default::default()
            },
        );
        assert_label_refusal(&provider);
    }
}

fn assert_label_refusal(provider: &Result<McpToolProvider, McpError>) {
    assert!(
        matches!(
            provider,
            Err(McpError::Configuration {
                reason: "invalid server label"
            })
        ),
        "the label is validated before the other options: {provider:?}"
    );
}
