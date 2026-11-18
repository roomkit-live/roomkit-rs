//! Public provider tool-name rules (RFC §6.7).

use roomkit_ai::ToolNameRule;

#[test]
fn custom_rules_match_entire_names_and_accept_an_iterator() {
    for (pattern, accepted, rejected) in [
        ("a|ab", "ab", "zab"),
        (r"(foo)\1", "foofoo", "foofoo!"),
        (r"(?i)foo", "FoO", "f"),
        ("(?x)foo # name", "foo", "foo "),
        (r"(?=read_)\w+", "read_docs", "write_docs"),
        ("", "", "x"),
    ] {
        let rule = ToolNameRule::new("acme", pattern).unwrap();
        assert_eq!(rule.vendor(), "acme");
        assert_eq!(rule.pattern(), pattern);
        rule.check(std::iter::once(accepted)).unwrap();
        assert!(rule.check([rejected]).is_err());
    }
    assert!(ToolNameRule::new("test", "(").is_err());
    assert!(ToolNameRule::new("test", "a)|(?:b").is_err());
}

#[test]
fn the_first_rejected_name_is_a_provider_error_before_the_request() {
    let rule = ToolNameRule::new("acme", "[a-z]+").unwrap();
    rule.check(std::iter::empty::<&str>()).unwrap();
    let error = rule.check(["ok", "a'b", "failed"]).unwrap_err();
    assert_eq!(
        error.message,
        r#"tool "a'b": acme accepts tool names matching [a-z]+"#
    );
    assert_eq!(error.provider, "acme");
    assert!(error.before_request);
    assert_eq!(error.context_overflow, Some(false));
    assert!(!error.retryable);
    assert_eq!(error.status_code, None);
}
