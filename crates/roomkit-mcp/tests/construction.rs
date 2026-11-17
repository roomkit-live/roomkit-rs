//! Public constructor validation follows Python's source order (RFC §21.2).

use roomkit_mcp::{
    FromCommandOptions, McpEnvironment, McpError, McpHeaders, McpToolProvider,
    McpToolProviderOptions,
};

fn stdio(command: &str) -> McpToolProviderOptions {
    McpToolProviderOptions {
        transport: "stdio".into(),
        command: Some(command.into()),
        ..Default::default()
    }
}

#[test]
fn direct_construction_checks_label_before_transport() {
    let options = McpToolProviderOptions {
        transport: "invalid".into(),
        ..Default::default()
    };
    let labelled = McpToolProviderOptions {
        label: Some("bad__label".into()),
        ..options.clone()
    };
    assert_label_refusal(&McpToolProvider::new(None, labelled));
    assert!(matches!(
        McpToolProvider::new(None, options),
        Err(McpError::Configuration {
            reason: "unsupported transport"
        })
    ));
}

// Python: test_direct_construction_refuses_crossed_options_in_source_order.
#[test]
fn direct_construction_checks_crossed_options_in_source_order() {
    let http = Some("http://example.test");
    let cases = [
        (http, stdio(""), "the program is empty"),
        (None, stdio(""), "the program is empty"),
        (http, stdio("server"), "URL and headers"),
        (
            None,
            McpToolProviderOptions {
                headers: Some(McpHeaders::new([("X-Test".into(), "1".into())])),
                ..stdio("server")
            },
            "URL and headers",
        ),
        (
            None,
            McpToolProviderOptions {
                transport: "sse".into(),
                command: Some("server".into()),
                ..Default::default()
            },
            "HTTP transport needs a URL",
        ),
        (
            Some(""),
            McpToolProviderOptions {
                env: Some(McpEnvironment::default()),
                ..Default::default()
            },
            "HTTP transport needs a URL",
        ),
        (
            http,
            McpToolProviderOptions {
                command: Some("server".into()),
                ..Default::default()
            },
            "for stdio",
        ),
        (
            http,
            McpToolProviderOptions {
                args: vec!["argument".into()],
                ..Default::default()
            },
            "for stdio",
        ),
        (
            http,
            McpToolProviderOptions {
                env: Some(McpEnvironment::default()),
                ..Default::default()
            },
            "for stdio",
        ),
        (
            http,
            McpToolProviderOptions {
                cwd: Some("".into()),
                ..Default::default()
            },
            "for stdio",
        ),
        (
            Some("invalid"),
            McpToolProviderOptions {
                headers: Some(McpHeaders::new([("Accept".into(), "invalid".into())])),
                ..Default::default()
            },
            "endpoint",
        ),
        (
            http,
            McpToolProviderOptions {
                headers: Some(McpHeaders::new([("Accept".into(), "invalid".into())])),
                ..Default::default()
            },
            "reserved for the transport",
        ),
    ];
    for (url, options, reason) in cases {
        let error = McpToolProvider::new(url, options).unwrap_err();
        assert!(
            error.to_string().contains(reason),
            "{error:?} must contain {reason}"
        );
    }
}

// Python: test_process_options_are_not_validated_during_construction.
#[test]
fn process_option_validation_is_deferred_until_entry() {
    for direct in [false, true] {
        let cases = [
            stdio("invalid\0command"),
            McpToolProviderOptions {
                args: vec!["invalid\0argument".into()],
                ..stdio("server")
            },
            McpToolProviderOptions {
                env: Some(McpEnvironment::new([("".into(), "value".into())].into())),
                ..stdio("server")
            },
            McpToolProviderOptions {
                env: Some(McpEnvironment::new(
                    [("invalid=key".into(), "value".into())].into(),
                )),
                ..stdio("server")
            },
            McpToolProviderOptions {
                env: Some(McpEnvironment::new(
                    [("key".into(), "invalid\0value".into())].into(),
                )),
                ..stdio("server")
            },
        ];
        for options in cases {
            let provider = if direct {
                McpToolProvider::new(None, options)
            } else {
                McpToolProvider::from_command(
                    options.command.unwrap(),
                    FromCommandOptions {
                        args: options.args,
                        env: options.env,
                        ..Default::default()
                    },
                )
            }
            .unwrap();
            assert!(!provider.connected());
        }
    }
}

// Python: test_http_construction_refuses_an_absent_host_or_empty_userinfo.
#[cfg(feature = "http")]
#[test]
fn http_construction_refuses_absent_authority_and_empty_userinfo() {
    for url in ["http:///mcp", "http://@example.test/mcp"] {
        for direct in [false, true] {
            let result = if direct {
                McpToolProvider::new(Some(url), McpToolProviderOptions::default())
            } else {
                McpToolProvider::from_url(url, roomkit_mcp::FromUrlOptions::default())
            };
            assert!(
                matches!(result, Err(McpError::Configuration { .. })),
                "{result:?}"
            );
        }
    }
}

// Python: test_direct_construction_is_lazy_and_keeps_accepted_empty_options.
#[test]
fn stdio_construction_is_lazy_and_accepts_empty_http_options() {
    for (url, options) in [
        (None, stdio("roomkit-does-not-exist")),
        (
            Some(""),
            McpToolProviderOptions {
                headers: Some(McpHeaders::default()),
                ..stdio("server")
            },
        ),
        (
            None,
            McpToolProviderOptions {
                env: Some(McpEnvironment::default()),
                cwd: Some("".into()),
                ..stdio("server")
            },
        ),
    ] {
        let provider = McpToolProvider::new(url, options).unwrap();
        assert!(!provider.connected());
        assert!(matches!(provider.get_tools(), Err(McpError::NotConnected)));
    }
}

// Python: the three accepted HTTP rows of the same empty-options witness.
#[cfg(feature = "http")]
#[test]
fn http_construction_is_lazy_and_accepts_empty_stdio_options() {
    for options in [
        McpToolProviderOptions::default(),
        McpToolProviderOptions {
            command: Some("".into()),
            ..Default::default()
        },
        McpToolProviderOptions {
            transport: "sse".into(),
            headers: Some(McpHeaders::default()),
            ..Default::default()
        },
    ] {
        let provider = McpToolProvider::new(Some("http://example.test"), options).unwrap();
        assert!(!provider.connected());
        assert!(matches!(provider.get_tools(), Err(McpError::NotConnected)));
    }
}

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
        ("invalid", None),
        (
            "http://example.test",
            Some(McpHeaders::new([(
                "Accept".to_owned(),
                "invalid".to_owned(),
            )])),
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

// Python: test_direct_stdio_copies_arguments_and_reads_the_retained_environment_on_entry
// and test_an_empty_environment_key_reaches_the_started_process.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn each_stdio_entry_reads_shared_environment_and_keeps_owned_arguments() {
    for discover in [false, true] {
        for direct in [false, true] {
            let environment = McpEnvironment::new(
                [
                    ("ROOMKIT_CONSTRUCTOR_TEST".into(), "before".into()),
                    ("".into(), "kept".into()),
                ]
                .into(),
            );
            let mut options = McpToolProviderOptions {
                args: vec!["normal".into()],
                env: Some(environment.clone()),
                cwd: Some(std::env::temp_dir()),
                discover,
                connect_timeout: Some(std::time::Duration::from_secs(3)),
                ..stdio(env!("CARGO_BIN_EXE_fake-mcp-server"))
            };
            let provider = if direct {
                McpToolProvider::new(None, options.clone())
            } else {
                McpToolProvider::from_command(
                    options.command.clone().unwrap(),
                    FromCommandOptions {
                        args: options.args.clone(),
                        env: options.env.clone(),
                        cwd: options.cwd.clone(),
                        discover,
                        connect_timeout: std::time::Duration::from_secs(3),
                        ..Default::default()
                    },
                )
            }
            .unwrap();
            options.args.clear();
            environment.insert("ROOMKIT_CONSTRUCTOR_TEST".into(), "after".into());
            for value in ["after", "next"] {
                provider.connect().await.unwrap();
                environment.insert("ROOMKIT_CONSTRUCTOR_TEST".into(), "next".into());
                let result = provider
                    .call_tool(
                        "constructor_probe",
                        serde_json::Map::default(),
                        roomkit_mcp::CallToolOptions::default(),
                    )
                    .await
                    .unwrap();
                let observed: serde_json::Value = serde_json::from_str(&result).unwrap();
                assert_eq!(
                    observed,
                    serde_json::json!({
                        "args": ["normal"], "env": value, "empty_env": "kept", "cwd": std::env::temp_dir().canonicalize().unwrap(),
                    })
                );
                assert_eq!(provider.tool_names().unwrap().is_empty(), !discover);
                provider.close().await;
                assert!(!provider.connected());
            }
        }
    }
}
