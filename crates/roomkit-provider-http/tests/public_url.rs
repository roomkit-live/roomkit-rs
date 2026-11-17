//! Applications can validate an outbound URL independently of a provider client.

use roomkit_provider_http::{ValidatePublicUrlOptions, validate_public_url};

#[test]
fn a_host_can_check_an_address_without_rewriting_it() {
    let url = "https://service.example:8443/A?q=1#fragment";
    let offline = ValidatePublicUrlOptions {
        resolve_dns: false,
        ..Default::default()
    };
    assert_eq!(validate_public_url(url, &offline).unwrap(), url);
    assert!(validate_public_url("https://localhost", &offline).is_err());
    assert!(validate_public_url("https://127.0.0.1", &offline).is_err());
}

#[test]
fn callers_can_set_the_schemes_and_the_default_checks_dns() {
    let defaults = ValidatePublicUrlOptions::default();
    assert!(defaults.resolve_dns);
    assert_eq!(defaults.allowed_schemes, ["http", "https"]);
    let secure = ValidatePublicUrlOptions {
        allowed_schemes: &["https"],
        resolve_dns: false,
    };
    assert!(validate_public_url("http://service.example", &secure).is_err());
    assert!(validate_public_url("https://service.example", &secure).is_ok());
}

#[test]
fn explicit_ports_are_checked_before_resolving_a_host() {
    for dns in [true, false] {
        let options = ValidatePublicUrlOptions {
            resolve_dns: dns,
            ..Default::default()
        };
        for host in ["service.example", "93.184.216.34"] {
            for port in ["invalid", "65536"] {
                let url = format!("https://{host}:{port}/");
                assert!(
                    validate_public_url(&url, &options)
                        .unwrap_err()
                        .contains("port")
                );
            }
        }
    }
}

#[test]
fn an_application_scheme_uses_the_same_address_policy() {
    let options = ValidatePublicUrlOptions {
        allowed_schemes: &["service"],
        resolve_dns: false,
    };
    let uri = "service://example.org/resource";
    assert_eq!(validate_public_url(uri, &options), Ok(uri));
    assert!(validate_public_url("service://127.0.0.1/resource", &options).is_err());
}
