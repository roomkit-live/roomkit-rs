//! Check an outbound HTTP destination before handing it to a transport.

mod addresses;
mod authority;

use std::net::{IpAddr, ToSocketAddrs};

use reqwest::Url;

/// The keyword arguments of Python's `validate_public_url`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValidatePublicUrlOptions<'a> {
    /// Schemes accepted by this caller.
    pub allowed_schemes: &'a [&'a str],
    /// Resolve a hostname and inspect every address it returns.
    pub resolve_dns: bool,
}

impl Default for ValidatePublicUrlOptions<'_> {
    fn default() -> Self {
        Self {
            allowed_schemes: &["http", "https"],
            resolve_dns: true,
        }
    }
}

/// Check an outbound URL and return the original spelling on success.
///
/// The default accepts HTTP and HTTPS and performs synchronous DNS resolution.
/// Supply internationalized hostnames as ASCII (Punycode), and IPv6 literals
/// without a zone identifier. Paths and queries may contain Unicode. Both
/// implementations use the same special-purpose address policy across the
/// supported Python patch releases.
/// Validation observes the answers at the time of the call. A transport that
/// needs to pin the connection must dial an address it has checked.
///
/// # Errors
/// Returns a diagnostic for an invalid URL, a disallowed scheme or address,
/// or a hostname with no usable DNS answers.
pub fn validate_public_url<'u>(
    url: &'u str,
    options: &ValidatePublicUrlOptions<'_>,
) -> Result<&'u str, String> {
    validate(url, options, |host, port| {
        (host, port)
            .to_socket_addrs()
            .map(|results| results.map(|addr| addr.ip()).collect())
            .unwrap_or_default()
    })
}

fn validate<'u>(
    original: &'u str,
    options: &ValidatePublicUrlOptions<'_>,
    resolve: impl FnOnce(&str, u16) -> Vec<IpAddr>,
) -> Result<&'u str, String> {
    let invalid = || "URL must have a scheme and host".to_owned();
    let (scheme, rest) = original
        .trim_start_matches([' ', '\t', '\r', '\n'])
        .split_once("://")
        .ok_or_else(invalid)?;
    let mut letters = scheme.bytes();
    if !letters
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic())
        || !letters.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'))
        || rest.is_empty()
        || rest.starts_with(['/', '?', '#', '\\'])
    {
        return Err(invalid());
    }
    let scheme = scheme.to_ascii_lowercase();
    if !options.allowed_schemes.contains(&scheme.as_str()) {
        return Err(format!(
            "URL scheme must be one of {:?}, got {scheme:?}",
            options.allowed_schemes
        ));
    }
    authority::check(rest)?;
    // Custom allowed schemes use the same hostname and address checks, even
    // when the generic URL parser would treat their host as an opaque string.
    let url = if matches!(scheme.as_str(), "http" | "https") {
        Url::parse(original)
    } else {
        Url::parse(&format!("http://{rest}"))
    }
    .map_err(|error| format!("Invalid URL: {error}"))?;
    let hostname = url.host_str().ok_or_else(invalid)?;
    let hostname = hostname.trim_end_matches('.');
    if hostname.is_empty() {
        return Err(invalid());
    }
    if ["localhost", "ip6-localhost", "ip6-loopback"]
        .iter()
        .any(|name| hostname.eq_ignore_ascii_case(name))
    {
        return Err(format!(
            "URL must not point to localhost (host={hostname:?})"
        ));
    }
    let host = hostname
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(hostname);
    if let Ok(address) = host.parse::<IpAddr>() {
        addresses::check(host, address)?;
    } else if options.resolve_dns {
        let default_port = if scheme == "https" { 443 } else { 80 };
        let port = url.port().filter(|p| *p != 0).unwrap_or(default_port);
        let addresses = resolve(host, port);
        if addresses.is_empty() {
            return Err(format!("URL host {host:?} did not resolve to any address"));
        }
        for address in addresses {
            addresses::check(host, address)?;
        }
    }
    Ok(original)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_dns_answer_is_checked_at_the_callers_port() {
        let options = ValidatePublicUrlOptions::default();
        let uri = "https://service.example:8443/path";
        let result = validate(uri, &options, |host, port| {
            assert_eq!((host, port), ("service.example", 8443));
            vec![
                "93.184.216.34".parse().unwrap(),
                "127.0.0.1".parse().unwrap(),
            ]
        });
        assert!(result.unwrap_err().contains("loopback"));
        assert!(validate(uri, &options, |_, _| Vec::new()).is_err());
        assert_eq!(
            validate(uri, &options, |_, _| vec!["93.184.216.34".parse().unwrap()]),
            Ok(uri)
        );
    }
    #[test]
    fn the_raw_authority_is_checked_before_resolution() {
        for authority in [
            r"name\part.example",
            "name\tpart.example",
            "name%2Epart.example",
            "one@two@service.example",
            "[2001:db8::1%25example]",
            "café.example",
            "\u{212a}.example",
        ] {
            for scheme in ["https", "service"] {
                for resolve_dns in [false, true] {
                    let options = ValidatePublicUrlOptions {
                        allowed_schemes: &[scheme],
                        resolve_dns,
                    };
                    let uri = format!("{scheme}://{authority}/");
                    assert!(
                        validate(&uri, &options, |_, _| panic!(
                            "invalid authority reached DNS"
                        ))
                        .is_err()
                    );
                }
            }
        }
    }

    #[test]
    fn ascii_dns_spelling_preserves_userinfo_path_and_query() {
        let uri = "service://user%40mail@XN--CAF-DMA.example/café?q=été";
        let options = ValidatePublicUrlOptions {
            allowed_schemes: &["service"],
            resolve_dns: true,
        };
        assert_eq!(
            validate(uri, &options, |host, port| {
                assert_eq!((host, port), ("xn--caf-dma.example", 80));
                vec!["93.184.216.34".parse().unwrap()]
            }),
            Ok(uri)
        );
    }

    #[test]
    fn mixed_dns_answers_include_the_ipv6_policy() {
        let result = validate(
            "https://service.example/",
            &ValidatePublicUrlOptions::default(),
            |_, _| {
                vec![
                    "93.184.216.34".parse().unwrap(),
                    std::net::Ipv6Addr::LOCALHOST.into(),
                ]
            },
        );
        assert!(result.unwrap_err().contains("loopback"));
    }
    #[test]
    fn custom_schemes_share_the_same_leading_whitespace_rule() {
        let options = ValidatePublicUrlOptions {
            allowed_schemes: &["service"],
            resolve_dns: false,
        };
        for prefix in [" ", "\t", "\r", "\n", "\u{00a0}"] {
            let uri = format!("{prefix}service://service.example/");
            assert_eq!(
                validate(&uri, &options, |_, _| unreachable!()).is_ok(),
                prefix != "\u{00a0}"
            );
        }
    }
}
