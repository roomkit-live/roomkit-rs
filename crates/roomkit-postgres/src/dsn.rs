//! A connection string cut as Python's `urllib.parse` (`urlsplit`, `parse_qs`)
//! and asyncpg 0.32 cut it: what the client is left to parse, and what this
//! crate reads itself. A refusal never repeats the string, which holds a
//! password.

use std::net::Ipv6Addr;

use roomkit::StoreError;

use crate::hosts::Given;

/// The parameters the client reads itself, besides those read here. asyncpg
/// passes any other to the server as a setting (`search_path=tenant`), and
/// so does this crate.
const CLIENT_PARAMS: [&str; 15] = [
    "user",
    "password",
    "dbname",
    "options",
    "application_name",
    "sslnegotiation",
    "connect_timeout",
    "tcp_user_timeout",
    "keepalives",
    "keepalives_idle",
    "keepalives_interval",
    "keepalives_retries",
    "target_session_attrs",
    "channel_binding",
    "load_balance_hosts",
];

/// A connection string, cut into what the client parses and what it does not.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Query {
    /// The string the client parses: no netloc, no parameter read here.
    pub(crate) dsn: String,
    pub(crate) mode: Option<String>,
    pub(crate) root_certificate: Option<String>,
    pub(crate) revocation_list: Option<String>,
    /// Each setting for the server, its value read.
    pub(crate) settings: Vec<(String, String)>,
    /// The `database` parameter, which names the database if the path does not.
    pub(crate) database: Option<String>,
    /// What the string says of its hosts.
    pub(crate) hosts: Given,
    /// The `passfile` parameter.
    pub(crate) passfile: Option<String>,
    /// The netloc's user and password, which a parameter does not override.
    pub(crate) user: Option<String>,
    pub(crate) password: Option<String>,
}

/// `value` with its `%XX` escapes read.
pub(crate) fn percent_decoded(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while let Some(&byte) = bytes.get(at) {
        let escape = (byte == b'%')
            .then(|| bytes.get(at + 1..at + 3))
            .flatten()
            .and_then(|hex| std::str::from_utf8(hex).ok())
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        if let Some(value) = escape {
            decoded.push(value);
            at += 3;
        } else {
            decoded.push(byte);
            at += 1;
        }
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

/// Whether `key` can be a parameter's name. What is left of an authority after
/// a raw `?` in a password cannot be one (`ss@host/db`): asyncpg refuses such a
/// string, and reading its tail as a query would drop the `sslmode` it holds.
fn is_param_name(key: &str) -> bool {
    !key.is_empty()
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
}

/// `text` with each `@` escaped: the client takes a string's credentials up
/// to its first `@`, wherever it is, and reads the escape back in a path and
/// a parameter.
fn at_escaped(text: &str) -> String {
    text.replace('@', "%40")
}

/// Python's `_check_bracketed_host`: an IPv6 address, or an `IPvFuture` one
/// (`v1.x`), never an IPv4 address.
fn is_bracketed_host(host: &str) -> bool {
    if let Some(future) = host.strip_prefix('v') {
        return future.split_once('.').is_some_and(|(version, rest)| {
            !version.is_empty()
                && version.chars().all(|c| c.is_ascii_hexdigit())
                && !rest.is_empty()
        });
    }
    let (address, zone) = host.split_once('%').unwrap_or((host, "x"));
    !zone.is_empty() && address.parse::<Ipv6Addr>().is_ok()
}

/// Whether Python's `urlsplit` takes `netloc`: brackets paired, and only around
/// an IPv6 address that starts the host part, a port alone after it
/// (`_check_bracketed_netloc`).
fn urlsplit_takes(netloc: &str) -> bool {
    let (open, close) = (netloc.contains('['), netloc.contains(']'));
    if !open && !close {
        return true;
    }
    if open != close {
        return false;
    }
    let host_and_port = netloc.rsplit_once('@').map_or(netloc, |(_, after)| after);
    let host = match host_and_port.split_once('[') {
        Some((before, _)) if !before.is_empty() => return false,
        Some((_, bracketed)) => {
            let (host, port) = bracketed.split_once(']').unwrap_or((bracketed, ""));
            if !port.is_empty() && !port.starts_with(':') {
                return false;
            }
            host
        }
        None => host_and_port
            .split_once(':')
            .map_or(host_and_port, |(host, _)| host),
    };
    is_bracketed_host(host)
}

/// What a connection string's netloc says, as Python's `urlsplit` and asyncpg
/// cut it: the netloc ends at the first `/`, its credentials at its first `@`,
/// the user's name at their first `:`, an empty name or password none.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Netloc {
    /// The string with no netloc: the client reads neither asyncpg's hosts nor
    /// its credentials.
    pub(crate) rest: String,
    pub(crate) hosts: String,
    pub(crate) user: Option<String>,
    pub(crate) password: Option<String>,
}

/// `base`'s netloc, and the string without it.
pub(crate) fn netloc(base: &str) -> Result<Netloc, StoreError> {
    let Some((scheme, after)) = base.split_once("://") else {
        return Ok(Netloc {
            rest: base.to_owned(),
            ..Netloc::default()
        });
    };
    let (netloc, path) = after.split_at(after.find('/').unwrap_or(after.len()));
    if !urlsplit_takes(netloc) {
        return Err(StoreError::Value(
            "invalid DSN: Invalid IPv6 URL".to_owned(),
        ));
    }
    let (credentials, hosts) = netloc.split_once('@').unwrap_or(("", netloc));
    let (user, password) = credentials.split_once(':').unwrap_or((credentials, ""));
    let given = |text: &str| (!text.is_empty()).then(|| percent_decoded(text));
    Ok(Netloc {
        rest: format!("{scheme}://{}", at_escaped(path)),
        hosts: hosts.to_owned(),
        user: given(user),
        password: given(password),
    })
}

/// `dsn` cut: its fragment dropped and its query split off, as `urlsplit`
/// does; each parameter's name and value read as `parse_qs` reads them, `+` a
/// space, a parameter with no value left out, one with no `=` refused.
pub(crate) fn read(dsn: &str) -> Result<Query, StoreError> {
    let dsn = dsn.split_once('#').map_or(dsn, |(before, _)| before);
    let (dsn, query) = dsn.split_once('?').unwrap_or((dsn, ""));
    let Netloc {
        rest: base,
        hosts,
        user,
        password,
    } = netloc(dsn)?;
    let mut read = Query {
        hosts: Given {
            netloc: hosts,
            ..Given::default()
        },
        user,
        password,
        ..Query::default()
    };
    let mut kept = Vec::new();
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let pair = pair.replace('+', "%20");
        let Some((key, value)) = pair.split_once('=') else {
            return Err(StoreError::Value(
                "invalid DSN: a parameter has no value".to_owned(),
            ));
        };
        if !is_param_name(key) {
            return Err(StoreError::Value(
                "invalid DSN: a parameter has no name; a ? in a password must be written %3F"
                    .to_owned(),
            ));
        }
        if value.is_empty() {
            continue;
        }
        let value = percent_decoded(value);
        match key {
            "sslmode" => read.mode = Some(value),
            "sslrootcert" => read.root_certificate = Some(value),
            "sslcrl" => read.revocation_list = Some(value),
            "database" => read.database = Some(value),
            "host" => read.hosts.host = Some(value),
            "port" => read.hosts.port = Some(value),
            "passfile" => read.passfile = Some(value),
            known if CLIENT_PARAMS.contains(&known) => kept.push(at_escaped(&pair)),
            setting => read.settings.push((setting.to_owned(), value)),
        }
    }
    read.dsn = if kept.is_empty() {
        base
    } else {
        format!("{base}?{}", kept.join("&"))
    };
    Ok(read)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_tls_params_leave_the_string_the_client_reads() {
        let query = read(
            "postgresql://u:p@h/d?sslmode=verify-full&sslrootcert=%2Fetc%2Fca.pem&sslcrl=%2Fetc%2Fca.crl&connect_timeout=3",
        )
        .unwrap();
        assert_eq!(query.dsn, "postgresql:///d?connect_timeout=3");
        assert_eq!(query.hosts.netloc, "h");
        assert_eq!(
            (query.user.as_deref(), query.password.as_deref()),
            (Some("u"), Some("p"))
        );
        assert_eq!(query.mode.as_deref(), Some("verify-full"));
        assert_eq!(query.root_certificate.as_deref(), Some("/etc/ca.pem"));
        assert_eq!(query.revocation_list.as_deref(), Some("/etc/ca.crl"));
        assert_eq!(
            read("postgresql://u:p@h/d?sslmode=disable").unwrap().dsn,
            "postgresql:///d"
        );
    }

    /// Python: `parse_qs` reads `+` as a space and leaves out a parameter with
    /// no value, which then counts as not given; `urlsplit` drops a fragment.
    #[test]
    fn a_parameter_is_read_as_parse_qs_reads_it() {
        let query =
            read("postgresql://h/d?passfile=%2Fa+b&password=&sslmode=&host=&x=1#y=2").unwrap();
        assert_eq!(query.passfile.as_deref(), Some("/a b"));
        assert_eq!(query.dsn, "postgresql:///d");
        assert_eq!((query.mode, query.hosts.host), (None, None));
        assert_eq!(query.settings, [("x".to_owned(), "1".to_owned())]);
        assert!(matches!(
            read("postgresql://h/d?sslmode"),
            Err(StoreError::Value(_))
        ));
    }

    /// Python: `urlsplit` refuses a bracket that does not hold an IPv6 address
    /// at the start of the host part, and stops the netloc at a `#`.
    #[test]
    fn a_netloc_urlsplit_refuses_is_refused() {
        for dsn in [
            "postgresql://u:p@h1:5433,h2,[::1]:5434/d",
            "postgresql://u:p@[::1],h2/d",
            "postgresql://u:p@[127.0.0.1]/d",
            "postgresql://u:p@[::1/d",
            "postgresql://u:p[x]@h/d",
        ] {
            assert!(matches!(read(dsn), Err(StoreError::Value(_))), "{dsn}");
        }
        for dsn in [
            "postgresql://u:p@[::1]:5434,h2/d",
            "postgresql://u:p@[fe80::1%25eth0]/d",
        ] {
            assert!(read(dsn).is_ok(), "{dsn}");
        }
        let cut = netloc("postgresql://u:pa").unwrap();
        assert_eq!(
            read("postgresql://u:pa#ss@h/d").unwrap().hosts.netloc,
            cut.hosts
        );
    }
}
