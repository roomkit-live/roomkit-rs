//! The hosts and ports a connection tries, read as asyncpg 0.32 reads them
//! (`_parse_connect_dsn_and_args` and `_parse_hostlist` in its
//! `connect_utils.py`): the string's netloc, else its `host` and `port`
//! parameters, else `PGHOST` and `PGPORT`, else the local sockets before
//! `localhost` (HITL fa9dd934).

use std::path::PathBuf;

use roomkit::StoreError;

/// Where a connection is tried.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Address {
    /// A Unix socket: its directory, and the port its file is named after.
    Socket { directory: PathBuf, port: u16 },
    /// A TCP host and port.
    Tcp { host: String, port: u16 },
}

/// The addresses a connection tries, in their order, and the hosts and ports
/// a password is looked up for in a password file.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Addresses {
    pub(crate) tried: Vec<Address>,
    /// asyncpg's `auth_hosts` beside their ports: `localhost` alone for the
    /// local defaults, a socket's directory read as `localhost`.
    pub(crate) for_password: Vec<(String, u16)>,
}

/// What a connection string says of its hosts and ports.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Given {
    /// The netloc's host list, `h1:5432,h2`, as written.
    pub(crate) netloc: String,
    /// The `host` parameter, its escapes read.
    pub(crate) host: Option<String>,
    /// The `port` parameter, its escapes read.
    pub(crate) port: Option<String>,
}

/// The directories asyncpg tries a socket in when nothing names a host, before
/// `localhost`.
#[cfg(unix)]
const LOCAL_HOSTS: [&str; 5] = [
    "/run/postgresql",
    "/var/run/postgresql",
    "/tmp",
    "/private/tmp",
    "localhost",
];

/// Where asyncpg connects when nothing names a host: there are no sockets here.
#[cfg(not(unix))]
const LOCAL_HOSTS: [&str; 1] = ["localhost"];

/// A refusal of the string's hosts or ports, as asyncpg's `ValueError`s.
fn refused(why: &str) -> StoreError {
    StoreError::Value(format!("invalid connection string: {why}"))
}

/// `text` read as a port, as Python's `int` reads it, blanks around it.
fn port_of(text: &str) -> Result<u16, StoreError> {
    text.trim()
        .parse()
        .map_err(|_| refused("a port is not a number from 0 to 65535"))
}

/// A comma-separated list of ports.
fn ports_of(text: &str) -> Result<Vec<u16>, StoreError> {
    text.split(',').map(port_of).collect()
}

/// `ports` for `hosts` hosts: one port for each, or the same one for all
/// (asyncpg's `_validate_port_spec`).
fn matched(ports: &[u16], hosts: usize) -> Result<Vec<u16>, StoreError> {
    match ports {
        [one] => Ok(vec![*one; hosts]),
        many if many.len() == hosts => Ok(many.to_vec()),
        many => Err(refused(&format!(
            "could not match {} port numbers to {hosts} hosts",
            many.len()
        ))),
    }
}

/// `[address]:port`'s address and the digits after its colon, as asyncpg's
/// `(?:\[([^\]]+)\])(?::([0-9]+))?`, which reads from the start and leaves
/// what follows.
fn bracketed(spec: &str) -> Result<(&str, Option<&str>), StoreError> {
    let inner = spec.strip_prefix('[').unwrap_or(spec);
    let Some((address, after)) = inner
        .split_once(']')
        .filter(|(address, _)| !address.is_empty())
    else {
        return Err(refused("invalid IPv6 address"));
    };
    let port = after.strip_prefix(':').map(|after| {
        let digits = after.bytes().take_while(u8::is_ascii_digit).count();
        after.get(..digits).unwrap_or_default()
    });
    Ok((address, port.filter(|port| !port.is_empty())))
}

/// `list`'s hosts and their ports, as asyncpg's `_parse_hostlist`: a port
/// given for all, else each host's own, else `PGPORT`'s, else 5432. The
/// netloc's are read with their escapes (`unquote`).
fn host_list(
    list: &str,
    given: Option<&[u16]>,
    unquote: bool,
    environment: &dyn Fn(&str) -> Option<String>,
) -> Result<(Vec<String>, Vec<u16>), StoreError> {
    let specs: Vec<&str> = list.split(',').collect();
    let (given, defaults) = if let Some(ports) = given {
        (Some(matched(ports, specs.len())?), Vec::new())
    } else {
        let default = environment("PGPORT").map_or(Ok(vec![5432]), |ports| ports_of(&ports))?;
        (None, matched(&default, specs.len())?)
    };
    let decoded = |text: &str| {
        if unquote {
            crate::dsn::percent_decoded(text)
        } else {
            text.to_owned()
        }
    };
    let mut hosts = Vec::with_capacity(specs.len());
    let mut ports = Vec::with_capacity(specs.len());
    for (at, spec) in specs.iter().enumerate() {
        let (address, port) = match spec.as_bytes().first() {
            None => return Err(refused("a host of the list is empty")),
            Some(b'/') => (*spec, None),
            Some(b'[') => bracketed(spec)?,
            Some(_) => spec
                .split_once(':')
                .map_or((*spec, None), |(address, port)| (address, Some(port))),
        };
        hosts.push(decoded(address));
        if given.is_none() {
            let port = port.filter(|port| !port.is_empty());
            ports.push(match port {
                Some(port) => port_of(&decoded(port))?,
                None => defaults.get(at).copied().unwrap_or(5432),
            });
        }
    }
    Ok((hosts, given.unwrap_or(ports)))
}

/// Where `host` at `port` is reached: a path is a socket's directory, or the
/// socket itself when it holds `.s.PGSQL.`, as asyncpg takes it. The client
/// joins a directory and a port, so such a path is its parent and the port
/// its name ends with; one no port names as the client would write it
/// (`.s.PGSQL.05432`) is refused.
fn address(host: String, port: u16) -> Result<Address, StoreError> {
    if !host.starts_with('/') {
        return Ok(Address::Tcp { host, port });
    }
    if !host.contains(".s.PGSQL.") {
        return Ok(Address::Socket {
            directory: PathBuf::from(host),
            port,
        });
    }
    let path = PathBuf::from(&host);
    let named = path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix(".s.PGSQL."))
        .and_then(|digits| {
            digits
                .parse::<u16>()
                .ok()
                .filter(|port| port.to_string() == digits)
        });
    match (named, path.parent()) {
        (Some(port), Some(directory)) => Ok(Address::Socket {
            directory: directory.to_path_buf(),
            port,
        }),
        _ => Err(refused(
            "a Unix socket's path names no port the client can join",
        )),
    }
}

/// The hosts `given` names and their ports, else `PGHOST`'s: the netloc's
/// first, its ports leaving the `port` parameter unread, then the `host`
/// parameter's, then the variable's; no host, and the ports the parameter
/// names if any, when none of them names one.
fn named_hosts(
    given: &Given,
    environment: &dyn Fn(&str) -> Option<String>,
) -> Result<(Vec<String>, Option<Vec<u16>>), StoreError> {
    if !given.netloc.is_empty() {
        let (hosts, ports) = host_list(&given.netloc, None, true, environment)?;
        return Ok((hosts, Some(ports)));
    }
    let ports = given
        .port
        .as_deref()
        .filter(|port| !port.is_empty())
        .map(ports_of)
        .transpose()?;
    let host = given
        .host
        .clone()
        .filter(|host| !host.is_empty())
        .or_else(|| environment("PGHOST"));
    match host {
        Some(host) => {
            let (hosts, ports) = host_list(&host, ports.as_deref(), false, environment)?;
            Ok((hosts, Some(ports)))
        }
        None => Ok((Vec::new(), ports)),
    }
}

/// The addresses `given` names, what it leaves out read from `environment`
/// (`PGHOST`, `PGPORT`) and asyncpg's local defaults.
pub(crate) fn addresses(
    given: &Given,
    environment: &dyn Fn(&str) -> Option<String>,
) -> Result<Addresses, StoreError> {
    let (mut hosts, ports) = named_hosts(given, environment)?;
    let local = hosts.is_empty();
    if local {
        hosts = LOCAL_HOSTS.iter().map(|host| (*host).to_owned()).collect();
    }
    let ports = match ports {
        Some(ports) => ports,
        None => environment("PGPORT").map_or(Ok(vec![5432]), |ports| ports_of(&ports))?,
    };
    let ports = matched(&ports, hosts.len())?;
    let password_hosts = if local {
        vec!["localhost".to_owned()]
    } else {
        hosts
            .iter()
            .map(|host| {
                if host.starts_with('/') {
                    "localhost".to_owned()
                } else {
                    host.clone()
                }
            })
            .collect()
    };
    Ok(Addresses {
        for_password: password_hosts
            .into_iter()
            .zip(ports.iter().copied())
            .collect(),
        tried: hosts
            .into_iter()
            .zip(ports)
            .map(|(host, port)| address(host, port))
            .collect::<Result<_, _>>()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tcp(host: &str, port: u16) -> Address {
        Address::Tcp {
            host: host.to_owned(),
            port,
        }
    }

    fn socket(directory: &str, port: u16) -> Address {
        Address::Socket {
            directory: PathBuf::from(directory),
            port,
        }
    }

    /// The environment `pairs` make.
    fn environment(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        move |name| {
            pairs
                .iter()
                .find(|(named, _)| named == name)
                .map(|(_, value)| value.clone())
        }
    }

    fn netloc(list: &str) -> Given {
        Given {
            netloc: list.to_owned(),
            ..Given::default()
        }
    }

    /// Python (asyncpg's `_parse_hostlist`): each host of the netloc its own
    /// port, else `PGPORT`'s, else 5432, an IPv6 address in brackets (first:
    /// `urlsplit` takes a bracket there only).
    #[test]
    fn a_netloc_list_gives_each_host_its_port() {
        let read = addresses(
            &netloc("[::1]:5434,h1:5433,h2,%2Frun%2Fpg"),
            &environment(&[]),
        )
        .unwrap();
        assert_eq!(
            read.tried,
            [
                tcp("::1", 5434),
                tcp("h1", 5433),
                tcp("h2", 5432),
                socket("/run/pg", 5432)
            ]
        );
        let from_variable =
            addresses(&netloc("h1,h2:7"), &environment(&[("PGPORT", "6")])).unwrap();
        assert_eq!(from_variable.tried, [tcp("h1", 6), tcp("h2", 7)]);
    }

    /// Python: the netloc's hosts leave the `port` parameter unread, as
    /// asyncpg reads it only where the netloc names none.
    #[test]
    fn the_port_parameter_counts_only_without_a_netloc_host() {
        let named = Given {
            netloc: "h".to_owned(),
            port: Some("6000".to_owned()),
            ..Given::default()
        };
        assert_eq!(
            addresses(&named, &environment(&[])).unwrap().tried,
            [tcp("h", 5432)]
        );
        let in_query = Given {
            host: Some("a,b".to_owned()),
            port: Some("1,2".to_owned()),
            ..Given::default()
        };
        assert_eq!(
            addresses(&in_query, &environment(&[("PGHOST", "x")]))
                .unwrap()
                .tried,
            [tcp("a", 1), tcp("b", 2)]
        );
    }

    /// Python: `PGHOST` and `PGPORT` hold lists, `PGHOST`'s hosts their own
    /// ports too.
    #[test]
    fn the_variables_hold_lists() {
        let read = addresses(
            &Given::default(),
            &environment(&[("PGHOST", "a,/sockets,b:9"), ("PGPORT", "1,2,3")]),
        )
        .unwrap();
        assert_eq!(
            read.tried,
            [tcp("a", 1), socket("/sockets", 2), tcp("b", 9)]
        );
        assert_eq!(
            read.for_password,
            [
                ("a".to_owned(), 1),
                ("localhost".to_owned(), 2),
                ("b".to_owned(), 9)
            ]
        );
    }

    /// Python: with nothing naming a host, the local sockets before
    /// `localhost`, a password looked up for `localhost` alone.
    #[cfg(unix)]
    #[test]
    fn no_host_tries_the_local_sockets_then_localhost() {
        let read = addresses(&Given::default(), &environment(&[("PGPORT", "5433")])).unwrap();
        assert_eq!(
            read.tried,
            [
                socket("/run/postgresql", 5433),
                socket("/var/run/postgresql", 5433),
                socket("/tmp", 5433),
                socket("/private/tmp", 5433),
                tcp("localhost", 5433),
            ]
        );
        assert_eq!(read.for_password, [("localhost".to_owned(), 5433)]);
    }

    /// Python: a path that names the socket itself is that socket, whatever
    /// the port; one the client cannot join from a directory and a port is
    /// refused.
    #[test]
    fn a_socket_s_own_path_is_that_socket() {
        let host = |path: &str| Given {
            host: Some(path.to_owned()),
            ..Given::default()
        };
        let read = addresses(&host("/run/pg/.s.PGSQL.6543"), &environment(&[])).unwrap();
        assert_eq!(read.tried, [socket("/run/pg", 6543)]);
        for odd in ["/run/pg/.s.PGSQL.05432", "/run/pg/.s.PGSQL.5432x/sub"] {
            let read = addresses(&host(odd), &environment(&[]));
            assert!(matches!(read, Err(StoreError::Value(_))), "{odd}: {read:?}");
        }
    }

    /// Python: asyncpg refuses ports that do not match the hosts, a port that
    /// is no number, an empty host and a bracket with no address.
    #[test]
    fn a_list_asyncpg_refuses_is_refused() {
        for (given, variables) in [
            (netloc("a,b"), vec![("PGPORT", "1,2,3")]),
            (netloc("a:x"), vec![]),
            (netloc("a,"), vec![]),
            (netloc("[]:5432"), vec![]),
            (Given::default(), vec![("PGPORT", "1,2")]),
            (Given::default(), vec![("PGPORT", "port")]),
        ] {
            let read = addresses(&given, &environment(&variables));
            assert!(
                matches!(read, Err(StoreError::Value(_))),
                "{given:?} {variables:?}: {read:?}"
            );
        }
    }
}
