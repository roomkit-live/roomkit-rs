//! A pool from a connection string, read as Python's asyncpg reads it; and the
//! close of a pool a component opened itself.

use std::net::IpAddr;
use std::time::Duration;

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use roomkit::StoreError;
use tokio_postgres::Config;
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::connect::{Connector, Tls};
use crate::hosts::{self, Address, Addresses, Given};
use crate::passfile::{self, Lookup};
use crate::tls::{Encryption, Files, tls_config};

/// asyncpg's default `timeout`, for a server that accepts and never answers.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(60);

/// The DSN parameters the client reads itself, besides those read here.
/// asyncpg passes any other to the server as a setting (`search_path=tenant`).
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

/// A connection string read: the client's configuration, and what the client
/// does not read itself.
struct Target {
    /// Everything but the hosts, which [`Connector`] tries one at a time.
    config: Config,
    encryption: Encryption,
    files: Files,
    addresses: Addresses,
    host_ips: Vec<IpAddr>,
    /// Where a connection with no password looks one up.
    passfile: Option<Lookup>,
}

/// A connection string's query, split into what the client reads and what it
/// does not.
#[derive(Debug, Default, PartialEq, Eq)]
struct Query {
    /// The string the client parses.
    dsn: String,
    mode: Option<String>,
    root_certificate: Option<String>,
    revocation_list: Option<String>,
    /// Each setting for the server, its value read.
    settings: Vec<(String, String)>,
    /// The `database` parameter, which names the database if the path does not.
    database: Option<String>,
    /// What the string says of its hosts.
    hosts: Given,
    /// The `hostaddr` parameter: the client's, not asyncpg's.
    hostaddr: Option<String>,
    /// The `passfile` parameter.
    passfile: Option<String>,
    /// The netloc's user and password, which a parameter does not override.
    user: Option<String>,
    password: Option<String>,
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

/// What a connection string's netloc says, as Python's `urlparse` and asyncpg
/// cut it: the netloc ends at the first `/`, its credentials at its first `@`,
/// the user's name at their first `:`, an empty name or password none.
#[derive(Debug, Default, PartialEq, Eq)]
struct Netloc {
    /// The string with no netloc: the client reads neither asyncpg's hosts nor
    /// its credentials.
    rest: String,
    hosts: String,
    user: Option<String>,
    password: Option<String>,
}

/// `base`'s netloc, and the string without it.
fn netloc(base: &str) -> Netloc {
    let Some((scheme, after)) = base.split_once("://") else {
        return Netloc {
            rest: base.to_owned(),
            ..Netloc::default()
        };
    };
    let (netloc, path) = after.split_at(after.find('/').unwrap_or(after.len()));
    let (credentials, hosts) = netloc.split_once('@').unwrap_or(("", netloc));
    let (user, password) = credentials.split_once(':').unwrap_or((credentials, ""));
    let given = |text: &str| (!text.is_empty()).then(|| percent_decoded(text));
    Netloc {
        rest: format!("{scheme}://{}", at_escaped(path)),
        hosts: hosts.to_owned(),
        user: given(user),
        password: given(password),
    }
}

/// `dsn`'s query read. The refusal does not repeat the string.
fn read_query(dsn: &str) -> Result<Query, StoreError> {
    let (dsn, query) = dsn.split_once('?').unwrap_or((dsn, ""));
    let Netloc {
        rest: base,
        hosts,
        user,
        password,
    } = netloc(dsn);
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
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if !is_param_name(key) {
            return Err(StoreError::Value(
                "invalid DSN: a parameter has no name; a ? in a password must be written %3F"
                    .to_owned(),
            ));
        }
        match key {
            "sslmode" => read.mode = Some(percent_decoded(value)),
            "sslrootcert" => read.root_certificate = Some(percent_decoded(value)),
            "sslcrl" => read.revocation_list = Some(percent_decoded(value)),
            "database" => read.database = Some(percent_decoded(value)),
            // Python's `parse_qs` leaves out a parameter with no value.
            "host" | "port" | "passfile" | "hostaddr" if value.is_empty() => {}
            "host" => read.hosts.host = Some(percent_decoded(value)),
            "port" => read.hosts.port = Some(percent_decoded(value)),
            "passfile" => read.passfile = Some(percent_decoded(value)),
            "hostaddr" => read.hostaddr = Some(percent_decoded(value)),
            known if CLIENT_PARAMS.contains(&known) => kept.push(at_escaped(pair)),
            setting => read
                .settings
                .push((setting.to_owned(), percent_decoded(value))),
        }
    }
    read.dsn = if kept.is_empty() {
        base
    } else {
        format!("{base}?{}", kept.join("&"))
    };
    Ok(read)
}

/// The value of the environment variable `name`, if it holds one.
fn variable(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

/// Adds `settings` to what the session starts with, each byte the server would
/// split or read a value on escaped, so that a value stays one.
fn with_settings(config: &mut Config, settings: &[(String, String)]) {
    if settings.is_empty() {
        return;
    }
    let mut options = config.get_options().unwrap_or_default().to_owned();
    for (name, value) in settings {
        let value: String = value
            .chars()
            .flat_map(|c| {
                // What C's `isspace` reads: `is_ascii_whitespace` leaves the vertical tab out.
                let escaped = c == '\\' || c == '\u{b}' || c.is_ascii_whitespace();
                escaped.then_some('\\').into_iter().chain([c])
            })
            .collect();
        options = format!("{options} -c {name}={value}");
    }
    config.options(options.trim_start());
}

/// Fills in what `config` leaves out from the `PG*` environment variables, as
/// asyncpg reads them; its hosts and ports are [`hosts::addresses`]'.
fn with_environment(config: &mut Config) {
    if config.get_user().is_none() {
        // Python's `getpass.getuser`, after `PGUSER`.
        let user = ["PGUSER", "LOGNAME", "USER", "LNAME", "USERNAME"]
            .into_iter()
            .find_map(variable);
        if let Some(user) = user {
            config.user(user);
        }
    }
    if config.get_password().is_none()
        && let Some(password) = variable("PGPASSWORD")
    {
        config.password(password);
    }
    if config.get_dbname().is_none()
        && let Some(database) = variable("PGDATABASE")
    {
        config.dbname(database);
    }
    if config.get_connect_timeout().is_none() {
        config.connect_timeout(CONNECT_TIMEOUT);
    }
}

/// What `dsn` names, what it leaves out read from the `PG*` environment
/// variables. A bad string is told without its text, which holds a password.
fn target(dsn: Option<&str>) -> Result<Target, StoreError> {
    let query = match dsn {
        Some(dsn) => {
            if !dsn.starts_with("postgresql://") && !dsn.starts_with("postgres://") {
                return Err(StoreError::Value(
                    "invalid DSN: scheme is expected to be either \"postgresql\" or \"postgres\""
                        .to_owned(),
                ));
            }
            read_query(dsn)?
        }
        None => Query::default(),
    };
    let mut config = if query.dsn.is_empty() {
        Config::new()
    } else {
        query
            .dsn
            .parse::<Config>()
            .map_err(StoreError::unavailable)?
    };
    with_settings(&mut config, &query.settings);
    if let Some(user) = query.user {
        config.user(user);
    }
    if let Some(password) = query.password {
        config.password(password);
    }
    if config.get_dbname().is_none()
        && let Some(database) = query.database
    {
        config.dbname(database);
    }
    with_environment(&mut config);
    let mode = query.mode.or_else(|| variable("PGSSLMODE"));
    let encryption = mode
        .as_deref()
        .map_or(Ok(Encryption::Prefer), Encryption::named)?;
    config.ssl_mode(encryption.client_mode());
    let addresses = hosts::addresses(&query.hosts, &variable)?;
    let host_ips = query
        .hostaddr
        .as_deref()
        .map_or(Ok(Vec::new()), |ips| {
            ips.split(',')
                .map(str::parse)
                .collect::<Result<Vec<IpAddr>, _>>()
        })
        .map_err(|_| {
            StoreError::Value("invalid DSN: a hostaddr is not an IP address".to_owned())
        })?;
    let passfile = if config.get_password().is_none() {
        // asyncpg reads `PGPASSFILE` as it is, an empty one a path too.
        passfile::file(query.passfile.or_else(|| std::env::var("PGPASSFILE").ok())).map(|file| {
            Lookup {
                file,
                hosts: addresses.for_password.clone(),
                database: config.get_dbname().map(str::to_owned),
                user: config.get_user().map(str::to_owned),
            }
        })
    } else {
        None
    };
    Ok(Target {
        config,
        encryption,
        files: Files {
            root_certificate: query.root_certificate.or_else(|| variable("PGSSLROOTCERT")),
            revocation_list: query.revocation_list.or_else(|| variable("PGSSLCRL")),
        },
        addresses,
        host_ips,
        passfile,
    })
}

/// A pool of `manager`, at most `max_size` connections, a connection given
/// `timeout` to be made whichever addresses it tries, as asyncpg's `timeout`
/// bounds the whole of its `connect`.
fn pool_of(manager: Manager, max_size: usize, timeout: Duration) -> Result<Pool, StoreError> {
    Pool::builder(manager)
        .max_size(max_size)
        .create_timeout(Some(timeout))
        .runtime(Runtime::Tokio1)
        .build()
        .map_err(StoreError::unavailable)
}

/// What a pool's connections use: Python's asyncpg checks a connection is open
/// when it is lent, no more.
fn manager_config() -> ManagerConfig {
    ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    }
}

/// A pool for `dsn`, with what a cancel request connects with when TLS may be
/// in use; `min_size` of its connections opened at once, so that a server out
/// of reach fails here rather than at the first operation, as asyncpg's
/// `create_pool`.
pub(crate) async fn open(
    dsn: Option<&str>,
    min_size: usize,
    max_size: usize,
) -> Result<(Pool, Option<Tls>), StoreError> {
    if max_size == 0 {
        return Err(StoreError::Value(
            "max_size is expected to be greater than zero".to_owned(),
        ));
    }
    if min_size > max_size {
        return Err(StoreError::Value(
            "min_size is greater than max_size".to_owned(),
        ));
    }
    let Target {
        config,
        encryption,
        files,
        addresses,
        host_ips,
        passfile,
    } = target(dsn)?;
    if let Some(lookup) = passfile.clone() {
        // A file asyncpg refuses fails here; each connection reads it again.
        let found = tokio::task::spawn_blocking(move || passfile::password(&lookup))
            .await
            .map_err(StoreError::unavailable)??;
        if let Some(warning) = found.warning {
            passfile::warn_once(&warning);
        }
    }
    // asyncpg asks no TLS of a Unix socket: with sockets alone, no
    // certificate is read.
    let any_tcp = addresses
        .tried
        .iter()
        .any(|address| matches!(address, Address::Tcp { .. }));
    let tls = if any_tcp {
        // The certificates are files, read off the runtime's threads.
        let tls = tokio::task::spawn_blocking(move || tls_config(encryption, &files))
            .await
            .map_err(StoreError::unavailable)??;
        Some(Tls(MakeRustlsConnect::new(tls)))
    } else {
        None
    };
    let timeout = config
        .get_connect_timeout()
        .copied()
        .unwrap_or(CONNECT_TIMEOUT);
    let connector = Connector {
        addresses: addresses.tried,
        host_ips,
        tls: tls.clone(),
        passfile,
    };
    let manager = Manager::from_connect(config, connector, manager_config());
    let pool = pool_of(manager, max_size, timeout)?;
    let mut opened = Vec::with_capacity(min_size);
    for _ in 0..min_size {
        opened.push(pool.get().await.map_err(StoreError::unavailable)?);
    }
    drop(opened);
    Ok((pool, tls))
}

/// How often a close looks whether the connections came back: a pool tells
/// no one.
const CLOSE_POLL: Duration = Duration::from_millis(5);

/// Closes `pool`, which `owner` opened, and gives the connections still in
/// use `timeout` to come back: whether they did (Python's `close_own_pool`).
/// A connection still out closes when its holder lets it go, so a close never
/// waits on work that holds one past its cut (RFC §12.10.4).
pub(crate) async fn close_own_pool(pool: &Pool, timeout: Duration, owner: &str) -> bool {
    pool.close();
    let deadline = tokio::time::Instant::now() + timeout;
    while pool.status().size > 0 {
        if tokio::time::Instant::now() >= deadline {
            tracing::warn!(
                target: "roomkit.store",
                "{owner} closed its connection pool with connections still in use after {:.1} s",
                timeout.as_secs_f64(),
            );
            return false;
        }
        tokio::time::sleep(CLOSE_POLL).await;
    }
    true
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn the_tls_params_leave_the_string_the_client_reads() {
        let read = read_query(
            "postgresql://u:p@h/d?sslmode=verify-full&sslrootcert=%2Fetc%2Fca.pem&sslcrl=%2Fetc%2Fca.crl&connect_timeout=3",
        )
        .unwrap();
        assert_eq!(read.dsn, "postgresql:///d?connect_timeout=3");
        assert_eq!(read.hosts.netloc, "h");
        assert_eq!(
            (read.user.as_deref(), read.password.as_deref()),
            (Some("u"), Some("p"))
        );
        assert_eq!(read.mode.as_deref(), Some("verify-full"));
        assert_eq!(read.root_certificate.as_deref(), Some("/etc/ca.pem"));
        assert_eq!(read.revocation_list.as_deref(), Some("/etc/ca.crl"));
        assert_eq!(
            read_query("postgresql://u:p@h/d?sslmode=disable")
                .unwrap()
                .dsn,
            "postgresql:///d"
        );
    }

    /// The client never reads a host: an `@` past the credentials does not
    /// make the string's path or a parameter a host of its, and is read back.
    #[test]
    fn the_client_reads_no_host_of_the_string() {
        for dsn in [
            "postgresql://h/d@b?application_name=a@b",
            "postgresql://u:p@h:1,k/d@b?application_name=a@b&host=x&hostaddr=127.0.0.1",
        ] {
            let read = target(Some(dsn)).unwrap();
            assert!(read.config.get_hosts().is_empty(), "{dsn}");
            assert!(read.config.get_ports().is_empty(), "{dsn}");
            assert!(read.config.get_hostaddrs().is_empty(), "{dsn}");
            assert_eq!(read.config.get_dbname(), Some("d@b"), "{dsn}");
            assert_eq!(read.config.get_application_name(), Some("a@b"), "{dsn}");
        }
    }

    /// Python: asyncpg takes the netloc's user and password over a parameter's,
    /// and an empty one for none, which the variables then fill in.
    #[test]
    fn the_netloc_s_credentials_come_first_and_an_empty_one_is_none() {
        let read = target(Some(
            "postgresql://u%40x:p%3Aw@h/d?user=other&password=other",
        ))
        .unwrap();
        assert_eq!(read.config.get_user(), Some("u@x"));
        assert_eq!(read.config.get_password(), Some(&b"p:w"[..]));
        let empty = netloc("postgresql://:@h/d");
        assert_eq!((empty.user, empty.password), (None, None));
        assert_eq!(empty.rest, "postgresql:///d");
    }

    /// Python: a `/` in a password ends asyncpg's netloc there, which it then
    /// refuses as a port; the client read it as the password.
    #[test]
    fn a_raw_slash_in_a_password_is_refused_without_its_text() {
        let Err(refused) = target(Some("postgresql://u:pa/hunter2@h/d")) else {
            panic!("the string was read");
        };
        assert!(matches!(refused, StoreError::Value(_)), "{refused:?}");
        assert!(!refused.to_string().contains("hunter2"), "{refused}");
    }

    /// A raw `?` in a password never drops the `sslmode` after it, as a plain
    /// connection in place of the one asked for: refused, as asyncpg refuses
    /// it, and without the password in the refusal.
    #[test]
    fn a_raw_question_mark_in_a_password_is_refused_without_its_text() {
        for dsn in [
            "postgresql://qm:pa?ss@127.0.0.1:1/postgres?sslmode=require",
            "postgresql://qm:123?sslmode=hunter2@127.0.0.1:1/db",
        ] {
            let Err(refused) = target(Some(dsn)) else {
                panic!("{dsn} was read");
            };
            assert!(matches!(refused, StoreError::Value(_)), "{refused:?}");
            let text = refused.to_string();
            assert!(
                !text.contains("pa?ss") && !text.contains("hunter2"),
                "{text}"
            );
        }
    }

    /// Python: asyncpg passes a parameter it does not read to the server as a
    /// setting, and takes `database` for the database's name.
    #[test]
    fn a_parameter_the_client_does_not_read_is_a_setting_for_the_server() {
        let read = target(Some(
            "postgresql://u:p@h/d?search_path=tenant&statement_timeout=5%20s&options=-cx%3Dy",
        ))
        .unwrap();
        assert_eq!(
            read.config.get_options(),
            Some("-cx=y -c search_path=tenant -c statement_timeout=5\\ s")
        );
        let named = target(Some("postgresql://u:p@h?database=mine")).unwrap();
        assert_eq!(named.config.get_dbname(), Some("mine"));
        let beside_a_path = target(Some("postgresql://u:p@h/path?database=mine")).unwrap();
        assert_eq!(beside_a_path.config.get_dbname(), Some("path"));
        for (escaped, blank) in [
            ("%09", '\t'),
            ("%0A", '\n'),
            ("%0B", '\u{b}'),
            ("%0C", '\u{c}'),
            ("%0D", '\r'),
        ] {
            let read = target(Some(&format!(
                "postgresql://u:p@h/d?search_path=a{escaped}-c{escaped}application_name%3Dinjected"
            )))
            .unwrap();
            assert_eq!(
                read.config.get_options(),
                Some(
                    format!("-c search_path=a\\{blank}-c\\{blank}application_name=injected")
                        .as_str()
                ),
                "{escaped}"
            );
        }
    }

    /// Python: asyncpg refuses a string that names no Postgres scheme, and
    /// the refusal does not repeat it.
    #[test]
    fn a_string_of_another_scheme_is_refused_without_its_text() {
        let Err(refused) = target(Some("mysql://user:secret@host/db")) else {
            panic!("a mysql string was read");
        };
        assert!(!refused.to_string().contains("secret"), "{refused}");
    }

    /// A connection that never answers does not hang `init`: asyncpg's 60 s.
    #[test]
    fn a_connection_has_asyncpg_s_timeout_unless_the_string_names_one() {
        let default = target(Some("postgresql://u@h/d")).unwrap();
        assert_eq!(default.config.get_connect_timeout(), Some(&CONNECT_TIMEOUT));
        let named = target(Some("postgresql://u@h/d?connect_timeout=3")).unwrap();
        assert_eq!(
            named.config.get_connect_timeout(),
            Some(&Duration::from_secs(3))
        );
    }

    /// `error`'s message, each of its causes' after it.
    fn full_text(error: &dyn Error) -> String {
        let mut text = error.to_string();
        let mut cause = error.source();
        while let Some(current) = cause {
            text = format!("{text}: {current}");
            cause = current.source();
        }
        text
    }

    /// A Unix socket connects, whatever the mode: the client asks a TLS
    /// connector for a name it has none to give, which refused every such
    /// string once connected. The socket here answers nothing and closes, so
    /// the connection fails after that point, on the socket, not on TLS.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_unix_socket_is_tried_whatever_the_sslmode() {
        // Not under TMPDIR, whose length a socket's path has no room for.
        let directory = std::path::Path::new("/tmp").join(format!("rk-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let listener =
            std::os::unix::net::UnixListener::bind(directory.join(".s.PGSQL.5432")).unwrap();
        std::thread::spawn(move || listener.incoming().for_each(drop));
        for mode in ["disable", "prefer", "require", "verify-full"] {
            // A password, or the connection would read the real ~/.pgpass.
            let dsn = format!(
                "postgresql://u:p@/d?host={}&port=5432&sslmode={mode}",
                directory.display()
            );
            let refused = open(Some(&dsn), 1, 1).await.err().unwrap();
            let text = full_text(&refused);
            assert!(!text.contains("dns"), "{mode}: {text}");
            assert!(!text.contains("root certificate"), "{mode}: {text}");
        }
        std::fs::remove_dir_all(&directory).ok();
    }
}
