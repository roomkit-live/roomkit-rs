//! A pool from a connection string, read as Python's asyncpg reads it; and the
//! close of a pool a component opened itself.

use std::time::Duration;

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use roomkit::StoreError;
use tokio_postgres::config::SslMode;
use tokio_postgres::{Config, NoTls};
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::tls::{Encryption, Files, tls_config};

/// asyncpg's default `timeout`, for a server that accepts and never answers.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(60);

/// The DSN parameters the client reads itself. asyncpg passes any other to the
/// server as a setting (`search_path=tenant`).
const CLIENT_PARAMS: [&str; 18] = [
    "user",
    "password",
    "dbname",
    "options",
    "application_name",
    "sslnegotiation",
    "host",
    "hostaddr",
    "port",
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
    config: Config,
    encryption: Encryption,
    files: Files,
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
}

/// `value` with its `%XX` escapes read.
fn percent_decoded(value: &str) -> String {
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

/// `dsn`'s query read. The refusal does not repeat the string.
fn read_query(dsn: &str) -> Result<Query, StoreError> {
    let Some((base, query)) = dsn.split_once('?') else {
        return Ok(Query {
            dsn: dsn.to_owned(),
            ..Query::default()
        });
    };
    let mut read = Query::default();
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
            known if CLIENT_PARAMS.contains(&known) => kept.push(pair.to_owned()),
            setting => read
                .settings
                .push((setting.to_owned(), percent_decoded(value))),
        }
    }
    read.dsn = if kept.is_empty() {
        base.to_owned()
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
/// asyncpg reads them.
fn with_environment(config: &mut Config) {
    if config.get_hosts().is_empty() {
        config.host(variable("PGHOST").unwrap_or_else(|| "localhost".to_owned()));
    }
    if config.get_ports().is_empty()
        && let Some(port) = variable("PGPORT").and_then(|port| port.parse().ok())
    {
        config.port(port);
    }
    if config.get_user().is_none() {
        let user = variable("PGUSER")
            .or_else(|| variable("USER"))
            .or_else(|| variable("LOGNAME"));
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
    Ok(Target {
        config,
        encryption,
        files: Files {
            root_certificate: query.root_certificate.or_else(|| variable("PGSSLROOTCERT")),
            revocation_list: query.revocation_list.or_else(|| variable("PGSSLCRL")),
        },
    })
}

/// Whether `config` connects through Unix sockets alone: asyncpg never asks
/// for TLS on one, and the client asks a TLS connector for a name it has none
/// to give.
#[cfg(unix)]
fn only_sockets(config: &Config) -> bool {
    use tokio_postgres::config::Host;
    config
        .get_hosts()
        .iter()
        .all(|host| matches!(host, Host::Unix(_)))
}

/// Whether `config` connects through Unix sockets alone: there are none here.
#[cfg(not(unix))]
fn only_sockets(_: &Config) -> bool {
    false
}

/// A pool of `manager`, at most `max_size` connections.
fn pool_of(manager: Manager, max_size: usize) -> Result<Pool, StoreError> {
    Pool::builder(manager)
        .max_size(max_size)
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
) -> Result<(Pool, Option<MakeRustlsConnect>), StoreError> {
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
        mut config,
        encryption,
        files,
    } = target(dsn)?;
    let (pool, tls) = if only_sockets(&config) {
        config.ssl_mode(SslMode::Disable);
        let manager = Manager::from_config(config, NoTls, manager_config());
        (pool_of(manager, max_size)?, None)
    } else {
        // The certificates are files, read off the runtime's threads.
        let tls = tokio::task::spawn_blocking(move || tls_config(encryption, &files))
            .await
            .map_err(StoreError::unavailable)??;
        let tls = MakeRustlsConnect::new(tls);
        let manager = Manager::from_config(config, tls.clone(), manager_config());
        (pool_of(manager, max_size)?, Some(tls))
    };
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
        assert_eq!(read.dsn, "postgresql://u:p@h/d?connect_timeout=3");
        assert_eq!(read.mode.as_deref(), Some("verify-full"));
        assert_eq!(read.root_certificate.as_deref(), Some("/etc/ca.pem"));
        assert_eq!(read.revocation_list.as_deref(), Some("/etc/ca.crl"));
        assert_eq!(
            read_query("postgresql://u:p@h/d?sslmode=disable")
                .unwrap()
                .dsn,
            "postgresql://u:p@h/d"
        );
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
            let dsn = format!(
                "postgresql://u@/d?host={}&port=5432&sslmode={mode}",
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
