//! A pool from a connection string, read as Python's asyncpg reads it; and the
//! close of a pool a component opened itself.

use std::time::Duration;

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use roomkit::StoreError;
use tokio_postgres::Config;
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::connect::{Connector, Tls};
use crate::dsn::{self, Query};
use crate::hosts::{self, Address, Addresses};
use crate::passfile::{self, Lookup};
use crate::tls::{Encryption, Files, tls_config};

/// asyncpg's default `timeout`, which bounds the whole of a connection: each
/// host it tries, and a server that accepts and never answers.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(60);

/// A connection string read: the client's configuration, and what the client
/// does not read itself.
struct Target {
    /// Everything but the hosts, which [`Connector`] tries one at a time.
    config: Config,
    encryption: Encryption,
    files: Files,
    addresses: Addresses,
    /// Where a connection with no password looks one up.
    passfile: Option<Lookup>,
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
    // asyncpg takes an empty `PGPASSWORD` for a password, and reads no file then.
    if config.get_password().is_none()
        && let Ok(password) = std::env::var("PGPASSWORD")
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

/// `dsn`'s string cut, after its scheme is checked.
fn query_of(dsn: Option<&str>) -> Result<Query, StoreError> {
    let Some(dsn) = dsn else {
        return Ok(Query::default());
    };
    if !dsn.starts_with("postgresql://") && !dsn.starts_with("postgres://") {
        return Err(StoreError::Value(
            "invalid DSN: scheme is expected to be either \"postgresql\" or \"postgres\""
                .to_owned(),
        ));
    }
    dsn::read(dsn)
}

/// The client's configuration of `query`: the string it parses, the settings,
/// the netloc's credentials, the database, then the `PG*` variables and the
/// TLS mode `encryption` names.
fn client_config(query: &mut Query, encryption: Encryption) -> Result<Config, StoreError> {
    let mut config = if query.dsn.is_empty() {
        Config::new()
    } else {
        query
            .dsn
            .parse::<Config>()
            .map_err(StoreError::unavailable)?
    };
    with_settings(&mut config, &query.settings);
    if let Some(user) = query.user.take() {
        config.user(user);
    }
    if let Some(password) = query.password.take() {
        config.password(password);
    }
    if config.get_dbname().is_none()
        && let Some(database) = query.database.take()
    {
        config.dbname(database);
    }
    with_environment(&mut config);
    config.ssl_mode(encryption.client_mode());
    Ok(config)
}

/// Where `config`, which has no password, looks one up: `passfile`, else
/// `PGPASSFILE` as it is (an empty one is a path too), else `~/.pgpass`; for
/// `addresses`' hosts, the database and the user.
fn password_lookup(
    config: &Config,
    passfile: Option<String>,
    addresses: &Addresses,
) -> Option<Lookup> {
    if config.get_password().is_some() {
        return None;
    }
    let file = passfile::file(passfile.or_else(|| std::env::var("PGPASSFILE").ok()))?;
    Some(Lookup {
        file,
        hosts: addresses.for_password.clone(),
        database: config.get_dbname().map(str::to_owned),
        user: config.get_user().map(str::to_owned),
    })
}

/// What `dsn` names, what it leaves out read from the `PG*` environment
/// variables. A bad string is told without its text, which holds a password.
fn target(dsn: Option<&str>) -> Result<Target, StoreError> {
    let mut query = query_of(dsn)?;
    let mode = query.mode.take().or_else(|| variable("PGSSLMODE"));
    let encryption = mode
        .as_deref()
        .map_or(Ok(Encryption::Prefer), Encryption::named)?;
    let config = client_config(&mut query, encryption)?;
    let addresses = hosts::addresses(&query.hosts, &variable)?;
    let passfile = password_lookup(&config, query.passfile, &addresses);
    Ok(Target {
        config,
        encryption,
        files: Files {
            root_certificate: query.root_certificate.or_else(|| variable("PGSSLROOTCERT")),
            revocation_list: query.revocation_list.or_else(|| variable("PGSSLCRL")),
        },
        addresses,
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

/// Reads the password file `lookup` names once, so that one asyncpg refuses
/// fails `init`, and warns of one left unread; each connection reads it again.
async fn password_file_checked(lookup: Option<Lookup>) -> Result<(), StoreError> {
    let Some(lookup) = lookup else {
        return Ok(());
    };
    let found = tokio::task::spawn_blocking(move || passfile::password(&lookup))
        .await
        .map_err(StoreError::unavailable)??;
    if let Some(warning) = found.warning {
        passfile::warn_once(&warning);
    }
    Ok(())
}

/// What `addresses`' TCP connections, and their cancel requests, connect with:
/// none for sockets alone, which asyncpg asks no TLS of, no certificate read.
async fn tls_for(
    addresses: &Addresses,
    encryption: Encryption,
    files: Files,
) -> Result<Option<Tls>, StoreError> {
    let any_tcp = addresses
        .tried
        .iter()
        .any(|address| matches!(address, Address::Tcp { .. }));
    if !any_tcp {
        return Ok(None);
    }
    // The certificates are files, read off the runtime's threads.
    let tls = tokio::task::spawn_blocking(move || tls_config(encryption, &files))
        .await
        .map_err(StoreError::unavailable)??;
    Ok(Some(Tls(MakeRustlsConnect::new(tls))))
}

/// What a connection of `config` is given whichever hosts it tries: asyncpg's
/// 60 s, or the string's `connect_timeout`, which bounds each host, when it is
/// longer.
fn whole_timeout(config: &Config) -> Duration {
    config
        .get_connect_timeout()
        .map_or(CONNECT_TIMEOUT, |each| (*each).max(CONNECT_TIMEOUT))
}

/// A pool for `dsn`, with what a cancel request connects with when TLS may be
/// in use; `min_size` of its connections opened at once, so that a server out
/// of reach fails here rather than at the first operation, as asyncpg's
/// `create_pool`. A connection is given asyncpg's 60 s whichever hosts it
/// tries, each host the string's `connect_timeout` if it names a longer one.
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
        passfile,
    } = target(dsn)?;
    password_file_checked(passfile.clone()).await?;
    let tls = tls_for(&addresses, encryption, files).await?;
    let timeout = whole_timeout(&config);
    let connector = Connector {
        addresses: addresses.tried,
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

    /// The client never reads a host: an `@` past the credentials does not
    /// make the string's path or a parameter a host of its, and is read back.
    #[test]
    fn the_client_reads_no_host_of_the_string() {
        for (dsn, settings) in [
            ("postgresql://h/d@b?application_name=a@b", None),
            (
                "postgresql://u:p@h:1,k/d@b?application_name=a@b&host=x&hostaddr=127.0.0.1",
                // asyncpg reads no `hostaddr`: a setting for the server.
                Some("-c hostaddr=127.0.0.1"),
            ),
        ] {
            let read = target(Some(dsn)).unwrap();
            assert!(read.config.get_hosts().is_empty(), "{dsn}");
            assert!(read.config.get_ports().is_empty(), "{dsn}");
            assert!(read.config.get_hostaddrs().is_empty(), "{dsn}");
            assert_eq!(read.config.get_options(), settings, "{dsn}");
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
        let empty = dsn::netloc("postgresql://:@h/d").unwrap();
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

    /// A `connect_timeout` bounds each host, the whole given 60 s unless it is
    /// longer.
    #[test]
    fn a_connection_is_given_60_s_whichever_hosts_it_tries() {
        for (named, whole) in [
            ("", 60),
            ("?connect_timeout=2", 60),
            ("?connect_timeout=90", 90),
        ] {
            let read = target(Some(&format!("postgresql://u:p@h1,h2/d{named}"))).unwrap();
            assert_eq!(
                whole_timeout(&read.config),
                Duration::from_secs(whole),
                "{named}"
            );
        }
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
