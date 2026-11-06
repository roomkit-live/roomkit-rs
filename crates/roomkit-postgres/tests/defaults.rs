//! A connection string read as asyncpg reads it, and its defaults for what it
//! leaves out (HITL fa9dd934, 22f38d15), on a real server: Python's tests of
//! the same names in `tests/test_store_postgres.py`. Each case opens a store in a child process
//! whose environment holds no `PG*` variable of the parent's, and whose `HOME`
//! is a directory of the test's: no test reads the real `~/.pgpass`. Each test
//! skips itself without `POSTGRES_DSN`; those on the server's Unix socket also
//! without `POSTGRES_SOCKET_DIR`, the directory that holds it, where the server
//! asks no password while TCP asks one.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod support;
#[path = "../../roomkit/tests/support/warnings.rs"]
mod warnings;

use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use roomkit::ConversationStore;
use roomkit::model::RoomId;
use roomkit_postgres::{InitOptions, PostgresStore, PostgresStoreOptions};
use tokio_postgres::NoTls;
use tokio_postgres::config::Host;

use crate::support::{TempSchema, dsn, full_text};

/// The connection string the child's store opens.
const CHILD_DSN: &str = "ROOMKIT_TEST_DEFAULTS_DSN";

/// Where the child of a case writes what came of its store.
const CHILD_OUT: &str = "ROOMKIT_TEST_DEFAULTS_OUT";

/// In the child of a case, opens a store on [`CHILD_DSN`] and writes what came
/// of it to [`CHILD_OUT`]: each warning of a password file, then `connected`,
/// or `refused: ` and why, a line each. Whether this process is such a child.
async fn ran_as_child() -> bool {
    let (Some(dsn), Some(out)) = (std::env::var(CHILD_DSN).ok(), std::env::var_os(CHILD_OUT))
    else {
        return false;
    };
    let (warnings, _capture) = warnings::Warnings::capture();
    let store = PostgresStore::new(PostgresStoreOptions {
        dsn: Some(dsn),
        ..PostgresStoreOptions::default()
    });
    let opened = store
        .init(InitOptions {
            min_size: 1,
            max_size: 1,
        })
        .await;
    let mut said: Vec<String> = warnings
        .containing("password file")
        .into_iter()
        .map(|warning| format!("warning: {}", warning.replace('\n', " ")))
        .collect();
    match opened {
        Ok(()) => {
            let room = RoomId::new("nobody's").unwrap();
            assert!(!store.room_exists(&room).await.unwrap());
            store.close().await;
            said.push("connected".to_owned());
        }
        Err(refused) => said.push(format!(
            "refused: {}",
            full_text(&refused).replace('\n', " ")
        )),
    }
    std::fs::write(out, said.join("\n")).unwrap();
    true
}

/// What a child's store came to.
#[derive(Debug)]
struct Opened {
    connected: bool,
    refusal: String,
    warnings: Vec<String>,
}

/// Runs `test` again in a child process whose environment holds only `vars`,
/// `HOME` set to `home`, and whose store opens `dsn`.
fn open_in_child(test: &str, home: &Path, dsn: &str, vars: &[(&str, &str)]) -> Opened {
    let out = home.with_extension("out");
    let child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--test-threads=1"])
        .env_clear()
        .env(CHILD_DSN, dsn)
        .env(CHILD_OUT, &out)
        .env("HOME", home)
        .envs(vars.iter().copied())
        .output()
        .unwrap();
    assert!(
        child.status.success(),
        "{}{}",
        String::from_utf8_lossy(&child.stdout),
        String::from_utf8_lossy(&child.stderr)
    );
    let said = std::fs::read_to_string(&out).unwrap();
    std::fs::remove_file(&out).unwrap();
    let line = |prefix: &str| {
        said.lines()
            .filter_map(|line| line.strip_prefix(prefix))
            .map(str::to_owned)
            .collect::<Vec<_>>()
    };
    let connected = said.lines().any(|line| line == "connected");
    let refusal = line("refused: ").concat();
    assert!(connected || !refusal.is_empty(), "{said}");
    Opened {
        connected,
        refusal,
        warnings: line("warning: "),
    }
}

/// `POSTGRES_DSN`'s parts, a test's own schema in the string its children open.
struct Server {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
    /// The query a child's string ends with: the schema, and `POSTGRES_DSN`'s
    /// `sslmode` if it names one.
    query: String,
    _schema: TempSchema,
}

/// `value` with each byte a URL's authority or query would read percent-encoded.
fn encoded(value: &str) -> String {
    value
        .bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
                char::from(byte).to_string()
            } else {
                format!("%{byte:02X}")
            }
        })
        .collect()
}

impl Server {
    /// The server `POSTGRES_DSN` names over TCP, a schema of the test's on it.
    async fn new() -> Option<Self> {
        let server = dsn()?;
        let config: tokio_postgres::Config = server.parse().unwrap();
        let Host::Tcp(host) = config.get_hosts().first()?.clone() else {
            return None;
        };
        let schema = TempSchema::new().await;
        let mode = server
            .split_once('?')
            .and_then(|(_, query)| query.split('&').find(|pair| pair.starts_with("sslmode=")))
            .map(|pair| format!("&{pair}"))
            .unwrap_or_default();
        Some(Self {
            host,
            port: *config.get_ports().first().unwrap_or(&5432),
            user: config.get_user().unwrap_or_default().to_owned(),
            password: String::from_utf8(config.get_password().unwrap_or_default().to_vec())
                .unwrap(),
            database: config.get_dbname().unwrap_or_default().to_owned(),
            query: format!("?search_path={}{mode}", schema.name()),
            _schema: schema,
        })
    }

    /// Whether a TCP connection with no password is refused: where it is not,
    /// a connection proves nothing of where a password came from.
    async fn asks_a_password(&self) -> bool {
        tokio_postgres::Config::new()
            .host(&self.host)
            .port(self.port)
            .user(&self.user)
            .dbname(&self.database)
            .connect(NoTls)
            .await
            .is_err()
    }

    /// `user@`, the authority's credentials with no password.
    fn user(&self) -> String {
        format!("{}@", encoded(&self.user))
    }

    /// `user:password@`.
    fn credentials(&self) -> String {
        format!("{}:{}@", encoded(&self.user), encoded(&self.password))
    }
}

/// The directory of the server's socket, if the environment names one that
/// holds a socket for `port`.
fn socket_directory(port: u16) -> Option<String> {
    std::env::var("POSTGRES_SOCKET_DIR")
        .ok()
        .filter(|directory| {
            Path::new(directory)
                .join(format!(".s.PGSQL.{port}"))
                .exists()
        })
}

/// A directory of the test's, removed with what it holds when dropped.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static MADE: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "rk-defaults-{}-{}",
            std::process::id(),
            MADE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    /// A file in it holding `text`, readable by `mode`.
    fn file(&self, name: &str, text: &str, mode: u32) -> PathBuf {
        let path = self.0.join(name);
        std::fs::write(&path, text).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).ok();
    }
}

/// A link at `link` to `target`, removed when dropped.
struct Link(PathBuf);

impl Drop for Link {
    fn drop(&mut self) {
        std::fs::remove_file(&self.0).ok();
    }
}

/// A local TCP port nothing listens on.
fn dead_port() -> u16 {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    probe.local_addr().unwrap().port()
}

/// What a fake server does with each connection it takes.
#[derive(Clone, Copy)]
enum Fake {
    /// Answers `N` to the TLS request.
    RefusesTls,
    /// Waits for the client's first bytes, then closes with them unread: the
    /// kernel resets the connection.
    Resets,
}

/// The port of a local fake server that answers as `fake` says.
fn fake_server(fake: Fake) -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else {
                return;
            };
            let mut first = [0_u8; 8];
            match fake {
                Fake::RefusesTls => {
                    stream.read_exact(&mut first).ok();
                    stream.write_all(b"N").ok();
                    stream.read_to_end(&mut Vec::new()).ok();
                }
                Fake::Resets => {
                    stream.peek(&mut first).ok();
                }
            }
        }
    });
    port
}

/// Python: `test_a_store_with_no_host_tries_the_local_sockets`. With no host in
/// the string nor `PGHOST`, asyncpg tries the sockets of `/run/postgresql`,
/// `/var/run/postgresql`, `/tmp` and `/private/tmp` before `localhost`: the
/// server's socket, linked into `/tmp`, lets the store in with no password.
/// `PGHOST` naming the socket's directory does too.
#[tokio::test]
async fn a_store_with_no_host_tries_the_local_sockets() {
    if ran_as_child().await {
        return;
    }
    let Some(server) = Server::new().await else {
        return;
    };
    let Some(directory) = socket_directory(server.port) else {
        return;
    };
    if !server.asks_a_password().await {
        return;
    }
    let home = TempDir::new();
    let port = server.port.to_string();
    let dsn = format!(
        "postgresql://{}/{}{}",
        server.user(),
        server.database,
        server.query
    );
    let named = open_in_child(
        "a_store_with_no_host_tries_the_local_sockets",
        &home.0,
        &dsn,
        &[("PGPORT", &port), ("PGHOST", &directory)],
    );
    assert!(named.connected, "{named:?}");
    let link = Path::new("/tmp").join(format!(".s.PGSQL.{port}"));
    // A socket of that port before /tmp's, or someone else's there: the
    // defaults would reach another server.
    let elsewhere = ["/run/postgresql", "/var/run/postgresql"]
        .iter()
        .any(|directory| {
            Path::new(directory)
                .join(link.file_name().unwrap())
                .exists()
        });
    if elsewhere || link.symlink_metadata().is_ok() {
        return;
    }
    std::os::unix::fs::symlink(Path::new(&directory).join(link.file_name().unwrap()), &link)
        .unwrap();
    let _link = Link(link);
    let defaults = open_in_child(
        "a_store_with_no_host_tries_the_local_sockets",
        &home.0,
        &dsn,
        &[("PGPORT", &port)],
    );
    assert!(defaults.connected, "{defaults:?}");
}

/// Python: `test_a_store_with_no_password_reads_the_password_file`. With no
/// password in the string nor `PGPASSWORD`, asyncpg takes it from the first
/// line of `~/.pgpass` that matches the host, port, database and user, `*`
/// matching any: or from the file `PGPASSFILE` or the string's `passfile`
/// names.
#[tokio::test]
async fn a_store_with_no_password_reads_the_password_file() {
    if ran_as_child().await {
        return;
    }
    let Some(server) = Server::new().await else {
        return;
    };
    if !server.asks_a_password().await {
        return;
    }
    let lines = format!(
        "# a comment, then a blank line\n\nelsewhere:{port}:*:{user}:wrong\n{host}:{port}:*:{user}:{password}\n",
        port = server.port,
        user = server.user,
        host = server.host,
        password = server.password,
    );
    let dsn = format!(
        "postgresql://{}{}:{}/{}{}",
        server.user(),
        server.host,
        server.port,
        server.database,
        server.query
    );
    for place in ["home", "PGPASSFILE", "passfile"] {
        let home = TempDir::new();
        let elsewhere = TempDir::new();
        let (opened, file) = if place == "home" {
            let file = home.file(".pgpass", &lines, 0o600);
            (
                open_in_child(
                    "a_store_with_no_password_reads_the_password_file",
                    &home.0,
                    &dsn,
                    &[],
                ),
                file,
            )
        } else {
            let file = elsewhere.file("passwords", &lines, 0o600);
            let named = file.display().to_string();
            let opened = if place == "PGPASSFILE" {
                open_in_child(
                    "a_store_with_no_password_reads_the_password_file",
                    &home.0,
                    &dsn,
                    &[("PGPASSFILE", &named)],
                )
            } else {
                open_in_child(
                    "a_store_with_no_password_reads_the_password_file",
                    &home.0,
                    &format!("{dsn}&passfile={}", encoded(&named)),
                    &[],
                )
            };
            (opened, file)
        };
        assert!(opened.connected, "{place} ({}): {opened:?}", file.display());
    }
}

/// Python: `test_a_password_file_others_can_read_is_not_read`. asyncpg ignores
/// a password file with any access for the group or others, as libpq does,
/// with a warning: the connection goes without a password, and the server
/// refuses it.
#[tokio::test]
async fn a_password_file_others_can_read_is_not_read() {
    if ran_as_child().await {
        return;
    }
    let Some(server) = Server::new().await else {
        return;
    };
    if !server.asks_a_password().await {
        return;
    }
    let home = TempDir::new();
    home.file(".pgpass", &format!("*:*:*:*:{}\n", server.password), 0o640);
    let dsn = format!(
        "postgresql://{}{}:{}/{}{}",
        server.user(),
        server.host,
        server.port,
        server.database,
        server.query
    );
    let opened = open_in_child(
        "a_password_file_others_can_read_is_not_read",
        &home.0,
        &dsn,
        &[],
    );
    assert!(!opened.connected, "{opened:?}");
    assert!(opened.refusal.contains("password"), "{opened:?}");
    assert_eq!(opened.warnings.len(), 1, "{opened:?}");
    assert!(
        opened.warnings[0].contains("has group or world access"),
        "{opened:?}"
    );
}

/// Python: `test_a_short_password_file_line_before_a_match_fails_init` (HITL
/// 8142aaf2). asyncpg cuts each line of the file into five fields and raises
/// on one with fewer, met before the line that matches: `init` fails,
/// `StoreError::Value`.
#[tokio::test]
async fn a_short_password_file_line_before_a_match_fails_init() {
    if ran_as_child().await {
        return;
    }
    let Some(server) = Server::new().await else {
        return;
    };
    let home = TempDir::new();
    home.file(
        ".pgpass",
        &format!("short:line\n*:*:*:*:{}\n", server.password),
        0o600,
    );
    let dsn = format!(
        "postgresql://{}{}:{}/{}{}",
        server.user(),
        server.host,
        server.port,
        server.database,
        server.query
    );
    let opened = open_in_child(
        "a_short_password_file_line_before_a_match_fails_init",
        &home.0,
        &dsn,
        &[],
    );
    assert!(!opened.connected, "{opened:?}");
    assert!(
        opened.refusal.contains("fields where five are expected"),
        "{opened:?}"
    );
}

/// Python: `test_an_empty_pgpassword_is_a_password`. asyncpg takes an empty
/// `PGPASSWORD` for the password, and reads no password file then: a
/// matching `~/.pgpass` is not read, and the server refuses the empty one.
#[tokio::test]
async fn an_empty_pgpassword_is_a_password() {
    if ran_as_child().await {
        return;
    }
    let Some(server) = Server::new().await else {
        return;
    };
    if !server.asks_a_password().await {
        return;
    }
    let home = TempDir::new();
    home.file(".pgpass", &format!("*:*:*:*:{}\n", server.password), 0o600);
    let dsn = format!(
        "postgresql://{}{}:{}/{}{}",
        server.user(),
        server.host,
        server.port,
        server.database,
        server.query
    );
    let opened = open_in_child(
        "an_empty_pgpassword_is_a_password",
        &home.0,
        &dsn,
        &[("PGPASSWORD", "")],
    );
    assert!(!opened.connected, "{opened:?}");
    assert!(opened.refusal.contains("password"), "{opened:?}");
}

/// Python: `test_a_host_list_goes_past_a_dead_host`. A host list, in the
/// string or in `PGHOST` and `PGPORT`, is tried in its order: a host nothing
/// listens on is passed for the next.
#[tokio::test]
async fn a_host_list_goes_past_a_dead_host() {
    if ran_as_child().await {
        return;
    }
    let Some(server) = Server::new().await else {
        return;
    };
    let home = TempDir::new();
    let dead = dead_port();
    let hosts = format!("127.0.0.1,{}", server.host);
    let ports = format!("{dead},{}", server.port);
    let credentials = server.credentials();
    let (database, query) = (&server.database, &server.query);
    let netloc = format!(
        "postgresql://{credentials}127.0.0.1:{dead},{}:{}/{database}{query}",
        server.host, server.port
    );
    let in_query =
        format!("postgresql://{credentials}/{database}{query}&host={hosts}&port={ports}");
    let bare = format!("postgresql://{credentials}/{database}{query}");
    for (form, dsn, vars) in [
        ("netloc", netloc, vec![]),
        ("query", in_query, vec![]),
        (
            "environment",
            bare,
            vec![("PGHOST", hosts.as_str()), ("PGPORT", ports.as_str())],
        ),
    ] {
        let opened = open_in_child("a_host_list_goes_past_a_dead_host", &home.0, &dsn, &vars);
        assert!(opened.connected, "{form}: {opened:?}");
    }
}

/// Python: `test_a_host_refusing_tls_is_passed_for_the_next`. A host that
/// refuses the TLS `sslmode=require` asks for is one asyncpg cannot reach: it
/// tries the next, here the server's socket, which asks no TLS.
#[tokio::test]
async fn a_host_refusing_tls_is_passed_for_the_next() {
    if ran_as_child().await {
        return;
    }
    let Some(server) = Server::new().await else {
        return;
    };
    let Some(directory) = socket_directory(server.port) else {
        return;
    };
    let home = TempDir::new();
    let fake = fake_server(Fake::RefusesTls);
    let dsn = format!(
        "postgresql://{}/{}{}&host=127.0.0.1,{directory}&port={fake},{}&sslmode=require",
        server.user(),
        server.database,
        server.query,
        server.port,
    );
    let opened = open_in_child(
        "a_host_refusing_tls_is_passed_for_the_next",
        &home.0,
        &dsn,
        &[],
    );
    assert!(opened.connected, "{opened:?}");
}

/// Python: `test_a_refusal_on_a_host_ends_the_list`. asyncpg goes to the next
/// host only when a host cannot be reached: a server that refuses the
/// connection, or a connection lost once open, ends the list, though the
/// server's socket next in it would let the store in with no password.
#[tokio::test]
async fn a_refusal_on_a_host_ends_the_list() {
    if ran_as_child().await {
        return;
    }
    let Some(server) = Server::new().await else {
        return;
    };
    let Some(directory) = socket_directory(server.port) else {
        return;
    };
    if !server.asks_a_password().await {
        return;
    }
    let home = TempDir::new();
    let dsn = format!(
        "postgresql://{}/{}{}&sslmode=disable",
        server.user(),
        server.database,
        server.query
    );
    let resets = fake_server(Fake::Resets);
    let cases = [
        (
            "refuses",
            server.host.clone(),
            server.port.to_string(),
            "password",
        ),
        (
            "resets",
            "127.0.0.1".to_owned(),
            format!("{resets},{}", server.port),
            "error communicating with the server",
        ),
    ];
    for (first, host, ports, why) in cases {
        let hosts = format!("{host},{directory}");
        let opened = open_in_child(
            "a_refusal_on_a_host_ends_the_list",
            &home.0,
            &dsn,
            &[("PGHOST", &hosts), ("PGPORT", &ports)],
        );
        assert!(!opened.connected, "{first}: {opened:?}");
        assert!(opened.refusal.contains(why), "{first}: {opened:?}");
    }
}

/// A store on `POSTGRES_DSN` with `setting` added to its query.
fn store_with(server: &str, setting: &str) -> PostgresStore {
    let separator = if server.contains('?') { '&' } else { '?' };
    PostgresStore::new(PostgresStoreOptions {
        dsn: Some(format!("{server}{separator}{setting}")),
        ..PostgresStoreOptions::default()
    })
}

/// Python: `test_a_parameter_asyncpg_does_not_read_goes_to_the_server` (HITL
/// 22f38d15, point C). asyncpg reads none of these parameters and passes each
/// to the server as a setting, which refuses the connection.
#[tokio::test]
async fn a_parameter_asyncpg_does_not_read_goes_to_the_server() {
    let Some(server) = dsn() else {
        return;
    };
    for setting in [
        "connect_timeout=3",
        "keepalives=1",
        "keepalives_idle=30",
        "keepalives_interval=10",
        "keepalives_retries=3",
        "channel_binding=prefer",
        "load_balance_hosts=random",
    ] {
        let refused = store_with(&server, setting)
            .init(InitOptions {
                min_size: 1,
                max_size: 1,
            })
            .await
            .err()
            .unwrap();
        let text = full_text(&refused);
        assert!(
            text.contains("unrecognized configuration parameter"),
            "{setting}: {text}"
        );
    }
}

/// Python: `test_a_tcp_user_timeout_is_the_server_s_setting` (HITL 22f38d15,
/// point C). asyncpg passes `tcp_user_timeout` to the server, which takes it
/// for its own setting of the session: the store opens.
#[tokio::test]
async fn a_tcp_user_timeout_is_the_server_s_setting() {
    let Some(server) = dsn() else {
        return;
    };
    let schema = TempSchema::new().await;
    drop(server);
    let store = store_with(&schema.dsn(), "tcp_user_timeout=1000");
    store
        .init(InitOptions {
            min_size: 1,
            max_size: 1,
        })
        .await
        .unwrap();
    store.close().await;
}
