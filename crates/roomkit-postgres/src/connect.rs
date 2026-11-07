//! A pool's connection, tried at each address in turn as asyncpg 0.32's
//! `_connect` tries them: an address that cannot be reached is passed for the
//! next, a server that answers and refuses ends the list (HITL fa9dd934).

use std::future::Future;
use std::pin::Pin;

use deadpool_postgres::Connect;
use tokio::task::JoinHandle;
use tokio_postgres::config::SslMode;
use tokio_postgres::error::SqlState;
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::{Client, Config, Connection, Error, NoTls, Socket};
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::hosts::Address;
use crate::passfile::{self, Lookup};
use crate::tls::{Encryption, Files, tls_config};

/// What a connection over TCP, and a cancel request, connect with: rustls. A
/// Unix socket's connection is asked no TLS (`SslMode::Disable`), and is
/// given a name for a connector it never uses: it has none.
#[derive(Clone)]
pub(crate) struct Tls(pub(crate) MakeRustlsConnect);

impl Tls {
    /// TLS that checks no certificate, `sslmode=prefer`'s: what a cancel request
    /// connects with for a pool whose own the crate does not know, one handed in.
    /// The connection's mode still decides whether the request asks for it.
    pub(crate) fn unchecked() -> Option<Self> {
        let config = tls_config(Encryption::Prefer, &Files::default()).ok()?;
        Some(Self(MakeRustlsConnect::new(config)))
    }
}

impl MakeTlsConnect<Socket> for Tls {
    type Stream = <MakeRustlsConnect as MakeTlsConnect<Socket>>::Stream;
    type TlsConnect = <MakeRustlsConnect as MakeTlsConnect<Socket>>::TlsConnect;
    type Error = <MakeRustlsConnect as MakeTlsConnect<Socket>>::Error;

    fn make_tls_connect(&mut self, domain: &str) -> Result<Self::TlsConnect, Self::Error> {
        let domain = if domain.is_empty() {
            "localhost"
        } else {
            domain
        };
        MakeTlsConnect::<Socket>::make_tls_connect(&mut self.0, domain)
    }
}

/// How a pool's connections are made: its addresses, what a TCP one connects
/// with, and the password file a connection with no password reads.
pub(crate) struct Connector {
    /// Never empty: [`crate::hosts::addresses`] falls back on the local defaults.
    pub(crate) addresses: Vec<Address>,
    pub(crate) tls: Option<Tls>,
    pub(crate) passfile: Option<Lookup>,
}

/// Whether `error` says its address could not be reached, which asyncpg passes
/// for the next: the socket's connect (no socket, no route, refused, a timeout,
/// a host not of the session asked for) or its TLS upgrade refused or failed
/// (`N` under `require`, a handshake), an `OSError` there; or a server that
/// cannot take a connection yet (`57P03`). A server that answered and
/// refused, or a connection lost, ends the list, as asyncpg's
/// `ConnectionDoesNotExistError` does. The client tells these apart by its
/// error's kind alone, which it shows only in its text, and reports a
/// connection lost on the TLS request itself as one lost later: asyncpg passes
/// that host, this list ends there (HITL 22f38d15).
fn unreachable(error: &Error) -> bool {
    if error.code() == Some(&SqlState::CANNOT_CONNECT_NOW) {
        return true;
    }
    let text = error.to_string();
    text.starts_with("error connecting to server")
        || text.starts_with("error performing TLS handshake")
}

/// `connection` run on a task of its own, as deadpool's own connector runs it.
fn spawned<T>(connection: Connection<Socket, T>) -> JoinHandle<()>
where
    Connection<Socket, T>: Future<Output = Result<(), Error>> + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::warn!(target: "deadpool.postgres", "Connection error: {error}");
        }
    })
}

impl Connector {
    /// A connection of `base` at `address`.
    async fn connect_at(
        &self,
        base: &Config,
        address: &Address,
    ) -> Result<(Client, JoinHandle<()>), Error> {
        let mut config = base.clone();
        match address {
            Address::Socket { directory, port } => {
                #[cfg(unix)]
                config.host_path(directory);
                #[cfg(not(unix))]
                config.host(directory.to_string_lossy());
                config.port(*port).ssl_mode(SslMode::Disable);
                let (client, connection) = config.connect(NoTls).await?;
                Ok((client, spawned(connection)))
            }
            Address::Tcp { host, port } => {
                config.host(host).port(*port);
                if let Some(tls) = &self.tls {
                    let (client, connection) = config.connect(tls.clone()).await?;
                    Ok((client, spawned(connection)))
                } else {
                    let (client, connection) = config.connect(NoTls).await?;
                    Ok((client, spawned(connection)))
                }
            }
        }
    }

    /// `base` with the password its file gives, when it has none: read at each
    /// connection, as asyncpg reads it, so that a password changed there is
    /// the next connection's.
    async fn with_password(&self, base: &Config) -> Config {
        let mut config = base.clone();
        let Some(lookup) = self
            .passfile
            .clone()
            .filter(|_| base.get_password().is_none())
        else {
            return config;
        };
        match tokio::task::spawn_blocking(move || passfile::password(&lookup)).await {
            Ok(Ok(found)) => {
                if let Some(warning) = found.warning {
                    passfile::warn_once(&warning);
                }
                if let Some(password) = found.password {
                    config.password(password);
                }
            }
            // A file broken since `init` read it: the connection goes with no
            // password, and the server says why it refuses it.
            Ok(Err(refused)) => tracing::warn!(target: "roomkit.store", "{refused}"),
            Err(lost) => tracing::warn!(
                target: "roomkit.store",
                "the password file's read did not finish: {lost}"
            ),
        }
        config
    }

    /// A connection of `base` at the first of the addresses that answers, in
    /// their order.
    async fn first_answer(&self, base: &Config) -> Result<(Client, JoinHandle<()>), Error> {
        let config = self.with_password(base).await;
        let Some((last, before)) = self.addresses.split_last() else {
            // No address, which `addresses` never gives: the client's own
            // refusal ("both host and hostaddr are missing").
            let (client, connection) = config.connect(NoTls).await?;
            return Ok((client, spawned(connection)));
        };
        for address in before {
            match self.connect_at(&config, address).await {
                Err(error) if unreachable(&error) => {}
                answered => return answered,
            }
        }
        self.connect_at(&config, last).await
    }
}

impl Connect for Connector {
    fn connect(
        &self,
        base: &Config,
    ) -> Pin<Box<dyn Future<Output = Result<(Client, JoinHandle<()>), Error>> + Send + '_>> {
        let base = base.clone();
        Box::pin(async move { self.first_answer(&base).await })
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;

    use super::*;
    use crate::tls::{Encryption, Files, tls_config};

    /// What a fake server does with the one connection it takes.
    #[derive(Clone, Copy)]
    enum Fake {
        /// Answers `N` to the TLS request.
        RefusesTls,
        /// Waits for the startup message, then closes with it unread: a reset.
        Resets,
        /// Answers the startup message with an error of this state.
        Fails(&'static str),
    }

    /// The port of a fake server that does `fake` once.
    fn fake(fake: Fake) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let Ok((mut stream, _)) = listener.accept() else {
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
                    // Unread data at its close makes the kernel reset it.
                    stream.peek(&mut first).ok();
                }
                Fake::Fails(state) => {
                    stream.read_exact(&mut first).ok();
                    let fields = format!("SFATAL\0VFATAL\0C{state}\0Mrefused\0\0");
                    let mut message = vec![b'E'];
                    let length = u32::try_from(fields.len() + 4).unwrap();
                    message.extend_from_slice(&length.to_be_bytes());
                    message.extend_from_slice(fields.as_bytes());
                    stream.write_all(&message).ok();
                    // Until the client closes: a close with its bytes unread
                    // would reset the connection under the answer.
                    stream.read_to_end(&mut Vec::new()).ok();
                }
            }
        });
        port
    }

    /// The client's error connecting to 127.0.0.1 at `port` under `mode`.
    async fn refusal(port: u16, mode: Encryption) -> Error {
        let tls = MakeRustlsConnect::new(tls_config(mode, &Files::default()).unwrap());
        let mut config = Config::new();
        config
            .host("127.0.0.1")
            .port(port)
            .user("u")
            .password("p")
            .ssl_mode(mode.client_mode());
        config.connect(tls).await.err().unwrap()
    }

    /// Python (`_connect`, `__connect_addr`): asyncpg passes a host for the
    /// next on an `OSError` (refused, a TLS upgrade refused) or `57P03`; a
    /// connection lost once open, or a server's refusal, ends the list.
    #[tokio::test]
    async fn only_a_host_that_cannot_be_reached_is_passed_for_the_next() {
        let dead = {
            let probe = TcpListener::bind("127.0.0.1:0").unwrap();
            probe.local_addr().unwrap().port()
        };
        let cases = [
            ("refused", dead, Encryption::Disable, true),
            (
                "TLS refused",
                fake(Fake::RefusesTls),
                Encryption::Require,
                true,
            ),
            (
                "starting up",
                fake(Fake::Fails("57P03")),
                Encryption::Disable,
                true,
            ),
            ("reset", fake(Fake::Resets), Encryption::Disable, false),
            (
                "password",
                fake(Fake::Fails("28P01")),
                Encryption::Disable,
                false,
            ),
        ];
        for (case, port, mode, passed) in cases {
            let error = refusal(port, mode).await;
            assert_eq!(unreachable(&error), passed, "{case}: {error}");
        }
    }
}
