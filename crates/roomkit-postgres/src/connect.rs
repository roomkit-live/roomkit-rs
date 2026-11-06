//! A pool's connection, tried at each address in turn as asyncpg 0.32's
//! `_connect` tries them: an address that cannot be reached is passed for the
//! next, a server that answers and refuses ends the list (HITL fa9dd934).

use std::collections::hash_map::RandomState;
use std::future::Future;
use std::hash::BuildHasher;
use std::net::IpAddr;
use std::pin::Pin;

use deadpool_postgres::Connect;
use tokio::task::JoinHandle;
use tokio_postgres::config::{LoadBalanceHosts, SslMode};
use tokio_postgres::error::SqlState;
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::{Client, Config, Connection, Error, NoTls, Socket};
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::hosts::Address;
use crate::passfile::{self, Lookup};

/// What a connection over TCP, and a cancel request, connect with: rustls. A
/// Unix socket's connection is asked no TLS (`SslMode::Disable`), and is
/// given a name for a connector it never uses: it has none.
#[derive(Clone)]
pub(crate) struct Tls(pub(crate) MakeRustlsConnect);

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
    pub(crate) addresses: Vec<Address>,
    /// The string's `hostaddr`, each the IP of the address at its place.
    pub(crate) host_ips: Vec<IpAddr>,
    pub(crate) tls: Option<Tls>,
    pub(crate) passfile: Option<Lookup>,
}

/// Whether `error` says its address could not be reached, which asyncpg passes
/// for the next: an `OSError` there (no socket, no route, refused, a timeout,
/// a TLS handshake that failed, a host not of the session asked for), or a
/// server that cannot take a connection yet (`57P03`).
fn unreachable(error: &Error) -> bool {
    error.code() == Some(&SqlState::CANNOT_CONNECT_NOW)
        || std::error::Error::source(error)
            .is_some_and(<dyn std::error::Error>::is::<std::io::Error>)
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

/// `addresses` in a random order, for a string's `load_balance_hosts=random`.
fn shuffled(addresses: &mut [(usize, &Address)]) {
    let random = RandomState::new();
    for at in (1..addresses.len()).rev() {
        let pick = usize::try_from(random.hash_one(at)).unwrap_or(at) % (at + 1);
        addresses.swap(at, pick);
    }
}

impl Connector {
    /// A connection of `base` at `address`, the `at`th of the list.
    async fn connect_at(
        &self,
        base: &Config,
        at: usize,
        address: &Address,
    ) -> Result<(Client, JoinHandle<()>), Error> {
        let mut config = base.clone();
        if let Some(ip) = self.host_ips.get(at) {
            config.hostaddr(*ip);
        }
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
            Err(_) => {}
        }
        config
    }

    async fn first_answer(&self, base: &Config) -> Result<(Client, JoinHandle<()>), Error> {
        let config = self.with_password(base).await;
        let mut order: Vec<(usize, &Address)> = self.addresses.iter().enumerate().collect();
        if config.get_load_balance_hosts() == LoadBalanceHosts::Random {
            shuffled(&mut order);
        }
        let Some((last, before)) = order.split_last() else {
            // No address: the client's own refusal ("host missing").
            let (client, connection) = config.connect(NoTls).await?;
            return Ok((client, spawned(connection)));
        };
        for (at, address) in before {
            match self.connect_at(&config, *at, address).await {
                Err(error) if unreachable(&error) => {}
                answered => return answered,
            }
        }
        self.connect_at(&config, last.0, last.1).await
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
