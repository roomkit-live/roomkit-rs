//! Owned HTTP/1 connections; TCP and TLS each have their own connect budget.

use std::{sync::Arc, time::Duration};

use hyper::client::conn::http1::SendRequest;
use hyper_util::rt::TokioIo;
use reqwest::Url;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::{
    body::Body,
    io::{Operations, Timed},
};
use crate::http::client::{Failure, HttpFailure};

pub(super) type Finished = Arc<oneshot::Sender<()>>;

pub(super) struct Connection {
    pub(super) sender: SendRequest<Body>,
    pub(super) operations: Arc<Operations>,
    pub(super) origin: String,
    pub(super) idle_since: Instant,
    // Dropping the connection stops its driver; that driver keeps Finished
    // alive until its socket has actually been released.
    _stop: oneshot::Sender<()>,
}

impl Connection {
    pub(super) async fn open(
        url: &Url,
        tls: Arc<rustls::ClientConfig>,
        done: Finished,
        failure: &Failure,
    ) -> Result<Self, HttpFailure> {
        let fail = || failure.record(None, "HTTP connection failed");
        let host = url.host_str().ok_or_else(fail)?;
        let host = host.trim_start_matches('[').trim_end_matches(']');
        let port = url.port_or_known_default().ok_or_else(fail)?;
        let stream = tokio::time::timeout(
            Duration::from_secs(5),
            tokio::net::TcpStream::connect((host, port)),
        )
        .await
        .map_err(|_| fail())?
        .map_err(|_| fail())?;
        stream.set_nodelay(true).map_err(|_| fail())?;
        if url.scheme() == "https" {
            let name =
                rustls::pki_types::ServerName::try_from(host.to_owned()).map_err(|_| fail())?;
            let connector = tokio_rustls::TlsConnector::from(tls);
            let stream =
                tokio::time::timeout(Duration::from_secs(5), connector.connect(name, stream))
                    .await
                    .map_err(|_| fail())?
                    .map_err(|_| fail())?;
            Self::handshake(stream, url, done, failure).await
        } else {
            Self::handshake(stream, url, done, failure).await
        }
    }

    async fn handshake<S>(
        stream: S,
        url: &Url,
        done: Finished,
        failure: &Failure,
    ) -> Result<Self, HttpFailure>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let operations = Operations::new();
        let io = TokioIo::new(Timed::new(stream, Arc::clone(&operations)));
        let (sender, connection) = hyper::client::conn::http1::handshake(io)
            .await
            .map_err(|_| failure.record(None, "HTTP connection failed"))?;
        let (stop, stopped) = oneshot::channel();
        tokio::spawn(async move {
            let _finished = done;
            tokio::select! {
                biased;
                _ = stopped => {}
                _ = connection => {}
            }
        });
        Ok(Self {
            sender,
            operations,
            origin: url.origin().ascii_serialization(),
            idle_since: Instant::now(),
            _stop: stop,
        })
    }
}
