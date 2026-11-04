//! A pool from a connection string, read as Python's asyncpg reads it, TLS
//! through rustls; and the close of a pool a component opened itself.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use roomkit::StoreError;
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms, ring};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
};
use tokio_postgres::Config;
use tokio_postgres::config::SslMode;
use tokio_postgres_rustls::MakeRustlsConnect;

/// How the connection to the server is encrypted and checked: asyncpg's
/// `sslmode`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Encryption {
    /// No TLS.
    Disable,
    /// TLS if the server offers it, unchecked.
    Prefer,
    /// TLS, checked against the root certificate if one is there.
    Require,
    /// TLS, the chain checked against the root certificate.
    VerifyCa,
    /// TLS, the chain and the host's name checked.
    VerifyFull,
}

impl Encryption {
    /// The mode `value` names; `allow`, which asyncpg tries in clear first,
    /// has no such order here, and asks for what it would prefer.
    fn named(value: &str) -> Result<Self, StoreError> {
        match value {
            "disable" => Ok(Self::Disable),
            "allow" | "prefer" => Ok(Self::Prefer),
            "require" => Ok(Self::Require),
            "verify-ca" => Ok(Self::VerifyCa),
            "verify-full" => Ok(Self::VerifyFull),
            _ => Err(StoreError::Value(format!(
                "sslmode {value:?} is not one of disable, allow, prefer, require, verify-ca, \
                 verify-full"
            ))),
        }
    }

    fn client_mode(self) -> SslMode {
        match self {
            Self::Disable => SslMode::Disable,
            Self::Prefer => SslMode::Prefer,
            Self::Require | Self::VerifyCa | Self::VerifyFull => SslMode::Require,
        }
    }
}

/// A connection string read: the client's configuration, and what the client
/// does not read itself.
struct Target {
    config: Config,
    encryption: Encryption,
    root_certificate: Option<String>,
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

/// `dsn` without its `sslmode` and `sslrootcert` parameters, which the client
/// does not read or reads only in part, and their values.
fn without_tls_params(dsn: &str) -> (String, Option<String>, Option<String>) {
    let Some((base, query)) = dsn.split_once('?') else {
        return (dsn.to_owned(), None, None);
    };
    let (mut mode, mut root) = (None, None);
    let mut kept = Vec::new();
    for pair in query.split('&') {
        match pair.split_once('=') {
            Some(("sslmode", value)) => mode = Some(percent_decoded(value)),
            Some(("sslrootcert", value)) => root = Some(percent_decoded(value)),
            Some(_) | None => kept.push(pair),
        }
    }
    let dsn = if kept.is_empty() {
        base.to_owned()
    } else {
        format!("{base}?{}", kept.join("&"))
    };
    (dsn, mode, root)
}

/// The value of the environment variable `name`, if it holds one.
fn variable(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

/// What `dsn` names, what it leaves out read from the `PG*` environment
/// variables, as asyncpg reads them. A bad string is told without its text,
/// which holds a password.
fn target(dsn: Option<&str>) -> Result<Target, StoreError> {
    let (mut config, mode, root) = match dsn {
        Some(dsn) => {
            if !dsn.starts_with("postgresql://") && !dsn.starts_with("postgres://") {
                return Err(StoreError::Value(
                    "invalid DSN: scheme is expected to be either \"postgresql\" or \"postgres\""
                        .to_owned(),
                ));
            }
            let (kept, mode, root) = without_tls_params(dsn);
            let config = kept.parse::<Config>().map_err(StoreError::unavailable)?;
            (config, mode, root)
        }
        None => (Config::new(), None, None),
    };
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
    let mode = mode.or_else(|| variable("PGSSLMODE"));
    let encryption = mode
        .as_deref()
        .map_or(Ok(Encryption::Prefer), Encryption::named)?;
    config.ssl_mode(encryption.client_mode());
    Ok(Target {
        config,
        encryption,
        root_certificate: root.or_else(|| variable("PGSSLROOTCERT")),
    })
}

/// The root certificates `sslrootcert` names (`system`: the webpki roots), or
/// the user's own file when it names none; `None` when there is none to read.
fn root_certificates(named: Option<&str>) -> Result<Option<RootCertStore>, StoreError> {
    if named == Some("system") {
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        return Ok(Some(roots));
    }
    let path = if let Some(path) = named {
        PathBuf::from(path)
    } else {
        let Some(default) = std::env::home_dir()
            .map(|home| home.join(".postgresql").join("root.crt"))
            .filter(|path| path.is_file())
        else {
            return Ok(None);
        };
        default
    };
    let certificates = CertificateDer::pem_file_iter(&path)
        .map_err(StoreError::unavailable)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(StoreError::unavailable)?;
    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(certificates);
    Ok(Some(roots))
}

/// Takes any certificate the server shows, its signatures checked: what
/// asyncpg and libpq do under `prefer` and `require`, which encrypt and
/// check nothing else.
#[derive(Debug)]
struct Unchecked(WebPkiSupportedAlgorithms);

impl ServerCertVerifier for Unchecked {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, certificate, signature, &self.0)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, certificate, signature, &self.0)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_schemes()
    }
}

/// Checks the chain and not the host's name: `verify-ca`.
#[derive(Debug)]
struct ChainOnly(Arc<WebPkiServerVerifier>);

impl ServerCertVerifier for ChainOnly {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self
            .0
            .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
        {
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0
            .verify_tls12_signature(message, certificate, signature)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0
            .verify_tls13_signature(message, certificate, signature)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_verify_schemes()
    }
}

/// The TLS configuration `encryption` asks for.
fn tls_config(
    encryption: Encryption,
    root_certificate: Option<&str>,
) -> Result<ClientConfig, StoreError> {
    let provider = Arc::new(ring::default_provider());
    let builder = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(StoreError::unavailable)?;
    let roots = match encryption {
        Encryption::Disable | Encryption::Prefer => None,
        Encryption::Require | Encryption::VerifyCa | Encryption::VerifyFull => {
            root_certificates(root_certificate)?
        }
    };
    let Some(roots) = roots else {
        if matches!(encryption, Encryption::VerifyCa | Encryption::VerifyFull) {
            return Err(StoreError::Value(
                "sslmode verify-ca and verify-full need a root certificate: sslrootcert, \
                 PGSSLROOTCERT or ~/.postgresql/root.crt"
                    .to_owned(),
            ));
        }
        return Ok(unchecked(builder, &provider));
    };
    if encryption == Encryption::VerifyFull {
        return Ok(builder.with_root_certificates(roots).with_no_client_auth());
    }
    let chain = WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider)
        .build()
        .map_err(StoreError::unavailable)?;
    Ok(builder
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(ChainOnly(chain)))
        .with_no_client_auth())
}

/// The configuration that encrypts and checks no certificate.
fn unchecked(
    builder: rustls::ConfigBuilder<ClientConfig, rustls::WantsVerifier>,
    provider: &CryptoProvider,
) -> ClientConfig {
    builder
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(Unchecked(
            provider.signature_verification_algorithms,
        )))
        .with_no_client_auth()
}

/// A pool for `dsn`, at most `max_size` connections, with what a cancel
/// request connects with; `min_size` of them opened at once, so that a server
/// out of reach fails here rather than at the first operation, as asyncpg's
/// `create_pool`.
pub(crate) async fn open(
    dsn: Option<&str>,
    min_size: usize,
    max_size: usize,
) -> Result<(Pool, MakeRustlsConnect), StoreError> {
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
    let target = target(dsn)?;
    let Target {
        config,
        encryption,
        root_certificate,
    } = target;
    // The root certificate is a file, read off the runtime's threads.
    let tls =
        tokio::task::spawn_blocking(move || tls_config(encryption, root_certificate.as_deref()))
            .await
            .map_err(StoreError::unavailable)??;
    let tls = MakeRustlsConnect::new(tls);
    let manager = Manager::from_config(
        config,
        tls.clone(),
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    let pool = Pool::builder(manager)
        .max_size(max_size)
        .runtime(Runtime::Tokio1)
        .build()
        .map_err(StoreError::unavailable)?;
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
    use super::*;

    #[test]
    fn the_tls_params_leave_the_string_the_client_reads() {
        let (dsn, mode, root) = without_tls_params(
            "postgresql://u:p@h/d?sslmode=verify-full&sslrootcert=%2Fetc%2Fca.pem&connect_timeout=3",
        );
        assert_eq!(dsn, "postgresql://u:p@h/d?connect_timeout=3");
        assert_eq!(mode.as_deref(), Some("verify-full"));
        assert_eq!(root.as_deref(), Some("/etc/ca.pem"));
        assert_eq!(
            without_tls_params("postgresql://u:p@h/d?sslmode=disable").0,
            "postgresql://u:p@h/d"
        );
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

    #[test]
    fn each_sslmode_names_its_encryption_and_a_bad_one_is_refused() {
        for (name, encryption) in [
            ("disable", Encryption::Disable),
            ("allow", Encryption::Prefer),
            ("prefer", Encryption::Prefer),
            ("require", Encryption::Require),
            ("verify-ca", Encryption::VerifyCa),
            ("verify-full", Encryption::VerifyFull),
        ] {
            assert_eq!(Encryption::named(name).unwrap(), encryption, "{name}");
        }
        assert!(Encryption::named("off").is_err());
    }

    /// A check of the server's name or chain needs a root certificate, as
    /// asyncpg says; a file that is not there is no silent downgrade.
    #[test]
    fn verifying_without_a_root_certificate_is_refused() {
        let refused = tls_config(Encryption::VerifyFull, Some("/nowhere/root.crt"));
        assert!(refused.is_err());
    }
}
