//! TLS through rustls, each `sslmode` doing what asyncpg's does.

use std::path::PathBuf;
use std::sync::Arc;

use roomkit::StoreError;
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms, ring};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, CertificateRevocationListDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
};
use tokio_postgres::config::SslMode;

/// How the connection to the server is encrypted and checked: asyncpg's
/// `sslmode`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Encryption {
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
    /// has no such order here, and asks for what it would prefer. The refusal
    /// does not repeat the value: a malformed string may hold a password in it.
    pub(crate) fn named(value: &str) -> Result<Self, StoreError> {
        match value {
            "disable" => Ok(Self::Disable),
            "allow" | "prefer" => Ok(Self::Prefer),
            "require" => Ok(Self::Require),
            "verify-ca" => Ok(Self::VerifyCa),
            "verify-full" => Ok(Self::VerifyFull),
            _ => Err(StoreError::Value(
                "sslmode must be one of disable, allow, prefer, require, verify-ca, verify-full"
                    .to_owned(),
            )),
        }
    }

    pub(crate) fn client_mode(self) -> SslMode {
        match self {
            Self::Disable => SslMode::Disable,
            Self::Prefer => SslMode::Prefer,
            Self::Require | Self::VerifyCa | Self::VerifyFull => SslMode::Require,
        }
    }
}

/// The file `named`, else the user's own under `~/.postgresql` when it exists.
fn file_of(named: Option<&str>, default: &str) -> Option<PathBuf> {
    match named {
        Some(path) => Some(PathBuf::from(path)),
        None => std::env::home_dir()
            .map(|home| home.join(".postgresql").join(default))
            .filter(|path| path.is_file()),
    }
}

/// The root certificates `sslrootcert` names, or the user's own file when it
/// names none; `None` when there is none to read. A file that is named and
/// cannot be read is refused, never a silent downgrade.
fn root_certificates(named: Option<&str>) -> Result<Option<RootCertStore>, StoreError> {
    let Some(path) = file_of(named, "root.crt") else {
        return Ok(None);
    };
    let certificates = CertificateDer::pem_file_iter(&path)
        .map_err(StoreError::unavailable)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(StoreError::unavailable)?;
    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(certificates);
    Ok(Some(roots))
}

/// The revocation lists `sslcrl` names, or the user's own file when it names
/// none: asyncpg checks them whenever it checks a chain.
fn revocation_lists(
    named: Option<&str>,
) -> Result<Vec<CertificateRevocationListDer<'static>>, StoreError> {
    let Some(path) = file_of(named, "root.crl") else {
        return Ok(Vec::new());
    };
    CertificateRevocationListDer::pem_file_iter(&path)
        .map_err(StoreError::unavailable)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(StoreError::unavailable)
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

/// What `tls_config` reads beside the mode: the DSN's or the environment's
/// `sslrootcert` and `sslcrl`.
#[derive(Clone, Debug, Default)]
pub(crate) struct Files {
    pub(crate) root_certificate: Option<String>,
    pub(crate) revocation_list: Option<String>,
}

/// The TLS configuration `encryption` asks for.
pub(crate) fn tls_config(
    encryption: Encryption,
    files: &Files,
) -> Result<ClientConfig, StoreError> {
    let provider = Arc::new(ring::default_provider());
    let builder = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(StoreError::unavailable)?;
    let roots = match encryption {
        Encryption::Disable | Encryption::Prefer => None,
        Encryption::Require | Encryption::VerifyCa | Encryption::VerifyFull => {
            root_certificates(files.root_certificate.as_deref())?
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
    let chain = WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider)
        .with_crls(revocation_lists(files.revocation_list.as_deref())?)
        .build()
        .map_err(StoreError::unavailable)?;
    if encryption == Encryption::VerifyFull {
        return Ok(builder.with_webpki_verifier(chain).with_no_client_auth());
    }
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

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};

    use rcgen::{
        BasicConstraints, CertificateParams, CertificateRevocationListParams, DnType, IsCa, Issuer,
        KeyIdMethod, KeyPair, RevocationReason, RevokedCertParams, SerialNumber, date_time_ymd,
    };
    use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
    use rustls::{ClientConnection, ServerConfig, ServerConnection};

    use super::*;

    const HOST: &str = "db.example.com";
    const REVOKED: u64 = 7;

    /// A file holding `text`, removed when dropped.
    struct TempPem(PathBuf);

    impl TempPem {
        fn new(text: &str) -> Self {
            static MADE: AtomicU64 = AtomicU64::new(0);
            let made = MADE.fetch_add(1, Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("roomkit-tls-{}-{made}.pem", std::process::id()));
            std::fs::write(&path, text).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }

        fn name(&self) -> String {
            self.path().display().to_string()
        }
    }

    impl Drop for TempPem {
        fn drop(&mut self) {
            std::fs::remove_file(&self.0).ok();
        }
    }

    /// A certificate authority, and the PEM of its certificate.
    struct Authority {
        issuer: Issuer<'static, KeyPair>,
        pem: String,
    }

    impl Authority {
        fn new(name: &str) -> Self {
            let key = KeyPair::generate().unwrap();
            let mut params = CertificateParams::new(Vec::new()).unwrap();
            params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            params.distinguished_name.push(DnType::CommonName, name);
            let pem = params.self_signed(&key).unwrap().pem();
            Self {
                issuer: Issuer::new(params, key),
                pem,
            }
        }

        /// A server's chain and key for `host`, with the serial `serial`.
        fn server(
            &self,
            host: &str,
            serial: u64,
        ) -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
            let key = KeyPair::generate().unwrap();
            let mut params = CertificateParams::new(vec![host.to_owned()]).unwrap();
            params.serial_number = Some(SerialNumber::from(serial));
            let certificate = params.signed_by(&key, &self.issuer).unwrap();
            let der = PrivatePkcs8KeyDer::from(key.serialize_der()).into();
            (vec![certificate.der().clone()], der)
        }

        /// The list revoking the serial [`REVOKED`], in PEM.
        fn revocation_list(&self) -> String {
            CertificateRevocationListParams {
                this_update: date_time_ymd(2020, 1, 1),
                next_update: date_time_ymd(2099, 1, 1),
                crl_number: SerialNumber::from(1_u64),
                issuing_distribution_point: None,
                revoked_certs: vec![RevokedCertParams {
                    serial_number: SerialNumber::from(REVOKED),
                    revocation_time: date_time_ymd(2020, 1, 1),
                    reason_code: Some(RevocationReason::KeyCompromise),
                    invalidity_date: None,
                }],
                key_identifier_method: KeyIdMethod::Sha256,
            }
            .signed_by(&self.issuer)
            .unwrap()
            .pem()
            .unwrap()
        }
    }

    /// Whether a client under `config`, calling on [`HOST`], accepts a server
    /// showing `shown`: a TLS handshake in memory, no network.
    fn accepts(
        config: ClientConfig,
        shown: (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>),
    ) -> bool {
        let provider = Arc::new(ring::default_provider());
        let server = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(shown.0, shown.1)
            .unwrap();
        let name = ServerName::try_from(HOST).unwrap();
        let mut client = ClientConnection::new(Arc::new(config), name).unwrap();
        let mut server = ServerConnection::new(Arc::new(server)).unwrap();
        for _ in 0..8 {
            let mut to_server = Vec::new();
            client.write_tls(&mut to_server).unwrap();
            server.read_tls(&mut Cursor::new(to_server)).unwrap();
            if server.process_new_packets().is_err() {
                return false;
            }
            let mut to_client = Vec::new();
            server.write_tls(&mut to_client).unwrap();
            client.read_tls(&mut Cursor::new(to_client)).unwrap();
            if client.process_new_packets().is_err() {
                return false;
            }
            if !client.is_handshaking() && !server.is_handshaking() {
                return true;
            }
        }
        false
    }

    /// Each mode against a right server, a server of another authority, and
    /// one of another host's name: what asyncpg accepts and refuses.
    #[test]
    fn each_sslmode_checks_what_asyncpg_checks() {
        let (ours, theirs) = (Authority::new("ours"), Authority::new("theirs"));
        let roots = TempPem::new(&ours.pem);
        let with_roots = Files {
            root_certificate: Some(roots.name()),
            revocation_list: None,
        };
        for (mode, files, right, other_authority, other_name) in [
            (Encryption::Prefer, &with_roots, true, true, true),
            (Encryption::Require, &with_roots, true, false, true),
            (Encryption::VerifyCa, &with_roots, true, false, true),
            (Encryption::VerifyFull, &with_roots, true, false, false),
        ] {
            let config = || tls_config(mode, files).unwrap();
            assert_eq!(accepts(config(), ours.server(HOST, 1)), right, "{mode:?}");
            assert_eq!(
                accepts(config(), theirs.server(HOST, 1)),
                other_authority,
                "{mode:?}"
            );
            assert_eq!(
                accepts(config(), ours.server("other.example.com", 1)),
                other_name,
                "{mode:?}"
            );
        }
    }

    /// Python: asyncpg's `PGSSLCRL`, `sslcrl` and `~/.postgresql/root.crl`: a
    /// revoked certificate is refused wherever a chain is checked.
    #[test]
    fn a_revoked_certificate_is_refused_where_a_chain_is_checked() {
        let ours = Authority::new("ours");
        let roots = TempPem::new(&ours.pem);
        let list = TempPem::new(&ours.revocation_list());
        let files = Files {
            root_certificate: Some(roots.name()),
            revocation_list: Some(list.name()),
        };
        for mode in [
            Encryption::Require,
            Encryption::VerifyCa,
            Encryption::VerifyFull,
        ] {
            let config = || tls_config(mode, &files).unwrap();
            assert!(accepts(config(), ours.server(HOST, 1)), "{mode:?}");
            assert!(!accepts(config(), ours.server(HOST, REVOKED)), "{mode:?}");
        }
    }

    /// A check of the server's name or chain needs a root certificate, as
    /// asyncpg says; a file that is not there is no silent downgrade.
    #[test]
    fn verifying_without_a_root_certificate_is_refused() {
        let missing = Files {
            root_certificate: Some("/nowhere/root.crt".to_owned()),
            revocation_list: None,
        };
        for mode in [
            Encryption::Require,
            Encryption::VerifyCa,
            Encryption::VerifyFull,
        ] {
            assert!(tls_config(mode, &missing).is_err(), "{mode:?}");
        }
    }

    /// Python: asyncpg has no `sslrootcert=system`: it is a file's name, and a
    /// mode that checks a chain does not take the public roots for it.
    #[test]
    fn system_is_a_file_s_name_not_the_public_roots() {
        let system = Files {
            root_certificate: Some("system".to_owned()),
            revocation_list: None,
        };
        assert!(tls_config(Encryption::VerifyFull, &system).is_err());
    }

    #[test]
    fn each_sslmode_names_its_encryption_and_a_bad_one_is_refused_without_its_text() {
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
        let refused = Encryption::named("hunter2@host").unwrap_err();
        assert!(!refused.to_string().contains("hunter2"), "{refused}");
    }
}
