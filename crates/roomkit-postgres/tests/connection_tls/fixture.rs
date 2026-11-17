use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use deadpool_postgres::{Manager, Pool};
use roomkit::{ConversationStore, StoreError};
use roomkit_postgres::{InitOptions, PostgresStore, PostgresStoreOptions};
use tokio_postgres::{Client, Config, Socket, config::SslMode, tls::MakeTlsConnect};
use tokio_postgres_rustls::MakeRustlsConnect;

pub(super) fn available() -> bool {
    std::env::var_os("TLS_POOL_PORT").is_some()
}

#[derive(Clone)]
struct CountedTls {
    inner: MakeRustlsConnect,
    calls: Arc<AtomicUsize>,
}

impl MakeTlsConnect<Socket> for CountedTls {
    type Stream = <MakeRustlsConnect as MakeTlsConnect<Socket>>::Stream;
    type TlsConnect = <MakeRustlsConnect as MakeTlsConnect<Socket>>::TlsConnect;
    type Error = <MakeRustlsConnect as MakeTlsConnect<Socket>>::Error;

    fn make_tls_connect(&mut self, domain: &str) -> Result<Self::TlsConnect, Self::Error> {
        assert_eq!(domain, "localhost");
        self.calls.fetch_add(1, Ordering::Relaxed);
        <MakeRustlsConnect as MakeTlsConnect<Socket>>::make_tls_connect(&mut self.inner, domain)
    }
}

fn tls(variable: &str) -> CountedTls {
    let der = std::fs::read(std::env::var(variable).unwrap()).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(rustls::pki_types::CertificateDer::from(der))
        .unwrap();
    CountedTls {
        inner: MakeRustlsConnect::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        ),
        calls: Arc::default(),
    }
}

fn config(name: &str, schema: &str) -> Config {
    let mut config = Config::new();
    config
        .host("localhost")
        .hostaddr("127.0.0.1".parse().unwrap())
        .port(std::env::var("TLS_POOL_PORT").unwrap().parse().unwrap())
        .user("postgres")
        .dbname("postgres")
        .application_name(name)
        .options(format!("-csearch_path={schema}"))
        .ssl_mode(SslMode::Require)
        .connect_timeout(Duration::from_secs(3));
    config
}

async fn direct(name: &str, schema: &str) -> Client {
    let (client, connection) = config(name, schema)
        .connect(tls("TLS_POOL_CA"))
        .await
        .unwrap();
    tokio::spawn(connection);
    client
}

pub(super) struct Fixture {
    pub(super) name: String,
    schema: String,
    pub(super) store: PostgresStore,
    pub(super) pool: Option<Pool>,
    pub(super) observer: Client,
    pub(super) holder: Client,
    connected: CountedTls,
    cancelled: CountedTls,
}

impl Fixture {
    pub(super) async fn new(name: &str, owned: bool, wrong_cancel: bool) -> Self {
        let name = format!("rk_scope_{name}_{}", std::process::id());
        let schema = name.clone();
        let observer = direct(&format!("{name}_observer"), &schema).await;
        observer
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        let holder = direct(&format!("{name}_holder"), &schema).await;
        let connected = tls("TLS_POOL_CA");
        let cancelled = if wrong_cancel {
            tls("TLS_POOL_BAD_CA")
        } else {
            connected.clone()
        };
        let pool = (!owned).then(|| {
            Pool::builder(Manager::new(config(&name, &schema), connected.clone()))
                .max_size(1)
                .build()
                .unwrap()
        });
        let ca =
            std::path::PathBuf::from(std::env::var("TLS_POOL_CA").unwrap()).with_extension("pem");
        let port = std::env::var("TLS_POOL_PORT").unwrap();
        let connector = cancelled.clone();
        let store = PostgresStore::new(PostgresStoreOptions {
            dsn: Some(format!(
                "postgresql://postgres:fixture@localhost:{port}/postgres?sslmode=verify-full&sslrootcert={}&search_path={schema}&application_name={name}",
                ca.display()
            )),
            pool: pool.clone(),
            cancel_query: Some(Arc::new(move |token| {
                let tls = connector.clone();
                Box::pin(async move {
                    token
                        .cancel_query(tls)
                        .await
                        .map_err(StoreError::unavailable)
                })
            })),
        });
        store
            .init(InitOptions {
                min_size: 1,
                max_size: 1,
            })
            .await
            .unwrap();
        Self {
            name,
            schema,
            store,
            pool,
            observer,
            holder,
            connected,
            cancelled,
        }
    }

    pub(super) async fn worker(&self) -> i32 {
        self.observer
            .query_one(
                "SELECT pid FROM pg_stat_activity WHERE application_name = $1",
                &[&self.name],
            )
            .await
            .unwrap()
            .get(0)
    }

    pub(super) fn assert_connector_calls(&self, calls: usize) {
        assert_eq!(self.connected.calls.load(Ordering::Relaxed), calls);
    }

    pub(super) fn assert_failed_connector_calls(&self) {
        assert_eq!(self.connected.calls.load(Ordering::Relaxed), 1);
        assert_eq!(self.cancelled.calls.load(Ordering::Relaxed), 1);
    }

    pub(super) async fn close(self) {
        self.store.close().await;
        if let Some(pool) = self.pool {
            pool.close();
        }
        self.observer
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .await
            .unwrap();
    }
}

pub(super) async fn blocked(observer: &Client, worker: i32) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let blockers: Vec<i32> = observer
                .query_one("SELECT pg_blocking_pids($1)", &[&worker])
                .await
                .unwrap()
                .get(0);
            if !blockers.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

pub(super) async fn client_state(observer: &Client, worker: i32) -> (String, bool) {
    let row = observer
        .query_one(
            "SELECT state, xact_start IS NULL FROM pg_stat_activity WHERE pid=$1",
            &[&worker],
        )
        .await
        .unwrap();
    (row.get(0), row.get(1))
}
