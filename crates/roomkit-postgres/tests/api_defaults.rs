//! Python's defaults, each Rust option's (step 5, lot 6): rustdoc shows no
//! default, so each line asserts the default of the Python parameter its
//! comment names, which `make api-parity` checks every defaulted parameter
//! in scope has.

use roomkit_postgres::{
    DedupeEventIndicesOptions, DropLegacyParentIndexOptions, InitOptions, MigrateOptions,
    PostgresStoreOptions,
};

#[test]
fn postgres_store_options_defaults_as_python() {
    let defaults = PostgresStoreOptions::default();
    // roomkit.store.PostgresStore.__init__(dsn) = None
    assert!(defaults.dsn.is_none());
    // roomkit.store.PostgresStore.__init__(pool) = None
    assert!(defaults.pool.is_none());
}

#[test]
fn init_options_defaults_as_python() {
    let defaults = InitOptions::default();
    // roomkit.store.PostgresStore.init(min_size) = 2
    assert_eq!(defaults.min_size, 2);
    // roomkit.store.PostgresStore.init(max_size) = 10
    assert_eq!(defaults.max_size, 10);
}

#[test]
fn migrate_options_defaults_as_python() {
    let defaults = MigrateOptions::default();
    // roomkit.store.PostgresStore.migrate(dry_run) = True
    assert!(defaults.dry_run);
    // roomkit.store.PostgresStore.migrate(confirm) = False
    assert!(!defaults.confirm);
}

#[test]
fn dedupe_event_indices_options_defaults_as_python() {
    // roomkit.store.PostgresStore.dedupe_event_indices(dry_run) = True
    assert!(DedupeEventIndicesOptions::default().dry_run);
}

#[test]
fn drop_legacy_parent_index_options_defaults_as_python() {
    // roomkit.store.PostgresStore.drop_legacy_parent_index(dry_run) = True
    assert!(DropLegacyParentIndexOptions::default().dry_run);
}
