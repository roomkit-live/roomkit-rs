//! Python's defaults, each Rust option's (step 5, lot 6): rustdoc shows no
//! default, so each line asserts the default of the Python parameter its
//! comment names, which `make api-parity` checks every defaulted parameter
//! in scope has.

use roomkit_postgres::{
    DedupeEventIndicesOptions, DropLegacyParentIndexOptions, InitOptions, MigrateOptions,
    PostgresAdvisoryLockManagerOptions, PostgresKnowledgeSourceOptions, PostgresStoreOptions,
};

#[test]
fn postgres_advisory_lock_manager_options_defaults_as_python() {
    let defaults = PostgresAdvisoryLockManagerOptions::default();
    // roomkit.store.PostgresAdvisoryLockManager.__init__(dsn) = None
    assert!(defaults.dsn.is_none());
    // roomkit.store.PostgresAdvisoryLockManager.__init__(pool) = None
    assert!(defaults.pool.is_none());
    // roomkit.store.PostgresAdvisoryLockManager.__init__(min_size) = 1
    assert_eq!(defaults.min_size, 1);
    // roomkit.store.PostgresAdvisoryLockManager.__init__(max_size) = 10
    assert_eq!(defaults.max_size, 10);
}

#[test]
fn postgres_store_options_defaults_as_python() {
    let defaults = PostgresStoreOptions::default();
    // roomkit.store.PostgresStore.__init__(dsn) = None
    assert!(defaults.dsn.is_none());
    // roomkit.store.PostgresStore.__init__(pool) = None
    assert!(defaults.pool.is_none());
}

#[test]
fn postgres_knowledge_source_options_defaults_as_python() {
    let defaults = PostgresKnowledgeSourceOptions::default();
    // roomkit.knowledge.PostgresKnowledgeSource.__init__(dsn) = None
    assert!(defaults.dsn.is_none());
    // roomkit.knowledge.PostgresKnowledgeSource.__init__(pool) = None
    assert!(defaults.pool.is_none());
    // roomkit.knowledge.PostgresKnowledgeSource.__init__(source_name) = 'postgres'
    assert_eq!(defaults.source_name, "postgres");
    // roomkit.knowledge.PostgresKnowledgeSource.__init__(language) = 'english'
    assert_eq!(defaults.language, "english");
}

#[test]
fn init_options_defaults_as_python() {
    let defaults = InitOptions::default();
    // roomkit.store.PostgresStore.init(min_size) = 2
    // roomkit.knowledge.PostgresKnowledgeSource.init(min_size) = 2
    assert_eq!(defaults.min_size, 2);
    // roomkit.store.PostgresStore.init(max_size) = 10
    // roomkit.knowledge.PostgresKnowledgeSource.init(max_size) = 10
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
