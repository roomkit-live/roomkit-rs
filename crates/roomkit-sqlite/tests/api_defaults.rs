//! Python's defaults, each Rust option's (step 5, lot 6): rustdoc shows no
//! default, so each line asserts the default of the Python parameter its
//! comment names, which `make api-parity` checks every defaulted parameter
//! in scope has.

use roomkit_sqlite::SearchEventsOptions;
use roomkit_sqlite::SqliteStoreOptions;
use std::path::PathBuf;

#[test]
fn search_events_options_defaults_as_python() {
    let defaults = SearchEventsOptions::default();
    // roomkit.SQLiteStore.search_events(limit) = 20
    assert_eq!(defaults.limit, 20);
    // roomkit.SQLiteStore.search_events(room_id) = None
    assert!(defaults.room_id.is_none());
}

#[test]
fn sqlite_store_options_defaults_as_python() {
    let defaults = SqliteStoreOptions::default();
    // roomkit.SQLiteStore.__init__(path) = 'roomkit.db'
    assert_eq!(defaults.path, PathBuf::from("roomkit.db"));
}
