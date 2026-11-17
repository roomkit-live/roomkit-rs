//! Python's ordinary text content controls and tool logs, through public turns.
#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod text_logging {
    pub(super) mod capture;
    mod diagnostics;
    mod gates;
    mod ordering;
    mod ordinary;
    pub(super) mod support;
}
