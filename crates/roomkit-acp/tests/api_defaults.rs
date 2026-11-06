//! Python's defaults, each Rust option's (step 5, lot 6): rustdoc shows no
//! default, so each line asserts the default of the Python parameter its
//! comment names, which `make api-parity` checks every defaulted parameter
//! in scope has.

use roomkit_acp::StdioAcpTransportOptions;

#[test]
fn stdio_acp_transport_options_defaults_as_python() {
    let defaults = StdioAcpTransportOptions::default();
    // roomkit.StdioACPTransport.__init__(env) = None
    assert!(defaults.env.is_none());
    // roomkit.StdioACPTransport.__init__(inherit_env) = None
    assert!(defaults.inherit_env.is_none());
}
