//! Python's defaults, each Rust option's (step 5, lot 6): rustdoc shows no
//! default, so each line asserts the default of the Python parameter its
//! comment names, which `make api-parity` checks every defaulted parameter
//! in scope has.

use roomkit_acp::{AcpChannelOptions, StdioAcpTransport, StdioAcpTransportOptions};

#[test]
fn stdio_acp_transport_options_defaults_as_python() {
    let defaults = StdioAcpTransportOptions::default();
    // roomkit.StdioACPTransport.__init__(env) = None
    assert!(defaults.env.is_none());
    // roomkit.StdioACPTransport.__init__(inherit_env) = None
    assert!(defaults.inherit_env.is_none());
}

#[test]
fn acp_channel_options_defaults_as_python() {
    let defaults = AcpChannelOptions::<StdioAcpTransport>::default();
    // roomkit.ACPChannel.__init__(command) = None
    assert!(defaults.command.is_none());
    // roomkit.ACPChannel.__init__(transport) = None
    assert!(defaults.transport.is_none());
    // roomkit.ACPChannel.__init__(env) = None
    assert!(defaults.env.is_none());
    // roomkit.ACPChannel.__init__(inherit_env) = None
    assert!(defaults.inherit_env.is_none());
    // roomkit.ACPChannel.__init__(authentication_method) = None
    assert!(defaults.authentication_method.is_none());
    // roomkit.ACPChannel.__init__(session_config) = None
    assert!(defaults.session_config.is_none());
}

#[test]
fn acp_session_invalidated_error_options_defaults_as_python() {
    let defaults = roomkit_acp::AcpSessionInvalidatedErrorOptions::default();
    // roomkit.ACPSessionInvalidatedError.__init__(recovery_authorized) = False
    assert!(!defaults.recovery_authorized);
}
