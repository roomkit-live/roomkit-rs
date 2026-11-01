//! Python's defaults, each Rust option's (step 5, lot 6): rustdoc shows no
//! default, so each line asserts the default of the Python parameter its
//! comment names, which `make api-parity` checks every defaulted parameter
//! in scope has.

use roomkit_mcp::AsToolHandlerOptions;
use roomkit_mcp::CallToolOptions;
use roomkit_mcp::FromCommandOptions;
#[cfg(feature = "http")]
use roomkit_mcp::FromUrlOptions;
use std::time::Duration;

#[test]
fn as_tool_handler_options_defaults_as_python() {
    let defaults = AsToolHandlerOptions::default();
    // roomkit.tools.MCPToolProvider.as_tool_handler(gate_discovery) = True
    assert!(defaults.gate_discovery);
}

#[test]
fn call_tool_options_defaults_as_python() {
    let defaults = CallToolOptions::default();
    // roomkit.tools.MCPToolProvider.call_tool(timeout) = 30.0
    assert_eq!(defaults.timeout, Duration::from_secs(30));
}

#[test]
fn from_command_options_defaults_as_python() {
    let defaults = FromCommandOptions::default();
    // roomkit.tools.MCPToolProvider.from_command(args) = ()
    assert_eq!(defaults.args, Vec::<std::ffi::OsString>::new());
    // roomkit.tools.MCPToolProvider.from_command(connect_timeout) = 60.0
    assert_eq!(defaults.connect_timeout, Duration::from_secs(60));
    // roomkit.tools.MCPToolProvider.from_command(cwd) = None
    assert!(defaults.cwd.is_none());
    // roomkit.tools.MCPToolProvider.from_command(env) = None
    assert!(defaults.env.is_empty());
    // roomkit.tools.MCPToolProvider.from_command(label) = None
    assert!(defaults.label.is_none());
    // roomkit.tools.MCPToolProvider.from_command(tool_filter) = None
    assert!(defaults.tool_filter.is_none());
}

#[cfg(feature = "http")]
#[test]
fn from_url_options_defaults_as_python() {
    let defaults = FromUrlOptions::default();
    // roomkit.tools.MCPToolProvider.from_url(headers) = None
    assert!(defaults.headers.is_empty());
    // roomkit.tools.MCPToolProvider.from_url(label) = None
    assert!(defaults.label.is_none());
    // roomkit.tools.MCPToolProvider.from_url(tool_filter) = None
    assert!(defaults.tool_filter.is_none());
}
