//! Python's defaults, each Rust option's (step 5, lot 6): rustdoc shows no
//! default, so each line asserts the default of the Python parameter its
//! comment names, which `make api-parity` checks every defaulted parameter
//! in scope has.

use roomkit_mcp::AsToolHandlerOptions;
use roomkit_mcp::FromCommandOptions;
#[cfg(feature = "http")]
use roomkit_mcp::FromUrlOptions;
use roomkit_mcp::{CallToolOptions, ReadResourceOptions, RequestOptions};
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
    // roomkit.tools.MCPToolProvider.from_command(discover) = True
    assert!(defaults.discover);
    // roomkit.tools.MCPToolProvider.from_command(args) = ()
    assert_eq!(defaults.args, Vec::<std::ffi::OsString>::new());
    // roomkit.tools.MCPToolProvider.from_command(connect_timeout) = 60.0
    assert_eq!(defaults.connect_timeout, Duration::from_secs(60));
    // roomkit.tools.MCPToolProvider.from_command(cwd) = None
    assert!(defaults.cwd.is_none());
    // roomkit.tools.MCPToolProvider.from_command(env) = None
    assert!(defaults.env.is_none());
    // roomkit.tools.MCPToolProvider.from_command(label) = None
    assert!(defaults.label.is_none());
    // roomkit.tools.MCPToolProvider.from_command(tool_filter) = None
    assert!(defaults.tool_filter.is_none());
}

#[cfg(feature = "http")]
#[test]
fn from_url_options_defaults_as_python() {
    let defaults = FromUrlOptions::default();
    // roomkit.tools.MCPToolProvider.from_url(transport) = 'streamable_http'
    assert_eq!(defaults.transport, "streamable_http");
    // roomkit.tools.MCPToolProvider.from_url(discover) = True
    assert!(defaults.discover);
    // roomkit.tools.MCPToolProvider.from_url(connect_timeout) = 60.0
    assert_eq!(defaults.connect_timeout, Duration::from_secs(60));
    // roomkit.tools.MCPToolProvider.from_url(headers) = None
    assert!(defaults.headers.is_none());
    // roomkit.tools.MCPToolProvider.from_url(label) = None
    assert!(defaults.label.is_none());
    // roomkit.tools.MCPToolProvider.from_url(tool_filter) = None
    assert!(defaults.tool_filter.is_none());
}

#[test]
fn common_constructor_options_default_as_python() {
    let defaults = roomkit_mcp::McpToolProviderOptions::default();
    // roomkit.tools.MCPToolProvider.__init__(url) = None
    assert!(roomkit_mcp::McpToolProvider::new(None, defaults.clone()).is_err());
    // roomkit.tools.MCPToolProvider.__init__(transport) = 'streamable_http'
    assert_eq!(defaults.transport, "streamable_http");
    // roomkit.tools.MCPToolProvider.__init__(tool_filter) = None
    assert!(defaults.tool_filter.is_none());
    // roomkit.tools.MCPToolProvider.__init__(headers) = None
    assert!(defaults.headers.is_none());
    // roomkit.tools.MCPToolProvider.__init__(command) = None
    assert!(defaults.command.is_none());
    // roomkit.tools.MCPToolProvider.__init__(args) = ()
    assert_eq!(defaults.args, Vec::<std::ffi::OsString>::new());
    // roomkit.tools.MCPToolProvider.__init__(env) = None
    assert!(defaults.env.is_none());
    // roomkit.tools.MCPToolProvider.__init__(cwd) = None
    assert!(defaults.cwd.is_none());
    // roomkit.tools.MCPToolProvider.__init__(discover) = True
    assert!(defaults.discover);
    // roomkit.tools.MCPToolProvider.__init__(label) = None
    assert!(defaults.label.is_none());
    // roomkit.tools.MCPToolProvider.__init__(connect_timeout) = None
    assert!(defaults.connect_timeout.is_none());
}

#[test]
fn host_options_defaults_as_python() {
    // roomkit.tools.MCPToolProvider.request(timeout) = 30.0
    assert_eq!(RequestOptions::default().timeout, Duration::from_secs(30));
    // roomkit.tools.MCPToolProvider.read_resource(timeout) = 30.0
    assert_eq!(
        ReadResourceOptions::default().timeout,
        Duration::from_secs(30)
    );
}
