//! Construction validates Python's options in source order, without I/O.

use std::collections::HashSet;
use std::time::Duration;

use super::{McpToolProviderOptions, Target};
use crate::command::McpCommand;
use crate::protocol::Protocol;
use crate::{McpError, McpHeaders};

pub(super) fn command_at_entry(
    command: &McpCommand,
    environment: Option<&crate::McpEnvironment>,
) -> McpCommand {
    let mut command = command.clone();
    if let Some(environment) = environment {
        for (key, value) in environment.get() {
            command = command.env(key, value);
        }
    }
    command
}

/// How the provider reaches its server and agrees on the MCP revision with
/// it: the transport checked first, the protocol next, then the transport's
/// own options, in Python's order.
pub(super) fn target(
    url: Option<&str>,
    options: &McpToolProviderOptions,
) -> Result<(Target, Protocol), McpError> {
    let stdio = match options.transport.as_str() {
        "stdio" => true,
        "streamable_http" | "sse" => false,
        _ => return Err(invalid("unsupported transport")),
    };
    let protocol = Protocol::new(&options.protocol, options.propagate_trace_context)?;
    let target = if stdio {
        command(url, options)?
    } else {
        http(url, options)?
    };
    Ok((target, protocol))
}

fn command(url: Option<&str>, options: &McpToolProviderOptions) -> Result<Target, McpError> {
    let Some(program) = options
        .command
        .as_ref()
        .filter(|program| !program.is_empty())
    else {
        return Err(McpError::Command {
            reason: "the program is empty".to_owned(),
        });
    };
    if url.is_some_and(|url| !url.is_empty())
        || options
            .headers
            .as_ref()
            .is_some_and(|headers| !headers.get().is_empty())
    {
        return Err(invalid(
            "URL and headers are for HTTP transports, not stdio",
        ));
    }
    let mut command = McpCommand::new(program.clone())
        .args(options.args.clone())
        .connect_timeout(options.connect_timeout.unwrap_or(Duration::from_secs(60)));
    if let Some(cwd) = &options.cwd {
        command = command.current_dir(cwd.clone());
    }
    Ok(Target::Command {
        command,
        environment: options.env.clone(),
    })
}

fn http(url: Option<&str>, options: &McpToolProviderOptions) -> Result<Target, McpError> {
    let Some(url) = url.filter(|url| !url.is_empty()) else {
        return Err(invalid("HTTP transport needs a URL"));
    };
    if options
        .command
        .as_ref()
        .is_some_and(|command| !command.is_empty())
        || !options.args.is_empty()
        || options.env.is_some()
        || options.cwd.is_some()
    {
        return Err(invalid(
            "command, arguments, environment and directory are for stdio",
        ));
    }
    endpoint(url)?;
    let headers = options
        .headers
        .as_ref()
        .map(McpHeaders::get)
        .unwrap_or_default();
    check_headers(&headers)?;
    #[cfg(feature = "http")]
    {
        let retained = if headers.is_empty() {
            McpHeaders::default()
        } else {
            options.headers.clone().unwrap_or_default()
        };
        let legacy = options.transport == "sse";
        if legacy {
            // Python's `DeprecationWarning`, at each construction too.
            tracing::warn!(
                target: "roomkit::mcp",
                "MCP's HTTP+SSE transport is deprecated by the MCP 2026-07-28 revision: \
                 use streamable_http"
            );
        }
        Ok(Target::Url {
            url: url.to_owned(),
            headers: retained,
            timeout: options.connect_timeout.unwrap_or(Duration::from_secs(60)),
            legacy,
        })
    }
    #[cfg(not(feature = "http"))]
    Err(invalid("HTTP transports require the http feature"))
}

fn endpoint(value: &str) -> Result<(), McpError> {
    let parsed = url::Url::parse(value).map_err(|_| invalid("invalid endpoint URL"))?;
    let clean: String = value
        .trim_start_matches(|character: char| character <= ' ')
        .chars()
        .filter(|character| !matches!(character, '\t' | '\r' | '\n'))
        .collect();
    let authority = clean
        .split_once("://")
        .map(|(_, rest)| rest.split(['/', '?', '#']).next().unwrap_or_default())
        .unwrap_or_default();
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || authority.is_empty()
        || authority.contains('@')
        || value.contains('#')
    {
        return Err(invalid(
            "use an HTTP(S) endpoint without URL credentials or a fragment",
        ));
    }
    Ok(())
}

fn check_headers(headers: &[(String, String)]) -> Result<(), McpError> {
    let mut names = HashSet::new();
    for (name, value) in headers {
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
        {
            return Err(invalid("invalid header name"));
        }
        let lowered = name.to_ascii_lowercase();
        if lowered.starts_with("mcp-")
            || matches!(
                lowered.as_str(),
                "accept"
                    | "content-type"
                    | "content-length"
                    | "host"
                    | "connection"
                    | "transfer-encoding"
                    | "last-event-id"
                    | "upgrade"
                    | "te"
                    | "trailer"
                    | "proxy-authorization"
            )
        {
            return Err(invalid("header is reserved for the transport"));
        }
        if value.len() > 8192 {
            return Err(invalid("header value exceeds 8192 bytes"));
        }
        if !value
            .bytes()
            .all(|byte| byte == b'\t' || (b' '..=b'~').contains(&byte))
        {
            return Err(invalid("invalid header value"));
        }
        names.insert(lowered);
    }
    if names.len() > 32 {
        return Err(invalid("more than 32 custom headers"));
    }
    Ok(())
}

fn invalid(reason: &'static str) -> McpError {
    McpError::Configuration { reason }
}
