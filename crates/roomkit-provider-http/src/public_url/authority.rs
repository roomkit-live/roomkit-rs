//! Check the original authority before WHATWG host or delimiter normalization.

use std::net::Ipv6Addr;

pub(super) fn check(rest: &str) -> Result<(), String> {
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    if authority.is_empty() {
        return Err("URL must have a scheme and host".to_owned());
    }
    if authority.contains('\\')
        || authority.bytes().any(|b| b <= b' ' || b == 0x7f)
        || authority.matches('@').count() > 1
    {
        return Err("Invalid URL authority".to_owned());
    }
    let host_port = authority.rsplit('@').next().unwrap_or_default();
    if let Some(bracketed) = host_port.strip_prefix('[') {
        let invalid = || "Invalid IPv6 host or port in URL authority".to_owned();
        let (literal, suffix) = bracketed.split_once(']').ok_or_else(invalid)?;
        if !suffix.is_empty()
            && !suffix
                .strip_prefix(':')
                .is_some_and(|port| port.bytes().all(|b| b.is_ascii_digit()))
        {
            return Err(invalid());
        }
        literal.parse::<Ipv6Addr>().map_err(|_| invalid())?;
        return Ok(());
    }
    let host = host_port.split(':').next().unwrap_or_default();
    if host.trim_end_matches('.').is_empty()
        || !host
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
    {
        return Err("URL host must be an ASCII DNS name or IP address".to_owned());
    }
    Ok(())
}
