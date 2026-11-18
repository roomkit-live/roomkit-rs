//! Address policy follows `CPython` `ipaddress` 3.12.12, 3.13.3 and 3.14.9.
//! The Python helper also applies these special-purpose ranges and mapped
//! IPv4 semantics on earlier patch releases.
//! The explicit exceptions distinguish special-purpose networks from their public subranges.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

const V4_PRIVATE: &[(u32, u32)] = &[
    (0x0000_0000, 8),  // 0.0.0.0/8
    (0x0a00_0000, 8),  // 10.0.0.0/8
    (0x7f00_0000, 8),  // 127.0.0.0/8
    (0xa9fe_0000, 16), // 169.254.0.0/16
    (0xac10_0000, 12), // 172.16.0.0/12
    (0xc000_0000, 24), // 192.0.0.0/24
    (0xc000_0200, 24), // 192.0.2.0/24
    (0xc0a8_0000, 16), // 192.168.0.0/16
    (0xc612_0000, 15), // 198.18.0.0/15
    (0xc633_6400, 24), // 198.51.100.0/24
    (0xcb00_7100, 24), // 203.0.113.0/24
    (0xf000_0000, 4),  // 240.0.0.0/4
];

const V4_EXCEPTIONS: &[(u32, u32)] = &[
    (0xc000_0009, 32), // 192.0.0.9/32
    (0xc000_000a, 32), // 192.0.0.10/32
];

const V6_PRIVATE: &[(u128, u32)] = &[
    (0x0000_0000_0000_0000_0000_0000_0000_0000, 127), // ::/127
    (0x0000_0000_0000_0000_0000_ffff_0000_0000, 96),  // ::ffff:0.0.0.0/96
    (0x0064_ff9b_0001_0000_0000_0000_0000_0000, 48),  // 64:ff9b:1::/48
    (0x0100_0000_0000_0000_0000_0000_0000_0000, 64),  // 100::/64
    (0x2001_0000_0000_0000_0000_0000_0000_0000, 23),  // 2001::/23
    (0x2001_0db8_0000_0000_0000_0000_0000_0000, 32),  // 2001:db8::/32
    (0x2002_0000_0000_0000_0000_0000_0000_0000, 16),  // 2002::/16
    (0x3fff_0000_0000_0000_0000_0000_0000_0000, 20),  // 3fff::/20
    (0xfc00_0000_0000_0000_0000_0000_0000_0000, 7),   // fc00::/7
    (0xfe80_0000_0000_0000_0000_0000_0000_0000, 10),  // fe80::/10
];

const V6_EXCEPTIONS: &[(u128, u32)] = &[
    (0x2001_0001_0000_0000_0000_0000_0000_0001, 128), // 2001:1::1/128
    (0x2001_0001_0000_0000_0000_0000_0000_0002, 128), // 2001:1::2/128
    (0x2001_0003_0000_0000_0000_0000_0000_0000, 32),  // 2001:3::/32
    (0x2001_0004_0112_0000_0000_0000_0000_0000, 48),  // 2001:4:112::/48
    (0x2001_0020_0000_0000_0000_0000_0000_0000, 27),  // 2001:20::/27
];

const V6_RESERVED: &[(u128, u32)] = &[
    (0x0000_0000_0000_0000_0000_0000_0000_0000, 3), // ::/3
    (0x4000_0000_0000_0000_0000_0000_0000_0000, 2), // 4000::/2
    (0x8000_0000_0000_0000_0000_0000_0000_0000, 2), // 8000::/2
    (0xc000_0000_0000_0000_0000_0000_0000_0000, 3), // c000::/3
    (0xe000_0000_0000_0000_0000_0000_0000_0000, 4), // e000::/4
    (0xf000_0000_0000_0000_0000_0000_0000_0000, 5), // f000::/5
    (0xf800_0000_0000_0000_0000_0000_0000_0000, 6), // f800::/6
    (0xfe00_0000_0000_0000_0000_0000_0000_0000, 9), // fe00::/9
];

pub(super) fn check(host: &str, address: IpAddr) -> Result<(), String> {
    let reason = match address {
        IpAddr::V4(v4) => ipv4_reason(v4),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => ipv4_reason(v4),
            None => ipv6_reason(v6),
        },
    };
    match reason {
        Some(reason) => Err(format!(
            "URL host {host:?} resolves to {reason} address {address}"
        )),
        None => Ok(()),
    }
}

fn ipv4_reason(address: Ipv4Addr) -> Option<&'static str> {
    let within = |ranges: &[(u32, u32)]| {
        ranges
            .iter()
            .any(|&(start, bits)| (address.to_bits() ^ start).leading_zeros() >= bits)
    };
    if address.is_loopback() {
        Some("loopback")
    } else if within(V4_PRIVATE) && !within(V4_EXCEPTIONS) {
        Some("private")
    } else if address.is_multicast() {
        Some("multicast")
    } else {
        None
    }
}

fn ipv6_reason(address: Ipv6Addr) -> Option<&'static str> {
    let within = |ranges: &[(u128, u32)]| {
        ranges
            .iter()
            .any(|&(start, bits)| (address.to_bits() ^ start).leading_zeros() >= bits)
    };
    if address.is_loopback() {
        Some("loopback")
    } else if within(V6_PRIVATE) && !within(V6_EXCEPTIONS) {
        Some("private")
    } else if within(V6_RESERVED) {
        Some("reserved")
    } else if address.is_multicast() {
        Some("multicast")
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn special_ranges_preserve_their_public_exceptions_and_mapped_ipv4_policy() {
        for (literal, public) in [
            ("2001:db8::1", false),
            ("3fff::1", false),
            ("ff02::1", false),
            ("fe80::1", false),
            ("2001:1::1", true),
            ("2001:3::1", true),
            ("2002::1", false),
            ("64:ff9b:1::1", false),
            ("192.0.0.8", false),
            ("192.0.0.9", true),
            ("::ffff:192.0.0.9", true),
            ("::ffff:127.0.0.1", false),
        ] {
            assert_eq!(
                check("service.example", literal.parse().unwrap()).is_ok(),
                public,
                "{literal}"
            );
        }
    }
}
