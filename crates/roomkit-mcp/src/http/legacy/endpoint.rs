//! The legacy endpoint announcement compares raw netloc, unlike HTTP redirects.

use reqwest::Url;

pub(super) fn resolve(base: &str, announcement: &str) -> Option<Url> {
    let configured = Url::parse(base).ok()?;
    let target = configured.join(announcement).ok()?;
    let expected = raw_origin(base)?;
    let clean = clean(announcement);
    let announced = if clean.starts_with("//") {
        raw_origin(&format!("{}:{clean}", configured.scheme()))?
    } else {
        raw_origin(&clean).unwrap_or_else(|| expected.clone())
    };
    if announced != expected || configured.origin() != target.origin() {
        return None;
    }
    Some(target)
}

fn raw_origin(value: &str) -> Option<(String, String)> {
    let value = clean(value);
    let (scheme, rest) = value.split_once(':')?;
    let authority = rest.strip_prefix("//")?;
    let netloc = authority.split(['/', '?', '#']).next()?;
    Some((scheme.to_ascii_lowercase(), netloc.to_owned()))
}

fn clean(value: &str) -> String {
    value
        .trim_start_matches(|character: char| character <= ' ')
        .chars()
        .filter(|character| !matches!(character, '\t' | '\r' | '\n'))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::resolve;

    // Python: tests/test_mcp_legacy_boundaries.py::test_legacy_endpoint_announcement_compares_the_configured_raw_netloc
    #[test]
    fn an_announcement_keeps_the_configured_raw_netloc() {
        let base = "http://localhost:8765/mcp";
        for target in [
            "messages?session=1",
            "/messages?session=1",
            "http://localhost:8765/messages?session=1",
        ] {
            assert_eq!(resolve(base, target).unwrap().path(), "/messages");
        }
        for target in [
            "http://LOCALHOST:8765/messages",
            "http://user@localhost:8765/messages",
            "http://localhost:8766/messages",
            "//elsewhere.test/messages",
            "https://localhost:8765/messages",
        ] {
            assert!(resolve(base, target).is_none(), "{target}");
        }
    }
}
