//! `SafeLoader` timestamp recognition and Python datetime rendering.

use super::ascii_digits;

pub(super) fn read(text: &str) -> Option<String> {
    let (date, rest) = match text.find(['T', 't', ' ', '\t']) {
        Some(at) => (text.get(..at)?, text.get(at + 1..)?.trim_start()),
        None => (text, ""),
    };
    let date: Vec<_> = date.split('-').collect();
    if date.len() != 3 || date.first()?.len() != 4 {
        return None;
    }
    let year = date.first()?.parse::<i32>().ok()?;
    let month = date.get(1)?.parse::<u32>().ok()?;
    let day = date.get(2)?.parse::<u32>().ok()?;
    let max = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        _ => return None,
    };
    if year == 0 || day == 0 || day > max {
        return None;
    }
    let date = format!("{year:04}-{month:02}-{day:02}");
    if rest.is_empty() {
        return Some(date);
    }
    let (time, zone) = match rest.find(['Z', '+', '-']) {
        Some(at) => (rest.get(..at)?.trim_end(), rest.get(at..)?),
        None => (rest, ""),
    };
    let parts: Vec<_> = time.split(':').collect();
    if parts.len() != 3 {
        return None;
    }
    let hour = parts.first()?.parse::<u32>().ok()?;
    let minute = parts.get(1)?.parse::<u32>().ok()?;
    let (second, fraction) = parts.get(2)?.split_once('.').unwrap_or((parts.get(2)?, ""));
    let second = second.parse::<u32>().ok()?;
    if hour > 23 || minute > 59 || second > 59 || !fraction.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let fraction: String = fraction
        .chars()
        .take(6)
        .chain(std::iter::repeat('0'))
        .take(6)
        .collect();
    let fraction = if fraction == "000000" {
        String::new()
    } else {
        format!(".{fraction}")
    };
    let zone = if zone == "Z" {
        "+00:00".to_owned()
    } else if zone.is_empty() {
        String::new()
    } else {
        let sign = zone.chars().next()?;
        let rest = zone.get(1..)?;
        let (h, m) = rest.split_once(':').unwrap_or((rest, "0"));
        let h = h.parse::<u32>().ok()?;
        let m = m.parse::<u32>().ok()?;
        let minutes = h.checked_mul(60)?.checked_add(m)?;
        if minutes >= 24 * 60 {
            return None;
        }
        let sign = if minutes == 0 { '+' } else { sign };
        format!("{sign}{:02}:{:02}", minutes / 60, minutes % 60)
    };
    Some(format!(
        "{date} {hour:02}:{minute:02}:{second:02}{fraction}{zone}"
    ))
}

pub(super) fn matches(text: &str) -> bool {
    let (date, rest) = match text.find(['T', 't', ' ', '\t']) {
        Some(at) => (
            text.get(..at).unwrap_or_default(),
            Some(
                text.get(at + 1..)
                    .unwrap_or_default()
                    .trim_start_matches([' ', '\t']),
            ),
        ),
        None => (text, None),
    };
    let date: Vec<_> = date.split('-').collect();
    let [year, month, day] = date.as_slice() else {
        return false;
    };
    if !ascii_digits(year, 4, 4) {
        return false;
    }
    let min = if rest.is_some() { 1 } else { 2 };
    if ![month, day].iter().all(|p| ascii_digits(p, min, 2)) {
        return false;
    }
    let Some(rest) = rest else {
        return true;
    };
    let (time, zone) = match rest.find(['Z', '+', '-']) {
        Some(at) => (
            rest.get(..at)
                .unwrap_or_default()
                .trim_end_matches([' ', '\t']),
            rest.get(at..),
        ),
        None => (rest, None),
    };
    let parts: Vec<_> = time.split(':').collect();
    let [hour, minute, second] = parts.as_slice() else {
        return false;
    };
    if !ascii_digits(hour, 1, 2) || !ascii_digits(minute, 2, 2) {
        return false;
    }
    let (second, fraction) = second.split_once('.').unwrap_or((second, ""));
    if !ascii_digits(second, 2, 2) || !fraction.bytes().all(|c| c.is_ascii_digit()) {
        return false;
    }
    match zone {
        None | Some("Z") => true,
        Some(zone) => {
            let zone = zone.get(1..).unwrap_or_default();
            let (hours, minutes) = zone
                .split_once(':')
                .map_or((zone, None), |(h, m)| (h, Some(m)));
            ascii_digits(hours, 1, 2) && minutes.is_none_or(|m| ascii_digits(m, 2, 2))
        }
    }
}

/// Python compares aware datetimes as instants before constructing a set/map.
pub(super) fn same_instant(left: &str, right: &str) -> Option<bool> {
    Some(instant(left)? == instant(right)?)
}

fn instant(text: &str) -> Option<i64> {
    let (date, rest) = text.split_once(' ')?;
    let zone_at = rest.find(['+', '-'])?;
    let (time, zone) = (rest.get(..zone_at)?, rest.get(zone_at..)?);
    let date: Vec<_> = date.split('-').collect();
    let year = date.first()?.parse::<i64>().ok()?;
    let month = date.get(1)?.parse::<usize>().ok()?;
    let day = date.get(2)?.parse::<i64>().ok()?;
    let prior = year - 1;
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let before_month = [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    let days = prior * 365 + prior / 4 - prior / 100
        + prior / 400
        + before_month.get(month)?
        + i64::from(month > 2 && leap)
        + day
        - 1;
    let parts: Vec<_> = time.split(':').collect();
    let hour = parts.first()?.parse::<i64>().ok()?;
    let minute = parts.get(1)?.parse::<i64>().ok()?;
    let (second, fraction) = parts
        .get(2)?
        .split_once('.')
        .unwrap_or((parts.get(2)?, "0"));
    let second = second.parse::<i64>().ok()?;
    let micros = fraction.parse::<i64>().ok()?;
    let (hours, minutes) = zone.get(1..)?.split_once(':')?;
    let offset = (hours.parse::<i64>().ok()? * 60 + minutes.parse::<i64>().ok()?)
        * if zone.starts_with('-') { -1 } else { 1 };
    Some(((days * 24 + hour) * 3600 + minute * 60 + second - offset * 60) * 1_000_000 + micros)
}
