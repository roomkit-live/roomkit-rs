//! A calendar day: Python's `datetime.date`, which the standard library
//! does not have.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::AiError;

/// A day of the proleptic Gregorian calendar, the years 1 to 9999 as
/// Python's `date` takes them, written `2026-08-05` as Python's
/// `isoformat()` and its JSON write it. Ordered by time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Date {
    year: u16,
    month: u8,
    day: u8,
}

impl Date {
    /// The day `day` of the month `month` of the year `year`: Python's
    /// `date(year, month, day)`.
    ///
    /// # Errors
    ///
    /// [`AiError::Date`] for a year outside 1 to 9999, a month outside 1 to
    /// 12, or a day its month does not have, as Python's `ValueError`.
    pub fn new(year: u16, month: u8, day: u8) -> Result<Self, AiError> {
        if !(1..=9999).contains(&year) {
            return Err(AiError::Date {
                reason: format!("year {year} is out of range"),
            });
        }
        if !(1..=12).contains(&month) {
            return Err(AiError::Date {
                reason: "month must be in 1..12".to_owned(),
            });
        }
        if day == 0 || day > days_in_month(year, month) {
            return Err(AiError::Date {
                reason: "day is out of range for month".to_owned(),
            });
        }
        Ok(Self { year, month, day })
    }

    /// The year.
    pub fn year(self) -> u16 {
        self.year
    }

    /// The month, 1 to 12.
    pub fn month(self) -> u8 {
        self.month
    }

    /// The day of the month, from 1.
    pub fn day(self) -> u8 {
        self.day
    }
}

/// How many days the month `month` of the year `year` has.
fn days_in_month(year: u16, month: u8) -> u8 {
    match month {
        2 if year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400)) => {
            29
        }
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

impl fmt::Display for Date {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{:04}-{:02}-{:02}",
            self.year, self.month, self.day
        )
    }
}

impl FromStr for Date {
    type Err = AiError;

    /// Reads a day written `YYYY-MM-DD`, as Python's `date.fromisoformat`
    /// reads its basic form.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = || AiError::Date {
            reason: format!("Invalid isoformat string: '{value}'"),
        };
        let mut parts = value.split('-');
        let (Some(year), Some(month), Some(day), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(invalid());
        };
        let digits = |part: &str, width: usize| -> Result<u16, AiError> {
            if part.len() != width || !part.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(invalid());
            }
            part.parse().map_err(|_| invalid())
        };
        let year = digits(year, 4)?;
        let month = u8::try_from(digits(month, 2)?).map_err(|_| invalid())?;
        let day = u8::try_from(digits(day, 2)?).map_err(|_| invalid())?;
        Self::new(year, month, day)
    }
}

impl Serialize for Date {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Date {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn a_date_reads_and_writes_python_s_isoformat() {
        let date: Date = "2026-08-05".parse().unwrap();
        assert_eq!(date, Date::new(2026, 8, 5).unwrap());
        assert_eq!(date.to_string(), "2026-08-05");
        assert_eq!(serde_json::to_value(date).unwrap(), "2026-08-05");
    }

    #[test]
    fn a_day_its_month_lacks_is_refused() {
        assert!(Date::new(2026, 2, 29).is_err());
        assert!(Date::new(2024, 2, 29).is_ok());
        assert!(Date::new(1900, 2, 29).is_err());
        assert!(Date::new(2000, 2, 29).is_ok());
        assert!(Date::new(2026, 13, 1).is_err());
        assert!(Date::new(0, 1, 1).is_err());
        assert!("2026-8-05".parse::<Date>().is_err());
        assert!("2026-08-05T00:00".parse::<Date>().is_err());
    }

    #[test]
    fn dates_are_ordered_by_time() {
        assert!(Date::new(2026, 8, 5).unwrap() < Date::new(2026, 9, 30).unwrap());
        assert!(Date::new(2025, 12, 31).unwrap() < Date::new(2026, 1, 1).unwrap());
    }
}
