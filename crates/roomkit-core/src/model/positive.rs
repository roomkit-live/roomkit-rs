//! The Python implementation's `gt=0` bound on a model's numbers: a value
//! read is refused unless it is greater than 0, and so is one a struct
//! literal built, by the check each door runs.

use super::ModelError;

/// Refuses a count that is 0, the only unsigned value `gt=0` refuses.
pub(crate) fn count(name: &'static str, value: Option<impl Into<u64>>) -> Result<(), ModelError> {
    match value.map(Into::into) {
        Some(0) => Err(ModelError::NotPositive {
            name,
            value: "0".to_owned(),
        }),
        Some(_) | None => Ok(()),
    }
}

/// Refuses a number that is not greater than 0, `NaN` included, as `gt=0`
/// does.
pub(crate) fn number(name: &'static str, value: f64) -> Result<(), ModelError> {
    if value > 0.0 {
        Ok(())
    } else {
        Err(ModelError::NotPositive {
            name,
            value: python_float(value),
        })
    }
}

/// `value` as Python's `repr` of a `float` writes it, which pydantic's
/// `input_value=` shows: the shortest digits that read back, positional
/// for an exponent from -4 to 15 (`0.0`, `-1.5`, `0.0001`), scientific
/// otherwise with a sign and two digits at least (`1e-05`, `1e+16`), and
/// `nan`, `inf`, `-inf`. Also how Python's `json.dumps` writes a finite
/// float, which the kit's JSON writers follow (RMK-1262); `json.dumps`
/// writes `NaN` and `Infinity`, which a `serde_json::Number` never holds.
#[doc(hidden)]
pub fn python_float(value: f64) -> String {
    if value.is_nan() {
        return "nan".to_owned();
    }
    if value.is_infinite() {
        return if value > 0.0 { "inf" } else { "-inf" }.to_owned();
    }
    // Rust's `{:e}` writes the shortest digits that read back. At 17
    // digits, the value exactly halfway between two that both read back, it
    // rounds up where Python rounds to the even one; `{:.16e}` writes those
    // 17 digits rounded half to even (RMK-1525).
    let mut scientific = format!("{value:e}");
    if significant_digits(&scientific) == 17 {
        scientific = format!("{value:.16e}");
    }
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((&scientific, "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    if (-4..16).contains(&exponent) {
        return positional(mantissa, exponent);
    }
    let sign = if exponent < 0 { '-' } else { '+' };
    format!("{mantissa}e{sign}{:02}", exponent.unsigned_abs())
}

/// How many significant digits `scientific` (`-d.ddde-x`) writes.
fn significant_digits(scientific: &str) -> usize {
    scientific
        .split('e')
        .next()
        .unwrap_or_default()
        .chars()
        .filter(char::is_ascii_digit)
        .count()
}

/// `mantissa` (`-d.ddd`) times ten to `exponent`, from -4 to 15, written
/// without an exponent as Python's `repr` writes it: `.0` after a whole
/// number (`1000.0`, `0.0001`, `-1.5`).
fn positional(mantissa: &str, exponent: i32) -> String {
    let (sign, unsigned) = mantissa
        .strip_prefix('-')
        .map_or(("", mantissa), |unsigned| ("-", unsigned));
    let digits: Vec<char> = unsigned.chars().filter(char::is_ascii_digit).collect();
    let whole = usize::try_from(exponent + 1).unwrap_or(0);
    if whole == 0 {
        let zeros = usize::try_from(-(exponent + 1)).unwrap_or(0);
        let digits: String = digits.into_iter().collect();
        return format!("{sign}0.{}{digits}", "0".repeat(zeros));
    }
    let integer: String = digits.iter().take(whole).collect();
    let fraction: String = digits.iter().skip(whole).collect();
    let padding = "0".repeat(whole.saturating_sub(digits.len()));
    if fraction.is_empty() {
        format!("{sign}{integer}{padding}.0")
    } else {
        format!("{sign}{integer}.{fraction}")
    }
}

/// Deserializers for bounded fields, one per field, named after it: each
/// reads its field, then refuses what `check` names, with the field's name.
macro_rules! deserializers {
    ($check:path => $($field:ident: $ty:ty),+ $(,)?) => {
        $(
            pub(super) fn $field<'de, D>(deserializer: D) -> Result<$ty, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let value = <$ty as serde::Deserialize>::deserialize(deserializer)?;
                $check(stringify!($field), value).map_err(serde::de::Error::custom)?;
                Ok(value)
            }
        )+
    };
}

pub(crate) use deserializers;

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    /// Python's `repr()` of each value (`python3 -c "print(repr(v))"`).
    #[test]
    #[expect(
        clippy::excessive_precision,
        reason = "the exact values of the ties, halfway between two 17-digit forms"
    )]
    fn a_float_is_written_as_python_s_repr_writes_it() {
        let values = [
            0.0,
            -1.0,
            1e-310,
            1.5,
            1e16,
            1e-5,
            0.0001,
            123_456_789_012_345.6,
            -0.0,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            2.5e-7,
            1e15,
            0.300_000_000_000_000_04,
            // Halfway between two 17-digit forms that both read back:
            // Python keeps the even one (RMK-1525).
            -1_548_782_250_018_927.25,
            -155_559_142_687_939.625,
        ];
        assert_eq!(
            values.map(python_float),
            [
                "0.0",
                "-1.0",
                "1e-310",
                "1.5",
                "1e+16",
                "1e-05",
                "0.0001",
                "123456789012345.6",
                "-0.0",
                "nan",
                "inf",
                "-inf",
                "2.5e-07",
                "1000000000000000.0",
                "0.30000000000000004",
                "-1548782250018927.2",
                "-155559142687939.62"
            ]
        );
    }
}
