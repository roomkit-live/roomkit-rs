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
/// `nan`, `inf`, `-inf`.
fn python_float(value: f64) -> String {
    if value.is_nan() {
        return "nan".to_owned();
    }
    if value.is_infinite() {
        return if value > 0.0 { "inf" } else { "-inf" }.to_owned();
    }
    // Rust's `{:e}` writes the shortest digits that read back.
    let scientific = format!("{value:e}");
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((&scientific, "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    if (-4..16).contains(&exponent) {
        let positional = format!("{value}");
        return if positional.contains('.') {
            positional
        } else {
            format!("{positional}.0")
        };
    }
    let sign = if exponent < 0 { '-' } else { '+' };
    format!("{mantissa}e{sign}{:02}", exponent.unsigned_abs())
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
                "2.5e-07"
            ]
        );
    }
}
