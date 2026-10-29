//! The Python implementation's `gt=0` bound on a model's numbers: a value
//! read is refused unless it is greater than 0, and so is one a struct
//! literal built, by the check each door runs.

use super::ModelError;

/// Refuses a count that is 0, the only unsigned value `gt=0` refuses.
pub(crate) fn count(name: &'static str, value: Option<impl Into<u64>>) -> Result<(), ModelError> {
    match value.map(Into::into) {
        Some(0) => Err(ModelError::NotPositive { name, value: 0.0 }),
        Some(_) | None => Ok(()),
    }
}

/// Refuses a number that is not greater than 0, `NaN` included, as `gt=0`
/// does.
pub(crate) fn number(name: &'static str, value: f64) -> Result<(), ModelError> {
    if value > 0.0 {
        Ok(())
    } else {
        Err(ModelError::NotPositive { name, value })
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
