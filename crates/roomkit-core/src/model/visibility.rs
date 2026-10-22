//! Visibility: which channels may see what a source writes (RFC §7.3).

use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{ChannelId, ModelError};

/// Which channels may see an event (RFC §7.3).
///
/// Written as the RFC writes it: `all`, `none`, `transport`, `intelligence`,
/// or a comma-separated list of channel ids. Parsing follows the grammar
/// decided in gap G-002: whitespace around an id is ignored, an empty
/// visibility or an empty item is refused, and a duplicated id counts once.
///
/// ```
/// use roomkit_core::model::Visibility;
///
/// let visibility: Visibility = "ws_advisor, sms".parse()?;
/// assert_eq!(visibility.to_string(), "sms,ws_advisor");
/// # Ok::<(), roomkit_core::model::ModelError>(())
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum Visibility {
    /// Every channel of the room.
    #[default]
    All,
    /// No channel: the event is stored in the timeline only.
    None,
    /// Transport channels only.
    Transport,
    /// Intelligence channels only.
    Intelligence,
    /// The listed channels only.
    Channels(ChannelSet),
}

impl Visibility {
    /// The list form: only the given channels.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidVisibility`] when `ids` is empty.
    pub fn channels(ids: impl IntoIterator<Item = ChannelId>) -> Result<Self, ModelError> {
        ChannelSet::new(ids).map(Self::Channels)
    }
}

impl fmt::Display for Visibility {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::All => formatter.write_str("all"),
            Self::None => formatter.write_str("none"),
            Self::Transport => formatter.write_str("transport"),
            Self::Intelligence => formatter.write_str("intelligence"),
            Self::Channels(channels) => fmt::Display::fmt(channels, formatter),
        }
    }
}

impl FromStr for Visibility {
    type Err = ModelError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "all" => Ok(Self::All),
            "none" => Ok(Self::None),
            "transport" => Ok(Self::Transport),
            "intelligence" => Ok(Self::Intelligence),
            _ => parse_channel_list(value).map(Self::Channels),
        }
    }
}

fn parse_channel_list(list: &str) -> Result<ChannelSet, ModelError> {
    let refuse = |reason: String| ModelError::InvalidVisibility {
        value: list.to_owned(),
        reason,
    };
    if list.trim().is_empty() {
        return Err(refuse("it is empty".to_owned()));
    }
    let ids = list
        .split(',')
        .map(|item| match item.trim() {
            "" => Err(refuse("it has an empty item".to_owned())),
            id => ChannelId::new(id).map_err(|error| refuse(error.to_string())),
        })
        .collect::<Result<Vec<_>, _>>()?;
    ChannelSet::new(ids)
}

impl Serialize for Visibility {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Visibility {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

/// The channels a list visibility names: never empty, each id once, in id
/// order.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ChannelSet(BTreeSet<ChannelId>);

impl ChannelSet {
    /// Builds the set of the given channels.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidVisibility`] when `ids` is empty: a list
    /// naming no channel is not a visibility (gap G-002).
    pub fn new(ids: impl IntoIterator<Item = ChannelId>) -> Result<Self, ModelError> {
        let ids: BTreeSet<ChannelId> = ids.into_iter().collect();
        if ids.is_empty() {
            return Err(ModelError::InvalidVisibility {
                value: String::new(),
                reason: "it names no channel".to_owned(),
            });
        }
        Ok(Self(ids))
    }

    /// Whether the set names `channel`.
    pub fn contains(&self, channel: &ChannelId) -> bool {
        self.0.contains(channel)
    }

    /// The channels, in id order.
    pub fn iter(&self) -> impl Iterator<Item = &ChannelId> {
        self.0.iter()
    }
}

impl fmt::Display for ChannelSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut separator = "";
        for id in &self.0 {
            write!(formatter, "{separator}{id}")?;
            separator = ",";
        }
        Ok(())
    }
}

impl<'set> IntoIterator for &'set ChannelSet {
    type Item = &'set ChannelId;
    type IntoIter = std::collections::btree_set::Iter<'set, ChannelId>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    fn parse(value: &str) -> Result<Visibility, ModelError> {
        value.parse()
    }

    fn channel(id: &str) -> ChannelId {
        ChannelId::new(id).unwrap()
    }

    #[test]
    fn each_keyword_parses_to_its_variant() {
        let parsed: Vec<_> = ["all", "none", "transport", "intelligence"]
            .into_iter()
            .map(|keyword| parse(keyword).unwrap())
            .collect();
        assert_eq!(
            parsed,
            [
                Visibility::All,
                Visibility::None,
                Visibility::Transport,
                Visibility::Intelligence
            ]
        );
    }

    #[test]
    fn whitespace_around_a_keyword_is_ignored() {
        assert_eq!(parse(" none ").unwrap(), Visibility::None);
    }

    #[test]
    fn a_single_id_parses_to_a_list_of_one() {
        assert_eq!(
            parse("ws_advisor").unwrap(),
            Visibility::channels([channel("ws_advisor")]).unwrap()
        );
    }

    #[test]
    fn whitespace_around_ids_is_ignored() {
        assert_eq!(
            parse(" sms , ws ").unwrap(),
            Visibility::channels([channel("sms"), channel("ws")]).unwrap()
        );
    }

    #[test]
    fn a_duplicated_id_counts_once() {
        assert_eq!(parse("sms,sms").unwrap().to_string(), "sms");
    }

    #[test]
    fn a_list_is_written_in_id_order() {
        assert_eq!(parse("ws,sms").unwrap().to_string(), "sms,ws");
    }

    #[test]
    fn an_empty_visibility_is_refused() {
        assert!(matches!(
            parse(""),
            Err(ModelError::InvalidVisibility { .. })
        ));
    }

    #[test]
    fn an_empty_item_is_refused() {
        assert!(matches!(
            parse("sms,,ws"),
            Err(ModelError::InvalidVisibility { .. })
        ));
    }

    #[test]
    fn a_trailing_comma_is_refused() {
        assert!(matches!(
            parse("sms,"),
            Err(ModelError::InvalidVisibility { .. })
        ));
    }

    #[test]
    fn a_keyword_inside_a_list_is_refused() {
        assert!(matches!(
            parse("all,sms"),
            Err(ModelError::InvalidVisibility { .. })
        ));
    }

    #[test]
    fn the_default_visibility_is_all() {
        assert_eq!(Visibility::default(), Visibility::All);
    }

    #[test]
    fn an_empty_channel_set_is_refused() {
        assert!(ChannelSet::new([]).is_err());
    }

    #[test]
    fn a_visibility_serializes_as_its_rfc_string() {
        let visibility = parse("ws,sms").unwrap();
        assert_eq!(serde_json::to_value(&visibility).unwrap(), json!("sms,ws"));
    }

    #[test]
    fn deserialization_applies_the_grammar() {
        assert!(serde_json::from_value::<Visibility>(json!("sms,")).is_err());
    }
}
