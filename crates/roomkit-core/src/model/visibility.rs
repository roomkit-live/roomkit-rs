//! Visibility: which channels may see what a source writes (RFC §7.3).

use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{ChannelCategory, ChannelId, ModelError};

/// Which channels may see an event (RFC §7.3).
///
/// Written as the RFC writes it: `all`, `none`, `internal`, `transport`,
/// `intelligence`, or a comma-separated list of channel ids. Parsing follows
/// the grammar decided in gap G-002: whitespace around an id is ignored, an
/// empty visibility or an empty item is refused, and a duplicated id counts
/// once.
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
    /// No channel, as `none`, on a record the framework wrote (RFC §6.4, gap
    /// G-002).
    Internal,
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

    /// Whether the channel `channel`, of `category`, may see an event of
    /// this visibility (RFC §7.3).
    pub fn admits(&self, channel: &ChannelId, category: ChannelCategory) -> bool {
        match self {
            Self::All => true,
            Self::None | Self::Internal => false,
            Self::Transport => category == ChannelCategory::Transport,
            Self::Intelligence => category == ChannelCategory::Intelligence,
            Self::Channels(channels) => channels.contains(channel),
        }
    }

    /// The visibility admitting exactly the channels both admit (gap G-009).
    ///
    /// `all` is neutral; `internal` absorbs, keeping the mark of a framework
    /// record, then `none` absorbs; `transport` and `intelligence` exclude
    /// each other; two lists intersect. A category intersected with a
    /// list keeps the listed channels of that category, which
    /// `category_of` tells; a channel it does not know matches nothing.
    #[must_use]
    pub fn intersect(
        &self,
        other: &Self,
        category_of: impl Fn(&ChannelId) -> Option<ChannelCategory>,
    ) -> Self {
        match (self, other) {
            (Self::All, kept) | (kept, Self::All) => kept.clone(),
            (Self::Internal, _) | (_, Self::Internal) => Self::Internal,
            (Self::None, _) | (_, Self::None) => Self::None,
            (Self::Transport, Self::Transport) => Self::Transport,
            (Self::Intelligence, Self::Intelligence) => Self::Intelligence,
            (Self::Transport, Self::Intelligence) | (Self::Intelligence, Self::Transport) => {
                Self::None
            }
            (Self::Transport, Self::Channels(listed))
            | (Self::Channels(listed), Self::Transport) => {
                Self::listing(listed.of_category(ChannelCategory::Transport, &category_of))
            }
            (Self::Intelligence, Self::Channels(listed))
            | (Self::Channels(listed), Self::Intelligence) => {
                Self::listing(listed.of_category(ChannelCategory::Intelligence, &category_of))
            }
            (Self::Channels(first), Self::Channels(second)) => {
                Self::listing(first.0.intersection(&second.0).cloned())
            }
        }
    }

    /// The list form of `ids`, or `none` when there is no id.
    fn listing(ids: impl IntoIterator<Item = ChannelId>) -> Self {
        ChannelSet::new(ids).map_or(Self::None, Self::Channels)
    }
}

impl fmt::Display for Visibility {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::All => formatter.write_str("all"),
            Self::None => formatter.write_str("none"),
            Self::Internal => formatter.write_str("internal"),
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
            "internal" => Ok(Self::Internal),
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

    /// The channels of the set that `category_of` puts in `category`.
    fn of_category(
        &self,
        category: ChannelCategory,
        category_of: impl Fn(&ChannelId) -> Option<ChannelCategory>,
    ) -> Vec<ChannelId> {
        self.0
            .iter()
            .filter(|id| category_of(id) == Some(category))
            .cloned()
            .collect()
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

    /// The categories of a room binding `sms` and `ws` (transports) and `ai`.
    fn category_of(id: &ChannelId) -> Option<ChannelCategory> {
        match id.as_str() {
            "sms" | "ws" => Some(ChannelCategory::Transport),
            "ai" => Some(ChannelCategory::Intelligence),
            _ => None,
        }
    }

    fn intersect(first: &str, second: &str) -> String {
        parse(first)
            .unwrap()
            .intersect(&parse(second).unwrap(), category_of)
            .to_string()
    }

    #[test]
    fn transport_visibility_admits_a_transport_channel() {
        assert!(Visibility::Transport.admits(&channel("sms"), ChannelCategory::Transport));
    }

    #[test]
    fn transport_visibility_does_not_admit_an_intelligence_channel() {
        assert!(!Visibility::Transport.admits(&channel("ai"), ChannelCategory::Intelligence));
    }

    #[test]
    fn a_list_admits_only_its_channels() {
        let visibility = parse("ws").unwrap();
        assert!(!visibility.admits(&channel("sms"), ChannelCategory::Transport));
    }

    #[test]
    fn none_admits_no_channel() {
        assert!(!Visibility::None.admits(&channel("ws"), ChannelCategory::Transport));
    }

    #[test]
    fn internal_admits_no_channel() {
        assert!(!Visibility::Internal.admits(&channel("ai"), ChannelCategory::Intelligence));
    }

    #[test]
    fn internal_absorbs_an_intersection_and_keeps_its_mark() {
        assert_eq!(
            [intersect("internal", "all"), intersect("none", "internal")],
            ["internal", "internal"]
        );
    }

    #[test]
    fn all_is_neutral_in_an_intersection() {
        assert_eq!(intersect("all", "sms,ai"), "ai,sms");
    }

    #[test]
    fn none_absorbs_an_intersection() {
        assert_eq!(intersect("none", "all"), "none");
    }

    #[test]
    fn transport_and_intelligence_intersect_to_none() {
        assert_eq!(intersect("transport", "intelligence"), "none");
    }

    #[test]
    fn a_category_keeps_the_listed_channels_of_that_category() {
        assert_eq!(intersect("transport", "sms,ai"), "sms");
    }

    #[test]
    fn a_category_drops_a_listed_channel_the_room_does_not_bind() {
        assert_eq!(intersect("intelligence", "ai,gone"), "ai");
    }

    #[test]
    fn two_lists_intersect() {
        assert_eq!(intersect("sms,ws", "ws,ai"), "ws");
    }

    #[test]
    fn disjoint_lists_intersect_to_none() {
        assert_eq!(intersect("sms", "ai"), "none");
    }

    #[test]
    fn each_keyword_parses_to_its_variant() {
        let parsed: Vec<_> = ["all", "none", "internal", "transport", "intelligence"]
            .into_iter()
            .map(|keyword| parse(keyword).unwrap())
            .collect();
        assert_eq!(
            parsed,
            [
                Visibility::All,
                Visibility::None,
                Visibility::Internal,
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

    /// Properties over every visibility of a room binding `sms`, `ws`, `crm`
    /// (transports), `ai` and `bot` (intelligence channels).
    mod properties {
        use proptest::prelude::*;

        use super::*;

        /// The room's channels, with their categories.
        const ROOM: [(&str, ChannelCategory); 5] = [
            ("sms", ChannelCategory::Transport),
            ("ws", ChannelCategory::Transport),
            ("crm", ChannelCategory::Transport),
            ("ai", ChannelCategory::Intelligence),
            ("bot", ChannelCategory::Intelligence),
        ];

        fn category_of(id: &ChannelId) -> Option<ChannelCategory> {
            ROOM.iter()
                .find(|(room, _)| *room == id.as_str())
                .map(|(_, category)| *category)
        }

        /// Any visibility: a keyword, or a list of the room's channels.
        fn visibility() -> impl Strategy<Value = Visibility> {
            let listed =
                proptest::sample::subsequence(ROOM.to_vec(), 1..=ROOM.len()).prop_map(|channels| {
                    Visibility::channels(channels.into_iter().map(|(id, _)| channel(id))).unwrap()
                });
            prop_oneof![
                Just(Visibility::All),
                Just(Visibility::None),
                Just(Visibility::Internal),
                Just(Visibility::Transport),
                Just(Visibility::Intelligence),
                listed,
            ]
        }

        proptest! {
            // A failing case is printed, never written to a file a later run
            // would replay.
            #![proptest_config(ProptestConfig {
                failure_persistence: None,
                ..ProptestConfig::default()
            })]

            #[test]
            fn an_intersection_does_not_depend_on_the_order(
                first in visibility(),
                second in visibility(),
            ) {
                prop_assert_eq!(
                    first.intersect(&second, category_of),
                    second.intersect(&first, category_of)
                );
            }

            /// Gap G-009: the intersection admits exactly the channels both
            /// admit.
            #[test]
            fn an_intersection_admits_what_both_admit(
                first in visibility(),
                second in visibility(),
            ) {
                let both = first.intersect(&second, category_of);
                for (id, category) in ROOM {
                    let id = channel(id);
                    prop_assert_eq!(
                        both.admits(&id, category),
                        first.admits(&id, category) && second.admits(&id, category),
                        "{} on {} and {}", id, first, second
                    );
                }
            }

            /// RFC §7.3: a visibility reads back from its text form.
            #[test]
            fn a_visibility_reads_back_from_its_text(visibility in visibility()) {
                prop_assert_eq!(parse(&visibility.to_string()).unwrap(), visibility);
            }
        }
    }
}
