//! A timeline read and the count that stands for it (RFC §14.1, "Reading a
//! timeline", "A timeline read serves what the room received", "A count
//! follows the rule of the page it stands for"), with the Python
//! implementation's arguments.

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelSet, ChannelType, EventStatus, EventType, RoomEvent, Timestamp, Visibility,
};
use roomkit::store::{GetEventCountOptions, ListEventsOptions};
use roomkit::{ConversationStore, EventFilter, StoreError};

use super::{channel, event, event_id, event_stored, participant_id, room_id, with_events};
use crate::Store;

/// The indexes of `room_1`'s page `query` reads.
async fn page(store: &Store, query: ListEventsOptions) -> Vec<u64> {
    store
        .list_events(&room_id(), query)
        .await
        .unwrap()
        .iter()
        .map(|event| event.index)
        .collect()
}

/// RFC §14.1: the window is chosen from its end, the page always rendered
/// ascending; `offset` counts back from the newest end under
/// `newest_first`, and a cursor ignores both.
#[tokio::test]
async fn each_window_is_read_in_ascending_index_order() {
    let store = with_events((0..6).map(event).collect()).await;
    let query = ListEventsOptions::default();
    let cases = [
        (
            "the default, the oldest 50",
            query.clone(),
            vec![0, 1, 2, 3, 4, 5],
        ),
        (
            "an offset and a limit",
            ListEventsOptions {
                offset: 1,
                limit: 2,
                ..query.clone()
            },
            vec![1, 2],
        ),
        (
            "the newest",
            ListEventsOptions {
                newest_first: true,
                limit: 2,
                ..query.clone()
            },
            vec![4, 5],
        ),
        (
            "the newest, an offset counted back",
            ListEventsOptions {
                newest_first: true,
                offset: 1,
                limit: 2,
                ..query.clone()
            },
            vec![3, 4],
        ),
        (
            "an offset past the newest end",
            ListEventsOptions {
                newest_first: true,
                offset: 9,
                limit: 2,
                ..query.clone()
            },
            vec![],
        ),
        (
            "after a cursor, the nearest",
            ListEventsOptions {
                after_index: Some(1),
                limit: 2,
                ..query.clone()
            },
            vec![2, 3],
        ),
        (
            "before a cursor, the nearest",
            ListEventsOptions {
                before_index: Some(4),
                limit: 2,
                ..query.clone()
            },
            vec![2, 3],
        ),
        (
            "after a cursor, offset and newest_first ignored",
            ListEventsOptions {
                after_index: Some(1),
                offset: 3,
                newest_first: true,
                limit: 2,
                ..query.clone()
            },
            vec![2, 3],
        ),
        (
            "before a cursor, offset ignored",
            ListEventsOptions {
                before_index: Some(4),
                offset: 3,
                limit: 2,
                ..query.clone()
            },
            vec![2, 3],
        ),
        (
            "a limit of none",
            ListEventsOptions { limit: 0, ..query },
            vec![],
        ),
    ];
    for (case, query, expected) in cases {
        assert_eq!(page(&store, query).await, expected, "{case}");
    }
}

/// A cursor as high as an index can be bounds the page as any other: no
/// index is above it, and every one below.
#[tokio::test]
async fn a_cursor_past_every_index_bounds_the_page() {
    let store = with_events((0..6).map(event).collect()).await;
    let after = ListEventsOptions {
        after_index: Some(u64::MAX),
        ..ListEventsOptions::default()
    };
    let before = ListEventsOptions {
        before_index: Some(u64::MAX),
        limit: 2,
        ..ListEventsOptions::default()
    };
    assert_eq!(
        (page(&store, after).await, page(&store, before).await),
        (vec![], vec![4, 5])
    );
}

/// RFC §14.1: `after_index` and `before_index` exclude each other.
#[tokio::test]
async fn both_cursors_are_refused() {
    let store = with_events((0..3).map(event).collect()).await;
    let both = ListEventsOptions {
        after_index: Some(0),
        before_index: Some(2),
        ..ListEventsOptions::default()
    };
    assert_eq!(
        store.list_events(&room_id(), both).await,
        Err(StoreError::Value(
            "after_index and before_index are mutually exclusive".to_owned()
        ))
    );
}

/// RFC §14.1: the refused rows are left out before the page is cut, so a
/// page of `limit` received events is full, and served on request.
#[tokio::test]
async fn refused_events_are_left_out_before_the_page_is_cut() {
    let events = (0..6)
        .map(|index| match index {
            1 | 4 => event_stored(index, EventStatus::Blocked),
            _ => event(index),
        })
        .collect();
    let store = with_events(events).await;
    let every_row = Some(EventFilter {
        include_blocked: true,
        ..EventFilter::default()
    });
    let pages = [
        page(
            &store,
            ListEventsOptions {
                limit: 2,
                ..ListEventsOptions::default()
            },
        )
        .await,
        page(
            &store,
            ListEventsOptions {
                newest_first: true,
                limit: 2,
                ..ListEventsOptions::default()
            },
        )
        .await,
        page(
            &store,
            ListEventsOptions {
                before_index: Some(5),
                limit: 2,
                ..ListEventsOptions::default()
            },
        )
        .await,
        page(
            &store,
            ListEventsOptions {
                newest_first: true,
                limit: 2,
                event_filter: every_row,
                ..ListEventsOptions::default()
            },
        )
        .await,
    ];
    assert_eq!(pages, [vec![0, 2], vec![3, 5], vec![2, 3], vec![4, 5]]);
}

/// The events the filter tests read: each differs from `evt_0` by one
/// member a criterion reads, `evt_0` a message from SMS at the epoch.
fn varied() -> Vec<RoomEvent> {
    let at = |seconds: i64| Timestamp::from_unix_millis(seconds * 1000).unwrap();
    (0..8)
        .map(|index| {
            let mut varied = event(index);
            varied.created_at = at(i64::try_from(index).unwrap());
            match index {
                1 => varied.r#type = EventType::System,
                2 => {
                    varied.source.channel_id = channel("ws");
                    varied.source.channel_type = ChannelType::WEBSOCKET;
                }
                3 => varied.source.participant_id = Some(participant_id("alice")),
                4 => varied.correlation_id = Some("turn_1".to_owned()),
                5 => varied.responds_to = Some(event_id("evt_0")),
                6 => varied.parent_event_id = Some(event_id("evt_0")),
                7 => varied.visibility = Visibility::Transport,
                _ => {}
            }
            varied
        })
        .collect()
}

/// The filter `set` makes of the default one.
fn only(set: impl FnOnce(&mut EventFilter)) -> EventFilter {
    let mut filter = EventFilter::default();
    set(&mut filter);
    filter
}

/// Each criterion of a filter, and the events of [`varied`] it selects.
fn criteria() -> Vec<(&'static str, EventFilter, Vec<u64>)> {
    let second = |seconds: i64| Some(Timestamp::from_unix_millis(seconds * 1000).unwrap());
    vec![
        (
            "of a type",
            only(|f| f.event_types = Some(vec![EventType::System])),
            vec![1],
        ),
        (
            "no types",
            only(|f| f.event_types = Some(Vec::new())),
            vec![],
        ),
        (
            "a type excluded, even when listed",
            only(|f| {
                f.event_types = Some(vec![EventType::Message, EventType::System]);
                f.exclude_types = Some(vec![EventType::Message]);
            }),
            vec![1],
        ),
        (
            "from a channel",
            only(|f| f.source_channel_id = Some(channel("ws"))),
            vec![2],
        ),
        (
            "from a channel type",
            only(|f| f.source_channel_type = Some(ChannelType::WEBSOCKET)),
            vec![2],
        ),
        (
            "of a participant",
            only(|f| f.participant_id = Some(participant_id("alice"))),
            vec![3],
        ),
        (
            "of a correlation group",
            only(|f| f.correlation_id = Some("turn_1".to_owned())),
            vec![4],
        ),
        (
            "answering an event",
            only(|f| f.responds_to = Some(event_id("evt_0"))),
            vec![5],
        ),
        (
            "replying in a thread",
            only(|f| f.parent_event_id = Some(event_id("evt_0"))),
            vec![6],
        ),
        (
            "outside any thread",
            only(|f| f.top_level_only = true),
            vec![0, 1, 2, 3, 4, 5, 7],
        ),
        (
            "of a visibility",
            only(|f| f.visibility = Some(Visibility::Transport)),
            vec![7],
        ),
        (
            "within a time range, both ends excluded",
            only(|f| {
                f.after_time = second(1);
                f.before_time = second(5);
            }),
            vec![2, 3, 4],
        ),
    ]
}

/// RFC §14.1: each criterion of a filter applies before the page is cut,
/// and a count with the filter counts the rows the page serves.
#[tokio::test]
async fn a_filter_s_criteria_select_the_page_and_its_count() {
    let store = with_events(varied()).await;
    for (case, filter, expected) in criteria() {
        let query = ListEventsOptions {
            event_filter: Some(filter.clone()),
            ..ListEventsOptions::default()
        };
        let counted = store
            .get_event_count(
                &room_id(),
                GetEventCountOptions {
                    event_filter: Some(filter),
                },
            )
            .await;
        let expected_count = u64::try_from(expected.len()).unwrap();
        assert_eq!(
            (page(&store, query).await, counted),
            (expected, Ok(expected_count)),
            "{case}"
        );
    }
}

/// The Python implementation's `visibility_filter` is a value compared as
/// it is, which the filter's own visibility takes precedence over.
#[tokio::test]
async fn a_visibility_filter_is_an_exact_value_the_filter_s_own_overrides() {
    let store = with_events(varied()).await;
    let transport = ListEventsOptions {
        visibility_filter: Some(Visibility::Transport),
        ..ListEventsOptions::default()
    };
    let overridden = ListEventsOptions {
        event_filter: Some(EventFilter {
            visibility: Some(Visibility::All),
            ..EventFilter::default()
        }),
        ..transport.clone()
    };
    let listed = ListEventsOptions {
        visibility_filter: Some(
            ChannelSet::new([channel("sms")])
                .map(Visibility::Channels)
                .unwrap(),
        ),
        ..ListEventsOptions::default()
    };
    assert_eq!(
        (
            page(&store, transport).await,
            page(&store, overridden).await.len(),
            page(&store, listed).await
        ),
        (vec![7], 7, vec![])
    );
}

/// The Python implementation's model refuses a filter that admits nothing
/// by construction; a read and a count refuse it alike.
#[tokio::test]
async fn a_filter_that_admits_nothing_by_construction_is_refused() {
    let store = with_events(varied()).await;
    let instant = Timestamp::from_unix_millis(1000).unwrap();
    let refused = [
        (
            EventFilter {
                after_time: Some(instant),
                before_time: Some(instant),
                ..EventFilter::default()
            },
            "after_time must be before before_time",
        ),
        (
            EventFilter {
                top_level_only: true,
                parent_event_id: Some(event_id("evt_0")),
                ..EventFilter::default()
            },
            "top_level_only and parent_event_id are mutually exclusive",
        ),
    ];
    for (filter, reason) in refused {
        let query = ListEventsOptions {
            event_filter: Some(filter.clone()),
            ..ListEventsOptions::default()
        };
        assert_eq!(
            (
                store.list_events(&room_id(), query).await.map(|_| ()),
                store
                    .get_event_count(
                        &room_id(),
                        GetEventCountOptions {
                            event_filter: Some(filter)
                        }
                    )
                    .await
            ),
            (
                Err(StoreError::Value(reason.to_owned())),
                Err(StoreError::Value(reason.to_owned()))
            )
        );
    }
}
