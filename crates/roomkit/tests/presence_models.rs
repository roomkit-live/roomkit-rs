//! Python: `tests/test_local_presence_models.py`, public data and coverage.

use roomkit::RoomKitError;
use roomkit::turns::{AgentPresence, ExecutorInfo, ExecutorInfoOptions};
use serde_json::json;
use std::collections::BTreeSet;

fn names(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|item| (*item).to_owned()).collect()
}

#[test]
fn executor_info_defaults_are_explicit_and_not_an_every_channel_declaration() {
    let info = ExecutorInfo::new("worker", ExecutorInfoOptions::default());
    assert_eq!(
        serde_json::to_value(&info).unwrap(),
        json!({
            "executor_id": "worker", "node": null, "hosts": null, "organizations": null,
            "room_hooks": [], "last_beat": null, "online": true
        })
    );
    assert_eq!(
        serde_json::from_value::<ExecutorInfo>(json!({"executor_id":"worker"})).unwrap(),
        info
    );
    assert!(!info.covers("helper", &[], None).unwrap());
    assert_eq!(
        serde_json::to_value(AgentPresence::new("", false, Vec::new())).unwrap(),
        json!({"channel_id":"", "online":false, "executors":[]})
    );
}

#[test]
fn public_executor_coverage_respects_channels_and_organizations() {
    for (hosts, organizations, channel, organization, expected) in [
        (&["helper"][..], None, "helper", None, true),
        (&["helper"][..], None, "other", None, false),
        (&["acme-*"][..], None, "acme-helper", None, true),
        (&["acme-*"][..], None, "acme", None, false),
        (&["*"][..], None, "any", None, true),
        (&["*"][..], None, "any", Some("tenant-a"), false),
        (
            &["*"][..],
            Some(&["tenant-a"][..]),
            "any",
            Some("tenant-a"),
            true,
        ),
        (&["*"][..], Some(&["tenant-a"][..]), "any", None, false),
        (&["*"][..], Some(&["*"][..]), "any", None, true),
        (&["*"][..], Some(&["*"][..]), "any", Some("tenant-b"), true),
        (&[][..], Some(&["*"][..]), "any", None, false),
    ] {
        let info = ExecutorInfo::new(
            "worker",
            ExecutorInfoOptions {
                hosts: Some(names(hosts)),
                organizations: organizations.map(names),
                ..Default::default()
            },
        );
        assert_eq!(info.covers(channel, &[], organization).unwrap(), expected);
    }
}

#[test]
fn coverage_gates_online_hooks_and_organization_before_validating_host_globs() {
    let bad = ExecutorInfo::new(
        "worker",
        ExecutorInfoOptions {
            hosts: Some(names(&["a*b"])),
            ..Default::default()
        },
    );
    let mut offline = bad.clone();
    offline.online = false;
    assert!(!offline.covers("ab", &[], None).unwrap());
    assert!(!bad.covers("ab", &["missing".into()], None).unwrap());
    assert!(!bad.covers("ab", &[], Some("other")).unwrap());
    assert_eq!(bad.covers("ab", &[], None), Err(RoomKitError::Value("hosts entry 'a*b': a '*' ends an entry to declare a prefix ('org-*'), and stands nowhere else".into())));
    let ready = ExecutorInfo::new(
        "worker",
        ExecutorInfoOptions {
            hosts: Some(names(&["*"])),
            room_hooks: names(&["audit", "trace"]),
            ..Default::default()
        },
    );
    assert!(ready.covers("any", &["audit".into()], None).unwrap());
    assert!(!ready.covers("any", &["unknown".into()], None).unwrap());
}
