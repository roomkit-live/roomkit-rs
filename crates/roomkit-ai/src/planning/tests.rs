use super::*;

#[test]
fn reads_refresh_the_lru_and_empty_plans_are_retained() {
    let planner = TaskPlanner::default();
    for number in 0..100 {
        planner.store(&format!("r{number}").parse().unwrap(), Vec::new());
    }
    assert_eq!(planner.plan_for(&"r0".parse().unwrap()), Some(Vec::new()));
    planner.store(&"new".parse().unwrap(), Vec::new());
    assert_eq!(planner.plan_for(&"r1".parse().unwrap()), None);
    assert_eq!(planner.plan_for(&"r0".parse().unwrap()), Some(Vec::new()));
    assert_eq!(planner.plan_for(&"new".parse().unwrap()), Some(Vec::new()));
    planner.forget(&"new".parse().unwrap());
    assert_eq!(planner.plan_for(&"new".parse().unwrap()), None);
}

#[test]
fn validation_is_atomic_and_notes_quote_titles_with_known_status_icons() {
    let planner = TaskPlanner::default();
    let room = "r".parse().unwrap();
    let original = tasks::validated(Some(&json!([
        {"title":"Read\n\"this\"", "status":"pending", "extra":"stripped"},
        {"title":"Next", "status":"in_progress"},
        {"title":"Done", "status":"completed"},
        {"title":"Wait", "status":"blocked"}
    ])))
    .unwrap();
    let original: Vec<_> = original.into_iter().map(PlanTaskHandle::new).collect();
    planner.store(&room, original.clone());
    let context = ToolContext::new(room.clone(), "agent".parse().unwrap());
    let invalid =
        serde_json::from_value(json!({"tasks":[{"title":"Valid", "status":"pending"}, null]}))
            .unwrap();
    assert_eq!(
        planner.handle(&invalid, &context),
        ToolResult::Text("{\"error\": \"Invalid plan: task 1 must be an object\"}".into())
    );
    assert_eq!(planner.plan_for(&room), Some(original));
    assert_eq!(
        planner.notes(&room, false),
        vec![
            "## Current Task Plan\n- [ ] “Read 'this'” (pending)\n- [-] “Next” (in_progress)\n- [x] “Done” (completed)\n- [!] “Wait” (blocked)"
        ]
    );
    assert_eq!(planner.notes(&room, true), Vec::<String>::new());
}
