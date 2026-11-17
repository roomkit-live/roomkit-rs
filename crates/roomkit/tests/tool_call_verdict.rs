//! Standalone verdict construction and the fields a caller reads back.

use roomkit::ToolCallVerdict;
use roomkit::model::ToolCallVerdict as ModelVerdict;
use roomkit::model::tool_verdict::{VerdictList, VerdictObject, VerdictResult, VerdictValue};
use serde_json::json;

#[test]
fn both_imports_expose_the_same_frozen_verdict() {
    let verdict: ModelVerdict = ToolCallVerdict::new(
        Some(VerdictResult::Text("refused".into())),
        true,
        true,
        None,
        Some("detail".into()),
    );
    assert!(matches!(verdict.result(), Some(VerdictResult::Text(text)) if text == "refused"));
    assert!(verdict.blocked());
    assert!(verdict.replaces_structured());
    assert!(verdict.structured_content().is_none());
    assert_eq!(verdict.error_detail(), Some("detail"));
}

#[test]
fn result_and_structured_copy_keep_their_live_objects() {
    let object = VerdictObject::default();
    object.insert("original", json!(1).into());
    let parts = VerdictList::new(vec![VerdictValue::Object(object.clone())]);
    let verdict = ToolCallVerdict::new(
        Some(VerdictResult::Parts(parts.clone())),
        false,
        false,
        Some(object.clone()),
        None,
    );
    parts.update(|items| items.push(json!("late").into()));
    object.insert("second", json!(2).into());
    object.insert("original", json!(3).into());
    let Some(VerdictResult::Parts(observed)) = verdict.result() else {
        panic!("expected a parts list")
    };
    let values = observed.snapshot();
    assert!(matches!(&values[1], VerdictValue::Json(v) if v == "late"));
    let VerdictValue::Object(nested) = &values[0] else {
        panic!("expected the shared object")
    };
    assert!(matches!(nested.get("original"), Some(VerdictValue::Json(v)) if v == 3));
    let kept = verdict.structured_content().unwrap().snapshot();
    assert_eq!(
        kept.iter().map(|(key, _)| key.as_str()).collect::<Vec<_>>(),
        ["original", "second"]
    );
}

#[test]
fn nested_lists_and_native_parts_keep_their_identity() {
    use std::sync::Arc;

    let native = Arc::new(String::from("native part"));
    let nested = VerdictList::default();
    let object = VerdictObject::default();
    object.insert("items", VerdictValue::List(nested.clone()));
    let verdict = ToolCallVerdict::new(None, false, true, Some(object.clone()), None);
    nested.update(|items| items.push(VerdictValue::Native(Arc::<String>::clone(&native))));
    let structured = verdict.structured_content().unwrap();
    let Some(VerdictValue::List(items)) = structured.get("items") else {
        panic!("missing list");
    };
    let current = items.snapshot();
    let VerdictValue::Native(value) = &current[0] else {
        panic!("missing native part")
    };
    assert!(Arc::ptr_eq(
        &Arc::clone(value).downcast::<String>().unwrap(),
        &native
    ));
    assert!(object.remove("items").is_some());
    assert!(structured.get("items").is_none());
}
