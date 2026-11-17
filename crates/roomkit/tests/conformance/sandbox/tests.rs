use super::*;

#[tokio::test]
async fn fixture_records_entry_before_failure_and_preserves_optional_arguments() {
    let fixture=ScriptedSandbox::new(&json!({"tools":[{"name":"not_a_sandbox_tool"}],"raises":{"kind":"ConnectionError","message":"unavailable"}})).unwrap();
    assert_eq!(
        fixture.tool_definitions().unwrap(),
        [json!({"name":"not_a_sandbox_tool"})
            .as_object()
            .unwrap()
            .clone()]
    );
    assert_eq!(
        fixture.execute("unfiltered", None).await.unwrap_err(),
        ToolError::raised_as("ConnectionError", "unavailable")
    );
    assert_eq!(
        fixture.snapshot()["calls"],
        json!([{"command":"unfiltered","arguments":null}])
    );
    fixture.close().await;
    assert_eq!(fixture.snapshot()["closes"], 1);
    let fixture = ScriptedSandbox::new(&json!({})).unwrap();
    assert_eq!(
        fixture
            .execute("read", Some(&Metadata::new()))
            .await
            .unwrap(),
        SandboxResult::new(0)
    );
    assert_eq!(
        fixture.snapshot()["calls"],
        json!([{"command":"read","arguments":{}}])
    );
}

#[test]
fn sandbox_controls_refuse_invalid_or_unknown_members() {
    for spec in [
        Value::Null,
        json!({"toools":[]}),
        json!({"tools":null}),
        json!({"tools":[null]}),
        json!({"tools":[{}]}),
        json!({"tools":[{"name":1}]}),
        json!({"tools":[{"name":"sandbox_x","parameters":null}]}),
        json!({"result":null}),
        json!({"result":{}}),
        json!({"result":{"exit_code":true}}),
        json!({"result":{"exit_code":0,"success":false}}),
        json!({"result":{"exit_code":0,"metadata":null}}),
        json!({"raises":null}),
        json!({"raises":{"kind":"Error"}}),
        json!({"raises":{"kind":1,"message":"x"}}),
    ] {
        let scenario = json!({"setup":{"channels":[{"id":"agent","type":"ai","sandbox":spec}]}});
        assert!(
            !crate::shape::unknown_members(&scenario).is_empty(),
            "{spec}"
        );
    }
    assert!(ScriptedSandbox::new(&Value::Null).is_err());
}
