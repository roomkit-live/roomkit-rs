//! A host executor witness: entry stays visible even if its file read fails.
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use roomkit::model::Metadata;
use roomkit_ai::{AiChannelOptions, ScriptExecutor, ScriptResult, Skill, ToolError};
use serde_json::{Value, json};

use crate::run::lock;

pub(super) type Executors = Mutex<BTreeMap<String, Arc<ScriptedExecutor>>>;

pub(super) struct ScriptedExecutor {
    result: ScriptResult,
    failure: Option<ToolError>,
    calls: Mutex<Vec<Value>>,
    closes: AtomicUsize,
}

impl ScriptedExecutor {
    fn new(spec: &Value) -> Result<Self, String> {
        spec.as_object().ok_or("script executor: not an object")?;
        let result = spec
            .get("result")
            .cloned()
            .unwrap_or_else(|| json!({"exit_code": 0}));
        let failure = spec
            .get("raises")
            .map(|failure| {
                let kind = failure["kind"]
                    .as_str()
                    .ok_or("script executor error kind: not text")?;
                let message = failure["message"]
                    .as_str()
                    .ok_or("script executor error message: not text")?;
                Ok::<_, String>(ToolError::raised_as(kind, message))
            })
            .transpose()?;
        Ok(Self {
            result: serde_json::from_value(result).map_err(|error| error.to_string())?,
            failure,
            calls: Mutex::default(),
            closes: AtomicUsize::new(0),
        })
    }

    fn snapshot(&self) -> Value {
        json!({"calls": lock(&self.calls).clone(), "closes": self.closes.load(Ordering::SeqCst)})
    }
}

impl ScriptExecutor for ScriptedExecutor {
    fn execute<'a>(
        &'a self,
        skill: &'a Skill,
        script_name: &'a str,
        arguments: Option<&'a Metadata>,
    ) -> Pin<Box<dyn Future<Output = Result<ScriptResult, ToolError>> + Send + 'a>> {
        Box::pin(async move {
            let at = {
                let mut calls = lock(&self.calls);
                let at = calls.len();
                calls.push(json!({"skill_name": skill.name(), "script_name": script_name, "arguments": arguments}));
                at
            };
            let (skill, name) = (skill.clone(), script_name.to_owned());
            let content = tokio::task::spawn_blocking(move || {
                let path = skill
                    .resolve_script(&name)
                    .map_err(|error| ToolError::raised(&error))?;
                std::fs::read_to_string(path).map_err(|error| ToolError::raised(&error))
            })
            .await
            .map_err(|error| ToolError::raised(&error))??;
            lock(&self.calls).get_mut(at).unwrap()["contents"] = json!(content);
            match &self.failure {
                Some(error) => Err(error.clone()),
                None => Ok(self.result.clone()),
            }
        })
    }

    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {
            self.closes.fetch_add(1, Ordering::SeqCst);
        })
    }
}

impl super::SkillDirectories {
    pub(crate) fn script_executor(
        &self,
        spec: &Value,
        options: &mut AiChannelOptions,
    ) -> Result<(), String> {
        let Some(executor) = spec.get("script_executor") else {
            return Ok(());
        };
        let executor = Arc::new(ScriptedExecutor::new(executor)?);
        options.script_executor = Some(Arc::<ScriptedExecutor>::clone(&executor));
        lock(&self.executors).insert(spec["id"].as_str().unwrap_or_default().to_owned(), executor);
        Ok(())
    }

    pub(crate) fn script_snapshots(&self) -> BTreeMap<String, Value> {
        lock(&self.executors)
            .iter()
            .map(|(id, executor)| (id.clone(), executor.snapshot()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shape::unknown_members;

    // Python: tests/conformance/test_script_executor_fixture.py::test_entry_is_recorded_before_the_fixture_checks_the_path
    #[tokio::test]
    async fn entry_is_recorded_before_the_fixture_checks_the_path() {
        let directories = super::super::SkillDirectories::default();
        let registry = directories.registry(
            &json!([{"name": "local", "description": "Scripts", "scripts": {"build.sh": "source text"}}]),
            &Value::Null,
        ).unwrap();
        let roomkit_ai::LoadedSkill::Local(skill) = registry.get_skill("local").unwrap().unwrap()
        else {
            panic!("expected local fixture");
        };
        let executor = ScriptedExecutor::new(&json!({})).unwrap();
        assert!(executor.execute(&skill, "../build.sh", None).await.is_err());
        assert_eq!(
            executor.snapshot()["calls"],
            json!([
                {"skill_name": "local", "script_name": "../build.sh", "arguments": null}
            ])
        );
        let result = executor
            .execute(&skill, "build.sh", Some(&Metadata::new()))
            .await
            .unwrap();
        assert_eq!(result, ScriptResult::new(0));
        assert_eq!(executor.snapshot()["calls"][1]["contents"], "source text");
        assert_eq!(executor.snapshot()["calls"][1]["arguments"], json!({}));
        executor.close().await;
        assert_eq!(executor.snapshot()["closes"], 1);
    }

    // Python: tests/conformance/test_script_executor_fixture.py::test_executor_controls_do_not_silently_ignore_unknown_members
    #[test]
    fn executor_controls_do_not_silently_ignore_unknown_members() {
        for (executor, member) in [
            (Value::Null, "script_executor: not an object"),
            (
                json!({"result": null}),
                "script_executor.result: not an object",
            ),
            (
                json!({"raises": null}),
                "script_executor.raises: not an object",
            ),
            (json!({"reslut": {}}), "reslut"),
            (
                json!({"result": {"exit_code": 0, "sucess": false}}),
                "sucess",
            ),
            (
                json!({"raises": {"kind": "Error", "mesage": "failure"}}),
                "mesage",
            ),
        ] {
            let scenario = json!({"setup": {"channels": [{
                "id": "agent", "type": "ai", "script_executor": executor
            }]}});
            assert!(
                unknown_members(&scenario)
                    .iter()
                    .any(|failure| failure.contains(member))
            );
        }
    }
}
