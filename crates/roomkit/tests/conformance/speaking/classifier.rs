//! The implementation's classifier and answer-only policies over a scripted
//! classifier (roomkit-specs `conformance/README.md`, `speak_policy`): each
//! call takes the next of `calls`, the last repeating, records its state and
//! questions, holds at its gate, then fails with a classifier error or
//! answers as the public mock answers that call's script.
use std::sync::{Arc, Weak};

use roomkit::ChannelError;
use roomkit_ai::classifiers::{
    Answers, Classifier, ClassifierError, ClassifierFuture, MockClassifier, MockClassifierOptions,
    Question, Questions, Script, SharedState,
};
use roomkit_ai::speaking::{
    AnswerOnly, ClassifierSpeakOverrides, ClassifierSpeakPolicy, ClassifierSpeakPolicyOptions,
    SpeakDecision, SpeakPolicy,
};
use serde_json::{Map, Value, json};

use crate::run::{Run, lock};

/// A scripted classifier whose calls `expect.classifier_calls[id]` reads.
struct Scripted {
    calls: Vec<Value>,
    id: String,
    run: Weak<Run>,
}

impl Classifier for Scripted {
    fn classify<'a>(
        &'a self,
        state: &'a SharedState,
        questions: &'a Questions,
    ) -> ClassifierFuture<'a, Answers> {
        Box::pin(async move {
            let run = self.run.upgrade().ok_or("scripted classifier run ended")?;
            let asked: Map<String, Value> = questions
                .iter()
                .map(|(name, question)| (name, question_record(&question)))
                .collect();
            let count = {
                let mut observed = lock(&run.observed);
                let calls = observed
                    .classifier_calls
                    .entry(self.id.clone())
                    .or_default();
                calls.push(json!({"state": state.snapshot(), "questions": asked}));
                calls.len()
            };
            let entry = &self.calls[count.min(self.calls.len()) - 1];
            if let Some(gate) = entry.get("gate").and_then(Value::as_str) {
                run.gates.hold(gate).await;
            }
            if let Some(message) = entry.get("fail").and_then(Value::as_str) {
                let failed: ChannelError = Box::new(ClassifierError::new(message));
                return Err(failed);
            }
            let found = answer_problems(&entry["answers"], questions);
            if !found.is_empty() {
                for problem in found {
                    run.problem(format!("classifier call {count}: {problem}"));
                }
                let failed: ChannelError = Box::new(ClassifierError::new(
                    "the scripted answers do not fit the questions asked",
                ));
                return Err(failed);
            }
            let script: Script =
                serde_json::from_value(entry["answers"].clone()).map_err(|e| e.to_string())?;
            let mock = MockClassifier::new(MockClassifierOptions {
                answers: vec![script],
                error: None,
            });
            mock.classify(state, questions).await
        })
    }
}

/// What in a call's script does not fit the questions it answers: a question
/// not asked, a choice that is no option of it, a number for a choice or a
/// name for a yes/no question or a score. The mock would fail the call, and
/// the policy's fallback would hide the scenario's mistake.
pub(super) fn answer_problems(script: &Value, questions: &Questions) -> Vec<String> {
    let mut found = Vec::new();
    for (name, value) in script.as_object().into_iter().flatten() {
        match questions.get(name) {
            None => found.push(format!(
                "answers.{name}: no question of that name was asked"
            )),
            Some(Question::Choice(choice)) => {
                if value
                    .as_str()
                    .is_none_or(|option| choice.options.get(option).is_none())
                {
                    found.push(format!("answers.{name}: not an option of the choice asked"));
                }
            }
            Some(_) if !value.is_number() => {
                found.push(format!("answers.{name}: the question asked takes a number"));
            }
            Some(_) => {}
        }
    }
    found
}

/// A question as `expect.classifier_calls` reads it: its members as written.
fn question_record(question: &Question) -> Value {
    serde_json::to_value(question).unwrap_or(Value::Null)
}

/// The scripted classifier `spec` describes, its calls recorded under `id`.
pub(crate) fn scripted(
    spec: &Value,
    id: &str,
    run: &Arc<Run>,
) -> Result<Arc<dyn Classifier>, String> {
    super::shape::classifier(spec)?;
    // The calls exist from the build on: `[]` proves the classifier was not asked.
    lock(&run.observed)
        .classifier_calls
        .entry(id.to_owned())
        .or_default();
    Ok(Arc::new(Scripted {
        calls: spec["calls"].as_array().ok_or("classifier calls")?.clone(),
        id: id.to_owned(),
        run: Arc::downgrade(run),
    }))
}

/// The questions a scenario writes, by name, in its order.
fn questions(spec: &Value) -> Result<Questions, String> {
    spec.as_object()
        .ok_or("questions: expected an object")?
        .iter()
        .map(|(name, question)| {
            serde_json::from_value::<Question>(question.clone())
                .map(|question| (name.clone(), question))
                .map_err(|e| format!("questions.{name}: {e}"))
        })
        .collect()
}

/// The host's composition (README, `speak_policy.composition`): every turn
/// the classifier answered is decided `decided`.
fn composition(decided: SpeakDecision) -> ClassifierSpeakOverrides {
    ClassifierSpeakOverrides {
        decision: Some(Arc::new(move |_, _, _, _| Ok(decided.clone()))),
        ..ClassifierSpeakOverrides::default()
    }
}

/// The implementation's classifier policy `spec` describes, its classifier's
/// calls recorded under `id`.
fn classifier_policy(
    spec: &Value,
    id: &str,
    run: &Arc<Run>,
) -> Result<ClassifierSpeakPolicy, String> {
    let mut options = ClassifierSpeakPolicyOptions::default();
    if let Some(role) = spec.get("agent_role").and_then(Value::as_str) {
        role.clone_into(&mut options.agent_role);
    }
    if let Some(asked) = spec.get("questions") {
        options.questions = Some(questions(asked)?);
    }
    if let Some(languages) = spec.get("languages") {
        options.languages =
            Some(serde_json::from_value(languages.clone()).map_err(|e| e.to_string())?);
    }
    if let Some(history) = spec.get("history").and_then(Value::as_i64) {
        options.history = history;
    }
    if let Some(proactivity) = spec.get("proactivity").and_then(Value::as_f64) {
        options.proactivity = proactivity;
    }
    if let Some(decided) = spec.get("composition") {
        options.overrides = composition(super::decision(&decided["decision"])?);
    }
    let name = spec["agent_name"]
        .as_str()
        .ok_or("speak_policy.agent_name")?;
    ClassifierSpeakPolicy::new(scripted(&spec["classifier"], id, run)?, name, options)
        .map_err(|e| e.to_string())
}

/// The policy `spec` describes: the classifier policy, or the scripted one
/// `scripted` builds, answer-only around it when it says so.
pub(super) fn policy(
    spec: &Value,
    id: &str,
    run: &Arc<Run>,
    scripted: impl FnOnce() -> Result<Arc<dyn SpeakPolicy>, String>,
) -> Result<Arc<dyn SpeakPolicy>, String> {
    let policy: Arc<dyn SpeakPolicy> = if spec.get("classifier").is_some() {
        Arc::new(classifier_policy(spec, id, run)?)
    } else {
        scripted()?
    };
    let Some(only) = spec.get("answer_only") else {
        return Ok(policy);
    };
    let people: Vec<&str> = only["people"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    Ok(Arc::new(
        AnswerOnly::new(policy, people).map_err(|e| e.to_string())?,
    ))
}
