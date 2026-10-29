//! How a chain of SYNC hooks decides on a payload (RFC §9.3, §9.4).
//!
//! The driver runs one hook at a time and reports what it observed; the
//! chain applies the RFC's consequences: the order, a block that stops the
//! chain, a modify that replaces the payload, and an unusable outcome that
//! allows or blocks depending on the hook and the trigger.

use crate::model::{ErrorRecord, HookDecision, HookResult, InjectedEvent, Observation, Task};
use crate::rules::hook_registry::{HookSpec, HookTrigger};

/// What running one hook produced, as the driver observed it.
#[derive(Clone, Debug, PartialEq)]
pub enum HookOutcome<P> {
    /// The hook returned a result.
    Returned(HookResult<P>),
    /// The hook raised.
    Raised(ErrorRecord),
    /// The hook ran past its timeout.
    TimedOut,
    /// The hook returned something unusable: not a hook result, or a modify
    /// with no payload or one not of the trigger's type (RFC §9.3).
    Invalid {
        /// What was unusable.
        what: String,
        /// The tasks the result carried, collected all the same.
        tasks: Vec<Task>,
        /// The observations it carried, collected all the same.
        observations: Vec<Observation>,
    },
}

impl<P> HookOutcome<P> {
    /// An unusable outcome that carried no side effect.
    pub fn invalid(what: impl Into<String>) -> Self {
        Self::Invalid {
            what: what.into(),
            tasks: Vec::new(),
            observations: Vec::new(),
        }
    }
}

/// What went wrong with a hook that produced no usable result (RFC §9.3).
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum HookProblem {
    /// It raised.
    Raised(ErrorRecord),
    /// It ran past its timeout.
    TimedOut,
    /// It returned something unusable.
    Invalid(String),
}

impl HookProblem {
    /// The code a fail-closed block names the problem with (RFC §9.3).
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::Raised(_) => "hook_error",
            Self::TimedOut => "hook_timeout",
            Self::Invalid(_) => "hook_invalid_result",
        }
    }
}

/// A hook that produced no usable result: the operation logs it and emits
/// the framework event it calls for (`hook_error`, `hook_timeout`).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct HookIncident {
    /// The hook's name.
    pub hook: String,
    /// What went wrong.
    pub problem: HookProblem,
    /// Whether the problem blocked the payload (the hook or its trigger fails
    /// closed) rather than letting it through.
    pub blocked: bool,
}

/// What the chain decided on its payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ChainDecision {
    /// The payload goes on.
    Allowed,
    /// A hook stopped it.
    Blocked {
        /// The hook that blocked: the RFC's `blocked_by`.
        by: String,
        /// Why: the hook's own reason, or the code of a fail-closed problem
        /// (`hook_timeout:<name>`, `hook_error:<name>`,
        /// `hook_invalid_result:<name>`).
        reason: Option<String>,
    },
}

/// Everything a finished chain produced.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ChainVerdict<P> {
    /// Allowed or blocked.
    pub decision: ChainDecision,
    /// The payload as the last modify before the end or the block left it:
    /// what goes on when allowed, what is stored when blocked.
    pub payload: P,
    /// The events the hooks injected, in hook order (gap G-017).
    pub injected_events: Vec<InjectedEvent>,
    /// The tasks the hooks created, always collected.
    pub tasks: Vec<Task>,
    /// The observations the hooks recorded, always collected.
    pub observations: Vec<Observation>,
    /// The hooks that produced no usable result.
    pub incidents: Vec<HookIncident>,
}

/// A chain of SYNC hooks on one payload, fed one outcome at a time.
#[derive(Clone, Debug)]
pub(crate) struct HookChain<P> {
    payload: P,
    trigger_fails_closed: bool,
    block: Option<(String, Option<String>)>,
    injected_events: Vec<InjectedEvent>,
    tasks: Vec<Task>,
    observations: Vec<Observation>,
    incidents: Vec<HookIncident>,
}

impl<P> HookChain<P> {
    /// A chain of `trigger`'s hooks on `payload`.
    pub(crate) fn new(trigger: HookTrigger, payload: P) -> Self {
        Self {
            payload,
            trigger_fails_closed: trigger.fails_closed(),
            block: None,
            injected_events: Vec::new(),
            tasks: Vec::new(),
            observations: Vec::new(),
            incidents: Vec::new(),
        }
    }

    /// The payload the next hook receives: the original, or the last
    /// replacement.
    pub(crate) fn payload(&self) -> &P {
        &self.payload
    }

    /// Whether a hook blocked: no further hook may run (RFC §9.3).
    pub(crate) fn is_blocked(&self) -> bool {
        self.block.is_some()
    }

    /// Applies what `hook` produced. Once the chain is blocked, nothing more
    /// is applied: a blocked chain runs no further hook.
    #[cfg(test)]
    pub(crate) fn apply(&mut self, hook: &HookSpec, outcome: HookOutcome<P>) {
        self.apply_kept(hook, outcome, |payload| payload);
    }

    /// Applies what `hook` produced, a replacement payload as `keep` keeps
    /// it: what the hook may change of the payload it was handed, the rest
    /// put back from it before the next hook reads it (RFC §9.4).
    pub(crate) fn apply_kept(
        &mut self,
        hook: &HookSpec,
        outcome: HookOutcome<P>,
        keep: impl FnOnce(P) -> P,
    ) {
        if self.is_blocked() {
            return;
        }
        match outcome {
            HookOutcome::Returned(result) => self.apply_result(hook, result, keep),
            HookOutcome::Raised(error) => self.apply_problem(hook, HookProblem::Raised(error)),
            HookOutcome::TimedOut => self.apply_problem(hook, HookProblem::TimedOut),
            HookOutcome::Invalid {
                what,
                tasks,
                observations,
            } => {
                // Its tasks and observations are kept, its injected events
                // are not (RFC §9.3).
                self.tasks.extend(tasks);
                self.observations.extend(observations);
                self.apply_problem(hook, HookProblem::Invalid(what));
            }
        }
    }

    /// Hands over the hooks that produced no usable result so far, so that
    /// a chain run in two phases reports each phase's when it ends (RFC
    /// §9.5.1: the off-lock hooks before the lock, the others under it).
    pub(crate) fn take_incidents(&mut self) -> Vec<HookIncident> {
        std::mem::take(&mut self.incidents)
    }

    /// What the chain decided, with everything its hooks produced.
    pub(crate) fn finish(self) -> ChainVerdict<P> {
        let decision = match self.block {
            Some((by, reason)) => ChainDecision::Blocked { by, reason },
            None => ChainDecision::Allowed,
        };
        ChainVerdict {
            decision,
            payload: self.payload,
            injected_events: self.injected_events,
            tasks: self.tasks,
            observations: self.observations,
            incidents: self.incidents,
        }
    }

    fn apply_result(&mut self, hook: &HookSpec, result: HookResult<P>, keep: impl FnOnce(P) -> P) {
        self.injected_events.extend(result.injected_events);
        self.tasks.extend(result.tasks);
        self.observations.extend(result.observations);
        match result.decision {
            HookDecision::Allow => {}
            HookDecision::Modify(payload) => self.payload = keep(payload),
            HookDecision::Block => self.block = Some((hook.name.clone(), result.reason)),
        }
    }

    /// An unusable outcome allows, unless the hook or its trigger fails
    /// closed: then it blocks, naming the hook and the problem (RFC §9.3).
    fn apply_problem(&mut self, hook: &HookSpec, problem: HookProblem) {
        let blocks = hook.fail_closed || self.trigger_fails_closed;
        if blocks {
            let reason = format!("{}:{}", problem.code(), hook.name);
            self.block = Some((hook.name.clone(), Some(reason)));
        }
        self.incidents.push(HookIncident {
            hook: hook.name.clone(),
            problem,
            blocked: blocks,
        });
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::model::{
        ChannelId, ChannelType, EventDirection, EventId, EventSource, EventType, RoomEvent, RoomId,
        TaskId, TextContent, Timestamp,
    };
    use crate::rules::hook_registry::HookExecution;

    fn hook(name: &str) -> HookSpec {
        HookSpec::new(name, HookTrigger::BeforeBroadcast, HookExecution::Sync)
    }

    fn fail_closed(name: &str) -> HookSpec {
        HookSpec {
            fail_closed: true,
            ..hook(name)
        }
    }

    fn chain() -> HookChain<String> {
        HookChain::new(HookTrigger::BeforeBroadcast, "original".to_owned())
    }

    fn decision(chain: HookChain<String>) -> ChainDecision {
        chain.finish().decision
    }

    fn allowed_payload(chain: HookChain<String>) -> Option<String> {
        let verdict = chain.finish();
        (verdict.decision == ChainDecision::Allowed).then_some(verdict.payload)
    }

    fn task(id: &str) -> Task {
        Task::new(
            TaskId::new(id).unwrap(),
            RoomId::new("room_1").unwrap(),
            "follow up",
            Timestamp::UNIX_EPOCH,
        )
    }

    fn injected() -> InjectedEvent {
        InjectedEvent::stored_only(RoomEvent::new(
            EventId::new("evt_9").unwrap(),
            RoomId::new("room_1").unwrap(),
            EventType::Message,
            EventSource::new(
                ChannelId::new("system").unwrap(),
                ChannelType::SYSTEM,
                EventDirection::Outbound,
            ),
            TextContent::new("notice").into(),
            Timestamp::UNIX_EPOCH,
        ))
    }

    #[test]
    fn a_chain_of_allows_keeps_the_payload() {
        let mut chain = chain();
        chain.apply(&hook("audit"), HookOutcome::Returned(HookResult::allow()));
        assert_eq!(allowed_payload(chain).as_deref(), Some("original"));
    }

    #[test]
    fn a_modify_replaces_the_payload() {
        let mut chain = chain();
        let modify = HookResult::modify("redacted".to_owned());
        chain.apply(&hook("pii"), HookOutcome::Returned(modify));
        assert_eq!(allowed_payload(chain).as_deref(), Some("redacted"));
    }

    #[test]
    fn a_modify_to_an_empty_payload_replaces_it() {
        let mut chain = chain();
        chain.apply(
            &hook("pii"),
            HookOutcome::Returned(HookResult::modify(String::new())),
        );
        assert_eq!(allowed_payload(chain).as_deref(), Some(""));
    }

    #[test]
    fn the_next_hook_receives_the_modified_payload() {
        let mut chain = chain();
        let modify = HookResult::modify("redacted".to_owned());
        chain.apply(&hook("pii"), HookOutcome::Returned(modify));
        assert_eq!(chain.payload(), "redacted");
    }

    #[test]
    fn a_block_names_the_hook_and_its_reason() {
        let mut chain = chain();
        chain.apply(
            &hook("sensitivity_scanner"),
            HookOutcome::Returned(HookResult::block("SIN detected")),
        );
        assert_eq!(
            decision(chain),
            ChainDecision::Blocked {
                by: "sensitivity_scanner".to_owned(),
                reason: Some("SIN detected".to_owned())
            }
        );
    }

    #[test]
    fn a_blocked_verdict_carries_the_payload_the_earlier_hooks_left() {
        let mut chain = chain();
        let modify = HookResult::modify("redacted".to_owned());
        chain.apply(&hook("pii"), HookOutcome::Returned(modify));
        chain.apply(
            &hook("policy"),
            HookOutcome::Returned(HookResult::block("no")),
        );
        assert_eq!(chain.finish().payload, "redacted");
    }

    #[test]
    fn a_block_stops_the_chain() {
        let mut chain = chain();
        chain.apply(
            &hook("first"),
            HookOutcome::Returned(HookResult::block("stop")),
        );
        chain.apply(
            &hook("second"),
            HookOutcome::Returned(HookResult::modify("late".to_owned())),
        );
        assert!(matches!(
            decision(chain),
            ChainDecision::Blocked { by, .. } if by == "first"
        ));
    }

    #[test]
    fn a_raising_fail_open_hook_allows() {
        let mut chain = chain();
        chain.apply(
            &hook("broken_logger"),
            HookOutcome::Raised(ErrorRecord::new("ValueError", "boom")),
        );
        assert_eq!(allowed_payload(chain).as_deref(), Some("original"));
    }

    #[test]
    fn a_raising_fail_open_hook_is_reported_as_an_incident() {
        let mut chain = chain();
        let error = ErrorRecord::new("ValueError", "boom");
        chain.apply(&hook("broken_logger"), HookOutcome::Raised(error.clone()));
        assert_eq!(
            chain.finish().incidents,
            [HookIncident {
                hook: "broken_logger".to_owned(),
                problem: HookProblem::Raised(error),
                blocked: false,
            }]
        );
    }

    #[test]
    fn a_fail_closed_hook_that_times_out_blocks_naming_the_timeout() {
        let mut chain = chain();
        chain.apply(&fail_closed("pii"), HookOutcome::TimedOut);
        assert_eq!(
            decision(chain),
            ChainDecision::Blocked {
                by: "pii".to_owned(),
                reason: Some("hook_timeout:pii".to_owned())
            }
        );
    }

    #[test]
    fn a_fail_closed_hook_that_raises_blocks_naming_the_error() {
        let mut chain = chain();
        chain.apply(
            &fail_closed("pii"),
            HookOutcome::Raised(ErrorRecord::new("ValueError", "boom")),
        );
        assert!(matches!(
            decision(chain),
            ChainDecision::Blocked { reason: Some(reason), .. } if reason == "hook_error:pii"
        ));
    }

    #[test]
    fn an_invalid_result_on_a_fail_closed_trigger_blocks() {
        let mut chain = HookChain::new(HookTrigger::BeforeTts, "say this".to_owned());
        let redactor = HookSpec::new("redactor", HookTrigger::BeforeTts, HookExecution::Sync);
        chain.apply(&redactor, HookOutcome::invalid("returned a number"));
        assert!(matches!(
            decision(chain),
            ChainDecision::Blocked { reason: Some(reason), .. }
                if reason == "hook_invalid_result:redactor"
        ));
    }

    #[test]
    fn the_tasks_of_an_unusable_result_are_collected() {
        let mut chain = chain();
        let unusable = HookOutcome::Invalid {
            what: "a modify with no payload".to_owned(),
            tasks: vec![task("task_1")],
            observations: Vec::new(),
        };
        chain.apply(&hook("triage"), unusable);
        assert_eq!(chain.finish().tasks, [task("task_1")]);
    }

    #[test]
    fn side_effects_of_an_allow_are_collected() {
        let mut chain = chain();
        let allow = HookResult::allow().with_task(task("task_1"));
        chain.apply(&hook("triage"), HookOutcome::Returned(allow));
        assert_eq!(chain.finish().tasks, [task("task_1")]);
    }

    #[test]
    fn side_effects_of_the_hooks_before_a_block_are_collected() {
        let mut chain = chain();
        let allow = HookResult::allow().with_task(task("task_1"));
        chain.apply(&hook("triage"), HookOutcome::Returned(allow));
        chain.apply(
            &hook("pii"),
            HookOutcome::Returned(HookResult::block("SIN")),
        );
        assert_eq!(chain.finish().tasks, [task("task_1")]);
    }

    #[test]
    fn events_injected_by_an_allow_are_collected() {
        let mut chain = chain();
        let allow = HookResult::allow().inject(injected());
        chain.apply(&hook("notifier"), HookOutcome::Returned(allow));
        assert_eq!(chain.finish().injected_events, [injected()]);
    }

    #[test]
    fn incidents_taken_are_not_reported_again() {
        let mut chain = chain();
        chain.apply(&hook("scanner"), HookOutcome::TimedOut);
        let taken = chain.take_incidents();
        chain.apply(&hook("policy"), HookOutcome::TimedOut);
        let names = |incidents: Vec<HookIncident>| -> Vec<String> {
            incidents
                .into_iter()
                .map(|incident| incident.hook)
                .collect()
        };
        assert_eq!(
            (names(taken), names(chain.finish().incidents)),
            (vec!["scanner".to_owned()], vec!["policy".to_owned()])
        );
    }

    #[test]
    fn events_injected_by_a_block_are_collected() {
        let mut chain = chain();
        let block = HookResult::block("SIN").inject(injected());
        chain.apply(&hook("pii"), HookOutcome::Returned(block));
        assert_eq!(chain.finish().injected_events, [injected()]);
    }
}
