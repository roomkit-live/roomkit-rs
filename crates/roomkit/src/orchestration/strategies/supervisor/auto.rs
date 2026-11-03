//! The supervisor's framework-driven delegation (RFC §19.7.3): with
//! `auto_delegate`, the supervisor's turns in the room it is installed in
//! run the team, no tool needed. With `refine_task`, a first pass of the
//! supervisor's own turn, the task-formulation instruction after its
//! prompt, turns the message into the team's task; without, the team is
//! handed the message's text. The supervisor then answers the message
//! from the workers' results, handed to it in place of the message.
//! Python's `orchestration/strategies/supervisor/_install_auto.py`
//! (`_DelegatingTurns`, its synchronous branch) and `delegate.py`
//! (`_two_pass_delegate`, `_one_pass_delegate`).

use std::sync::Arc;

use roomkit_core::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, EventSource, RoomContext, RoomEvent,
    RoomId, TextContent,
};
use roomkit_core::rules::edits::opens_no_turn;
use roomkit_core::rules::transcript::fence;
use serde_json::{Value, json};

use super::results::{Presented, present_worker_results};
use super::supervised::run_supervised_sequential;
use super::team::run_parallel;
use super::{Team, WorkerStrategy};
use crate::error::RoomKitError;
use crate::kit::RoomKit;
use crate::orchestration::worker_run::task_input;
use crate::store::ConversationStore;
use crate::turn_runners::{Answering, FirstPass, Next, Passes, Taken, Then, TurnRunner};

/// The binding metadata key of the instruction the first pass carries,
/// which roomkit-ai's channel writes after the turn's prompt: Python's
/// `TURN_INSTRUCTION`.
const TURN_INSTRUCTION: &str = "_turn_instruction";

/// The first pass's own instruction: Python's `_build_pass1_instruction`.
const PASS1_INSTRUCTION: &str = "Extract the core topic or subject from the user's request. \
Output only the topic, nothing else. No questions, no instructions, no formatting. Example: user \
says 'analyse anthropic' \u{2192} 'Anthropic'";

/// What the supervisor answers when its first pass stopped short of an
/// answer: Python's `FALLBACK_FAILED`.
const FALLBACK_FAILED: &str = "The delegated work could not be completed.";

/// The heading of the message a one-pass answer quotes: Python's
/// `task_heading('The user asked:')`, the asker unnamed (RMK-1274).
const ASKED: &str = "The user asked:";

/// One install's framework-driven delegation: the supervisor's turns run
/// the workers, then the supervisor answers with what they found (Python's
/// `_DelegatingTurns`).
pub(super) struct DelegatingTurns {
    pub(super) team: Arc<Team>,
    pub(super) strategy: WorkerStrategy,
    /// Whether a first pass turns the message into the team's task.
    pub(super) refine: bool,
    /// The first pass's instruction, when not the implementation's own.
    pub(super) instruction: Option<String>,
}

impl<S: ConversationStore> TurnRunner<S> for DelegatingTurns {
    fn take(
        &self,
        kit: RoomKit<S>,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Taken {
        let source = &event.source;
        if source.channel_id == self.team.supervisor.channel_id
            || source.channel_type == ChannelType::AI
        {
            return Taken::Answered(Box::pin(async { Ok(ChannelOutput::empty()) }));
        }
        if opens_no_turn(&event) {
            // A notice, an edit or a delete is no request to delegate (RFC
            // §6.4, §10.3): the supervisor's own channel reads it.
            return Taken::Own(Box::new((event, binding, context)));
        }
        let asked = Asked {
            kit,
            room_id: context.room.id.clone(),
            team: Arc::clone(&self.team),
            strategy: self.strategy,
            event,
            binding,
            context,
        };
        if self.refine {
            return two_passes(asked, self.instruction.as_deref());
        }
        one_pass(asked)
    }
}

/// A message the supervisor is asked, and the install's team.
struct Asked<S> {
    kit: RoomKit<S>,
    room_id: RoomId,
    team: Arc<Team>,
    strategy: WorkerStrategy,
    event: RoomEvent,
    binding: ChannelBinding,
    context: RoomContext,
}

impl<S: ConversationStore> Asked<S> {
    /// The supervisor's own turn on the workers' results for `task`,
    /// presented as `present` writes them, in place of the message.
    async fn answered_from(
        self,
        task: &str,
        present: impl FnOnce(&[Presented]) -> String,
    ) -> Result<Next, RoomKitError> {
        let results =
            run_workers(&self.kit, &self.room_id, &self.team, self.strategy, task).await?;
        let stand_in = results_event(&self.event, present(&results));
        Ok(Next::Own(Box::new(Answering {
            event: stand_in,
            binding: self.binding,
            context: self.context,
            asked: self.event,
        })))
    }
}

/// The first pass, the instruction riding a copy of the binding, after the
/// prompt the turn resolves, so that no other room's turn reads it; then
/// the team on its answer, and the supervisor's answer from their results
/// (Python's `_formulate_task`, `_two_pass_delegate`).
fn two_passes<S: ConversationStore>(asked: Asked<S>, instruction: Option<&str>) -> Taken {
    let mut first = asked.binding.clone();
    // Python's `instruction or _build_pass1_instruction()`: an empty one
    // is none.
    let instruction = instruction
        .filter(|instruction| !instruction.is_empty())
        .unwrap_or(PASS1_INSTRUCTION);
    first
        .metadata
        .insert(TURN_INSTRUCTION.to_owned(), json!(instruction));
    let pass = (asked.event.clone(), first, asked.context.clone());
    let then = Then(Box::new(move |pass: FirstPass| {
        Box::pin(async move {
            if pass.answer.is_empty() {
                return Ok(first_pass_answer(pass));
            }
            asked
                .answered_from(&pass.answer, present_worker_results)
                .await
        })
    }));
    Taken::Passes(Passes {
        first: Box::new(pass),
        then,
    })
}

/// What answers a first pass that handed on no task: its own output,
/// carrying its record, when it completed with nothing to say or was
/// stopped on purpose; else the supervisor's fallback, the turn's record on
/// it, so that the message gets an answer (Python's `_pass1_answer`).
fn first_pass_answer(pass: FirstPass) -> Next {
    match pass.end.as_deref() {
        None | Some("completed" | "cancelled") => Next::FirstStands,
        Some(end) => {
            let mut record = pass.record;
            record.insert("loop_end_reason".to_owned(), Value::from(end));
            Next::Fallback(FALLBACK_FAILED.to_owned(), record)
        }
    }
}

/// The team on the message's text, then the supervisor's answer from the
/// message, quoted as the task, and their results; a message with no text
/// is the supervisor's own turn (Python's `_one_pass_delegate`).
fn one_pass<S: ConversationStore>(asked: Asked<S>) -> Taken {
    let message = task_input(&asked.event).to_owned();
    if message.is_empty() {
        return Taken::Own(Box::new((asked.event, asked.binding, asked.context)));
    }
    Taken::Later(Box::pin(async move {
        let present = |results: &[Presented]| one_pass_results(&message, results);
        asked.answered_from(&message, present).await
    }))
}

/// What the supervisor presents from in one pass: the message as a `<task>`
/// block, then each worker's output (Python's `_one_pass_results`).
fn one_pass_results(message: &str, results: &[Presented]) -> String {
    format!(
        "{ASKED}\n{}\n\n{}",
        fence("task", message),
        present_worker_results(results)
    )
}

/// The team's results on `task`, as the install's strategy runs it: a
/// sequential team supervised step by step, a parallel one at once
/// (Python's `_run_workers`).
async fn run_workers<S: ConversationStore>(
    kit: &RoomKit<S>,
    room_id: &RoomId,
    team: &Arc<Team>,
    strategy: WorkerStrategy,
    task: &str,
) -> Result<Vec<Presented>, RoomKitError> {
    Ok(match strategy {
        WorkerStrategy::Sequential => run_supervised_sequential(kit, room_id, team, task)
            .await?
            .into_iter()
            .map(Presented::from)
            .collect(),
        WorkerStrategy::Parallel => run_parallel(kit, room_id, team, task)
            .await?
            .into_iter()
            .map(Presented::from)
            .collect(),
    })
}

/// The workers' results, standing in for the event the supervisor
/// answers: as deep as the event and in its thread, so that the answer
/// stays one deeper than the event, answering it (RFC §8.3, §8.5). Never
/// stored, it has no index: the turn reads the room as it is, the first
/// pass's calls among its earlier rounds (Python's `_results_event`).
fn results_event(event: &RoomEvent, body: String) -> RoomEvent {
    let source = EventSource::new(ChannelId::system(), ChannelType::SYSTEM);
    let mut stand_in = RoomEvent::new(event.room_id.clone(), source, TextContent::new(body));
    stand_in.r#type = event.r#type;
    stand_in.chain_depth = event.chain_depth;
    stand_in.parent_event_id.clone_from(&event.parent_event_id);
    stand_in.responds_to = Some(event.id.clone());
    stand_in
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use roomkit_core::model::{
        ChannelBinding, ChannelId, ChannelType, EventContent, EventSource, EventType, Metadata,
        Room, RoomContext, RoomEvent, RoomId, TextContent,
    };
    use serde_json::json;

    use super::{
        DelegatingTurns, FirstPass, Next, PASS1_INSTRUCTION, TURN_INSTRUCTION, first_pass_answer,
        results_event,
    };
    use crate::orchestration::agents::tests::held_agent;
    use crate::orchestration::strategies::supervisor::{Team, WorkerStrategy};
    use crate::store::InMemoryStore;
    use crate::turn_runners::{Taken, TurnRunner};
    use crate::{RoomKit, RoomKitOptions};

    fn id(name: &str) -> ChannelId {
        ChannelId::new(name).unwrap_or_else(|error| panic!("{error}"))
    }

    fn room() -> RoomId {
        RoomId::new("r1").unwrap_or_else(|error| panic!("{error}"))
    }

    fn turns(refine: bool, instruction: Option<&str>) -> DelegatingTurns {
        let team = Team {
            supervisor: held_agent("sup"),
            workers: Vec::new(),
            share_channels: Vec::new(),
            task_timeout: Duration::from_secs(120),
            max_revisions: 3,
        };
        DelegatingTurns {
            team: Arc::new(team),
            strategy: WorkerStrategy::Parallel,
            refine,
            instruction: instruction.map(str::to_owned),
        }
    }

    /// What the install does with an event of `content` from `source`, of
    /// type `channel_type`, in room r1.
    fn taken(
        runner: &DelegatingTurns,
        (source, channel_type): &(&str, ChannelType),
        content: impl Into<EventContent>,
    ) -> Taken {
        let source = EventSource::new(id(source), channel_type.clone());
        let event = RoomEvent::new(room(), source, content);
        let binding = ChannelBinding::new(id("sup"), room(), ChannelType::AI);
        let in_room = RoomContext::new(Room::new(room()));
        let kit = RoomKit::new(RoomKitOptions::default());
        TurnRunner::<InMemoryStore>::take(runner, kit, event, binding, in_room)
    }

    // Python: tests/test_supervisor.py::TestAutoDelegate::test_sync_auto_delegate_skips_supervisor_events
    // Python: tests/test_supervisor.py::TestAutoDelegate::test_sync_auto_delegate_skips_ai_events
    #[tokio::test]
    async fn the_supervisor_s_own_event_and_an_agent_s_answer_nothing()
    -> Result<(), crate::channel::ChannelError> {
        for source in [("sup", ChannelType::AI), ("other-ai", ChannelType::AI)] {
            let Taken::Answered(answer) =
                taken(&turns(true, None), &source, TextContent::new("Hi"))
            else {
                panic!("{source:?} was delegated");
            };
            assert!(!answer.await?.responded);
        }
        Ok(())
    }

    // Python: tests/test_supervisor.py::TestOnePassDelegate::test_one_pass_empty_message_uses_original
    // Python: tests/test_supervisor.py::TestOnePassDelegate::test_one_pass_runs_workers_with_raw_message
    #[test]
    fn one_pass_delegates_a_message_with_text_only() {
        let runner = turns(false, None);
        let sms = ("sms", ChannelType::SMS);
        assert!(matches!(
            taken(&runner, &sms, TextContent::new("")),
            Taken::Own(..)
        ));
        assert!(matches!(
            taken(&runner, &sms, TextContent::new("Look into Acme.")),
            Taken::Later(..)
        ));
    }

    // Python: tests/test_supervisor.py::TestTwoPassDelegate::test_two_pass_custom_instruction
    // Python: tests/test_supervisor.py::TestTwoPassDelegate::test_two_pass_restores_prompt_on_error
    #[test]
    fn the_first_pass_carries_its_instruction_on_a_copy_of_the_binding() {
        let sms = ("sms", ChannelType::SMS);
        for (instruction, carried) in [
            (None, PASS1_INSTRUCTION),
            (Some("Name the company."), "Name the company."),
        ] {
            let Taken::Passes(passes) =
                taken(&turns(true, instruction), &sms, TextContent::new("Hi"))
            else {
                panic!("a refined delegation has a first pass");
            };
            let (_, binding, _) = *passes.first;
            assert_eq!(
                binding.metadata.get(TURN_INSTRUCTION),
                Some(&json!(carried))
            );
        }
    }

    // Python: tests/test_supervisor.py::TestSupervisorInstall::test_install_auto_delegate_wraps_on_event
    // (a notice, an edit or a delete is the supervisor's own turn, RFC §10.3)
    #[test]
    fn an_event_that_opens_no_turn_is_the_supervisor_s_own() {
        let notice = TextContent::new("A notice.");
        let event = RoomEvent::new(
            room(),
            EventSource::new(id("sms"), ChannelType::SMS),
            notice,
        );
        let notice = RoomEvent {
            r#type: EventType::System,
            ..event
        };
        let binding = ChannelBinding::new(id("sup"), room(), ChannelType::AI);
        let in_room = RoomContext::new(Room::new(room()));
        let kit = RoomKit::new(RoomKitOptions::default());
        let taken =
            TurnRunner::<InMemoryStore>::take(&turns(true, None), kit, notice, binding, in_room);
        assert!(matches!(taken, Taken::Own(..)));
    }

    // Python: tests/orchestration/test_supervisor_pass1_cut.py::test_a_pass_stopped_on_purpose_says_nothing
    // Python: tests/orchestration/test_strategy_supervisor.py::test_pass_one_cut_by_its_round_cap_hands_on_no_task
    #[test]
    fn a_first_pass_with_no_task_answers_its_fallback_only_when_cut_short() {
        for end in [None, Some("completed"), Some("cancelled")] {
            let pass = FirstPass {
                end: end.map(str::to_owned),
                ..FirstPass::default()
            };
            assert!(
                matches!(first_pass_answer(pass), Next::FirstStands),
                "{end:?}"
            );
        }
        let pass = FirstPass {
            end: Some("max_rounds".to_owned()),
            record: Metadata::from_iter([("ai_usage".to_owned(), json!({}))]),
            ..FirstPass::default()
        };
        let Next::Fallback(text, record) = first_pass_answer(pass) else {
            panic!("a cut pass answers its fallback");
        };
        assert_eq!(text, "The delegated work could not be completed.");
        assert_eq!(record.get("loop_end_reason"), Some(&json!("max_rounds")));
        assert_eq!(record.get("ai_usage"), Some(&json!({})));
    }

    // Python: tests/orchestration/test_strategy_supervisor.py::test_the_supervisors_turns_answer_at_the_depth_of_its_event
    #[test]
    fn the_results_stand_in_for_the_event_at_its_depth_and_answer_it() {
        let source = EventSource::new(id("sms"), ChannelType::SMS);
        let mut asked = RoomEvent::new(room(), source, TextContent::new("Look into Acme."));
        asked.chain_depth = 2;
        asked.index = 4;
        let stand_in = results_event(&asked, "Results.".to_owned());
        assert_eq!(stand_in.chain_depth, 2);
        assert_eq!(stand_in.index, 0);
        assert_eq!(stand_in.responds_to.as_ref(), Some(&asked.id));
        assert_eq!(stand_in.source.channel_type, ChannelType::SYSTEM);
        assert_eq!(stand_in.r#type, EventType::Message);
    }
}
