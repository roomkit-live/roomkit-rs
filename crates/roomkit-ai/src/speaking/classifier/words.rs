//! The standard classifier questions of RFC §6.4.

use crate::classifiers::{Question, Questions, ScoreQuestion, YesNoQuestion};

pub(super) const RESUME_NOTE: &str = "You were cut off after about {seconds:.0f} s of your last answer. If what you were saying still matters, go on from where you were cut: a short link back (\"so, as I was saying\"), then only what was left. Do not repeat what was heard, do not apologise.";

pub(super) const LANGUAGE_INSTRUCTIONS: &str = "In which language should the assistant answer the person who spoke in `last_turn`? It is the language that person speaks, judged on `last_turn` together with their turns in `recent_turns`. A short turn in another language, a word or an exclamation, maybe misheard by the speech recognizer, does not change it; a whole sentence in another language, or a request to switch, does.";

fn directness() -> Question {
    Question::Score(ScoreQuestion { instructions: "How directly does `last_turn` bring in the assistant named in `agent.name`? When `people` lists a single person, that person is talking with the assistant: a greeting, a 'tu' or 'you', or a question that names no one else is direct. When `people` lists several people, a 'tu' or 'you' said to one of them does not bring in the assistant.".into(), levels: vec!["Not mentioned: `last_turn` does not name or refer to the assistant, talks about it to someone else ('she is listening'), or asks someone else to tell or explain something to it ('explain it to her'): that person speaks next.".into(), "Tentatively: the speaker wonders whether the assistant could help or might know.".into(), "Indirectly: the speaker proposes that the assistant be asked something it could answer now ('we could ask the assistant').".into(), "Directly: the assistant is asked by name or as 'you', even with polite conditionals.".into()] })
}

fn deferred() -> Question {
    Question::YesNo(YesNoQuestion::new(
        "In `last_turn`, does the speaker postpone or decline asking the assistant or the question being discussed (for example 'later', 'after the meeting', 'we will check ourselves')?",
    ))
}

fn unfinished() -> Question {
    Question::YesNo(YesNoQuestion::new(
        "Look at how `last_turn` ends: does the speaker stop in the middle of a sentence, or say they are not done (\"wait\", \"let me think\", \"I am looking for it\")? Dots or a restart earlier in the turn do not count when its last sentence is complete, and neither does saying one does not know, does not remember or will look it up.",
    ))
}

fn hush() -> Question {
    Question::YesNo(YesNoQuestion::new(
        "In `last_turn`, does the speaker ask the assistant not to answer, to stay quiet, or only to listen from now on? Asking whether it is ready to listen, or to listen to what they are about to say, expects an answer: it is not a request for quiet.",
    ))
}

fn request() -> Question {
    Question::YesNo(YesNoQuestion::new(
        "Does `last_turn` ask the assistant to answer a question or to do something now? A remark or a reproach about the assistant is not a request.",
    ))
}

fn answered() -> Question {
    Question::YesNo(YesNoQuestion::new(
        "Did the assistant ask a question in its last turn of `recent_turns`, and does `last_turn` answer it, even with 'no', 'never mind' or 'it was just a test'?",
    ))
}

fn answers() -> Question {
    Question::YesNo(YesNoQuestion::new(
        "Does one of `assistant_thought.want_to_say` answer a question or a need still open at `last_turn`: asked or stated in `last_turn`, or in `recent_turns` and answered by no one since, the assistant included?",
    ))
}

fn corrects() -> Question {
    Question::YesNo(YesNoQuestion::new(
        "Does one of `assistant_thought.want_to_say` correct, or warn about a problem with, something said or proposed in `last_turn` itself?",
    ))
}

pub(super) fn resume() -> Question {
    Question::YesNo(YesNoQuestion::new(
        "The assistant was cut off while saying `assistant_cut_off.was_saying`: the speaker of `last_turn` started talking over it after `assistant_cut_off.heard_seconds` seconds. Does `last_turn` leave the assistant free to go on and finish: an acknowledgement, a thanks, a short reaction, an invitation to continue, or talking over it by accident? A question (even about what the assistant was saying), a request, a correction, an objection, or asking the assistant to stop means the speaker wants to talk instead.",
    ))
}

fn waiting() -> Question {
    Question::YesNo(YesNoQuestion::new(
        "Is a question in `last_turn` or `recent_turns`, put to a person other than the assistant (by their name, or as 'you' to one of them), still waiting for that person's first try: they have not answered it, said they do not know, nor given up yet? A question put to everyone ('does anyone know?') waits for no one.",
    ))
}

fn listen_request() -> Question {
    Question::YesNo(YesNoQuestion::new(
        "Does `last_turn` ask the assistant to stop talking, to stay quiet or to only listen from now on, until told otherwise? Asking it not to answer this one turn only, or talking about someone else being quiet, does not count.",
    ))
}

fn asked_me() -> Question {
    Question::YesNo(YesNoQuestion::new(
        "The assistant was asked to only listen (`agent.listening_only`). Does `last_turn` put a question or a request to the assistant itself, expecting it to answer now? Thinking aloud, talking to someone else, or a question about the topic that is not put to the assistant does not count.",
    ))
}

fn lift() -> Question {
    Question::YesNo(YesNoQuestion::new(
        "The assistant was asked to only listen (`agent.listening_only`). Does `last_turn` tell the assistant it may talk or intervene again on its own, from now on? A question or a request put to it is not, by itself, letting it talk again: it answers that one and goes on listening.",
    ))
}

pub(super) fn base() -> Questions {
    [
        ("directness", directness()),
        ("deferred", deferred()),
        ("unfinished", unfinished()),
        ("hush", hush()),
        ("request", request()),
        ("answered", answered()),
    ]
    .into_iter()
    .collect()
}
pub(super) fn thought() -> Questions {
    [
        ("answers", answers()),
        ("corrects", corrects()),
        ("waiting", waiting()),
    ]
    .into_iter()
    .collect()
}
pub(super) fn listening(name: &str) -> Question {
    match name {
        "asked_me" => asked_me(),
        "lift" => lift(),
        _ => listen_request(),
    }
}
