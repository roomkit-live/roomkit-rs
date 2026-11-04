//! The capability tags a model listing reads off what a model is: Python's
//! `roomkit.providers.ai.model_tags`.
//!
//! A chat provider's live listing also surfaces its speech models, and a
//! caller picking a chat model must be able to set them apart. A
//! speech-to-text model's id says `transcribe` or `whisper`, a text-to-speech
//! model's says `tts`. A model that converses in audio is neither and stays
//! untagged.

use super::ModelInfo;

/// The [`ModelInfo::capabilities`] tag of a speech-to-text model.
pub const TRANSCRIPTION_CAPABILITY: &str = "transcription";
/// The [`ModelInfo::capabilities`] tag of a text-to-speech model.
pub const SPEECH_CAPABILITY: &str = "speech";

/// The speech tag `model_id` names itself by, or none.
pub fn speech_tags(model_id: &str) -> Vec<String> {
    let name = model_id.to_lowercase();
    if name.contains("transcrib") || names_word(&name, "whisper") {
        return vec![TRANSCRIPTION_CAPABILITY.to_owned()];
    }
    if names_word(&name, "tts") {
        return vec![SPEECH_CAPABILITY.to_owned()];
    }
    Vec::new()
}

/// `models`, each one the listing left untagged given its speech tag.
pub fn with_speech_tags(models: Vec<ModelInfo>) -> Vec<ModelInfo> {
    models
        .into_iter()
        .map(|model| {
            if model.capabilities.is_empty() {
                ModelInfo {
                    capabilities: speech_tags(&model.id),
                    ..model
                }
            } else {
                model
            }
        })
        .collect()
}

/// Whether `word` stands in `name` as Python's pattern reads it,
/// `(?:^|[-_/])word(?:[-_]|$)`.
fn names_word(name: &str, word: &str) -> bool {
    name.match_indices(word).any(|(at, _)| {
        let before = name.get(..at).and_then(|head| head.chars().next_back());
        let after = name
            .get(at + word.len()..)
            .and_then(|tail| tail.chars().next());
        matches!(before, None | Some('-' | '_' | '/')) && matches!(after, None | Some('-' | '_'))
    })
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    /// The ids OpenAI, Gemini and Mistral listed on 2026-10-02, as Python's
    /// test names them.
    const TRANSCRIBING: [&str; 13] = [
        "whisper-1",
        "gpt-4o-transcribe",
        "gpt-4o-mini-transcribe-2025-12-15",
        "gpt-4o-transcribe-diarize",
        "gpt-transcribe",
        "gpt-live-transcribe",
        "gpt-realtime-whisper",
        "gemini-3.5-transcribe",
        "gemini-3.5-transcribe-live",
        "voxtral-mini-transcribe-realtime-2602",
        "whisper-large-v3-turbo",
        "cohere-transcribe-03-2026",
        "openai/whisper-1",
    ];
    const SPEAKING: [&str; 8] = [
        "tts-1",
        "tts-1-hd-1106",
        "gpt-4o-mini-tts",
        "gpt-4o-mini-tts-2025-12-15",
        "gemini-2.5-flash-preview-tts",
        "gemini-3.1-flash-tts-preview",
        "gemini-3.8-flash-lite-tts",
        "voxtral-mini-tts-latest",
    ];
    const NEITHER: [&str; 11] = [
        "gpt-audio",
        "gpt-audio-mini-2025-12-15",
        "gpt-realtime-2.1",
        "gpt-realtime-translate",
        "gemini-2.5-flash-native-audio-latest",
        "lyria-realtime-exp",
        "voxtral-mini-latest",
        "voxtral-small-2507",
        "voxtral-mini-realtime-2602",
        "gpt-5.4",
        "qwen-3.8-27b",
    ];

    // Python: tests/test_providers/test_model_tags.py::test_a_speech_to_text_model_is_tagged_transcription
    #[test]
    fn a_speech_to_text_model_is_tagged_transcription() {
        for id in TRANSCRIBING {
            assert_eq!(speech_tags(id), [TRANSCRIPTION_CAPABILITY], "{id}");
        }
    }

    // Python: tests/test_providers/test_model_tags.py::test_a_text_to_speech_model_is_tagged_speech
    #[test]
    fn a_text_to_speech_model_is_tagged_speech() {
        for id in SPEAKING {
            assert_eq!(speech_tags(id), [SPEECH_CAPABILITY], "{id}");
        }
    }

    // Python: tests/test_providers/test_model_tags.py::test_a_model_that_converses_is_not_tagged
    #[test]
    fn a_model_that_converses_is_not_tagged() {
        for id in NEITHER {
            assert_eq!(speech_tags(id), Vec::<String>::new(), "{id}");
        }
    }

    /// Python's patterns, where the listed ids do not reach them: the word
    /// stands alone between `-`, `_` or a start, a `/` before it counting, in
    /// any case.
    #[test]
    fn a_word_stands_alone_in_a_model_s_name() {
        for id in ["whisperer", "stts-1", "tts/1", "tts.1"] {
            assert_eq!(speech_tags(id), Vec::<String>::new(), "{id}");
        }
        assert_eq!(speech_tags("Whisper_1"), [TRANSCRIPTION_CAPABILITY]);
        assert_eq!(speech_tags("vendor/TTS-1_hd"), [SPEECH_CAPABILITY]);
    }

    // Python: tests/test_providers/test_model_tags.py::test_a_listing_keeps_the_tags_it_reported
    #[test]
    fn a_listing_keeps_the_tags_it_reported() {
        let listed = vec![
            ModelInfo::new("whisper-1"),
            ModelInfo {
                capabilities: vec!["completion".to_owned()],
                ..ModelInfo::new("tts-1")
            },
            ModelInfo::new("gpt-5.4"),
        ];
        let tagged: Vec<Vec<String>> = with_speech_tags(listed)
            .into_iter()
            .map(|model| model.capabilities)
            .collect();
        assert_eq!(
            tagged,
            [
                vec!["transcription".to_owned()],
                vec!["completion".to_owned()],
                Vec::new()
            ]
        );
    }
}
