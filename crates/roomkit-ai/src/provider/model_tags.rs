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

    // Python: providers/ai/model_tags.py, the names its docstring measured on
    // OpenAI, Gemini and Mistral.
    #[test]
    fn a_speech_model_is_told_by_its_name() {
        let tags = |id: &str| speech_tags(id);
        for id in [
            "whisper-1",
            "gpt-4o-transcribe",
            "voxtral-mini-transcribe",
            "openai/whisper-large",
            "Whisper_1",
        ] {
            assert_eq!(tags(id), ["transcription"], "{id}");
        }
        for id in [
            "tts-1",
            "gpt-4o-mini-tts",
            "gemini-3-tts",
            "vendor/TTS-1_hd",
        ] {
            assert_eq!(tags(id), ["speech"], "{id}");
        }
        for id in [
            "gpt-audio",
            "gpt-realtime",
            "voxtral-small",
            "whisperer",
            "stts-1",
            "tts/1",
            "gpt-4o",
        ] {
            assert_eq!(tags(id), Vec::<String>::new(), "{id}");
        }
    }

    #[test]
    fn a_tag_a_listing_reported_stays() {
        let reported = ModelInfo {
            capabilities: vec!["tools".to_owned()],
            ..ModelInfo::new("tts-1")
        };
        let tagged = with_speech_tags(vec![reported.clone(), ModelInfo::new("tts-1")]);
        assert_eq!(
            tagged
                .iter()
                .map(|m| &m.capabilities[..])
                .collect::<Vec<_>>(),
            [&reported.capabilities[..], &["speech".to_owned()][..]]
        );
    }
}
