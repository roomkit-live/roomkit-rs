//! RFC Appendix A.9 "Per-turn settings": a turn's system prompt from its
//! binding's metadata first, then the channel's own; RFC §23.3 step 6: a
//! provider's failure met before the loop is raised as Python's
//! `ProviderError`.

use std::sync::Arc;

use pretty_assertions::assert_eq;
use roomkit::UpdateBindingMetadataOptions;
use roomkit::model::Metadata;
use roomkit_ai::memory::{
    BudgetAwareMemory, BudgetAwareMemoryOptions, SlidingWindowMemory, SlidingWindowMemoryOptions,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannelOptions, Tools};

use serde_json::{Value, json};

use crate::support::{Room, channel, room_id};

// Python: tests/test_channels/test_turn_config.py, the binding's explicit
// override and test_a_binding_null_keeps_the_channel_default[system_prompt]:
// a text there is the turn's prompt, `null` or no key keeps the channel's,
// an empty one lifts it, the turn sending none.
#[tokio::test]
async fn a_binding_s_system_prompt_comes_before_the_channel_s() {
    let cases = [
        (Some(json!("Binding's.")), Some("Binding's.")),
        (Some(Value::Null), Some("Channel's.")),
        (None, Some("Channel's.")),
        (Some(json!("")), None),
    ];
    for (bound, expected) in cases {
        let model = ScriptedProvider::new([ScriptedRound::answering("Hello.")]);
        let room = Room::equipped(model.clone(), Tools::new(), |options| AiChannelOptions {
            system_prompt: Some("Channel's.".into()),
            ..options
        })
        .await;
        if let Some(bound) = bound.clone() {
            let mut metadata = Metadata::new();
            metadata.insert("system_prompt".to_owned(), bound);
            room.kit
                .update_binding_metadata(
                    &room_id(),
                    &channel("assistant"),
                    metadata,
                    UpdateBindingMetadataOptions::default(),
                )
                .await
                .unwrap();
        }

        room.says("Hello").await;

        let prompt = model.requests().remove(0).system_prompt;
        assert_eq!((&bound, prompt.as_deref()), (&bound, expected));
    }
}

// Python: memory/budget_aware.py raises ProviderError(context_overflow=True)
// while the turn's context is built, which the room reports under its class.
#[tokio::test]
async fn a_memory_s_context_overflow_is_reported_as_a_provider_s_failure() {
    let inner = Arc::new(SlidingWindowMemory::new(
        SlidingWindowMemoryOptions::default(),
    ));
    let memory = Arc::new(BudgetAwareMemory::new(
        inner,
        10,
        BudgetAwareMemoryOptions::default(),
    ));
    let model = ScriptedProvider::new([ScriptedRound::answering("Hello.")]);
    let room = Room::equipped(model.clone(), Tools::new(), |options| AiChannelOptions {
        memory: Some(memory),
        ..options
    })
    .await;

    let result = room.says(&"x".repeat(400)).await;

    let kind = result.error.map(|error| error.error_type);
    assert_eq!(
        (kind.as_deref(), model.requests().len()),
        (Some("ProviderError"), 0)
    );
}
