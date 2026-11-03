//! A turn a fallback provider answered (RFC §6.4): its record names the
//! provider that served it.

use std::sync::Arc;

use pretty_assertions::assert_eq;
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiProvider, MockAiProvider, MockAiProviderOptions, Tools};
use serde_json::json;

use super::support::Room;

#[tokio::test]
async fn a_turn_the_fallback_answered_names_the_fallback_in_its_record() {
    let primary = ScriptedProvider::new([ScriptedRound::answering("")
        .refusing(503, "overloaded")
        .retrying()]);
    let fallback = MockAiProvider::new(MockAiProviderOptions {
        responses: vec!["from fallback".to_owned()],
        ..MockAiProviderOptions::default()
    });
    let names = (
        json!(fallback.model_name()),
        json!(AiProvider::name(&fallback)),
    );
    let room = Room::equipped(primary, Tools::default(), |options| {
        roomkit_ai::AiChannelOptions {
            fallback_provider: Some(Arc::new(fallback)),
            ..options
        }
    })
    .await;
    room.says("Hello?").await;
    let rows = room.timeline().await;
    let answer = rows
        .iter()
        .find(|row| row.metadata.contains_key("loop_end_reason"))
        .unwrap();
    assert_eq!(
        (
            answer.metadata["ai_model"].clone(),
            answer.metadata["ai_provider"].clone()
        ),
        names
    );
    room.kit.close().await.unwrap();
}
