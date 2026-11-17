use super::*;
use roomkit_ai::{AiChannelTurnConfig, Skill, SkillMetadata, SkillRegistry};
use std::sync::Arc;

// Python: test_planning_skills.py, actual public provider declarations.
#[tokio::test]
async fn a_turn_registry_keeps_skill_declarations_before_planning() {
    for streaming in [false, true] {
        let skills = SkillRegistry::default();
        skills
            .add(Skill {
                metadata: SkillMetadata::new("helper", "Help"),
                instructions: "Help.".into(),
                path: ".".into(),
            })
            .unwrap();
        let configure = move |_, _| {
            let config = AiChannelTurnConfig {
                skills: Some(skills.clone()),
                ..Default::default()
            };
            let response: std::pin::Pin<
                Box<
                    dyn Future<Output = Result<Option<AiChannelTurnConfig>, roomkit::ChannelError>>
                        + Send,
                >,
            > = Box::pin(async move { Ok(Some(config)) });
            response
        };
        let model = model(vec![AiResponse::new("Done.")], streaming);
        let channel = AiChannel::new(
            "agent".parse().unwrap(),
            model.clone(),
            AiChannelOptions {
                skills: Some(SkillRegistry::default()),
                enable_planning: true,
                config_provider: Some(Arc::new(configure)),
                ..Default::default()
            },
        )
        .unwrap();
        let kit = setup_channel(channel, &["r"], RoomKitOptions::default()).await;
        say(&kit, "r", "Plan.").await;
        let calls = model.calls();
        let tools = &calls.first().unwrap().tools;
        let at = |name| tools.iter().position(|tool| tool.name == name).unwrap();
        assert!(at("activate_skill") < at("read_skill_reference"));
        assert!(at("read_skill_reference") < at("plan_tasks"));
        kit.close().await.unwrap();
    }
}
