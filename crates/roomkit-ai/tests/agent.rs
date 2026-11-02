//! Python's `Agent`: an AI channel with an identity, its block written into
//! its turns' system prompt (the prompt itself is the conformance
//! scenario `orchestration/an-agent-s-turn-prompt-ends-with-its-identity`).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use roomkit::Channel;
use roomkit::model::ChannelId;
use roomkit_ai::{
    Agent, AgentOptions, BuildIdentityBlockOptions, MockAiProvider, MockAiProviderOptions,
};

fn agent(options: AgentOptions) -> Agent<MockAiProvider> {
    let id = ChannelId::new("agent-test").unwrap();
    Agent::new(
        id,
        MockAiProvider::new(MockAiProviderOptions::default()),
        options,
    )
    .unwrap()
}

fn in_language(language: &str) -> BuildIdentityBlockOptions {
    BuildIdentityBlockOptions {
        language: Some(language.into()),
    }
}

// Python: tests/test_channels/test_agent.py::TestAgentBasics::test_fields_stored
#[test]
fn test_fields_stored() {
    let agent = agent(AgentOptions {
        role: Some("Triage".into()),
        description: Some("Routes callers".into()),
        scope: Some("Financial only".into()),
        voice: Some("voice-id-123".into()),
        greeting: Some("Welcome! How can I help?".into()),
        language: Some("fr".into()),
        ..AgentOptions::default()
    });
    assert_eq!(agent.role(), Some("Triage"));
    assert_eq!(agent.description(), Some("Routes callers"));
    assert_eq!(agent.scope(), Some("Financial only"));
    assert_eq!(agent.voice(), Some("voice-id-123"));
    assert_eq!(agent.greeting(), Some("Welcome! How can I help?"));
    assert_eq!(agent.language(), Some("fr"));
}

// Python: tests/test_channels/test_agent.py::TestIdentityBlock::test_identity_block_all_fields
// and test_identity_block_role_only
#[test]
fn test_identity_block_all_fields() {
    let all = agent(AgentOptions {
        role: Some("Triage".into()),
        description: Some("Routes callers".into()),
        scope: Some("Financial only".into()),
        ..AgentOptions::default()
    });
    assert_eq!(
        all.build_identity_block(BuildIdentityBlockOptions::default())
            .as_deref(),
        Some(
            "\n--- Agent Identity ---\nRole: Triage\nDescription: Routes callers\nScope: Financial only"
        ),
    );
    let role = agent(AgentOptions {
        role: Some("Triage receptionist".into()),
        ..AgentOptions::default()
    });
    assert_eq!(
        role.build_identity_block(BuildIdentityBlockOptions::default())
            .as_deref(),
        Some("\n--- Agent Identity ---\nRole: Triage receptionist"),
    );
}

// Python: tests/test_channels/test_agent.py::TestIdentityBlock::test_no_metadata_returns_none,
// test_voice_not_in_identity_block and TestAgentBasics::test_greeting_not_in_identity_block
#[test]
fn test_no_metadata_returns_none() {
    for options in [
        AgentOptions::default(),
        AgentOptions {
            voice: Some("voice-id-123".into()),
            greeting: Some("Hello there!".into()),
            name: Some("Ana".into()),
            purpose: Some("Answers billing questions".into()),
            ..AgentOptions::default()
        },
    ] {
        assert_eq!(
            agent(options).build_identity_block(BuildIdentityBlockOptions::default()),
            None
        );
    }
}

// Python: tests/test_channels/test_agent.py::TestLanguage::test_language_in_identity_block,
// test_language_only_identity_block and test_language_override_in_identity_block
#[test]
fn test_language_override_in_identity_block() {
    let english = agent(AgentOptions {
        role: Some("Triage".into()),
        language: Some("English".into()),
        ..AgentOptions::default()
    });
    assert_eq!(
        english
            .build_identity_block(BuildIdentityBlockOptions::default())
            .as_deref(),
        Some("\n--- Agent Identity ---\nRole: Triage\nLanguage: Always respond in English"),
    );
    assert_eq!(
        english
            .build_identity_block(in_language("French"))
            .as_deref(),
        Some("\n--- Agent Identity ---\nRole: Triage\nLanguage: Always respond in French"),
    );
    let spanish = agent(AgentOptions {
        language: Some("Spanish".into()),
        ..AgentOptions::default()
    });
    assert_eq!(
        spanish
            .build_identity_block(BuildIdentityBlockOptions::default())
            .as_deref(),
        Some("\n--- Agent Identity ---\nLanguage: Always respond in Spanish"),
    );
}

// Python: tests/test_channels/test_agent.py::TestIdentityInPromptOff::test_the_public_block_is_none_whatever_the_language,
// test_the_fields_stay_readable and test_on_by_default
#[test]
fn test_the_public_block_is_none_whatever_the_language() {
    let off = agent(AgentOptions {
        role: Some("Advisor".into()),
        scope: Some("Personal finance".into()),
        language: Some("French".into()),
        identity_in_prompt: false,
        ..AgentOptions::default()
    });
    assert_eq!(
        off.build_identity_block(BuildIdentityBlockOptions::default()),
        None
    );
    assert_eq!(off.build_identity_block(in_language("German")), None);
    assert_eq!(
        (off.role(), off.scope(), off.language()),
        (Some("Advisor"), Some("Personal finance"), Some("French")),
    );
    let on = agent(AgentOptions {
        role: Some("Advisor".into()),
        ..AgentOptions::default()
    });
    assert!(
        on.build_identity_block(BuildIdentityBlockOptions::default())
            .is_some()
    );
}

// Python: channels/agent.py::Agent.declaration, the identity it declares
#[test]
fn an_agent_declares_its_identity() {
    let agent = agent(AgentOptions {
        name: Some("Ana".into()),
        role: Some("Triage".into()),
        greeting: Some("Hello".into()),
        auto_greet: false,
        ..AgentOptions::default()
    });
    let declaration = agent.declaration();
    let identity = declaration.identity.unwrap();
    assert_eq!(identity.name.as_deref(), Some("Ana"));
    assert_eq!(identity.role.as_deref(), Some("Triage"));
    assert_eq!(identity.greeting.as_deref(), Some("Hello"));
    assert!(!identity.auto_greet);
    assert!(
        declaration.takes_owner_tools,
        "the AI channel's declaration"
    );
    assert_eq!(agent.channel().declaration().identity, None);
}
