//! What a registered channel says serves it (roomkit-specs
//! `conformance/README.md`, Calls, `channel.provider`): an AI channel's
//! provider, by its name, and every channel's `provider_name`, read from the
//! channel as it is registered, the kit holding it by its object then.
//! Python's `_calls._channel_provider`.

use roomkit::Channel;
use roomkit_ai::AiProvider;
use serde_json::{Value, json};

use crate::agents::Scripted;
use crate::calls::{Called, Unreadable, field};
use crate::run::{Run, lock};

pub(crate) fn arguments(call: &str) -> Option<&'static [&'static str]> {
    (call == "channel.provider").then_some(&["channel_id"])
}

/// What the scripted agent says serves it: the name of the provider it was
/// given, none for one a router serves, and its `provider_name`.
pub(crate) fn of_agent(scripted: &Scripted) -> Value {
    let (provider, name) = match scripted {
        Scripted::Channel(channel) => (
            channel
                .provider()
                .map(|provider| provider.name().to_owned()),
            Channel::provider_name(channel.as_ref()),
        ),
        Scripted::Agent(agent) => (
            agent.provider().map(|provider| provider.name().to_owned()),
            Channel::provider_name(agent.as_ref()),
        ),
        Scripted::RoutedChannel(channel) => (
            channel
                .provider()
                .map(|provider| provider.name().to_owned()),
            Channel::provider_name(channel.as_ref()),
        ),
        Scripted::RoutedAgent(agent) => (
            agent.provider().map(|provider| provider.name().to_owned()),
            Channel::provider_name(agent.as_ref()),
        ),
    };
    json!({ "provider": provider, "provider_name": name })
}

/// What a channel that is no AI channel says serves it: no provider, and
/// its `provider_name`.
pub(crate) fn of_channel(channel: &impl Channel) -> Value {
    json!({ "provider": null, "provider_name": channel.provider_name() })
}

/// The `channel.provider` call: what the channel it names said as it was
/// registered.
pub(crate) fn call(run: &Run, spec: &Value) -> Result<Called, Unreadable> {
    let id: String = field(spec, "channel_id")?;
    let read = lock(&run.channel_providers).get(&id).cloned();
    Ok(Ok(read.ok_or_else(|| {
        format!("channel.provider: no registered channel {id}")
    })?))
}
