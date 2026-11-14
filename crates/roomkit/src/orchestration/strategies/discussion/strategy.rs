//! The Discussion orchestration strategy (RFC §19.7.5): Python's
//! `orchestration/strategies/discussion/strategy.py`.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use roomkit_core::model::{ChannelId, ChannelType, RoomId};
use roomkit_core::rules::casefold;
use roomkit_core::rules::discussion::names::{is_blank, is_handle, name_key};
use roomkit_core::rules::python_repr;

use crate::channel::BoxFuture;
use crate::error::RoomKitError;
use crate::kit::{GetRoomOptions, RoomKit, strategy_address};
use crate::orchestration::{Orchestration, StrategyAgent, StrategyFuture};
use crate::store::ConversationStore;

use super::config::config_of;
use super::dispatch::DispatchPolicy;
use super::room::{Install, NAMES, SILENT};

/// `done(room_id)`: whether the discussion is over. Python's `DoneFn`, sync
/// or async; here always a future.
pub type DoneFn = Arc<dyn Fn(RoomId) -> BoxFuture<'static, bool> + Send + Sync>;

/// What [`Discussion::new`] takes beside the agents: Python's
/// `Discussion(agents, *, handles=None, people=None, addressed_only=False,
/// everyone=None, max_turns=None, max_depth=None, done=None,
/// silent_token='(silent)', rules=None, dispatch=None,
/// dispatch_timeout=5.0)`.
#[derive(Clone)]
pub struct DiscussionOptions {
    /// The name each agent is addressed by (`@handle`), by channel id. An
    /// agent it leaves out is addressed by its channel id when that can be a
    /// name, else by its `name` kept to a name's characters (`Research Desk`
    /// gives `ResearchDesk`): an `agent:<uuid>` id cannot be one. A handle is
    /// letters, digits, `_ . -`, at most 32, with no final `.`, and not
    /// `all`; two agents' handles differ ignoring case. Notes, the transcript
    /// and the dispatch policy present agents by their handles; the queue,
    /// `addressed_to` and every hook keep channel ids.
    pub handles: Option<BTreeMap<ChannelId, String>>,
    /// The names agents address people by. `None`: the names of the room's
    /// participants that are neither agents nor bots.
    pub people: Option<Vec<String>>,
    /// A person's message asks only the agents it names; one that names
    /// nobody asks no agent.
    pub addressed_only: bool,
    /// The agents a person's message that answers no one asks, in this order
    /// (the one that should open first). `None`: every agent, in `agents`
    /// order.
    pub everyone: Option<Vec<ChannelId>>,
    /// Turns given in the room's lifetime, after which the discussion is
    /// over. `None`: no bound but `max_depth`.
    pub max_turns: Option<u32>,
    /// The depth limit of the turns it gives, in place of the kit's
    /// `max_chain_depth`. `None`: the kit's.
    pub max_depth: Option<u32>,
    /// `done(room_id)`, checked before each turn: once it holds, the
    /// discussion is over.
    pub done: Option<DoneFn>,
    /// The answer of an agent with nothing to add: `(silent)` by default.
    pub silent_token: String,
    /// How the room works, as each turn's notes give it, in place of the
    /// default (rule 14): `{handle}` in them stands for another agent's
    /// `@handle`, an example the agent can follow, and `{silent}` for
    /// `silent_token`. `None`: the default, which tells the agent to end its
    /// message with the handle of the agent that must act next.
    pub rules: Option<String>,
    /// Decides which agents take a person's message that names none and
    /// answers none, in place of asking `everyone` (rule 18). Not with
    /// `addressed_only`, where such a message asks no agent.
    pub dispatch: Option<Arc<dyn DispatchPolicy>>,
    /// How long a decision may take; past it, or when the policy fails, the
    /// message asks `everyone` it reaches.
    pub dispatch_timeout: Duration,
}

impl Default for DiscussionOptions {
    fn default() -> Self {
        Self {
            handles: None,
            people: None,
            addressed_only: false,
            everyone: None,
            max_turns: None,
            max_depth: None,
            done: None,
            silent_token: "(silent)".to_owned(),
            rules: None,
            dispatch: None,
            dispatch_timeout: Duration::from_secs(5),
        }
    }
}

impl fmt::Debug for DiscussionOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DiscussionOptions")
            .field("handles", &self.handles)
            .field("people", &self.people)
            .field("addressed_only", &self.addressed_only)
            .field("everyone", &self.everyone)
            .field("max_turns", &self.max_turns)
            .field("max_depth", &self.max_depth)
            .field("done", &self.done.is_some())
            .field("silent_token", &self.silent_token)
            .field("rules", &self.rules)
            .field("dispatch", &self.dispatch.is_some())
            .field("dispatch_timeout", &self.dispatch_timeout)
            .finish()
    }
}

/// Several agents and people hold one conversation; one agent speaks at a
/// time (RFC §19.7.5): Python's `roomkit.Discussion`.
///
/// Any agent may address any other by `@handle`, a person may address any
/// agent, and who speaks next follows from who was addressed: the strategy
/// queues the agents each message asks for and gives them their turns one at
/// a time, each turn reading the room as it is when it starts. The room is
/// the discussion's: its intelligence channels are the discussion's agents
/// and no other, and no router or other strategy shares it. Text only in this
/// version.
///
/// ```
/// use std::collections::BTreeMap;
///
/// use roomkit::model::ChannelId;
/// use roomkit::orchestration::DiscussionOptions;
/// use roomkit::Discussion;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let dev = ChannelId::new("dev")?;
/// let keyed = ChannelId::new("agent:9b1c")?;
/// let discussion = Discussion::new(
///     vec![dev.clone().into(), keyed.clone().into()],
///     DiscussionOptions {
///         handles: Some(BTreeMap::from([(keyed.clone(), "counsel".to_owned())])),
///         everyone: Some(vec![dev.clone()]),
///         ..DiscussionOptions::default()
///     },
/// )?;
/// assert_eq!(discussion.handles()[&keyed], "counsel");
/// assert_eq!(discussion.handles()[&dev], "dev");
/// # Ok(())
/// # }
/// ```
pub struct Discussion {
    agents: Vec<StrategyAgent>,
    /// The handles the host gave, by channel id.
    given_handles: BTreeMap<ChannelId, String>,
    /// `None` while an agent named by an id that is no handle waits for the
    /// name its declaration gives, which only the kit's install reads.
    handles: Mutex<Option<BTreeMap<ChannelId, String>>>,
    pub(super) people: Option<Vec<String>>,
    pub(super) addressed_only: bool,
    pub(super) everyone: Option<Vec<ChannelId>>,
    pub(super) max_turns: Option<u32>,
    pub(super) max_depth: Option<u32>,
    pub(super) done: Option<DoneFn>,
    pub(super) silent_token: String,
    pub(super) rules: Option<String>,
    pub(super) dispatch: Option<Arc<dyn DispatchPolicy>>,
    pub(super) dispatch_timeout: Duration,
}

impl Discussion {
    /// The discussion of `agents`, AI channels or Agents with distinct
    /// channel ids, each live or named by the id of one the kit holds.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Value`] for what Python's constructor refuses: no
    /// agent, two agents under one channel id, an `everyone` or a `handles`
    /// naming a channel that is no agent, a `max_turns` under 1, a `max_depth`
    /// under 2, a blank `silent_token` or `rules`, a handle that cannot name
    /// an agent, an agent without one, two handles reading alike, a
    /// `dispatch` with `addressed_only`, or a zero `dispatch_timeout`.
    /// [`RoomKitError::Type`] for a channel given live that is no AI channel.
    pub fn new(
        agents: Vec<StrategyAgent>,
        options: DiscussionOptions,
    ) -> Result<Self, RoomKitError> {
        let DiscussionOptions {
            handles,
            people,
            addressed_only,
            everyone,
            max_turns,
            max_depth,
            done,
            silent_token,
            rules,
            dispatch,
            dispatch_timeout,
        } = options;
        check_agents(&agents)?;
        check_everyone(&agents, everyone.as_deref())?;
        check_bounds(max_turns, max_depth)?;
        check_texts(&silent_token, rules.as_deref())?;
        check_dispatch(dispatch.is_some(), dispatch_timeout, addressed_only)?;
        let given = handles.unwrap_or_default();
        check_given(&agents, &given)?;
        // An agent named by an id that is no handle is addressed by the name
        // its declaration gives, which only the kit's install reads.
        let named_later = agents.iter().any(|agent| {
            agent.live().is_none()
                && !given.contains_key(agent.channel_id())
                && !is_handle(agent.channel_id().as_str())
        });
        let handles = if named_later {
            None
        } else {
            Some(read_handles(&agents, &given, &names(&agents))?)
        };
        Ok(Self {
            agents,
            given_handles: given,
            handles: Mutex::new(handles),
            people,
            addressed_only,
            everyone,
            max_turns,
            max_depth,
            done,
            silent_token,
            rules,
            dispatch,
            dispatch_timeout,
        })
    }

    /// The agents of the discussion, each live or named by the id of one the
    /// kit holds (RFC §10.6.2).
    #[must_use]
    pub fn agents(&self) -> Vec<StrategyAgent> {
        self.agents.clone()
    }

    /// The name each agent is addressed by, by channel id; an agent named by
    /// an id that is no handle has one once the discussion is installed.
    #[must_use]
    pub fn handles(&self) -> BTreeMap<ChannelId, String> {
        self.read_handles().clone().unwrap_or_default()
    }

    fn read_handles(&self) -> MutexGuard<'_, Option<BTreeMap<ChannelId, String>>> {
        self.handles.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Each agent's handle once the kit's `names` are read: the install's
    /// reading of what the constructor could not read, the handles the host
    /// gave among it.
    fn handles_named(
        &self,
        names: &HashMap<&ChannelId, String>,
    ) -> Result<BTreeMap<ChannelId, String>, RoomKitError> {
        read_handles(&self.agents, &self.given_handles, names)
    }
}

impl fmt::Debug for Discussion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Discussion")
            .field("agents", &self.agents)
            .field("given_handles", &self.given_handles)
            .field("handles", &*self.read_handles())
            .field("people", &self.people)
            .field("addressed_only", &self.addressed_only)
            .field("everyone", &self.everyone)
            .field("max_turns", &self.max_turns)
            .field("max_depth", &self.max_depth)
            .field("done", &self.done.is_some())
            .field("silent_token", &self.silent_token)
            .field("rules", &self.rules)
            .field("dispatch", &self.dispatch.is_some())
            .field("dispatch_timeout", &self.dispatch_timeout)
            .finish()
    }
}

impl Discussion {
    /// Gives the room to the discussion (rule 1): its agents attached by the
    /// kit, its stored queue read, its turns started. Python's `install`.
    async fn install_in<S: ConversationStore>(
        &self,
        kit: &RoomKit<S>,
        room_id: &RoomId,
    ) -> Result<(), RoomKitError> {
        let owner = strategy_address::<S>(self);
        kit.claim_for(room_id, owner)?;
        kit.register_strategy_agents(&self.agents)?;
        if self.read_handles().is_none() {
            let held = self.named_by_the_kit(kit);
            let named =
                self.handles_named(&held.iter().map(|(id, name)| (id, name.clone())).collect())?;
            *self.read_handles() = Some(named);
        }
        let room = kit.get_room(room_id, GetRoomOptions::default()).await?;
        let install = Install {
            config: config_of(self, kit.inner.config().max_chain_depth),
            done: self.done.clone(),
            dispatch: self
                .dispatch
                .clone()
                .map(|policy| (policy, self.dispatch_timeout)),
            owner,
        };
        kit.install_discussion(room, install).await
    }

    /// Each agent's name, an id's from the channel the kit holds under it.
    fn named_by_the_kit<S: ConversationStore>(&self, kit: &RoomKit<S>) -> Vec<(ChannelId, String)> {
        self.agents
            .iter()
            .filter_map(|agent| {
                let name = match agent.live() {
                    Some(live) => live.channel.name(),
                    None => kit
                        .inner
                        .channel(agent.channel_id())
                        .and_then(|channel| channel.name()),
                };
                Some((agent.channel_id().clone(), name?))
            })
            .collect()
    }

    /// Gives the room back to its policy: the turns stopped, the queue
    /// dropped. Python's `uninstall`.
    async fn uninstall_in<S: ConversationStore>(
        &self,
        kit: &RoomKit<S>,
        room_id: &RoomId,
    ) -> Result<(), RoomKitError> {
        // Until its configuration is forgotten, its own driver's turn under
        // way reads it stored: this process follows it no more.
        let leaving = kit.inner.discussions.leaving_room(room_id);
        let Some(running) = kit.inner.discussions.release(room_id) else {
            return Ok(());
        };
        for hook in [SILENT, NAMES] {
            kit.hook_engine().remove_room_hook(room_id, hook);
        }
        let forgotten = running.forget().await;
        drop(leaving);
        forgotten
    }
}

impl<S: ConversationStore> Orchestration<S> for Discussion {
    fn agents(&self) -> Vec<StrategyAgent> {
        self.agents.clone()
    }

    fn install<'call>(
        &'call self,
        kit: &'call RoomKit<S>,
        room_id: &'call RoomId,
    ) -> StrategyFuture<'call> {
        Box::pin(self.install_in(kit, room_id))
    }

    fn uninstall<'call>(
        &'call self,
        kit: &'call RoomKit<S>,
        room_id: &'call RoomId,
    ) -> StrategyFuture<'call> {
        Box::pin(self.uninstall_in(kit, room_id))
    }
}

/// Refuses a dispatch policy with `addressed_only`, which gives a message
/// naming nobody to no agent, and a bound that is not positive.
fn check_dispatch(
    dispatch: bool,
    timeout: Duration,
    addressed_only: bool,
) -> Result<(), RoomKitError> {
    if dispatch && addressed_only {
        return Err(RoomKitError::Value(
            "A dispatch policy decides who takes a message that names nobody, which \
             addressed_only gives to no agent: use one or the other"
                .to_owned(),
        ));
    }
    if timeout.is_zero() {
        return Err(RoomKitError::Value(
            "dispatch_timeout must be positive".to_owned(),
        ));
    }
    Ok(())
}

/// Refuses no agent, a channel given live that is no AI channel, and two
/// agents under one channel id.
fn check_agents(agents: &[StrategyAgent]) -> Result<(), RoomKitError> {
    if agents.is_empty() {
        return Err(RoomKitError::Value(
            "A discussion needs at least one agent".to_owned(),
        ));
    }
    for agent in agents {
        if agent
            .live()
            .is_some_and(|live| live.config.channel_type != ChannelType::AI)
        {
            return Err(RoomKitError::Type(format!(
                "{} is neither an AI channel nor an ACP agent channel, nor the id of one: a \
                 discussion holds those only",
                python_repr(agent.channel_id().as_str())
            )));
        }
    }
    let ids: Vec<&ChannelId> = agents.iter().map(StrategyAgent::channel_id).collect();
    let distinct = ids
        .iter()
        .enumerate()
        .all(|(at, id)| !ids.iter().take(at).any(|before| before == id));
    if distinct {
        Ok(())
    } else {
        Err(RoomKitError::Value(
            "A discussion's agents need distinct channel ids".to_owned(),
        ))
    }
}

/// Refuses an `everyone` naming a channel that is no agent.
fn check_everyone(
    agents: &[StrategyAgent],
    everyone: Option<&[ChannelId]>,
) -> Result<(), RoomKitError> {
    let unknown = not_agents(agents, everyone.unwrap_or_default());
    if unknown.is_empty() {
        return Ok(());
    }
    Err(RoomKitError::Value(format!(
        "everyone names channels that are not agents: {}",
        ids_repr(&unknown)
    )))
}

/// Refuses a `max_turns` under 1 and a `max_depth` under 2.
fn check_bounds(max_turns: Option<u32>, max_depth: Option<u32>) -> Result<(), RoomKitError> {
    if max_turns.is_some_and(|turns| turns < 1) {
        return Err(RoomKitError::Value(
            "max_turns must be at least 1".to_owned(),
        ));
    }
    // A person's message is at depth 0 and the first turn answering it at 1.
    if max_depth.is_some_and(|depth| depth < 2) {
        return Err(RoomKitError::Value(
            "max_depth must be at least 2".to_owned(),
        ));
    }
    Ok(())
}

/// Refuses a blank `silent_token` and blank `rules`.
fn check_texts(silent_token: &str, rules: Option<&str>) -> Result<(), RoomKitError> {
    if is_blank(silent_token) {
        return Err(RoomKitError::Value(
            "silent_token must not be blank".to_owned(),
        ));
    }
    if rules.is_some_and(is_blank) {
        return Err(RoomKitError::Value(
            "rules must not be blank: None gives the default".to_owned(),
        ));
    }
    Ok(())
}

/// Refuses `given` handles naming a channel that is no agent.
fn check_given(
    agents: &[StrategyAgent],
    given: &BTreeMap<ChannelId, String>,
) -> Result<(), RoomKitError> {
    let named: Vec<ChannelId> = given.keys().cloned().collect();
    let unknown = not_agents(agents, &named);
    if unknown.is_empty() {
        return Ok(());
    }
    Err(RoomKitError::Value(format!(
        "handles names channels that are not agents: {}",
        ids_repr(&unknown)
    )))
}

/// The ids among `named` that name none of `agents`.
fn not_agents<'a>(agents: &[StrategyAgent], named: &'a [ChannelId]) -> Vec<&'a ChannelId> {
    named
        .iter()
        .filter(|id| !agents.iter().any(|agent| agent.channel_id() == *id))
        .collect()
}

/// `ids` as Python's `repr` of a list of `str` writes them.
fn ids_repr(ids: &[&ChannelId]) -> String {
    let written: Vec<String> = ids.iter().map(|id| python_repr(id.as_str())).collect();
    format!("[{}]", written.join(", "))
}

/// Each live agent's name, by channel id; an agent named by its id has none
/// here, the kit's install reading the registered channel's local name.
fn names(agents: &[StrategyAgent]) -> HashMap<&ChannelId, String> {
    agents
        .iter()
        .filter_map(|agent| Some((agent.channel_id(), agent.live()?.channel.name()?)))
        .collect()
}

/// Each agent's handle, by channel id: the one `given`, else its channel id,
/// else its name (`names`) kept to a name's characters, whichever can name it
/// (RFC §19.7.5 rule 4). A [`RoomKitError::Value`] when `given` gives a
/// handle that cannot name an agent, an agent has no handle, or two agents'
/// handles read alike.
fn read_handles(
    agents: &[StrategyAgent],
    given: &BTreeMap<ChannelId, String>,
    names: &HashMap<&ChannelId, String>,
) -> Result<BTreeMap<ChannelId, String>, RoomKitError> {
    let mut handles = BTreeMap::new();
    for agent in agents {
        let channel_id = agent.channel_id();
        let handle = handle_of(
            channel_id,
            given.get(channel_id).map(String::as_str),
            names.get(channel_id).map(String::as_str),
        )?;
        handles.insert(channel_id.clone(), handle);
    }
    let mut seen: Vec<(String, &ChannelId)> = Vec::new();
    for agent in agents {
        let channel_id = agent.channel_id();
        let handle = handles.get(channel_id).map_or("", String::as_str);
        let key = casefold(handle);
        if let Some((_, other)) = seen.iter().find(|(known, _)| *known == key) {
            return Err(RoomKitError::Value(format!(
                "{} and {} share the handle @{handle}: give one of them another with handles=",
                python_repr(other.as_str()),
                python_repr(channel_id.as_str()),
            )));
        }
        seen.push((key, channel_id));
    }
    Ok(handles)
}

/// The handle the agent `channel_id` is addressed by: `given`, else its
/// channel id, else its `name` kept to a name's characters.
fn handle_of(
    channel_id: &ChannelId,
    given: Option<&str>,
    name: Option<&str>,
) -> Result<String, RoomKitError> {
    if let Some(given) = given {
        if is_handle(given) {
            return Ok(given.to_owned());
        }
        return Err(RoomKitError::Value(format!(
            "handles gives {} {}, which cannot name an agent: letters, digits, '_ . -', at most \
             32, no final '.', not 'all'",
            python_repr(channel_id.as_str()),
            python_repr(given),
        )));
    }
    if is_handle(channel_id.as_str()) {
        return Ok(channel_id.to_string());
    }
    let derived = name.map(name_key).unwrap_or_default();
    if is_handle(&derived) {
        return Ok(derived);
    }
    Err(RoomKitError::Value(format!(
        "{} cannot be addressed by its channel id and has no name that can: give it a handle \
         with handles=",
        python_repr(channel_id.as_str()),
    )))
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::testing::{RecordingTransport, RecordingTransportOptions};

    fn id(agent: &str) -> ChannelId {
        ChannelId::new(agent).unwrap()
    }

    // Python: tests/orchestration/test_strategy_discussion.py::test_a_discussion_is_checked_when_built
    #[test]
    fn a_discussion_refuses_a_channel_given_live_that_is_no_ai_channel() {
        let error = Discussion::new(
            vec![StrategyAgent::channel(RecordingTransport::new(
                id("x"),
                ChannelType::SMS,
                RecordingTransportOptions::default(),
            ))],
            DiscussionOptions::default(),
        )
        .unwrap_err();
        assert!(
            matches!(&error, RoomKitError::Type(message)
                if message.contains("neither an AI channel nor an ACP agent channel")),
            "{error:?}"
        );
    }

    // Python: strategy.py install, `_handles(self._agents, self._given_handles,
    // _names(self._agents, kit))`.
    #[test]
    fn the_handles_the_host_gave_wait_for_the_install_with_the_rest() {
        let agents = ["dev", "agent:1", "agent:2"].map(id);
        let discussion = Discussion::new(
            agents.iter().cloned().map(StrategyAgent::from).collect(),
            DiscussionOptions {
                handles: Some(BTreeMap::from([
                    (id("dev"), "ops".to_owned()),
                    (id("agent:1"), "counsel".to_owned()),
                ])),
                ..DiscussionOptions::default()
            },
        )
        .unwrap();
        assert_eq!(discussion.handles(), BTreeMap::new());

        let desk = id("agent:2");
        let names = HashMap::from([(&desk, "Research Desk".to_owned())]);
        assert_eq!(
            discussion.handles_named(&names).unwrap(),
            BTreeMap::from([
                (id("dev"), "ops".to_owned()),
                (id("agent:1"), "counsel".to_owned()),
                (id("agent:2"), "ResearchDesk".to_owned()),
            ])
        );
    }

    // Python: strategy.py _handles, every handle read before any is compared.
    #[test]
    fn an_agent_without_a_handle_is_refused_before_two_alike() {
        let agents: Vec<StrategyAgent> = ["DEV", "dev", "agent:1"]
            .iter()
            .map(|agent| id(agent).into())
            .collect();
        let error = read_handles(&agents, &BTreeMap::new(), &HashMap::new()).unwrap_err();
        assert!(
            matches!(&error, RoomKitError::Value(message)
                if message.starts_with("'agent:1' cannot be addressed")),
            "{error:?}"
        );
    }

    /// A refusal's text is Python's.
    #[test]
    fn a_refusal_says_what_python_s_says() {
        let refused = |agents: &[&str], options: DiscussionOptions| {
            let agents = agents.iter().map(|agent| id(agent).into()).collect();
            match Discussion::new(agents, options) {
                Err(RoomKitError::Value(message)) => message,
                other => panic!("{other:?}"),
            }
        };
        let everyone = DiscussionOptions {
            everyone: Some(vec![id("b")]),
            ..DiscussionOptions::default()
        };
        assert_eq!(
            refused(&["a"], everyone),
            "everyone names channels that are not agents: ['b']"
        );
        let given = DiscussionOptions {
            handles: Some(BTreeMap::from([(id("a"), "all".to_owned())])),
            ..DiscussionOptions::default()
        };
        assert_eq!(
            refused(&["a"], given),
            "handles gives 'a' 'all', which cannot name an agent: letters, digits, '_ . -', at \
             most 32, no final '.', not 'all'"
        );
        let alike = DiscussionOptions {
            handles: Some(BTreeMap::from([(id("b"), "A".to_owned())])),
            ..DiscussionOptions::default()
        };
        assert_eq!(
            refused(&["a", "b"], alike),
            "'a' and 'b' share the handle @A: give one of them another with handles="
        );
    }

    #[test]
    fn a_discussion_keeps_what_it_is_given_and_python_s_defaults() {
        let plain = Discussion::new(vec![id("a").into()], DiscussionOptions::default()).unwrap();
        assert_eq!(plain.people, None);
        assert!(!plain.addressed_only);
        assert_eq!(plain.everyone, None);
        assert_eq!(plain.max_turns, None);
        assert_eq!(plain.max_depth, None);
        assert!(plain.done.is_none());
        assert_eq!(plain.silent_token, "(silent)");
        assert_eq!(plain.rules, None);

        let done: DoneFn = Arc::new(|_room| Box::pin(async { true }));
        let full = Discussion::new(
            vec![id("a").into()],
            DiscussionOptions {
                people: Some(vec!["Ana".to_owned()]),
                addressed_only: true,
                done: Some(done),
                ..DiscussionOptions::default()
            },
        )
        .unwrap();
        assert_eq!(full.people, Some(vec!["Ana".to_owned()]));
        assert!(full.addressed_only);
        assert!(full.done.is_some());
    }
}
