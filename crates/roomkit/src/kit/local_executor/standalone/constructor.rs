//! The two host declarations of a worker: advertised names or a local predicate.
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::{Arc, Mutex};

use roomkit_core::model::ChannelId;

use super::{Built, Generations, HostPredicate, Pool, Standalone};
use crate::channel::{BoxFuture, DynChannel};
use crate::turns::executor;
use crate::{
    Channel, InMemoryStore, RoomKit, RoomKitError, RoomKitOptions, TurnExecutor, TurnTimeouts,
};

/// The executor's advertised identity, tenant/host scope, and liveness bounds.
#[derive(Clone, Debug)]
pub struct TurnExecutorOptions {
    /// Exact channel IDs and terminal-wildcard prefixes. Omit for a local-only open declaration.
    pub hosts: Option<BTreeSet<String>>,
    /// Exact organizations, or "*" for all. Omit to serve only rooms without an organization.
    pub organizations: Option<BTreeSet<String>>,
    /// The identity advertised to owners. Defaults to a freshly generated executor ID.
    pub executor_id: Option<String>,
    /// The heartbeat bounds for work on this executor.
    pub timeouts: TurnTimeouts,
    /// Application error types this standalone worker can reconstruct and report.
    pub error_classes: crate::turns::ErrorClasses,
    /// The provider that records the worker's turns and hooks.
    pub telemetry: Option<Arc<dyn crate::telemetry::TelemetryProvider>>,
    /// Build, cache and close channels. Disable for a host-owned registry lookup on every use.
    pub owns_channels: bool,
    /// Maximum idle and running builds to retain. Active work can temporarily exceed the bound.
    pub max_channels: Option<usize>,
}

impl Default for TurnExecutorOptions {
    fn default() -> Self {
        Self {
            hosts: None,
            organizations: None,
            executor_id: None,
            timeouts: TurnTimeouts::default(),
            error_classes: crate::turns::ErrorClasses::default(),
            telemetry: None,
            owns_channels: true,
            max_channels: None,
        }
    }
}

impl TurnExecutor<InMemoryStore> {
    /// Construct an independent executor whose factory builds a channel the first
    /// time work needs it. With `owns_channels` enabled, concurrent turns of the same
    /// channel share the build. A host-owned registry supplies its channel on each use.
    ///
    /// ```
    /// use roomkit::model::ChannelId;
    /// use roomkit::testing::{EchoAgent, EchoAgentOptions};
    /// use roomkit::{TurnExecutor, TurnExecutorOptions};
    ///
    /// let worker = TurnExecutor::new(
    ///     |id| std::future::ready(Ok(Some(EchoAgent::new(id, "Hello", EchoAgentOptions::default())))),
    ///     TurnExecutorOptions {
    ///         hosts: Some(["support-*".to_owned()].into()),
    ///         max_channels: Some(8),
    ///         ..Default::default()
    ///     },
    /// ).unwrap();
    /// assert!(worker.hosts_channel(&ChannelId::new("support-ca").unwrap()));
    /// assert!(worker.serves(None));
    /// ```
    ///
    /// # Errors
    /// Refuses a misplaced host wildcard, an empty organization declaration or a
    /// wildcard in an organization ID other than "*".
    pub fn new<F, Fut, C>(
        build_channel: F,
        options: TurnExecutorOptions,
    ) -> Result<Self, RoomKitError>
    where
        F: Fn(ChannelId) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<C>, RoomKitError>> + Send + 'static,
        C: Channel,
    {
        Self::create(build_channel, options, None)
    }

    /// Construct a worker with a dynamic, process-local host predicate.
    /// Its host list cannot be advertised to a distributed owner.
    ///
    /// # Errors
    /// Returns the declaration errors of [`Self::new`], including combining
    /// a predicate with an explicit list of hosts.
    pub fn new_with_host_predicate<F, Fut, C, P>(
        build_channel: F,
        predicate: P,
        options: TurnExecutorOptions,
    ) -> Result<Self, RoomKitError>
    where
        F: Fn(ChannelId) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<C>, RoomKitError>> + Send + 'static,
        C: Channel,
        P: Fn(&ChannelId) -> bool + Send + Sync + 'static,
    {
        Self::create(build_channel, options, Some(Box::new(predicate)))
    }

    fn create<F, Fut, C>(
        build_channel: F,
        options: TurnExecutorOptions,
        predicate: Option<Box<HostPredicate>>,
    ) -> Result<Self, RoomKitError>
    where
        F: Fn(ChannelId) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<C>, RoomKitError>> + Send + 'static,
        C: Channel,
    {
        if options.max_channels == Some(0) {
            return Err(RoomKitError::Value(
                "max_channels is at least 1, not 0".into(),
            ));
        }
        if options.hosts.is_some() && predicate.is_some() {
            return Err(RoomKitError::Value(
                "hosts takes a collection or a predicate".into(),
            ));
        }
        if options
            .hosts
            .iter()
            .flatten()
            .any(|name| name.strip_suffix('*').unwrap_or(name).contains('*'))
        {
            return Err(RoomKitError::Value(
                "a host pattern may contain '*' only at its end".into(),
            ));
        }
        if options.organizations.as_ref().is_some_and(|names| {
            names.is_empty() || names.iter().any(|name| name.contains('*') && name != "*")
        }) {
            return Err(RoomKitError::Value(
                "organizations must be nonempty and contain exact IDs or '*'".into(),
            ));
        }
        let kit = RoomKit::new(RoomKitOptions {
            telemetry: options.telemetry.map(Into::into),
            ..RoomKitOptions::default()
        });
        kit.turn_executor()
            .use_wiring(Arc::new(crate::turns::wiring::WorkerWiringEnv::default()));
        kit.inner.turns.set_timeouts(options.timeouts.bounded());
        kit.inner.turns.set_error_classes(options.error_classes);
        let id = options
            .executor_id
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| kit.inner.turns.executor_id().to_owned());
        let registry = kit.clone();
        let owns = options.owns_channels;
        let build_channel = Arc::new(build_channel);
        let factory = move |id: ChannelId| -> BoxFuture<'static, Built> {
            let kit = registry.clone();
            let factory = Arc::clone(&build_channel);
            Box::pin(async move {
                if let Some(channel) = kit.inner.channel(&id) {
                    return Ok(Some(channel));
                }
                let Some(channel) = factory(id.clone())
                    .await
                    .map_err(|error| executor::factory_error(&error))?
                else {
                    return Ok(None);
                };
                if channel.channel_id() != id {
                    return Err(executor::factory_error(&RoomKitError::Value(format!(
                        "the factory returned {} for {id}",
                        channel.channel_id()
                    ))));
                }
                if !owns {
                    let erased: Arc<dyn DynChannel> = Arc::new(channel);
                    return Ok(Some(erased));
                }
                kit.register_channel(channel)
                    .map_err(|error| executor::factory_error(&error))?;
                Ok(kit.inner.channel(&id))
            })
        };
        let generations = Arc::new(Generations::default());
        let pool = options
            .max_channels
            .filter(|_| owns)
            .map(|bound| Pool::new(bound, kit.clone(), Arc::clone(&generations)));
        let standalone = Standalone {
            id,
            hosts: options.hosts,
            organizations: options.organizations,
            factory: Box::new(factory),
            building: Mutex::default(),
            generations,
            pool,
            owns_channels: owns,
            predicate,
        };
        Ok(Self {
            kit,
            standalone: Some(Arc::new(standalone)),
        })
    }
}
