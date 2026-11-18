//! Standalone room owner for turns submitted to an application transport.
mod admissions;
mod calls;
mod inputs;
pub use inputs::TurnClientInputs;
mod output;
mod port;
mod ports;
use port::ClientOwner;
pub(super) mod publications;
mod services;
pub use output::TurnClientOutput;
pub use ports::{OwnerEvents, OwnerHost, OwnerTools};
pub(super) use services::Services;

use super::wire::{HostResult, OwnerMessage, OwnerMessageEnvelope};
use super::{ChannelControl, ChannelResult};
use super::{ErrorClasses, ServedExecutor, SteerAddress, TurnBackendHandle, TurnTimeouts, Turns};
use crate::human_input::PendingInputEvent;
use crate::steering::SteeringDirective;
use crate::telemetry::TelemetryProvider;
use crate::turn::TurnKit;
use crate::{
    InMemoryStore, InputRequestOptions, RoomKit, RoomKitError, RoomKitOptions, SteerOptions,
};
use roomkit_core::model::{ChannelBinding, ChannelId, RoomContext, RoomEvent, RoomId};
use std::fmt;
use std::sync::Arc;

/// The optional services of a standalone room owner (RFC §10.6).
#[derive(Clone, Default)]
pub struct TurnClientOptions {
    /// Executor to serve on first use, if this client has local channels.
    pub executor: Option<Arc<dyn ServedExecutor>>,
    /// Bounds on admission, reads and calls for turns started by this client.
    pub timeouts: TurnTimeouts,
    /// Tools this owner serves its turns.
    pub tools: Option<Arc<dyn OwnerTools>>,
    /// Host verbs and channel history supplied by this owner.
    pub host: Option<Arc<dyn OwnerHost>>,
    /// Destination for events emitted by this owner's turns.
    pub events: Option<Arc<dyn OwnerEvents>>,
    /// Application error types this owner carries and rebuilds.
    pub error_classes: ErrorClasses,
    /// Provider that injects the active span into requests.
    pub telemetry: Option<Arc<dyn TelemetryProvider>>,
}
impl fmt::Debug for TurnClientOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TurnClientOptions")
            .field("timeouts", &self.timeouts)
            .field("executor", &self.executor.as_ref().map(|e| e.executor_id()))
            .field("error_classes", &self.error_classes)
            .finish_non_exhaustive()
    }
}

/// Optional arguments to a standalone owner's turn.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TurnRunOptions {
    /// Whether this turn runs the channel's strategy generation.
    pub strategy_turn: bool,
}

/// A standalone room owner using the supplied backend for its turns and controls.
#[derive(Clone)]
pub struct TurnClient {
    state: Arc<State>,
}

struct State {
    calls: calls::Calls,
    backend: TurnBackendHandle,
    services: Arc<Services>,
    turns: Arc<Turns>,
    // Supplies task ownership and cancellation to the existing frame reader.
    runtime: RoomKit<InMemoryStore>,
}

impl State {
    fn runtime(&self) -> Arc<dyn TurnKit> {
        Arc::new(Arc::downgrade(&self.runtime.inner))
    }
    async fn receive(&self, message: OwnerMessage) -> Option<HostResult> {
        self.turns
            .receive_owner_message(self.runtime(), OwnerMessageEnvelope { message })
            .await
    }
}

impl fmt::Debug for TurnClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TurnClient")
            .field("services", &self.state.services)
            .finish_non_exhaustive()
    }
}

impl TurnClient {
    /// Create the owner. The first run or control registers it with the backend.
    pub fn new(backend: TurnBackendHandle, options: TurnClientOptions) -> Self {
        let timeouts = options.timeouts;
        let services = Arc::new(Services::new(options));
        let application = super::application::Application::new(backend.clone());
        let turns = Arc::new(Turns {
            application: Some(application),
            client: Some(Arc::clone(&services)),
            ..Turns::default()
        });
        turns.set_timeouts(timeouts);
        turns.set_error_classes(services.errors.clone());
        let state = Arc::new(State {
            calls: calls::Calls::default(),
            backend,
            services,
            turns,
            runtime: RoomKit::new(RoomKitOptions::default()),
        });
        if let Some(app) = &state.turns.application {
            app.bind(Arc::new(ClientOwner(Arc::downgrade(&state))));
        }
        Self { state }
    }
    /// The configured turn transport.
    pub fn backend(&self) -> &TurnBackendHandle {
        &self.state.backend
    }
    /// The executor served at first use, when supplied.
    pub fn executor(&self) -> Option<&Arc<dyn ServedExecutor>> {
        self.state.services.executor.as_ref()
    }
    /// The telemetry used for this owner's requests.
    pub fn telemetry(&self) -> Option<&Arc<dyn TelemetryProvider>> {
        self.state.services.telemetry.as_ref()
    }
    /// The application error classes used by this owner.
    pub fn errors(&self) -> &ErrorClasses {
        &self.state.services.errors
    }

    /// Submit one turn. A streaming answer advances when the caller reads its stream.
    ///
    /// # Errors
    /// Returns the executor's error or a transport failure.
    pub async fn run(
        &self,
        event: RoomEvent,
        binding: &ChannelBinding,
        context: RoomContext,
        options: TurnRunOptions,
    ) -> Result<TurnClientOutput, RoomKitError> {
        let answer = self
            .state
            .turns
            .run(
                &self.state.runtime(),
                binding,
                event,
                context,
                options.strategy_turn,
                |_| (),
            )
            .await
            .map_err(|failure| self.errors().rebuild(&failure.into_wire()))?;
        Ok(TurnClientOutput::from_answer(answer, self.errors()))
    }

    /// Enable the named executor hook for the room, in registration order.
    pub fn enable_room_hook(&self, room_id: &RoomId, name: &str) {
        self.state.services.enable(room_id, name);
    }
    /// Disable a named executor hook, returning whether it was enabled.
    pub fn disable_room_hook(&self, room_id: &RoomId, name: &str) -> bool {
        self.state.services.disable(room_id, name)
    }
    /// Return the hooks carried by the next turn in this room.
    pub fn room_hook_names(&self, room_id: &RoomId) -> Vec<String> {
        self.state.services.hook_names(room_id)
    }

    /// Send a channel control, preserving this owner's previous-turn record.
    ///
    /// # Errors
    /// Returns a transport failure when the control cannot reach its executor.
    pub async fn control(
        &self,
        message: ChannelControl,
    ) -> Result<Option<ChannelResult>, RoomKitError> {
        self.state.turns.ready().await?;
        self.state
            .turns
            .client_control(self.backend(), message)
            .await
    }
    /// Read and settle human-input requests of this owner.
    pub fn inputs(&self) -> TurnClientInputs<'_> {
        TurnClientInputs(&self.state.turns)
    }

    /// Read an open human-input request in the requested organization.
    pub fn input_request(
        &self,
        request_id: &str,
        options: &InputRequestOptions,
    ) -> Option<PendingInputEvent> {
        self.state
            .turns
            .inputs
            .request(request_id, options.organization_id.as_deref())
    }
    /// Steer this owner's running turns in the room and return the number of loops reached.
    pub async fn steer(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        directive: impl Into<SteeringDirective>,
        options: SteerOptions,
    ) -> usize {
        let address = SteerAddress {
            room: room_id,
            channel: channel_id,
            turn: options.turn_id.as_deref(),
            organization: options.organization_id.as_deref(),
        };
        self.state
            .services
            .wait_for_admissions(address, self.state.turns.timeouts().bounded().accept)
            .await;
        self.state.turns.steer(&directive.into(), address).await
    }

    /// Cut the open turns, release their calls and close the backend.
    ///
    /// # Errors
    /// Returns failures from closing the runtime or backend.
    pub async fn close(&self) -> Result<(), RoomKitError> {
        let state = &self.state;
        state.turns.cut_all();
        state.turns.close();
        if let Some(executor) = self.executor() {
            executor.close().await;
        }
        super::lock(&state.turns.started).abandon();
        state.calls.stop().await;
        let closed = state.runtime.close().await;
        state.backend.close().await?;
        closed
    }
}
