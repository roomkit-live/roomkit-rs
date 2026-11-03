//! Python's defaults, each Rust option's (step 5, lot 6): rustdoc shows no
//! default, so each line asserts the default of the Python parameter its
//! comment names, which `make api-parity` checks every defaulted parameter
//! in scope has.

use roomkit::AddRoomHookOptions;
use roomkit::ArchiveRoomOptions;
use roomkit::AttachChannelOptions;
use roomkit::CheckAllTimersOptions;
use roomkit::CheckRoomTimersOptions;
use roomkit::CloseRoomOptions;
use roomkit::CommitEventOptions;
use roomkit::CreateRoomOptions;
use roomkit::DefaultInboundRoomRouterOptions;
use roomkit::DeleteEventOptions;
use roomkit::DetachChannelOptions;
use roomkit::GetBindingOptions;
use roomkit::GetRoomOptions;
use roomkit::GetTimelineOptions;
use roomkit::HasHooksOptions;
use roomkit::HookExecution;
use roomkit::HookOptions;
use roomkit::HostChannelOptions;
use roomkit::IdentityHookOptions;
use roomkit::InMemoryStore;
use roomkit::ListBindingsOptions;
use roomkit::ListMembersOptions;
use roomkit::ListObservationsOptions;
use roomkit::ListTasksOptions;
use roomkit::MuteOptions;
use roomkit::MuteOutputOptions;
use roomkit::NormalizeOptions;
use roomkit::PauseRoomOptions;
use roomkit::PostOptions;
use roomkit::PostStatusOptions;
use roomkit::ProcessInboundOptions;
use roomkit::RemoveRoomHookOptions;
use roomkit::ReopenRoomOptions;
use roomkit::ResumeRoomOptions;
use roomkit::RoomKitOptions;
use roomkit::RouteOptions;
use roomkit::SendEventOptions;
use roomkit::SetAccessOptions;
use roomkit::SetVisibilityOptions;
use roomkit::SubmitFeedbackOptions;
use roomkit::TransportChannelOptions;
use roomkit::UnmuteOptions;
use roomkit::UnmuteOutputOptions;
use roomkit::UpdateBindingMetadataOptions;
use roomkit::UpdateEventOptions;
use roomkit::UpdateRoomMetadataOptions;
use roomkit::identity::MockIdentityResolverOptions;
use roomkit::model::Access;
use roomkit::model::AgentResponsePolicy;
use roomkit::model::EventType;
use roomkit::model::IdentificationStatus;
use roomkit::model::Visibility;
use roomkit::model::hook::AllowOptions;
use roomkit::model::hook::BlockOptions;
use roomkit::model::hook::ModifyOptions;
use roomkit::model::identity::ChallengeOptions;
use roomkit::model::identity::PendingOptions;
use roomkit::model::identity::RejectOptions;
use roomkit::orchestration;
use roomkit::store;
use roomkit::testing::EchoAgentOptions;
use roomkit::testing::RecordingTransportOptions;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn add_room_hook_options_defaults_as_python() {
    let defaults = AddRoomHookOptions::default();
    // roomkit.RoomKit.add_room_hook(priority) = 0
    assert_eq!(defaults.priority, 0);
    // roomkit.RoomKit.add_room_hook(name) = ''
    assert_eq!(defaults.name, "");
    // roomkit.RoomKit.add_room_hook(fail_closed) = False
    assert!(!defaults.fail_closed);
    // roomkit.RoomKit.add_room_hook(needs_lock) = True
    assert!(defaults.needs_lock);
    // roomkit.RoomKit.add_room_hook(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn archive_room_options_defaults_as_python() {
    let defaults = ArchiveRoomOptions::default();
    // roomkit.RoomKit.archive_room(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn attach_channel_options_defaults_as_python() {
    let defaults = AttachChannelOptions::default();
    // roomkit.RoomKit.attach_channel(access) = <Access.READ_WRITE: 'read_write'>
    assert_eq!(defaults.access, Access::ReadWrite);
    // roomkit.RoomKit.attach_channel(category) = None
    assert!(defaults.category.is_none());
    // roomkit.RoomKit.attach_channel(organization_id) = None
    assert!(defaults.organization_id.is_none());
    // roomkit.RoomKit.attach_channel(visibility) = <Visibility.ALL: 'all'>
    assert_eq!(defaults.visibility, Visibility::All);
}

#[test]
fn check_all_timers_options_defaults_as_python() {
    let defaults = CheckAllTimersOptions::default();
    // roomkit.RoomKit.check_all_timers(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn check_room_timers_options_defaults_as_python() {
    let defaults = CheckRoomTimersOptions::default();
    // roomkit.RoomKit.check_room_timers(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn close_room_options_defaults_as_python() {
    let defaults = CloseRoomOptions::default();
    // roomkit.RoomKit.close_room(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn commit_event_options_defaults_as_python() {
    let defaults = CommitEventOptions::default();
    // roomkit.RoomKit.commit_event(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn create_room_options_defaults_as_python() {
    let defaults: CreateRoomOptions = CreateRoomOptions::default();
    // roomkit.RoomKit.create_room(orchestration) = <object object>
    assert!(defaults.orchestration.is_none());
    // roomkit.RoomKit.create_room(agent_response_policy) = None
    assert!(defaults.agent_response_policy.is_none());
    // roomkit.RoomKit.create_room(metadata) = None
    assert!(defaults.metadata.is_empty());
    // roomkit.RoomKit.create_room(organization_id) = None
    assert!(defaults.organization_id.is_none());
    // roomkit.RoomKit.create_room(room_id) = None
    assert!(defaults.room_id.is_none());
    // roomkit.RoomKit.create_room(timers) = None
    assert!(defaults.timers.is_none());
}

#[test]
fn default_inbound_room_router_options_defaults_as_python() {
    let defaults = DefaultInboundRoomRouterOptions::default();
    // roomkit.DefaultInboundRoomRouter.__init__(channel_of) = None
    assert!(defaults.channel_of.is_none());
}

#[test]
fn delete_event_options_defaults_as_python() {
    let defaults = DeleteEventOptions::default();
    // roomkit.RoomKit.delete_event(cascade_replies) = True
    assert!(defaults.cascade_replies);
    // roomkit.RoomKit.delete_event(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn detach_channel_options_defaults_as_python() {
    let defaults = DetachChannelOptions::default();
    // roomkit.RoomKit.detach_channel(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn get_binding_options_defaults_as_python() {
    let defaults = GetBindingOptions::default();
    // roomkit.RoomKit.get_binding(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn get_room_options_defaults_as_python() {
    let defaults = GetRoomOptions::default();
    // roomkit.RoomKit.get_room(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn ephemeral_options_default_as_python() {
    let typing = roomkit::PublishTypingOptions::default();
    // roomkit.RoomKit.publish_typing(is_typing) = True
    assert!(typing.is_typing);
    // roomkit.RoomKit.publish_typing(data) = None
    assert!(typing.data.is_none());
    // roomkit.RoomKit.publish_typing(organization_id) = None
    assert!(typing.organization_id.is_none());
    // roomkit.RoomKit.publish_presence(organization_id) = None
    assert!(
        roomkit::PublishPresenceOptions::default()
            .organization_id
            .is_none()
    );
    // roomkit.RoomKit.publish_read_receipt(organization_id) = None
    assert!(
        roomkit::PublishReadReceiptOptions::default()
            .organization_id
            .is_none()
    );
    let reaction = roomkit::PublishReactionOptions::default();
    // roomkit.RoomKit.publish_reaction(action) = 'add'
    assert_eq!(reaction.action, "add");
    // roomkit.RoomKit.publish_reaction(organization_id) = None
    assert!(reaction.organization_id.is_none());
    let tool_call = roomkit::PublishToolCallOptions::default();
    // roomkit.RoomKit.publish_tool_call(event_type) = <EphemeralEventType.TOOL_CALL_START: 'tool_call_start'>
    assert_eq!(
        tool_call.event_type,
        roomkit::realtime::EphemeralEventType::ToolCallStart
    );
    // roomkit.RoomKit.publish_tool_call(duration_ms) = None
    assert!(tool_call.duration_ms.is_none());
    // roomkit.RoomKit.publish_tool_call(organization_id) = None
    assert!(tool_call.organization_id.is_none());
    // roomkit.RoomKit.subscribe_room(organization_id) = None
    assert!(
        roomkit::SubscribeRoomOptions::default()
            .organization_id
            .is_none()
    );
    // roomkit.realtime.InMemoryRealtime.__init__(max_queue_size) = 100
    assert_eq!(
        roomkit::realtime::InMemoryRealtimeOptions::default().max_queue_size,
        100
    );
}

#[test]
fn read_marker_options_default_as_python() {
    // roomkit.RoomKit.mark_read(organization_id) = None
    assert!(
        roomkit::MarkReadOptions::default()
            .organization_id
            .is_none()
    );
    // roomkit.RoomKit.mark_all_read(organization_id) = None
    assert!(
        roomkit::MarkAllReadOptions::default()
            .organization_id
            .is_none()
    );
    // roomkit.RoomKit.list_read_markers(organization_id) = None
    assert!(
        roomkit::ListReadMarkersOptions::default()
            .organization_id
            .is_none()
    );
}

#[test]
fn get_timeline_options_defaults_as_python() {
    let defaults = GetTimelineOptions::default();
    // roomkit.RoomKit.get_timeline(after_index) = None
    assert!(defaults.after_index.is_none());
    // roomkit.RoomKit.get_timeline(before_index) = None
    assert!(defaults.before_index.is_none());
    // roomkit.RoomKit.get_timeline(include_blocked) = False
    assert!(!defaults.include_blocked);
    // roomkit.RoomKit.get_timeline(limit) = 50
    assert_eq!(defaults.limit, 50);
    // roomkit.RoomKit.get_timeline(newest_first) = False
    assert!(!defaults.newest_first);
    // roomkit.RoomKit.get_timeline(offset) = 0
    assert_eq!(defaults.offset, 0);
    // roomkit.RoomKit.get_timeline(organization_id) = None
    assert!(defaults.organization_id.is_none());
    // roomkit.RoomKit.get_timeline(visibility_filter) = None
    assert!(defaults.visibility_filter.is_none());
}

#[test]
fn has_hooks_options_defaults_as_python() {
    let defaults = HasHooksOptions::default();
    // roomkit.HookEngine.has_hooks(trigger) = None
    assert!(defaults.trigger.is_none());
}

#[test]
fn hook_options_defaults_as_python() {
    let defaults = HookOptions::default();
    // roomkit.RoomKit.hook(channel_ids) = None
    assert!(defaults.channel_ids.is_none());
    // roomkit.RoomKit.hook(channel_types) = None
    assert!(defaults.channel_types.is_none());
    // roomkit.RoomKit.hook(directions) = None
    assert!(defaults.directions.is_none());
    // roomkit.RoomKit.hook(event_types) = None
    assert!(defaults.event_types.is_none());
    // roomkit.RoomKit.hook(execution) = <HookExecution.SYNC: 'sync'>
    assert_eq!(defaults.execution, HookExecution::Sync);
    // roomkit.RoomKit.hook(fail_closed) = False
    assert!(!defaults.fail_closed);
    // roomkit.RoomKit.hook(name) = ''
    assert_eq!(defaults.name, "");
    // roomkit.RoomKit.hook(needs_lock) = True
    assert!(defaults.needs_lock);
    // roomkit.RoomKit.hook(priority) = 0
    assert_eq!(defaults.priority, 0);
    // roomkit.RoomKit.hook(timeout) = 30.0
    assert_eq!(defaults.timeout, Duration::from_secs(30));
}

#[test]
fn host_channel_options_defaults_as_python() {
    let defaults = HostChannelOptions::default();
    // roomkit.HostChannel.__init__(channel_id) = 'host'
    assert_eq!(defaults.channel_id.as_str(), "host");
}

#[test]
fn list_bindings_options_defaults_as_python() {
    let defaults = ListBindingsOptions::default();
    // roomkit.RoomKit.list_bindings(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn list_members_options_defaults_as_python() {
    let defaults = ListMembersOptions::default();
    // roomkit.RoomKit.list_members(include_left) = False
    assert!(!defaults.include_left);
    // roomkit.RoomKit.list_members(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn mute_options_defaults_as_python() {
    let defaults = MuteOptions::default();
    // roomkit.RoomKit.mute(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn mute_output_options_defaults_as_python() {
    let defaults = MuteOutputOptions::default();
    // roomkit.RoomKit.mute_output(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn normalize_options_defaults_as_python() {
    let defaults = NormalizeOptions::default();
    // roomkit.ChannelAddressing.normalize(country_code) = None
    assert!(defaults.country_code.is_none());
}

#[test]
fn pause_room_options_defaults_as_python() {
    let defaults = PauseRoomOptions::default();
    // roomkit.RoomKit.pause_room(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn post_options_defaults_as_python() {
    let defaults = PostOptions::default();
    // roomkit.RoomKit.post(addressed_to) = ()
    assert_eq!(defaults.addressed_to, Vec::new());
    // roomkit.RoomKit.post(chain_depth) = 0
    assert_eq!(defaults.chain_depth, 0);
    // roomkit.RoomKit.post(channel_id) = None
    assert!(defaults.channel_id.is_none());
    // roomkit.RoomKit.post(idempotency_key) = None
    assert!(defaults.idempotency_key.is_none());
    // roomkit.RoomKit.post(metadata) = None
    assert!(defaults.metadata.is_empty());
    // roomkit.RoomKit.post(organization_id) = None
    assert!(defaults.organization_id.is_none());
    // roomkit.RoomKit.post(participant_id) = None
    assert!(defaults.participant_id.is_none());
}

#[test]
fn process_inbound_options_defaults_as_python() {
    let defaults = ProcessInboundOptions::default();
    // roomkit.RoomKit.process_inbound(defer_delivery) = False
    assert!(!defaults.defer_delivery);
    // roomkit.RoomKit.process_inbound(organization_id) = None
    assert!(defaults.organization_id.is_none());
    // roomkit.RoomKit.process_inbound(room_id) = None
    assert!(defaults.room_id.is_none());
}

#[test]
fn remove_room_hook_options_defaults_as_python() {
    let defaults = RemoveRoomHookOptions::default();
    // roomkit.RoomKit.remove_room_hook(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn reopen_room_options_defaults_as_python() {
    let defaults = ReopenRoomOptions::default();
    // roomkit.RoomKit.reopen_room(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn resume_room_options_defaults_as_python() {
    let defaults = ResumeRoomOptions::default();
    // roomkit.RoomKit.resume_room(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn wait_task_options_defaults_as_python() {
    // roomkit.TurnHost.wait_task(timeout) = None
    assert!(roomkit::WaitTaskOptions::default().timeout.is_none());
}

#[test]
fn submit_options_defaults_as_python() {
    let defaults = roomkit::tasks::SubmitOptions::default();
    // roomkit.tasks.TaskRunner.submit(context) = None
    assert!(defaults.context.is_none());
    // roomkit.tasks.TaskRunner.submit(on_complete) = None
    assert!(defaults.on_complete.is_none());
}

#[test]
fn delegated_task_options_defaults_as_python() {
    let defaults = roomkit::tasks::DelegatedTaskOptions::default();
    // roomkit.tasks.DelegatedTask.__init__(brief) = None
    assert!(defaults.brief.is_none());
    // roomkit.tasks.DelegatedTask.__init__(description) = None
    assert!(defaults.description.is_none());
    // roomkit.tasks.DelegatedTask.__init__(hand_back) = 'relay'
    assert_eq!(defaults.hand_back, "relay");
}

#[test]
fn delegated_task_wait_options_defaults_as_python() {
    // roomkit.tasks.DelegatedTask.wait(timeout) = None
    assert!(roomkit::tasks::WaitOptions::default().timeout.is_none());
}

#[test]
fn delegated_task_result_defaults_as_python() {
    let room = roomkit::model::RoomId::new("r").unwrap();
    let agent = roomkit::model::ChannelId::new("a").unwrap();
    let result = roomkit::tasks::DelegatedTaskResult::new("t", room.clone(), room, agent);
    // roomkit.tasks.DelegatedTaskResult.__init__(status) = <TaskStatus.COMPLETED: 'completed'>
    assert_eq!(result.status, roomkit::model::TaskStatus::Completed);
    // roomkit.tasks.DelegatedTaskResult.__init__(output) = None
    assert!(result.output.is_none());
    // roomkit.tasks.DelegatedTaskResult.__init__(error) = None
    assert!(result.error.is_none());
    // roomkit.tasks.DelegatedTaskResult.__init__(duration_ms) = 0.0
    assert!(result.duration_ms.abs() < f64::EPSILON);
    // roomkit.tasks.DelegatedTaskResult.__init__(metadata) = <factory>
    assert!(result.metadata.is_empty());
}

#[test]
fn delegate_options_defaults_as_python() {
    let defaults = roomkit::DelegateOptions::default();
    // roomkit.RoomKit.delegate(wait) = False
    assert!(!defaults.wait);
    // roomkit.RoomKit.delegate(context) = None
    assert!(defaults.context.is_none());
    // roomkit.RoomKit.delegate(share_channels) = None
    assert!(defaults.share_channels.is_none());
    // roomkit.RoomKit.delegate(notify) = None
    assert!(defaults.notify.is_none());
    // roomkit.RoomKit.delegate(on_complete) = None
    assert!(defaults.on_complete.is_none());
    // roomkit.RoomKit.delegate(post_status) = True
    assert!(defaults.post_status);
    // roomkit.RoomKit.delegate(require_structured_result) = False
    assert!(!defaults.require_structured_result);
    // roomkit.RoomKit.delegate(max_result_retries) = 3
    assert_eq!(defaults.max_result_retries, 3);
    // roomkit.RoomKit.delegate(result_tool) = None
    assert!(defaults.result_tool.is_none());
    // roomkit.RoomKit.delegate(brief) = None
    assert!(defaults.brief.is_none());
    // roomkit.RoomKit.delegate(conversation) = 0
    assert_eq!(defaults.conversation, 0);
    // roomkit.RoomKit.delegate(hand_back) = 'relay'
    assert_eq!(defaults.hand_back, "relay");
    // roomkit.RoomKit.delegate(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn room_kit_options_defaults_as_python() {
    let defaults = RoomKitOptions::default();
    // roomkit.RoomKit.__init__(agent_response_policy) = <AgentResponsePolicy.AGENT_CHAIN: 'agent_chain'>
    assert_eq!(
        defaults.agent_response_policy,
        AgentResponsePolicy::AgentChain
    );
    // roomkit.RoomKit.__init__(inbound_router) = None
    assert!(defaults.inbound_router.is_none());
    // roomkit.RoomKit.__init__(max_chain_depth) = 5
    assert_eq!(defaults.max_chain_depth, 5);
    // roomkit.RoomKit.__init__(process_timeout) = 30.0
    assert_eq!(defaults.process_timeout, Duration::from_secs(30));
    // roomkit.RoomKit.__init__(realtime) = None
    assert!(defaults.realtime.is_none());
    // roomkit.RoomKit.__init__(status_bus) = None
    assert!(defaults.status_bus.is_none());
    // roomkit.RoomKit.__init__(identity_resolver) = None
    assert!(defaults.identity_resolver.is_none());
    // roomkit.RoomKit.__init__(identity_channel_types) = None
    assert!(defaults.identity_channel_types.is_none());
    // roomkit.RoomKit.__init__(identity_timeout) = 10.0
    assert_eq!(defaults.identity_timeout, Duration::from_secs(10));
    // roomkit.RoomKit.__init__(task_runner) = None
    assert!(defaults.task_runner.is_none());
    // roomkit.RoomKit.__init__(verification_policy) = None
    assert!(defaults.verification_policy.is_none());
    // roomkit.RoomKit.__init__(media_resolver) = None
    assert!(defaults.media_resolver.is_none());
    // roomkit.RoomKit.__init__(attached_media) = None
    assert!(defaults.attached_media.is_none());
    // roomkit.RoomKit.__init__(orchestration) = None
    assert!(defaults.orchestration.is_none());
    // roomkit.RoomKit.__init__(store) = None
    let store: Arc<InMemoryStore> = defaults.store;
    assert_eq!(Arc::strong_count(&store), 1);
}

#[test]
fn identity_hook_options_defaults_as_python() {
    let defaults = IdentityHookOptions::default();
    // roomkit.RoomKit.identity_hook(channel_types) = None
    assert!(defaults.channel_types.is_none());
    // roomkit.RoomKit.identity_hook(channel_ids) = None
    assert!(defaults.channel_ids.is_none());
    // roomkit.RoomKit.identity_hook(directions) = None
    assert!(defaults.directions.is_none());
}

#[test]
fn mock_identity_resolver_options_defaults_as_python() {
    let defaults = MockIdentityResolverOptions::default();
    // roomkit.identity.MockIdentityResolver.__init__(mapping) = None
    assert!(defaults.mapping.is_empty());
    // roomkit.identity.MockIdentityResolver.__init__(ambiguous) = None
    assert!(defaults.ambiguous.is_empty());
    // roomkit.identity.MockIdentityResolver.__init__(unknown_status) = <IdentificationStatus.UNKNOWN: 'unknown'>
    assert_eq!(defaults.unknown_status, IdentificationStatus::Unknown);
}

#[test]
fn identity_hook_result_options_defaults_as_python() {
    // roomkit.models.IdentityHookResult.challenge(message) = None
    assert!(ChallengeOptions::default().message.is_none());
    let pending = PendingOptions::default();
    // roomkit.models.IdentityHookResult.pending(display_name) = None
    assert!(pending.display_name.is_none());
    // roomkit.models.IdentityHookResult.pending(candidates) = None
    assert!(pending.candidates.is_none());
    // roomkit.models.IdentityHookResult.reject(reason) = 'Unknown sender'
    assert_eq!(RejectOptions::default().reason, "Unknown sender");
}

#[test]
fn route_options_defaults_as_python() {
    let defaults = RouteOptions::default();
    // roomkit.InboundRoomRouter.route(channel_data) = None
    assert!(defaults.channel_data.is_none());
    // roomkit.InboundRoomRouter.route(identity) = None
    assert!(defaults.identity.is_none());
    // roomkit.InboundRoomRouter.route(organization_id) = None
    assert!(defaults.organization_id.is_none());
    // roomkit.InboundRoomRouter.route(participant_id) = None
    assert!(defaults.participant_id.is_none());
}

#[test]
fn send_event_options_defaults_as_python() {
    let defaults = SendEventOptions::default();
    // roomkit.RoomKit.send_event(addressed_to) = None
    assert!(defaults.addressed_to.is_none());
    // roomkit.RoomKit.send_event(chain_depth) = 0
    assert_eq!(defaults.chain_depth, 0);
    // roomkit.RoomKit.send_event(created_at) = None
    assert!(defaults.created_at.is_none());
    // roomkit.RoomKit.send_event(event_type) = <EventType.MESSAGE: 'message'>
    assert_eq!(defaults.event_type, EventType::Message);
    // roomkit.RoomKit.send_event(idempotency_key) = None
    assert!(defaults.idempotency_key.is_none());
    // roomkit.RoomKit.send_event(metadata) = None
    assert!(defaults.metadata.is_empty());
    // roomkit.RoomKit.send_event(organization_id) = None
    assert!(defaults.organization_id.is_none());
    // roomkit.RoomKit.send_event(parent_event_id) = None
    assert!(defaults.parent_event_id.is_none());
    // roomkit.RoomKit.send_event(participant_id) = None
    assert!(defaults.participant_id.is_none());
    // roomkit.RoomKit.send_event(provider) = None
    assert!(defaults.provider.is_none());
    // roomkit.RoomKit.send_event(response_visibility) = None
    assert!(defaults.response_visibility.is_none());
    // roomkit.RoomKit.send_event(standalone) = False
    assert!(!defaults.standalone);
    // roomkit.RoomKit.send_event(visibility) = <Visibility.ALL: 'all'>
    assert_eq!(defaults.visibility, Visibility::All);
}

#[test]
fn set_access_options_defaults_as_python() {
    let defaults = SetAccessOptions::default();
    // roomkit.RoomKit.set_access(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn set_visibility_options_defaults_as_python() {
    let defaults = SetVisibilityOptions::default();
    // roomkit.RoomKit.set_visibility(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn transport_channel_options_defaults_as_python() {
    let defaults = TransportChannelOptions::default();
    // roomkit.TransportChannel.__init__(addressing) = None
    assert!(defaults.addressing.is_none());
    // roomkit.TransportChannel.__init__(capabilities) = None
    assert!(defaults.capabilities.is_none());
    // roomkit.TransportChannel.__init__(country_code) = None
    assert!(defaults.country_code.is_none());
    // roomkit.TransportChannel.__init__(defaults) = None
    assert!(defaults.defaults.is_none());
    // roomkit.TransportChannel.__init__(requires_recipient) = True
    assert!(defaults.requires_recipient);
}

#[test]
fn unmute_options_defaults_as_python() {
    let defaults = UnmuteOptions::default();
    // roomkit.RoomKit.unmute(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn unmute_output_options_defaults_as_python() {
    let defaults = UnmuteOutputOptions::default();
    // roomkit.RoomKit.unmute_output(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn update_binding_metadata_options_defaults_as_python() {
    let defaults = UpdateBindingMetadataOptions::default();
    // roomkit.RoomKit.update_binding_metadata(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn update_event_options_defaults_as_python() {
    let defaults = UpdateEventOptions::default();
    // roomkit.RoomKit.update_event(content) = None
    assert!(defaults.content.is_none());
    // roomkit.RoomKit.update_event(metadata) = None
    assert!(defaults.metadata.is_none());
    // roomkit.RoomKit.update_event(organization_id) = None
    assert!(defaults.organization_id.is_none());
    // roomkit.RoomKit.update_event(source) = None
    assert!(defaults.source.is_none());
}

#[test]
fn allow_options_defaults_as_python() {
    let defaults = AllowOptions::default();
    // roomkit.HookResult.allow(injected) = None
    assert!(defaults.injected.is_none());
}

#[test]
fn block_options_defaults_as_python() {
    let defaults = BlockOptions::default();
    // roomkit.HookResult.block(injected) = None
    assert!(defaults.injected.is_none());
    // roomkit.HookResult.block(observations) = None
    assert!(defaults.observations.is_none());
    // roomkit.HookResult.block(tasks) = None
    assert!(defaults.tasks.is_none());
}

#[test]
fn modify_options_defaults_as_python() {
    let defaults = ModifyOptions::default();
    // roomkit.HookResult.modify(injected) = None
    assert!(defaults.injected.is_none());
}

#[test]
fn store_advance_delivered_index_options_defaults_as_python() {
    let defaults = store::AdvanceDeliveredIndexOptions::default();
    // roomkit.ConversationStore.advance_delivered_index(force) = False
    assert!(!defaults.force);
}

#[test]
fn store_commit_event_options_defaults_as_python() {
    let defaults = store::CommitEventOptions::default();
    // roomkit.ConversationStore.commit_event(activity) = True
    assert!(defaults.activity);
    // roomkit.ConversationStore.commit_event(target) = None
    assert!(defaults.target.is_none());
}

#[test]
fn store_delete_event_options_defaults_as_python() {
    let defaults = store::DeleteEventOptions::default();
    // roomkit.ConversationStore.delete_event(cascade_replies) = True
    assert!(defaults.cascade_replies);
}

#[test]
fn store_find_room_id_by_binding_options_defaults_as_python() {
    let defaults = store::FindRoomIdByBindingOptions::default();
    // roomkit.ConversationStore.find_room_id_by_binding(identity_id) = None
    assert!(defaults.identity_id.is_none());
    // roomkit.ConversationStore.find_room_id_by_binding(recipient_key) = None
    assert!(defaults.recipient_key.is_none());
    // roomkit.ConversationStore.find_room_id_by_binding(room_filter) = None
    assert!(defaults.room_filter.is_none());
}

#[test]
fn store_find_room_id_by_participant_options_defaults_as_python() {
    let defaults = store::FindRoomIdByParticipantOptions::default();
    // roomkit.ConversationStore.find_room_id_by_participant(identity_id) = None
    assert!(defaults.identity_id.is_none());
    // roomkit.ConversationStore.find_room_id_by_participant(room_filter) = None
    assert!(defaults.room_filter.is_none());
}

#[test]
fn store_find_room_ids_by_channel_options_defaults_as_python() {
    let defaults = store::FindRoomIdsByChannelOptions::default();
    // roomkit.ConversationStore.find_room_ids_by_channel(group) = None
    assert!(defaults.group.is_none());
    // roomkit.ConversationStore.find_room_ids_by_channel(limit) = 2
    assert_eq!(defaults.limit, 2);
    // roomkit.ConversationStore.find_room_ids_by_channel(room_filter) = None
    assert!(defaults.room_filter.is_none());
}

#[test]
fn store_find_latest_room_options_defaults_as_python() {
    let defaults = store::FindLatestRoomOptions::default();
    // roomkit.ConversationStore.find_latest_room(channel_type) = None
    assert!(defaults.channel_type.is_none());
    // roomkit.ConversationStore.find_latest_room(status) = None
    assert!(defaults.status.is_none());
}

#[test]
fn store_find_room_id_by_channel_options_defaults_as_python() {
    let defaults = store::FindRoomIdByChannelOptions::default();
    // roomkit.ConversationStore.find_room_id_by_channel(status) = None
    assert!(defaults.status.is_none());
}

#[test]
fn store_find_rooms_options_defaults_as_python() {
    let defaults = store::FindRoomsOptions::default();
    // roomkit.ConversationStore.find_rooms(limit) = 100
    assert_eq!(defaults.limit, 100);
    // roomkit.ConversationStore.find_rooms(metadata_filter) = None
    assert!(defaults.metadata_filter.is_none());
    // roomkit.ConversationStore.find_rooms(offset) = 0
    assert_eq!(defaults.offset, 0);
    // roomkit.ConversationStore.find_rooms(organization_id) = None
    assert!(defaults.organization_id.is_none());
    // roomkit.ConversationStore.find_rooms(status) = None
    assert!(defaults.status.is_none());
}

#[test]
fn store_get_conversation_options_defaults_as_python() {
    let defaults = store::GetConversationOptions::default();
    // roomkit.ConversationStore.get_conversation(after_index) = None
    assert!(defaults.after_index.is_none());
    // roomkit.ConversationStore.get_conversation(limit) = 50
    assert_eq!(defaults.limit, 50);
}

#[test]
fn store_get_event_count_options_defaults_as_python() {
    let defaults = store::GetEventCountOptions::default();
    // roomkit.ConversationStore.get_event_count(event_filter) = None
    assert!(defaults.event_filter.is_none());
}

#[test]
fn store_get_timeline_options_defaults_as_python() {
    let defaults = store::GetTimelineOptions::default();
    // roomkit.ConversationStore.get_timeline(after_index) = None
    assert!(defaults.after_index.is_none());
    // roomkit.ConversationStore.get_timeline(event_filter) = None
    assert!(defaults.event_filter.is_none());
    // roomkit.ConversationStore.get_timeline(limit) = 50
    assert_eq!(defaults.limit, 50);
    // roomkit.ConversationStore.get_timeline(newest_first) = False
    assert!(!defaults.newest_first);
}

#[test]
fn store_get_verification_options_defaults_as_python() {
    let defaults = store::GetVerificationOptions::default();
    // roomkit.ConversationStore.get_verification(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn store_link_address_options_defaults_as_python() {
    let defaults = store::LinkAddressOptions::default();
    // roomkit.ConversationStore.link_address(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn store_list_events_options_defaults_as_python() {
    let defaults = store::ListEventsOptions::default();
    // roomkit.ConversationStore.list_events(after_index) = None
    assert!(defaults.after_index.is_none());
    // roomkit.ConversationStore.list_events(before_index) = None
    assert!(defaults.before_index.is_none());
    // roomkit.ConversationStore.list_events(event_filter) = None
    assert!(defaults.event_filter.is_none());
    // roomkit.ConversationStore.list_events(limit) = 50
    assert_eq!(defaults.limit, 50);
    // roomkit.ConversationStore.list_events(newest_first) = False
    assert!(!defaults.newest_first);
    // roomkit.ConversationStore.list_events(offset) = 0
    assert_eq!(defaults.offset, 0);
    // roomkit.ConversationStore.list_events(visibility_filter) = None
    assert!(defaults.visibility_filter.is_none());
}

#[test]
fn store_list_rooms_options_defaults_as_python() {
    let defaults = store::ListRoomsOptions::default();
    // roomkit.ConversationStore.list_rooms(limit) = 50
    assert_eq!(defaults.limit, 50);
    // roomkit.ConversationStore.list_rooms(offset) = 0
    assert_eq!(defaults.offset, 0);
}

#[test]
fn store_list_tasks_options_defaults_as_python() {
    let defaults = store::ListTasksOptions::default();
    // roomkit.ConversationStore.list_tasks(status) = None
    assert!(defaults.status.is_none());
}

#[test]
fn store_patch_room_metadata_options_defaults_as_python() {
    let defaults = store::PatchRoomMetadataOptions::default();
    // roomkit.ConversationStore.patch_room_metadata(unset) = ()
    assert_eq!(defaults.unset, Vec::<String>::new());
}

#[test]
fn store_resolve_identity_options_defaults_as_python() {
    let defaults = store::ResolveIdentityOptions::default();
    // roomkit.ConversationStore.resolve_identity(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn store_unlink_address_options_defaults_as_python() {
    let defaults = store::UnlinkAddressOptions::default();
    // roomkit.ConversationStore.unlink_address(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn echo_agent_options_defaults_as_python() {
    let defaults = EchoAgentOptions::default();
    // roomkit.testing.EchoAgent.__init__(quote) = False
    assert!(!defaults.quote);
}

#[test]
fn recording_transport_options_defaults_as_python() {
    let defaults = RecordingTransportOptions::default();
    // roomkit.testing.RecordingTransport.__init__(capabilities) = None
    assert!(defaults.capabilities.is_none());
}

#[test]
fn process_delivery_status_options_defaults_as_python() {
    let defaults = roomkit::ProcessDeliveryStatusOptions::default();
    // roomkit.RoomKit.process_delivery_status(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn process_webhook_options_defaults_as_python() {
    let defaults = roomkit::ProcessWebhookOptions::default();
    // roomkit.RoomKit.process_webhook(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn list_observations_options_defaults_as_python() {
    let defaults = ListObservationsOptions::default();
    // roomkit.RoomKit.list_observations(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn update_room_metadata_options_defaults_as_python() {
    let defaults = UpdateRoomMetadataOptions::default();
    // roomkit.RoomKit.update_room_metadata(organization_id) = None
    // roomkit.TurnHost.update_room_metadata(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn submit_feedback_options_defaults_as_python() {
    let defaults = SubmitFeedbackOptions::default();
    // roomkit.RoomKit.submit_feedback(event_id) = None
    assert!(defaults.event_id.is_none());
    // roomkit.RoomKit.submit_feedback(channel_id) = None
    assert!(defaults.channel_id.is_none());
    // roomkit.RoomKit.submit_feedback(comment) = ''
    assert_eq!(defaults.comment, "");
    // roomkit.RoomKit.submit_feedback(dimension) = 'overall'
    assert_eq!(defaults.dimension, "overall");
    // roomkit.RoomKit.submit_feedback(metadata) = None
    assert!(defaults.metadata.is_none());
    // roomkit.RoomKit.submit_feedback(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn post_status_options_defaults_as_python() {
    let defaults = PostStatusOptions::default();
    // roomkit.TurnHost.post_status(detail) = ''
    assert_eq!(defaults.detail, "");
    // roomkit.TurnHost.post_status(metadata) = None
    assert!(defaults.metadata.is_none());
}

#[test]
fn status_bus_options_defaults_as_python() {
    let defaults = orchestration::StatusBusOptions::default();
    // roomkit.orchestration.StatusBus.__init__(backend) = None
    assert!(defaults.backend.is_none());
    // roomkit.orchestration.StatusBus.__init__(persist_path) = None
    assert!(defaults.persist_path.is_none());
    let post = orchestration::PostOptions::default();
    // roomkit.orchestration.StatusBus.post(detail) = ''
    assert_eq!(post.detail, "");
    // roomkit.orchestration.StatusBus.post(metadata) = None
    assert!(post.metadata.is_none());
    let post_async = orchestration::PostAsyncOptions::default();
    // roomkit.orchestration.StatusBus.post_async(detail) = ''
    assert_eq!(post_async.detail, "");
    // roomkit.orchestration.StatusBus.post_async(metadata) = None
    assert!(post_async.metadata.is_none());
    let recent = orchestration::RecentOptions::default();
    // roomkit.orchestration.StatusBus.recent(n) = 10
    assert_eq!(recent.n, 10);
    // roomkit.orchestration.StatusBus.recent(agent_id) = None
    assert!(recent.agent_id.is_none());
    // roomkit.orchestration.StatusBus.recent(status) = None
    assert!(recent.status.is_none());
    // roomkit.orchestration.StatusBus.has_completed(agent_id) = None
    assert!(
        orchestration::HasCompletedOptions::default()
            .agent_id
            .is_none()
    );
}

#[test]
fn status_backend_options_defaults_as_python() {
    let backend = orchestration::InMemoryStatusBackendOptions::default();
    // roomkit.orchestration.InMemoryStatusBackend.__init__(max_entries) = 500
    assert_eq!(backend.max_entries, 500);
    // roomkit.orchestration.InMemoryStatusBackend.__init__(persist_path) = None
    assert!(backend.persist_path.is_none());
    let recent = orchestration::backend::RecentOptions::default();
    // roomkit.orchestration.StatusBackend.recent(agent_id) = None
    assert!(recent.agent_id.is_none());
    // roomkit.orchestration.StatusBackend.recent(status) = None
    assert!(recent.status.is_none());
}

#[test]
fn recent_text_options_defaults_as_python() {
    let defaults = orchestration::RecentTextOptions::default();
    // roomkit.orchestration.StatusBus.recent_text(n) = 10
    assert_eq!(defaults.n, 10);
}

#[test]
fn setup_background_options_defaults_as_python() {
    let defaults = roomkit::SetupBackgroundOptions::default();
    // roomkit.RoomKit.setup_background(worker) = None
    assert!(defaults.worker.is_none());
    // roomkit.RoomKit.setup_background(hand_back) = 'relay'
    assert_eq!(defaults.hand_back, "relay");
}

#[test]
fn start_task_options_defaults_as_python() {
    let defaults = roomkit::StartTaskOptions::default();
    // roomkit.RoomKit.start_task(hand_back) = None
    assert!(defaults.hand_back.is_none());
    // roomkit.RoomKit.start_task(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn cancel_task_options_defaults_as_python() {
    let defaults = roomkit::CancelTaskOptions::default();
    // roomkit.RoomKit.cancel_task(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn delivery_handle_cancel_options_defaults_as_python() {
    let defaults = roomkit::CancelOptions::default();
    // roomkit.DeliveryHandle.cancel(reason) = 'caller_cancelled'
    assert_eq!(defaults.reason, "caller_cancelled");
    // roomkit.DeliveryHandle.cancel(timeout) = 5.0
    assert_eq!(defaults.timeout, Duration::from_secs(5));
}

#[test]
fn cancel_delivery_options_defaults_as_python() {
    let defaults = roomkit::CancelDeliveryOptions::default();
    // roomkit.RoomKit.cancel_delivery(reason) = 'caller_cancelled'
    assert_eq!(defaults.reason, "caller_cancelled");
    // roomkit.RoomKit.cancel_delivery(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn list_tasks_options_defaults_as_python() {
    let defaults = ListTasksOptions::default();
    // roomkit.RoomKit.list_tasks(status) = None
    assert!(defaults.status.is_none());
    // roomkit.RoomKit.list_tasks(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn deliver_options_defaults_as_python() {
    let defaults: roomkit::DeliverOptions = roomkit::DeliverOptions::default();
    // roomkit.RoomKit.deliver(channel_id) = None
    assert!(defaults.channel_id.is_none());
    // roomkit.RoomKit.deliver(strategy) = None
    assert!(defaults.strategy.is_none());
    // roomkit.RoomKit.deliver(metadata) = None
    assert!(defaults.metadata.is_empty());
    // roomkit.RoomKit.deliver(addressed_to) = None
    assert!(defaults.addressed_to.is_none());
    // roomkit.RoomKit.deliver(idempotency_key) = None
    assert!(defaults.idempotency_key.is_none());
    // roomkit.RoomKit.deliver(session_id) = None
    assert!(defaults.session_id.is_none());
    // roomkit.RoomKit.deliver(chain_depth) = 0
    assert_eq!(defaults.chain_depth, 0);
    // roomkit.RoomKit.deliver(instruction) = False
    assert!(!defaults.instruction);
    // roomkit.RoomKit.deliver(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn delivery_strategy_options_defaults_as_python() {
    let idle = roomkit::WaitForIdleOptions::default();
    // roomkit.WaitForIdle.__init__(buffer) = 1.0
    assert_eq!(idle.buffer, Duration::from_secs(1));
    // roomkit.WaitForIdle.__init__(playback_timeout) = 15.0
    assert_eq!(idle.playback_timeout, Duration::from_secs(15));
    let queued = roomkit::QueuedOptions::default();
    // roomkit.Queued.__init__(buffer) = 1.0
    assert_eq!(queued.buffer, Duration::from_secs(1));
    // roomkit.Queued.__init__(playback_timeout) = 15.0
    assert_eq!(queued.playback_timeout, Duration::from_secs(15));
    // roomkit.Queued.__init__(separator) = '\n\n'
    assert_eq!(queued.separator, "\n\n");
}

#[test]
fn delivery_backend_options_defaults_as_python() {
    let backend = roomkit::InMemoryDeliveryBackendOptions::default();
    // roomkit.InMemoryDeliveryBackend.__init__(max_queue_size) = 1000
    assert_eq!(backend.max_queue_size, 1000);
    // roomkit.InMemoryDeliveryBackend.__init__(max_dead_letter_size) = 1000
    assert_eq!(backend.max_dead_letter_size, 1000);
    // roomkit.InMemoryDeliveryBackend.__init__(retry_delay) = 1.0
    assert_eq!(backend.retry_delay, Duration::from_secs(1));
    // roomkit.InMemoryDeliveryBackend.__init__(max_retry_delay) = 60.0
    assert_eq!(backend.max_retry_delay, Duration::from_secs(60));
    let dequeue = roomkit::DequeueOptions::default();
    // roomkit.DeliveryBackend.dequeue(batch_size) = 1
    // roomkit.InMemoryDeliveryBackend.dequeue(batch_size) = 1
    assert_eq!(dequeue.batch_size, 1);
    // roomkit.DeliveryBackend.dequeue(timeout) = 5.0
    // roomkit.InMemoryDeliveryBackend.dequeue(timeout) = 5.0
    assert_eq!(dequeue.timeout, Duration::from_secs(5));
    let nack = roomkit::NackOptions::default();
    // roomkit.DeliveryBackend.nack(error) = None
    // roomkit.InMemoryDeliveryBackend.nack(error) = None
    assert!(nack.error.is_none());
    let dead = roomkit::GetDeadLetterItemsOptions::default();
    // roomkit.DeliveryBackend.get_dead_letter_items(limit) = 50
    // roomkit.InMemoryDeliveryBackend.get_dead_letter_items(limit) = 50
    assert_eq!(dead.limit, 50);
    let worker = roomkit::delivery::RunWorkerLoopOptions::default();
    // roomkit.delivery.run_worker_loop(batch_size) = 1
    assert_eq!(worker.batch_size, 1);
    // roomkit.delivery.run_worker_loop(poll_timeout) = 5.0
    assert_eq!(worker.poll_timeout, Duration::from_secs(5));
    let execute: roomkit::delivery::ExecuteDeliveryOptions =
        roomkit::delivery::ExecuteDeliveryOptions::default();
    // roomkit.delivery.execute_delivery(strategy) = None
    assert!(execute.strategy.is_none());
    // roomkit.delivery.execute_delivery(hook_engine) = None
    assert!(execute.hook_engine.is_none());
    let kit = RoomKitOptions::default();
    // roomkit.RoomKit.__init__(delivery_strategy) = None
    assert!(kit.delivery_strategy.is_none());
    // roomkit.RoomKit.__init__(delivery_backend) = None
    assert!(kit.delivery_backend.is_none());
    // roomkit.RoomKit.__init__(delivery_gap_timeout) = 30.0
    assert_eq!(kit.delivery_gap_timeout, Duration::from_secs(30));
    // roomkit.RoomKit.__init__(lock_manager) = None
    assert!(kit.lock_manager.is_none());
    // roomkit.RoomKit.__init__(delivery_claim_lock_manager) = None
    assert!(kit.delivery_claim_lock_manager.is_none());
    // roomkit.RoomKit.__init__(telemetry) = None
    assert!(kit.telemetry.is_none());
}

#[test]
fn in_memory_lock_manager_options_defaults_as_python() {
    let defaults = roomkit::InMemoryLockManagerOptions::default();
    // roomkit.InMemoryLockManager.__init__(max_locks) = 1024
    assert_eq!(defaults.max_locks, 1024);
}

#[test]
fn ensure_participant_options_defaults_as_python() {
    let defaults = roomkit::EnsureParticipantOptions::default();
    // roomkit.RoomKit.ensure_participant(display_name) = None
    assert!(defaults.display_name.is_none());
    // roomkit.RoomKit.ensure_participant(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn resolve_participant_options_defaults_as_python() {
    let defaults = roomkit::ResolveParticipantOptions::default();
    // roomkit.RoomKit.resolve_participant(resolved_by) = 'manual'
    assert_eq!(defaults.resolved_by, "manual");
    // roomkit.RoomKit.resolve_participant(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn add_member_options_defaults_as_python() {
    let defaults = roomkit::AddMemberOptions::default();
    // roomkit.RoomKit.add_member(identity_id) = None
    assert!(defaults.identity_id.is_none());
    // roomkit.RoomKit.add_member(display_name) = None
    assert!(defaults.display_name.is_none());
    // roomkit.RoomKit.add_member(role) = ParticipantRole.MEMBER
    assert_eq!(defaults.role, roomkit::model::ParticipantRole::Member);
    // roomkit.RoomKit.add_member(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn remove_member_options_defaults_as_python() {
    let defaults = roomkit::RemoveMemberOptions::default();
    // roomkit.RoomKit.remove_member(status) = ParticipantStatus.LEFT
    assert_eq!(defaults.status, roomkit::model::ParticipantStatus::Left);
    // roomkit.RoomKit.remove_member(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn rename_member_options_defaults_as_python() {
    let defaults = roomkit::RenameMemberOptions::default();
    // roomkit.RoomKit.rename_member(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn is_member_options_defaults_as_python() {
    let defaults = roomkit::IsMemberOptions::default();
    // roomkit.RoomKit.is_member(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn steer_options_defaults_as_python() {
    let defaults = roomkit::SteerOptions::default();
    // roomkit.RoomKit.steer(room_id) = None
    assert!(defaults.room_id.is_none());
    // roomkit.RoomKit.steer(turn_id) = None
    assert!(defaults.turn_id.is_none());
    // roomkit.RoomKit.steer(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn reject_input_options_defaults_as_python() {
    let defaults = roomkit::RejectInputOptions::default();
    // roomkit.RoomKit.reject_input(reason) = ''
    assert_eq!(defaults.reason, "");
}

#[test]
fn turn_timeouts_input_hold_timeout_defaults_as_python() {
    let defaults = roomkit::TurnTimeouts::default();
    // roomkit.TurnTimeouts.__init__(input_hold_timeout) = 60.0
    assert_eq!(defaults.input_hold_timeout, Duration::from_secs(60));
}

#[test]
fn human_input_handler_options_defaults_as_python() {
    use roomkit::human_input::{
        CloseOptions, CreateDetachedOptions, CreateOptions, HumanInputHandlerOptions,
        RejectOptions, WaitOptions,
    };
    // roomkit.HumanInputHandler.__init__(retention) = 128
    assert_eq!(HumanInputHandlerOptions::default().retention, 128);
    let create = CreateOptions::default();
    // roomkit.HumanInputHandler.create(room_id) = ''
    assert_eq!(create.room_id, "");
    // roomkit.HumanInputHandler.create(tool_call_id) = ''
    assert_eq!(create.tool_call_id, "");
    // roomkit.HumanInputHandler.create(channel_id) = ''
    assert_eq!(create.channel_id, "");
    // roomkit.HumanInputHandler.create(channel_type) = <ChannelType.AI: 'ai'>
    assert_eq!(create.channel_type, roomkit::model::ChannelType::AI);
    // roomkit.HumanInputHandler.create(actor_id) = None
    assert!(create.actor_id.is_none());
    let detached = CreateDetachedOptions::default();
    // roomkit.HumanInputHandler.create_detached(room_id) = ''
    assert_eq!(detached.room_id, "");
    // roomkit.HumanInputHandler.create_detached(tool_call_id) = ''
    assert_eq!(detached.tool_call_id, "");
    // roomkit.HumanInputHandler.create_detached(channel_id) = ''
    assert_eq!(detached.channel_id, "");
    // roomkit.HumanInputHandler.create_detached(channel_type) = <ChannelType.AI: 'ai'>
    assert_eq!(detached.channel_type, roomkit::model::ChannelType::AI);
    // roomkit.HumanInputHandler.create_detached(actor_id) = None
    assert!(detached.actor_id.is_none());
    // roomkit.HumanInputHandler.wait(timeout) = 300
    assert_eq!(WaitOptions::default().timeout, Duration::from_secs(300));
    let close = CloseOptions::default();
    // roomkit.HumanInputHandler.close(channel_id) = None
    assert!(close.channel_id.is_none());
    // roomkit.HumanInputHandler.close(registration) = None
    assert!(close.registration.is_none());
    // roomkit.HumanInputHandler.reject(reason) = ''
    assert_eq!(RejectOptions::default().reason, "");
}

#[test]
fn request_verification_options_defaults_as_python() {
    let defaults = roomkit::RequestVerificationOptions::default();
    // roomkit.RoomKit.request_verification(method) = 'link'
    assert_eq!(defaults.method, "link");
    // roomkit.RoomKit.request_verification(window_start) = None
    assert!(defaults.window_start.is_none());
    // roomkit.RoomKit.request_verification(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn complete_verification_options_defaults_as_python() {
    let defaults = roomkit::CompleteVerificationOptions::default();
    // roomkit.RoomKit.complete_verification(method) = None
    assert!(defaults.method.is_none());
    // roomkit.RoomKit.complete_verification(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn record_failed_attempt_options_defaults_as_python() {
    let defaults = roomkit::RecordFailedAttemptOptions::default();
    // roomkit.RoomKit.record_failed_attempt(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn revoke_verification_options_defaults_as_python() {
    let defaults = roomkit::RevokeVerificationOptions::default();
    // roomkit.RoomKit.revoke_verification(reason) = 'revoked'
    assert_eq!(defaults.reason, "revoked");
    // roomkit.RoomKit.revoke_verification(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn verification_of_options_defaults_as_python() {
    let defaults = roomkit::VerificationOfOptions::default();
    // roomkit.RoomKit.verification_of(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn verification_policy_defaults_as_python() {
    let defaults = roomkit::model::VerificationPolicy::default();
    // roomkit.VerificationPolicy.__init__(ttl) = 900.0
    assert_eq!(defaults.ttl, Duration::from_mins(15));
    // roomkit.VerificationPolicy.__init__(request_ttl) = 600.0
    assert_eq!(defaults.request_ttl, Duration::from_mins(10));
    // roomkit.VerificationPolicy.__init__(code_request_ttl) = 300.0
    assert_eq!(defaults.code_request_ttl, Duration::from_mins(5));
    // roomkit.VerificationPolicy.__init__(max_attempts) = 3
    assert_eq!(defaults.max_attempts, 3);
    // roomkit.VerificationPolicy.__init__(code_length) = 6
    assert_eq!(defaults.code_length, 6);
    // roomkit.VerificationPolicy.__init__(release_on_verify) = True
    assert!(defaults.release_on_verify);
    // roomkit.VerificationPolicy.__init__(release_max_messages) = 20
    assert_eq!(defaults.release_max_messages, 20);
    // roomkit.VerificationPolicy.__init__(release_max_age) = 3600.0
    assert_eq!(defaults.release_max_age, Duration::from_hours(1));
    // roomkit.VerificationPolicy.__init__(hold_unverified) = True
    assert!(defaults.hold_unverified);
}

#[test]
fn install_strategy_options_defaults_as_python() {
    let defaults = roomkit::InstallStrategyOptions::default();
    // roomkit.RoomKit.install_strategy(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn uninstall_strategy_options_defaults_as_python() {
    let defaults = roomkit::UninstallStrategyOptions::default();
    // roomkit.RoomKit.uninstall_strategy(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn claim_room_strategy_options_defaults_as_python() {
    let defaults = roomkit::ClaimRoomStrategyOptions::default();
    // roomkit.RoomKit.claim_room_strategy(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn room_strategy_options_defaults_as_python() {
    let defaults = roomkit::RoomStrategyOptions::default();
    // roomkit.RoomKit.room_strategy(organization_id) = None
    assert!(defaults.organization_id.is_none());
}

#[test]
fn conversation_router_options_defaults_as_python() {
    let defaults = orchestration::ConversationRouterOptions::default();
    // roomkit.ConversationRouter.__init__(rules) = None
    assert!(defaults.rules.is_empty());
    // roomkit.ConversationRouter.__init__(default_agent_id) = None
    assert!(defaults.default_agent_id.is_none());
    // roomkit.ConversationRouter.__init__(supervisor_id) = None
    assert!(defaults.supervisor_id.is_none());
}

#[test]
fn loop_options_defaults_as_python() {
    let defaults = orchestration::LoopOptions::default();
    // roomkit.Loop.__init__(reviewers) = None
    assert!(defaults.reviewers.is_empty());
    // roomkit.Loop.__init__(reviewer) = None
    assert!(defaults.reviewer.is_none());
    // roomkit.Loop.__init__(max_iterations) = 3
    assert_eq!(defaults.max_iterations, 3);
    // roomkit.Loop.__init__(strategy) = None
    assert!(defaults.strategy.is_none());
}

#[test]
fn supervisor_options_defaults_as_python() {
    let defaults = orchestration::SupervisorOptions::default();
    // roomkit.Supervisor.__init__(strategy) = None
    assert!(defaults.strategy.is_none());
    // roomkit.Supervisor.__init__(share_channels) = None
    assert!(defaults.share_channels.is_none());
    // roomkit.Supervisor.__init__(task_timeout) = 120.0
    assert_eq!(defaults.task_timeout, Duration::from_secs(120));
    // roomkit.Supervisor.__init__(max_revisions) = 3
    assert_eq!(defaults.max_revisions, 3);
    // roomkit.Supervisor.__init__(auto_delegate) = False
    assert!(!defaults.auto_delegate);
    // roomkit.Supervisor.__init__(refine_task) = True
    assert!(defaults.refine_task);
    // roomkit.Supervisor.__init__(refine_instruction) = None
    assert!(defaults.refine_instruction.is_none());
    // roomkit.Supervisor.__init__(delegation_message) = "I'm dispatching my team to work on this."
    assert_eq!(
        defaults.delegation_message.as_deref(),
        Some("I'm dispatching my team to work on this.")
    );
    // roomkit.Supervisor.__init__(wait_for_result) = True
    assert!(defaults.wait_for_result);
}

#[test]
fn transition_options_defaults_as_python() {
    let defaults = orchestration::TransitionOptions::default();
    // roomkit.ConversationState.transition(to_agent) = None
    assert!(defaults.to_agent.is_none());
    // roomkit.ConversationState.transition(reason) = ''
    assert_eq!(defaults.reason, "");
    // roomkit.ConversationState.transition(metadata) = None
    assert!(defaults.metadata.is_empty());
}

#[test]
fn console_tool_auditor_options_defaults_as_python() {
    let defaults = orchestration::ConsoleToolAuditorOptions::default();
    // roomkit.orchestration.ConsoleToolAuditor.__init__(level) = 20
    assert_eq!(defaults.level, tracing::Level::INFO);
}

#[test]
fn console_telemetry_provider_options_defaults_as_python() {
    let defaults = roomkit::telemetry::ConsoleTelemetryProviderOptions::default();
    // roomkit.telemetry.ConsoleTelemetryProvider.__init__(level) = 20
    assert_eq!(defaults.level, tracing::Level::INFO);
}

#[test]
fn start_span_options_defaults_as_python() {
    let defaults = roomkit::telemetry::StartSpanOptions::default();
    // roomkit.telemetry.TelemetryProvider.start_span(parent_id) = None
    assert!(defaults.parent_id.is_none());
    // roomkit.telemetry.TelemetryProvider.start_span(attributes) = None
    assert!(defaults.attributes.is_empty());
    // roomkit.telemetry.TelemetryProvider.start_span(room_id) = None
    assert!(defaults.room_id.is_none());
    // roomkit.telemetry.TelemetryProvider.start_span(session_id) = None
    assert!(defaults.session_id.is_none());
    // roomkit.telemetry.TelemetryProvider.start_span(channel_id) = None
    assert!(defaults.channel_id.is_none());
}

#[test]
fn end_span_options_defaults_as_python() {
    let defaults = roomkit::telemetry::EndSpanOptions::default();
    // roomkit.telemetry.TelemetryProvider.end_span(status) = 'ok'
    assert_eq!(defaults.status, "ok");
    // roomkit.telemetry.TelemetryProvider.end_span(error_message) = None
    assert!(defaults.error_message.is_none());
    // roomkit.telemetry.TelemetryProvider.end_span(attributes) = None
    assert!(defaults.attributes.is_empty());
}

#[test]
fn record_metric_options_defaults_as_python() {
    let defaults = roomkit::telemetry::RecordMetricOptions::default();
    // roomkit.telemetry.TelemetryProvider.record_metric(unit) = ''
    assert_eq!(defaults.unit, "");
    // roomkit.telemetry.TelemetryProvider.record_metric(attributes) = None
    assert!(defaults.attributes.is_empty());
}

#[cfg(feature = "opentelemetry")]
#[test]
fn open_telemetry_provider_options_defaults_as_python() {
    let defaults = roomkit::telemetry::OpenTelemetryProviderOptions::default();
    // roomkit.telemetry.OpenTelemetryProvider.__init__(tracer_provider) = None
    assert!(defaults.tracer_provider.is_none());
    // roomkit.telemetry.OpenTelemetryProvider.__init__(meter_provider) = None
    assert!(defaults.meter_provider.is_none());
    // roomkit.telemetry.OpenTelemetryProvider.__init__(service_name) = 'roomkit'
    assert_eq!(defaults.service_name, "roomkit");
    // roomkit.telemetry.OpenTelemetryProvider.__init__(metadata) = None
    assert!(defaults.metadata.is_empty());
    // roomkit.telemetry.OpenTelemetryProvider.__init__(shutdown_flush_timeout) = 4.0
    assert_eq!(defaults.shutdown_flush_timeout, Duration::from_secs(4));
}

#[test]
fn handoff_handler_options_defaults_as_python() {
    let defaults = orchestration::HandoffHandlerOptions::default();
    // roomkit.HandoffHandler.__init__(agent_aliases) = None
    assert!(defaults.agent_aliases.is_none());
    // roomkit.HandoffHandler.__init__(phase_map) = None
    assert!(defaults.phase_map.is_none());
    // roomkit.HandoffHandler.__init__(allowed_transitions) = None
    assert!(defaults.allowed_transitions.is_none());
    // roomkit.HandoffHandler.__init__(known_agents) = None
    assert!(defaults.known_agents.is_none());
    // roomkit.HandoffHandler.__init__(on_handoff_complete) = None
    assert!(defaults.on_handoff_complete.is_none());
    // roomkit.HandoffHandler.__init__(event_channel_id) = None
    assert!(defaults.event_channel_id.is_none());
}

#[test]
fn conversation_router_install_options_defaults_as_python() {
    let defaults = orchestration::InstallOptions::default();
    // roomkit.ConversationRouter.install(agent_aliases) = None
    assert!(defaults.agent_aliases.is_none());
    // roomkit.ConversationRouter.install(phase_map) = None
    assert!(defaults.phase_map.is_none());
    // roomkit.ConversationRouter.install(hook_priority) = -100
    assert_eq!(defaults.hook_priority, -100);
}
