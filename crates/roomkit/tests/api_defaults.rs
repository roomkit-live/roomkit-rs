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
use roomkit::InMemoryStore;
use roomkit::ListBindingsOptions;
use roomkit::ListMembersOptions;
use roomkit::MuteOptions;
use roomkit::MuteOutputOptions;
use roomkit::NormalizeOptions;
use roomkit::PauseRoomOptions;
use roomkit::PostOptions;
use roomkit::ProcessInboundOptions;
use roomkit::RemoveRoomHookOptions;
use roomkit::ReopenRoomOptions;
use roomkit::ResumeRoomOptions;
use roomkit::RoomKitOptions;
use roomkit::RouteOptions;
use roomkit::SendEventOptions;
use roomkit::SetAccessOptions;
use roomkit::SetVisibilityOptions;
use roomkit::TransportChannelOptions;
use roomkit::UnmuteOptions;
use roomkit::UnmuteOutputOptions;
use roomkit::UpdateBindingMetadataOptions;
use roomkit::UpdateEventOptions;
use roomkit::model::Access;
use roomkit::model::AgentResponsePolicy;
use roomkit::model::EventType;
use roomkit::model::Visibility;
use roomkit::model::hook::AllowOptions;
use roomkit::model::hook::BlockOptions;
use roomkit::model::hook::ModifyOptions;
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
    let defaults = CreateRoomOptions::default();
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
    // roomkit.RoomKit.__init__(store) = None
    let store: Arc<InMemoryStore> = defaults.store;
    assert_eq!(Arc::strong_count(&store), 1);
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
