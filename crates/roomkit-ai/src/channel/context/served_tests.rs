//! The provider-owned call still ends the prepared loop with one terminal marker.

use crate::tool_loop::TurnItem;
use crate::{
    AiChannel, AiChannelOptions, AiResponse, AiToolCall, MockAiProvider, MockAiProviderOptions,
    ServedCall,
};
use roomkit::model::{
    ChannelBinding, ChannelType, EventSource, LoopEndReason, Room, RoomContext, RoomEvent,
    TextContent,
};
use roomkit::{Channel, TurnRecord};

// Python: tests/test_channels/test_loop_end_marker.py::test_provider_owned_tool_calls_still_end_with_a_marker
#[tokio::test]
async fn provider_owned_tool_calls_still_end_with_a_marker() {
    for streaming in [false, true] {
        let mut call = AiToolCall::new("c1", "lookup");
        call.served = Some(ServedCall {
            result: "found".into(),
            is_error: false,
        });
        let mut response = AiResponse::new("");
        response.tool_calls.push(call);
        let model = MockAiProvider::new(MockAiProviderOptions {
            streaming,
            ai_responses: Some(vec![response]),
            ..Default::default()
        });
        let agent = AiChannel::new(
            "agent".parse().unwrap(),
            model.clone(),
            AiChannelOptions::default(),
        )
        .unwrap();
        let room = "r".parse().unwrap();
        let binding = ChannelBinding::new("agent".parse().unwrap(), room, ChannelType::AI);
        let event = RoomEvent::new(
            binding.room_id.clone(),
            EventSource::new("ws".parse().unwrap(), ChannelType::WEBSOCKET),
            TextContent::new("Go"),
        );
        let context = RoomContext::new(Room::new(binding.room_id.clone()));
        // `answer` is the preparation used by public on_event. Read its items
        // directly because the standalone public call buffers them as rows.
        let mut prepared = agent
            .answer(event, binding, context, TurnRecord::default(), None)
            .await
            .unwrap()
            .unwrap();
        let mut ends = Vec::new();
        let mut last_was_end = false;
        while let Some(item) = crate::stream::next(&mut prepared.items).await {
            let item = item.unwrap();
            last_was_end = matches!(item, TurnItem::End(_));
            if let TurnItem::End(end) = item {
                ends.push(end);
            }
        }
        assert!(last_was_end);
        assert_eq!(ends.len(), 1);
        assert_eq!(ends.first().unwrap().reason, LoopEndReason::Completed);
        assert_eq!(model.calls().len(), 1);
        agent.close().await;
    }
}
