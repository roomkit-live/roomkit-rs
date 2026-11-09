//! Stream segments share the cascade's hook-call budget, apart from its passes.

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit::{ResponseItem, current_turn_host};

use super::*;

struct Segments(VecDeque<ResponseItem>);

impl Stream for Segments {
    type Item = Result<ResponseItem, ChannelError>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.get_mut().0.pop_front().map(Ok))
    }
}

struct Streaming(usize);

impl Channel for Streaming {
    fn channel_id(&self) -> ChannelId {
        channel("agent-stream")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        if event.source.channel_type != ChannelType::SMS {
            return ready(Ok(ChannelOutput::empty()));
        }
        let segments = (0..self.0)
            .flat_map(|n| {
                [
                    ResponseItem::Text(format!("part {n}. ")),
                    ResponseItem::SegmentBreak,
                ]
            })
            .collect();
        ready(Ok(current_turn_host()
            .unwrap()
            .respond_with_stream(Box::pin(Segments(segments)))))
    }
}

fn notice_per_row(kit: &RoomKit) -> Kept<Option<String>> {
    let notices = kept();
    let (hook_kit, hook_notices) = (kit.clone(), Arc::clone(&notices));
    kit.hook(
        HookTrigger::AfterBroadcast,
        move |event: RoomEvent, _| {
            let (kit, notices) = (hook_kit.clone(), Arc::clone(&hook_notices));
            async move {
                if !event.source.channel_id.as_str().starts_with("agent-")
                    || event.blocked_by.is_some()
                {
                    return;
                }
                let noted = kit
                    .send_event(
                        &room_id(),
                        &channel("sms"),
                        TextContent::new("Noted."),
                        SendEventOptions {
                            addressed_to: Some(Vec::new()),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                push(&notices, noted.blocked_by);
            }
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "notice".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    notices
}

async fn streamed_rows(
    depth: u32,
    rows: usize,
    answers: bool,
) -> (Vec<RoomEvent>, Vec<Option<String>>) {
    let kit = room(depth).await;
    kit.register_channel(Streaming(rows)).unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("agent-stream"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    if answers {
        kit.register_channel(Answering::new("agent-answers"))
            .unwrap();
        kit.attach_channel(
            &room_id(),
            &channel("agent-answers"),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    }
    let notices = notice_per_row(&kit);
    tokio::time::timeout(Duration::from_secs(5), async {
        kit.process_inbound(
            message("hi"),
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        roomkit::testing::idle(&kit).await;
    })
    .await
    .unwrap();
    let events = kit
        .store()
        .list_events(&room_id(), every_row())
        .await
        .unwrap();
    kit.close().await.unwrap();
    (events, taken(&notices))
}

// Python: tests/test_reentrance.py::TestTheBudgetOfAHooksCalls::test_a_notice_per_streamed_row_costs_its_answers_nothing
#[tokio::test]
async fn a_notice_per_streamed_row_costs_its_answers_nothing() {
    let (events, notices) = streamed_rows(3, 12, true).await;
    let mut rows: Vec<_> = events
        .into_iter()
        .filter(|event| {
            event.source.channel_id.as_str().starts_with("agent-")
                && event.blocked_by.as_deref() != Some("event_chain_depth_limit")
        })
        .map(|event| (event.source.channel_id, event.blocked_by))
        .collect();
    rows.sort();
    let expected: Vec<_> = std::iter::repeat_n((channel("agent-answers"), None), 13)
        .chain(std::iter::repeat_n((channel("agent-stream"), None), 12))
        .collect();
    assert_eq!((rows, notices), (expected, vec![None; 25]));
}

// Python: tests/test_reentrance.py::TestTheBudgetOfAHooksCalls::test_a_notice_per_row_past_the_hook_cap_is_blocked
#[tokio::test]
async fn a_notice_per_row_past_the_hook_cap_is_blocked() {
    let (events, notices) = streamed_rows(2, 22, false).await;
    let rows: Vec<_> = events
        .into_iter()
        .filter(|event| event.source.channel_id == channel("agent-stream"))
        .map(|event| event.blocked_by)
        .collect();
    let expected: Vec<_> = std::iter::repeat_n(None, 20)
        .chain(std::iter::repeat_n(Some("reentry_loop_cap".to_owned()), 2))
        .collect();
    assert_eq!((rows, notices), (vec![None; 22], expected));
}
