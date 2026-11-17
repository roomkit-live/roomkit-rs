//! Reporting failures are logged once, before or after real hook delegation.
use super::*;
use roomkit::model::{HookResult, InboundMessage};
use roomkit::{
    CancelOptions, HookExecution, HookOptions, HookTrigger, ProcessInboundOptions, ToolCallEvent,
};
use roomkit_ai::{OnToolRefusedOptions, ServedCall, ToolError};

#[derive(Clone, Copy, PartialEq, Eq)]
enum End {
    Served,
    ProviderServed,
    ProviderFailed,
    Refused,
    Cancelled,
}

struct Failing {
    state: ExternalToolHandlerState,
    after: bool,
    end: End,
    entered: Notify,
}
impl ExternalToolHandler for Failing {
    fn state(&self) -> &ExternalToolHandlerState {
        &self.state
    }
    fn process_tool_call<'a>(
        &'a self,
        _: &'a str,
        _: &'a Metadata,
        _: ProcessToolCallOptions,
    ) -> ExternalToolFuture<'a, ToolDecision> {
        Box::pin(async move {
            self.entered.notify_one();
            if self.end == End::Cancelled {
                std::future::pending::<()>().await;
            }
            Ok(ToolDecision {
                reason: "not on this host".into(),
                ..ToolDecision::new(self.end != End::Refused)
            })
        })
    }
    fn on_tool_result<'a>(
        &'a self,
        name: &'a str,
        input: &'a Metadata,
        result: &'a str,
        options: OnToolResultOptions,
    ) -> ExternalToolFuture<'a, ()> {
        Box::pin(async move {
            if self.after {
                self.state
                    .fire_on_tool_hook(
                        name,
                        input,
                        result,
                        FireOnToolHookOptions {
                            tool_call_id: options.tool_call_id,
                            room_id: options.room_id,
                            is_error: options.is_error,
                            ..FireOnToolHookOptions::default()
                        },
                    )
                    .await;
            }
            Err(ToolError::Failed("audit sink down".into()))
        })
    }
    fn on_tool_refused<'a>(
        &'a self,
        name: &'a str,
        input: &'a Metadata,
        reason: &'a str,
        options: OnToolRefusedOptions,
    ) -> ExternalToolFuture<'a, ()> {
        Box::pin(async move {
            if self.after {
                self.state
                    .on_tool_refused(name, input, reason, options)
                    .await;
            }
            Err(ToolError::Failed("audit sink down".into()))
        })
    }
    fn on_tool_cancelled<'a>(
        &'a self,
        name: &'a str,
        input: &'a Metadata,
        options: OnToolCancelledOptions,
    ) -> ExternalToolFuture<'a, ()> {
        Box::pin(async move {
            if self.after {
                self.state.on_tool_cancelled(name, input, options).await;
            }
            Err(ToolError::Failed("audit sink down".into()))
        })
    }
}

// Python: test_handler_report_raise.py::test_a_handler_raising_while_reporting_leaves_the_call_reported_once
// The five AI outcomes and before/after variants; Rust rejects an override with
// an incompatible trait signature at compilation, not as a dynamic TypeError.
#[tokio::test]
async fn reporting_errors_are_logged_once_and_preserve_the_outcome() {
    for streaming in [false, true] {
        for end in [
            End::Served,
            End::ProviderServed,
            End::ProviderFailed,
            End::Refused,
            End::Cancelled,
        ] {
            for after in [false, true] {
                run(streaming, end, after).await;
            }
        }
    }
}

async fn run(streaming: bool, end: End, after: bool) {
    let (warnings, _capture) = warnings::Warnings::capture_at(tracing::Level::ERROR);
    let handler = Arc::new(Failing {
        state: ExternalToolHandlerState::default(),
        after,
        end,
        entered: Notify::new(),
    });
    let mut call = AiToolCall::new("c1", "remote");
    if matches!(end, End::ProviderServed | End::ProviderFailed) {
        call.served = Some(ServedCall {
            result: "known result".into(),
            is_error: end == End::ProviderFailed,
        });
    }
    let agent = AiChannel::new(
        "ai".parse().unwrap(),
        model(streaming, call),
        AiChannelOptions {
            external_tool_handler: Some(Arc::<Failing>::clone(&handler)),
            ..AiChannelOptions::default()
        },
    )
    .unwrap();
    let kit = reports::room(agent).await;
    let sync = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&sync);
    kit.hook(
        HookTrigger::OnToolCall,
        move |_: ToolCallEvent, _| {
            count.fetch_add(1, Ordering::SeqCst);
            std::future::ready(HookResult::<ToolCallEvent>::allow(
                roomkit::model::hook::AllowOptions::default(),
            ))
        },
        HookOptions {
            name: "sync".into(),
            ..HookOptions::default()
        },
    )
    .unwrap();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let capture = Arc::clone(&observed);
    kit.hook(
        HookTrigger::OnToolCall,
        move |event: ToolCallEvent, _| {
            capture
                .lock()
                .unwrap()
                .push((event.cancelled, event.refused, event.is_error));
            std::future::ready(())
        },
        HookOptions {
            name: "audit".into(),
            execution: HookExecution::Async,
            ..HookOptions::default()
        },
    )
    .unwrap();
    let sent = kit
        .process_inbound(
            InboundMessage::new("sms".parse().unwrap(), "u", TextContent::new("go")),
            ProcessInboundOptions {
                room_id: Some("r".parse().unwrap()),
                defer_delivery: true,
                ..ProcessInboundOptions::default()
            },
        )
        .await
        .unwrap();
    let delivery = sent.delivery.unwrap();
    if end == End::Cancelled {
        bounded(handler.entered.notified()).await;
        bounded(delivery.cancel(CancelOptions::default()))
            .await
            .unwrap();
    } else {
        bounded(delivery.wait()).await.unwrap();
    }
    bounded(roomkit::testing::idle(&kit)).await;
    bounded(kit.close()).await.unwrap();
    assert_reports(
        end,
        &observed.lock().unwrap(),
        sync.load(Ordering::SeqCst),
        &warnings.containing(""),
    );
}

fn assert_reports(end: End, observed: &[(bool, bool, bool)], sync: usize, errors: &[String]) {
    assert_eq!(
        observed,
        [(
            end == End::Cancelled,
            end == End::Refused,
            matches!(end, End::Cancelled | End::Refused | End::ProviderFailed)
        )]
    );
    assert_eq!(
        sync,
        usize::from(!matches!(end, End::Cancelled | End::Refused))
    );
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(
        errors.first().unwrap().contains("c1"),
        "the report failure identifies its call: {errors:?}"
    );
    assert!(
        errors
            .first()
            .unwrap()
            .contains("External tool handler failed")
    );
}
