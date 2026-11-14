//! An empty request needs host content, not merely missed room history.

use std::sync::{Arc, Mutex};

use roomkit::Channel;
use roomkit::model::{ChannelOutput, RoomEvent, TextContent};
use serde_json::json;
use tokio::io::DuplexStream;
use tokio::sync::mpsc;

use super::super::{Agent, PATIENCE};
use super::{arguments, channel, transport};
use crate::{AcpChannelOptions, AcpContextContributor};

const HOST_CONTENT: &str = "The user supplied an image without a caption.";

// Python: tests/test_channels/test_acp_empty_prompt.py::test_host_content_supplies_a_prompt_without_mutating_the_event
#[tokio::test]
async fn test_host_content_supplies_a_prompt_without_mutating_the_event() {
    for body in ["", "   "] {
        tokio::time::timeout(PATIENCE, host_supplies_prompt(body))
            .await
            .unwrap();
    }
}

async fn host_supplies_prompt(body: &str) {
    let seen = Arc::new(Mutex::new(Vec::<RoomEvent>::new()));
    let observed = Arc::clone(&seen);
    let contributor = AcpContextContributor::new(move |_, event| {
        observed.lock().unwrap().push(event);
        std::future::ready(Ok(vec![HOST_CONTENT.to_owned()]))
    });
    let (transport, mut opened) = transport();
    let channel = channel(AcpChannelOptions {
        transport: Some(transport),
        context_contributor: Some(contributor),
        ..Default::default()
    });
    let (mut trigger, binding, mut context) = arguments();
    trigger.content = TextContent::new(body).into();
    let before = serde_json::to_value(&trigger).unwrap();
    context.recent_events = vec![trigger.clone()];
    let (output, mut agent) = tokio::join!(
        channel.on_event(trigger.clone(), binding, context),
        answer_prompt(&mut opened, body)
    );
    output.unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![trigger.clone()]);
    assert_eq!(serde_json::to_value(&trigger).unwrap(), before);
    let (_, ()) = tokio::join!(channel.close(), close_after_single_prompt(&mut agent));
}

async fn answer_prompt(opened: &mut mpsc::UnboundedReceiver<DuplexStream>, body: &str) -> Agent {
    let mut agent = Agent::new(opened.recv().await.unwrap());
    let prompt = agent.prompted(true).await;
    let expected = if body.is_empty() {
        HOST_CONTENT.to_owned()
    } else {
        format!("{HOST_CONTENT}\n\n{body}")
    };
    assert_eq!(
        prompt["params"]["prompt"],
        json!([{"type":"text","text":expected}])
    );
    agent
        .answer(&prompt, json!({"stopReason":"end_turn"}))
        .await;
    agent
}

async fn close_after_single_prompt(agent: &mut Agent) {
    let closing = agent.read().await;
    assert_eq!(closing["method"], "session/close");
    agent.answer(&closing, json!({})).await;
    assert!(agent.lines.next_line().await.unwrap().is_none());
}

// Python: tests/test_channels/test_acp_empty_prompt.py::test_no_turn_without_text_or_host_content
#[tokio::test]
async fn test_no_turn_without_text_or_host_content() {
    for body in ["", "   "] {
        for blocks in [None, Some(Vec::new()), Some(vec![" ".to_owned()])] {
            no_prompt(body, blocks).await;
        }
    }
}

async fn no_prompt(body: &str, blocks: Option<Vec<String>>) {
    let contributor = blocks.map(|blocks| {
        AcpContextContributor::new(move |_, _| std::future::ready(Ok(blocks.clone())))
    });
    let (transport, mut opened) = transport();
    let channel = channel(AcpChannelOptions {
        transport: Some(transport),
        context_contributor: contributor,
        ..Default::default()
    });
    let (mut trigger, binding, mut context) = arguments();
    trigger.content = TextContent::new(body).into();
    let mut missed = trigger.clone();
    missed.content = TextContent::new("Earlier text").into();
    context.recent_events = vec![missed, trigger.clone()];
    let output = channel.on_event(trigger, binding, context).await.unwrap();
    assert_eq!(output, ChannelOutput::empty());
    assert!(opened.try_recv().is_err());
    assert_eq!(channel.active_turns(), 0);
    channel.close().await;
}
