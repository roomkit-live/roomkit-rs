//! The host's trace context on a provider's requests (RFC §21.2,
//! RK-21.2-057): Python's
//! `tests/test_mcp_wire_reading.py::test_a_provider_that_propagates_the_trace_context_sends_the_host_s`,
//! end to end: a tool served to an agent sends its requests under the
//! kit's current span.

use std::collections::BTreeMap;
use std::sync::Arc;

use pretty_assertions::assert_eq;
use roomkit::RoomKitOptions;
use roomkit::model::Metadata;
use roomkit::telemetry::{
    EndSpanOptions, MockTelemetryProvider, RecordMetricOptions, SpanKind, StartSpanOptions,
    TelemetryProvider,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions, ToolError, ToolHandler, ToolResult};
use roomkit_mcp::{CallToolOptions, McpToolProvider, RequestOptions};
use serde_json::{Value, json};

use super::support::{Room, Scripted, channel};

/// The trace context a caller writes itself.
const CALLER_TRACE: &str = "00-0000000000000000000000000000abcd-000000000000beef-01";

/// A kit's telemetry whose spans name themselves to another process as W3C
/// does, with the host's baggage: the trace id `1234`.
#[derive(Default)]
struct W3c(MockTelemetryProvider);

impl TelemetryProvider for W3c {
    fn name(&self) -> &'static str {
        "w3c"
    }
    fn start_span(&self, kind: SpanKind, name: &str, options: StartSpanOptions) -> String {
        self.0.start_span(kind, name, options)
    }
    fn end_span(&self, span_id: &str, options: EndSpanOptions) {
        self.0.end_span(span_id, options);
    }
    fn set_attribute(&self, span_id: &str, key: &str, value: Value) {
        self.0.set_attribute(span_id, key, value);
    }
    fn record_metric(&self, name: &str, value: f64, options: RecordMetricOptions) {
        self.0.record_metric(name, value, options);
    }
    fn inject_context(&self, span_id: &str) -> BTreeMap<String, String> {
        BTreeMap::from([
            (
                "traceparent".to_owned(),
                format!("00-00000000000000000000000000001234-{span_id}-01"),
            ),
            ("baggage".to_owned(), "tenant=t1".to_owned()),
        ])
    }
}

/// A tool that, under the kit's span, sends a request of its own, one
/// whose caller writes a trace context, and a call of the server's tool.
fn relaying(server: &McpToolProvider) -> ToolHandler {
    let server = server.clone();
    ToolHandler::new(move |_name, _arguments| {
        let server = server.clone();
        async move {
            let failed = |error: roomkit_mcp::McpError| ToolError::failed(error.to_string());
            server
                .request("x/read", None, RequestOptions::default())
                .await
                .map_err(failed)?;
            let own: Metadata = json!({"_meta":{"traceparent":CALLER_TRACE}})
                .as_object()
                .cloned()
                .unwrap();
            server
                .request("x/read", Some(&own), RequestOptions::default())
                .await
                .map_err(failed)?;
            let text = server
                .call_tool("lookup", Metadata::new(), CallToolOptions::default())
                .await
                .map_err(failed)?;
            Ok(ToolResult::Text(text))
        }
    })
}

/// The trace keys of each request the server read, but the negotiation's.
fn traces(scripted: &Scripted) -> Vec<(String, Value)> {
    scripted
        .received()
        .into_iter()
        .filter(|message| matches!(message["method"].as_str(), Some("x/read" | "tools/call")))
        .map(|message| {
            let meta = message.pointer("/params/_meta").unwrap_or(&Value::Null);
            let trace: serde_json::Map<String, Value> = ["traceparent", "tracestate", "baggage"]
                .into_iter()
                .filter_map(|key| Some((key.to_owned(), meta.get(key)?.clone())))
                .collect();
            (
                message
                    .get("method")
                    .and_then(Value::as_str)
                    .unwrap()
                    .to_owned(),
                Value::Object(trace),
            )
        })
        .collect()
}

#[tokio::test]
async fn a_provider_that_propagates_the_trace_context_sends_the_host_s() {
    let plan = json!({
        "server/discover": {"error":{"code":-32601,"message":"Method not found"}},
        "initialize": {"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},
            "serverInfo":{"name":"traced","version":"1"}}},
        "x/read": {"result":{}},
        "tools/call": {"result":{"content":[{"type":"text","text":"found"}]}},
    });
    for propagating in [false, true] {
        let scripted = Scripted::new();
        let mut server = scripted.server(&plan).discover(false);
        if propagating {
            server = server.propagating_trace_context();
        }
        let server = server.connect().await.unwrap();
        let model = ScriptedProvider::new([
            ScriptedRound::calling("", [("call_1", "relay", "{}")]),
            ScriptedRound::answering("Done."),
        ]);
        let options = AiChannelOptions {
            tools: vec![
                roomkit_ai::AiTool::new("relay", "Relays to the server.")
                    .unwrap()
                    .into(),
            ],
            tool_handler: Some(relaying(&server)),
            ..AiChannelOptions::default()
        };
        let agent = AiChannel::new(channel("assistant"), model, options).unwrap();
        let telemetry: Arc<dyn TelemetryProvider> = Arc::new(W3c::default());
        let kit = RoomKitOptions {
            telemetry: Some(telemetry.into()),
            ..RoomKitOptions::default()
        };
        let room = Room::on(kit, agent).await;
        room.says("Look it up.").await;
        room.kit.close().await.unwrap();
        server.close().await;
        let traces = traces(&scripted);
        let methods: Vec<&str> = traces.iter().map(|(method, _)| method.as_str()).collect();
        assert_eq!(methods, ["x/read", "x/read", "tools/call"], "{propagating}");
        let caller = &traces[1].1;
        assert_eq!(
            caller,
            &json!({"traceparent": CALLER_TRACE}),
            "{propagating}"
        );
        for (method, trace) in [&traces[0], &traces[2]] {
            if propagating {
                assert!(
                    trace["traceparent"].as_str().is_some_and(
                        |parent| parent.starts_with("00-00000000000000000000000000001234-")
                    ) && trace["baggage"] == "tenant=t1",
                    "{method}: {trace}"
                );
            } else {
                assert_eq!(trace, &json!({}), "{method}");
            }
        }
    }
}
