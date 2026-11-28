//! How a connection agrees on the MCP revision with its server when
//! `protocol` is `"auto"` (RFC §21.2, RK-21.2-056): Python's MCP SDK's
//! `negotiate_auto`, step by step. roomkit-mcp sends the probe, and the
//! handshake it falls back to, itself on the raw transport; rmcp then opens
//! the connection as the negotiation settled it (`Discover` or
//! `Initialize`), its own negotiation request answered with the server's
//! answer, never sent again.
//!
//! rmcp's `Auto` lifecycle bounds every probe together at 10 seconds, fails
//! a connection Python's SDK falls back from, and never probes again after
//! a handshake refused with `-32022`.

use std::future::Future;
use std::time::Duration;

use rmcp::model::{
    ClientJsonRpcMessage, ClientRequest, DiscoverRequest, DiscoverRequestParams, DiscoverResult,
    ErrorData, InitializeRequest, NumberOrString, ProtocolVersion, RequestMetaObject,
};
use rmcp::service::ClientInitializeError;
use serde_json::Value;

use crate::protocol::{HANDSHAKE_VERSIONS, MODERN, client};

/// How long each probe may go unanswered before the handshake is tried, as
/// Python's SDK bounds it (`DISCOVER_TIMEOUT_SECONDS`).
pub(crate) const PROBE_BOUND: Duration = Duration::from_secs(10);

/// What the id of each request roomkit-mcp negotiates with starts with:
/// rmcp numbers its own, so that neither is taken for the other.
const ID_PREFIX: &str = "roomkit-negotiation-";

/// `-32022`: the server speaks none of the revisions the request offered.
const UNSUPPORTED_PROTOCOL_VERSION: i64 = -32022;

/// What carries the negotiation's requests to the server and their answers
/// back, on the raw transport.
pub(crate) trait Link: Send {
    /// Sends `request` and waits for its answer, the server's JSON-RPC
    /// response or error as it sent it, within `bound` when there is one:
    /// `None` past it.
    ///
    /// # Errors
    /// Returns why the transport failed.
    fn exchange(
        &mut self,
        request: ClientJsonRpcMessage,
        bound: Option<Duration>,
    ) -> impl Future<Output = Result<Option<Value>, String>> + Send;
}

/// What the negotiation agreed on: the server's answer, as it sent it.
#[derive(Debug)]
pub(crate) enum Settled {
    /// A discovery that agreed on MCP 2026-07-28.
    Modern(Value),
    /// The `initialize` handshake.
    Legacy(Value),
}

impl Settled {
    /// The answer's `result`.
    pub(crate) fn result(&self) -> &Value {
        let (Self::Modern(answer) | Self::Legacy(answer)) = self;
        answer.get("result").unwrap_or(&Value::Null)
    }

    pub(crate) fn is_modern(&self) -> bool {
        matches!(self, Self::Modern(_))
    }
}

/// Whether `id` names a request of the negotiation roomkit-mcp sent: an
/// answer to it that comes late is no answer to anything rmcp sent.
pub(crate) fn is_ours(id: &NumberOrString) -> bool {
    matches!(id, NumberOrString::String(id) if id.starts_with(ID_PREFIX))
}

/// Agrees on the MCP revision over `link`, as Python's SDK's
/// `negotiate_auto` does: the probe at 2026-07-28, each one bounded by
/// [`PROBE_BOUND`] of its own; a `-32022` naming 2026-07-28 probes once
/// more; a refusal, a probe unanswered or an answer that agrees on no
/// revision falls back to the handshake, unless a `-32022` names only
/// revisions neither knows; a handshake refused with `-32022` naming
/// 2026-07-28 probes once more.
///
/// # Errors
/// Returns why the negotiation failed, as rmcp words it: the transport, a
/// refusal of the handshake, or no revision in common.
pub(crate) async fn negotiate_auto(link: &mut impl Link) -> Result<Settled, String> {
    let mut sent = 0;
    for attempt in 0..2 {
        sent += 1;
        let probe = link.exchange(discover(sent), Some(PROBE_BOUND)).await?;
        let refusal = match &probe {
            None => Some(Value::Null),
            Some(answer) => answer.get("error").cloned(),
        };
        let Some(refusal) = refusal else {
            let answer = probe.unwrap_or_default();
            if answer.get("result").is_some_and(agrees) {
                return Ok(Settled::Modern(answer));
            }
            // A discovery that agrees on no revision both speak is an
            // earlier revision's advertisement.
            sent += 1;
            return handshake(link, sent).await.map(Settled::Legacy);
        };
        if code(&refusal) == Some(UNSUPPORTED_PROTOCOL_VERSION) {
            let supported = supported_versions(refusal.get("data"));
            if attempt == 0 && names_modern(supported.as_deref()) {
                continue;
            }
            if supported.is_some_and(|versions| !names_a_handshake_revision(&versions)) {
                return Err(failure(&refusal));
            }
        }
        sent += 1;
        let answer = link
            .exchange(initialize(sent), None)
            .await?
            .unwrap_or_default();
        let Some(refused) = answer.get("error") else {
            return Ok(Settled::Legacy(answer));
        };
        // A probe that timed out here but reached the server may have made
        // its connection 2026-07-28's: the handshake's -32022 says so.
        let again = attempt == 0
            && code(refused) == Some(UNSUPPORTED_PROTOCOL_VERSION)
            && names_modern(supported_versions(refused.get("data")).as_deref());
        if !again {
            return Err(failure(refused));
        }
    }
    Err("the MCP negotiation agreed on no revision".to_owned())
}

/// The handshake, its result as the server sent it.
async fn handshake(link: &mut impl Link, sent: usize) -> Result<Value, String> {
    let answer = link
        .exchange(initialize(sent), None)
        .await?
        .unwrap_or_default();
    match answer.get("error") {
        Some(refused) => Err(failure(refused)),
        None => Ok(answer),
    }
}

/// The `sent`-th probe, at 2026-07-28, saying who the client is.
fn discover(sent: usize) -> ClientJsonRpcMessage {
    let config = client();
    let meta = RequestMetaObject::with_client_context(
        ProtocolVersion::V_2026_07_28,
        config.client_info,
        config.capabilities,
    );
    let mut request = DiscoverRequest::new(DiscoverRequestParams {});
    request.extensions.insert(meta);
    ClientJsonRpcMessage::request(ClientRequest::DiscoverRequest(request), id(sent))
}

/// The handshake, at 2025-11-25, as rmcp writes it.
fn initialize(sent: usize) -> ClientJsonRpcMessage {
    let request = InitializeRequest::new(client());
    ClientJsonRpcMessage::request(ClientRequest::InitializeRequest(request), id(sent))
}

fn id(sent: usize) -> NumberOrString {
    NumberOrString::String(format!("{ID_PREFIX}{sent}").into())
}

/// Whether a discovery's `result` agrees on 2026-07-28, as Python's SDK
/// reads it: a result its model reads (the cache members it defaults
/// filled in), naming that revision.
fn agrees(result: &Value) -> bool {
    let mut read = result.clone();
    crate::protocol::with_discovery_defaults(&mut read);
    serde_json::from_value::<DiscoverResult>(read).is_ok_and(|result| {
        result
            .supported_versions
            .iter()
            .any(|version| version.as_str() == MODERN)
    })
}

fn code(error: &Value) -> Option<i64> {
    error.get("code").and_then(Value::as_i64)
}

/// The `supported` revisions a `-32022`'s data names, as Python's SDK reads
/// it: `None` unless it names them and the revision requested.
fn supported_versions(data: Option<&Value>) -> Option<Vec<String>> {
    let data = data?.as_object()?;
    data.get("requested")?.as_str()?;
    data.get("supported")?
        .as_array()?
        .iter()
        .map(|version| version.as_str().map(str::to_owned))
        .collect()
}

fn names_modern(versions: Option<&[String]>) -> bool {
    versions.is_some_and(|versions| versions.iter().any(|version| version == MODERN))
}

fn names_a_handshake_revision(versions: &[String]) -> bool {
    versions
        .iter()
        .any(|version| HANDSHAKE_VERSIONS.contains(&version.as_str()))
}

/// A refusal that fails the negotiation, as rmcp words a JSON-RPC error.
fn failure(error: &Value) -> String {
    serde_json::from_value::<ErrorData>(error.clone()).map_or_else(
        |_| "the MCP server refused the negotiation".to_owned(),
        |error| ClientInitializeError::JsonRpcError(error).to_string(),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    /// A server answering each request of the negotiation in turn, `None`
    /// for a request it leaves unanswered past its bound.
    struct Scripted {
        answers: VecDeque<Option<Value>>,
        sent: Vec<String>,
    }

    impl Link for Scripted {
        fn exchange(
            &mut self,
            request: ClientJsonRpcMessage,
            _bound: Option<Duration>,
        ) -> impl Future<Output = Result<Option<Value>, String>> + Send {
            let request = serde_json::to_value(request).unwrap();
            self.sent
                .push(request["method"].as_str().unwrap().to_owned());
            let answer = self
                .answers
                .pop_front()
                .ok_or_else(|| "closed".to_owned())
                .map(|answer| {
                    answer.map(|mut answer| {
                        answer["id"] = request["id"].clone();
                        answer
                    })
                });
            std::future::ready(answer)
        }
    }

    fn discovery(versions: &Value) -> Value {
        json!({"jsonrpc":"2.0","result":{"supportedVersions":versions,"capabilities":{}}})
    }

    fn refused(code: i64, data: &Value) -> Value {
        json!({"jsonrpc":"2.0","error":{"code":code,"message":"no","data":data}})
    }

    fn handshake() -> Value {
        json!({"jsonrpc":"2.0","result":{"protocolVersion":"2025-11-25","capabilities":{},
            "serverInfo":{"name":"legacy","version":"1"}}})
    }

    // Python: mcp.client._probe.negotiate_auto, read on SDK 2.3: each way a
    // negotiation ends, and the requests it took.
    #[tokio::test]
    async fn a_negotiation_ends_as_python_s_sdk_ends_it() {
        let both = json!({"supported":["2026-07-28","2025-11-25"],"requested":"2026-07-28"});
        let modern_only = json!({"supported":["2026-07-28"],"requested":"2025-11-25"});
        let unknown = json!({"supported":["2099-01-01"],"requested":"2026-07-28"});
        let not_named = json!({"supported":["2099-01-01"]});
        let modern = discovery(&json!(["2026-07-28"]));
        let earlier = discovery(&json!(["2025-11-25"]));
        let unreadable = json!({"jsonrpc":"2.0","result":{}});
        let unknown_method = refused(-32601, &Value::Null);
        let header_mismatch = refused(-32020, &Value::Null);
        let probe = "server/discover";
        let initialize = "initialize";
        for (answers, ended, sent) in [
            (vec![Some(modern.clone())], "modern", vec![probe]),
            (
                vec![Some(earlier), Some(handshake())],
                "legacy",
                vec![probe, initialize],
            ),
            (
                vec![Some(unreadable), Some(handshake())],
                "legacy",
                vec![probe, initialize],
            ),
            (
                vec![Some(unknown_method.clone()), Some(handshake())],
                "legacy",
                vec![probe, initialize],
            ),
            (
                vec![Some(header_mismatch), Some(handshake())],
                "legacy",
                vec![probe, initialize],
            ),
            (
                vec![None, Some(handshake())],
                "legacy",
                vec![probe, initialize],
            ),
            (
                vec![Some(refused(-32022, &both)), Some(modern.clone())],
                "modern",
                vec![probe, probe],
            ),
            (
                vec![
                    Some(refused(-32022, &both)),
                    Some(refused(-32022, &both)),
                    Some(handshake()),
                ],
                "legacy",
                vec![probe, probe, initialize],
            ),
            (vec![Some(refused(-32022, &unknown))], "failed", vec![probe]),
            (
                vec![Some(refused(-32022, &not_named)), Some(handshake())],
                "legacy",
                vec![probe, initialize],
            ),
            // A probe that timed out reached a server that went 2026-07-28's.
            (
                vec![
                    None,
                    Some(refused(-32022, &modern_only)),
                    Some(modern.clone()),
                ],
                "modern",
                vec![probe, initialize, probe],
            ),
            (
                vec![
                    None,
                    Some(refused(-32022, &modern_only)),
                    None,
                    Some(refused(-32022, &modern_only)),
                ],
                "failed",
                vec![probe, initialize, probe, initialize],
            ),
            (
                vec![Some(unknown_method), Some(refused(-32603, &Value::Null))],
                "failed",
                vec![probe, initialize],
            ),
        ] {
            let mut link = Scripted {
                answers: answers.into(),
                sent: Vec::new(),
            };
            let outcome = negotiate_auto(&mut link).await;
            let ended_as = match &outcome {
                Ok(Settled::Modern(_)) => "modern",
                Ok(Settled::Legacy(_)) => "legacy",
                Err(_) => "failed",
            };
            let sent_now: Vec<&str> = link.sent.iter().map(String::as_str).collect();
            assert_eq!((ended_as, sent_now), (ended, sent), "{outcome:?}");
        }
    }

    #[test]
    fn only_the_negotiation_s_own_ids_are_ours() {
        assert!(is_ours(&id(1)));
        assert!(!is_ours(&NumberOrString::Number(1)));
        assert!(!is_ours(&NumberOrString::String("1".into())));
    }
}
