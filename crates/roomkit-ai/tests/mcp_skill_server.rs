//! Python: roomkit.skills.mcp.MCPSkillServer, independent of an MCP transport.

use std::collections::BTreeMap;

use roomkit::model::Metadata;
use roomkit_ai::{McpSkillServer, McpSkillServerFuture};
use serde_json::{Value, json};

#[derive(Debug, thiserror::Error)]
#[error("custom server refused")]
struct HostFailure;

struct Host;

impl McpSkillServer for Host {
    fn server_extensions(
        &self,
    ) -> Result<BTreeMap<String, Metadata>, roomkit_ai::McpSkillServerError> {
        Ok(BTreeMap::from([("custom".into(), Metadata::new())]))
    }

    fn request<'a>(
        &'a self,
        method: &'a str,
        params: Option<&'a Metadata>,
    ) -> McpSkillServerFuture<'a, Metadata> {
        Box::pin(async move {
            if method == "fail" {
                return Err(HostFailure.into());
            }
            Ok(params.cloned().unwrap_or_default())
        })
    }

    fn read_resource<'a>(&'a self, uri: &'a str) -> McpSkillServerFuture<'a, Value> {
        Box::pin(async move { Ok(json!([uri, null])) })
    }
}

#[tokio::test]
async fn a_custom_server_needs_no_mcp_transport_and_keeps_its_error() {
    let server: &dyn McpSkillServer = &Host;
    assert!(server.server_extensions().unwrap().contains_key("custom"));
    let params = Metadata::from_iter([("nested".into(), json!({"value": null}))]);
    assert_eq!(server.request("echo", Some(&params)).await.unwrap(), params);
    assert!(server.request("echo", None).await.unwrap().is_empty());
    assert_eq!(
        server.read_resource("anything").await.unwrap(),
        json!(["anything", null])
    );
    assert!(
        server
            .request("fail", None)
            .await
            .unwrap_err()
            .downcast_ref::<HostFailure>()
            .is_some()
    );
}
