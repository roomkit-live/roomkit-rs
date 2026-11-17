//! Negotiation and optional discovery are identical on both HTTP transports.

use rmcp::{
    ClientCacheConfig, RoleClient, ServiceExt, service::RunningService, transport::Transport,
};

use super::client::Failure;
use crate::McpError;
use crate::listing::{self, Listed, ToolFilter};
use rmcp::model::ClientConfig;

pub(super) async fn open(
    transport: impl Transport<RoleClient> + 'static,
    failure: &Failure,
    filter: Option<&ToolFilter>,
    discover: bool,
) -> Result<(RunningService<RoleClient, ClientConfig>, Listed), McpError> {
    let service = crate::protocol::client()
        .serve(transport)
        .await
        .map_err(|_| failure.error("MCP handshake failed"))?;
    if !service
        .peer_info()
        .is_some_and(|info| crate::protocol::supported(&info.protocol_version))
    {
        let _closed = service.cancel().await;
        return Err(failure.error("unsupported MCP protocol version"));
    }
    service
        .peer()
        .set_response_cache_config(ClientCacheConfig::disabled())
        .await;
    if !discover {
        return Ok((service, Listed::default()));
    }
    if let Ok(tools) = listing::tools(service.peer(), "http", filter).await {
        Ok((service, tools))
    } else {
        let error = failure.error("MCP tool discovery failed");
        let _closed = service.cancel().await;
        Err(error)
    }
}
