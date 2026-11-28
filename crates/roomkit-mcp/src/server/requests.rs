//! Preserve SDK request/result handling while legacy callers own queued writes.

use rmcp::model::{CallToolRequestParams, CallToolResult, ClientRequest, ServerResult};
use rmcp::service::ServiceError;

use super::McpServer;

impl McpServer {
    pub(crate) async fn host_request(
        &self,
        request: ClientRequest,
    ) -> Result<ServerResult, ServiceError> {
        #[cfg(feature = "http")]
        if let Some(writer) = &self.inner.legacy {
            return crate::http::legacy::request::send(&self.inner.peer, writer, request).await;
        }
        self.inner.peer.send_request(request).await
    }

    pub(super) async fn tool_request(
        &self,
        params: CallToolRequestParams,
    ) -> Result<CallToolResult, ServiceError> {
        #[cfg(feature = "http")]
        if let Some(writer) = &self.inner.legacy {
            let request = ClientRequest::CallToolRequest(rmcp::model::CallToolRequest::new(params));
            let result =
                crate::http::legacy::request::send(&self.inner.peer, writer, request).await?;
            return if let ServerResult::CallToolResult(result) = result {
                Ok(result)
            } else {
                Err(ServiceError::UnexpectedResponse)
            };
        }
        self.inner.peer.call_tool(params).await
    }
}
