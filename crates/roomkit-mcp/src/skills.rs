//! The connected MCP provider implements the transport-independent skill protocol.

use std::collections::BTreeMap;

use roomkit::model::Metadata;
use roomkit_ai::{McpSkillServer, McpSkillServerError, McpSkillServerFuture};
use serde_json::Value;

use crate::{McpToolProvider, ReadResourceOptions, RequestOptions};

impl McpSkillServer for McpToolProvider {
    fn server_extensions(&self) -> Result<BTreeMap<String, Metadata>, McpSkillServerError> {
        Ok(Self::server_extensions(self)?)
    }

    fn request<'a>(
        &'a self,
        method: &'a str,
        params: Option<&'a Metadata>,
    ) -> McpSkillServerFuture<'a, Metadata> {
        Box::pin(async move {
            Ok(Self::request(self, method, params, RequestOptions::default()).await?)
        })
    }

    fn read_resource<'a>(&'a self, uri: &'a str) -> McpSkillServerFuture<'a, Value> {
        Box::pin(async move {
            Ok(Value::Object(
                Self::read_resource(self, uri, ReadResourceOptions::default()).await?,
            ))
        })
    }
}
