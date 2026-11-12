//! Host-native prompt refusals, kept separate from the fake agent's wire.

use std::sync::{Arc, Mutex};

use agent_client_protocol_schema::v1::{
    AuthenticateRequest, AuthenticateResponse, CancelNotification, CloseSessionRequest,
    CloseSessionResponse, InitializeRequest, InitializeResponse, NewSessionRequest,
    NewSessionResponse, PromptRequest, PromptResponse, SetSessionConfigOptionRequest,
    SetSessionConfigOptionResponse,
};
use roomkit_acp::{
    AcpConnection, AcpError, AcpSessionInvalidatedError, AcpSessionInvalidatedErrorOptions,
    ClientSideConnection,
};
use serde_json::{Value, json};

use super::Wire;

pub(crate) struct Refusing {
    pub(super) connection: ClientSideConnection,
    pub(super) refusals: Arc<Mutex<std::vec::IntoIter<Value>>>,
    pub(super) attempts: Wire,
}

macro_rules! forward {
    ($method:ident, $request:ty, $response:ty) => {
        async fn $method(&self, request: $request) -> Result<$response, AcpError> {
            self.connection.$method(request).await
        }
    };
}

impl AcpConnection for Refusing {
    forward!(initialize, InitializeRequest, InitializeResponse);
    forward!(authenticate, AuthenticateRequest, AuthenticateResponse);
    forward!(new_session, NewSessionRequest, NewSessionResponse);
    forward!(cancel, CancelNotification, ());
    forward!(close_session, CloseSessionRequest, CloseSessionResponse);
    forward!(
        set_config_option,
        SetSessionConfigOptionRequest,
        SetSessionConfigOptionResponse
    );

    async fn prompt(&self, request: PromptRequest) -> Result<PromptResponse, AcpError> {
        let text: String = request
            .prompt
            .iter()
            .filter_map(|block| match block {
                agent_client_protocol_schema::v1::ContentBlock::Text(text) => {
                    Some(text.text.as_str())
                }
                agent_client_protocol_schema::v1::ContentBlock::Image(_)
                | agent_client_protocol_schema::v1::ContentBlock::Audio(_)
                | agent_client_protocol_schema::v1::ContentBlock::ResourceLink(_)
                | agent_client_protocol_schema::v1::ContentBlock::Resource(_)
                | _ => None,
            })
            .collect();
        self.attempts.lock().unwrap().push(json!({
            "session_id": request.session_id,
            "event_id": request.meta.as_ref().and_then(|meta| meta.get("roomkit.live/eventId")),
            "text": text,
        }));
        let refusal = self.refusals.lock().unwrap().next().unwrap_or(Value::Null);
        if refusal.is_null() || refusal["after_prompt"].as_bool().unwrap_or(false) {
            let answer = self.connection.prompt(request).await?;
            if refusal.is_null() {
                return Ok(answer);
            }
        }
        Err(AcpSessionInvalidatedError::new(
            refusal["reason"].as_str().unwrap(),
            AcpSessionInvalidatedErrorOptions {
                recovery_authorized: refusal["recovery_authorized"].as_bool().unwrap_or(false),
            },
        )
        .into())
    }
}
