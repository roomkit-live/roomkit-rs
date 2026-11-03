//! The span kinds and the well-known attribute keys, Python's `SpanKind`
//! and `Attr`.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Python `SpanKind`: what a span measures. Its value, what it displays as,
/// is Python's: the `roomkit.span_kind` attribute an exporter records.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum SpanKind {
    /// `pipeline.inbound`.
    #[serde(rename = "pipeline.inbound")]
    PipelineInbound,
    /// `pipeline.outbound`.
    #[serde(rename = "pipeline.outbound")]
    PipelineOutbound,
    /// `stt.transcribe`.
    #[serde(rename = "stt.transcribe")]
    SttTranscribe,
    /// `stt.stream`.
    #[serde(rename = "stt.stream")]
    SttStream,
    /// `tts.synthesize`.
    #[serde(rename = "tts.synthesize")]
    TtsSynthesize,
    /// `llm.generate`: an AI channel's generation.
    #[serde(rename = "llm.generate")]
    LlmGenerate,
    /// `llm.tool_call`: a tool call of a generation.
    #[serde(rename = "llm.tool_call")]
    LlmToolCall,
    /// `realtime.session`.
    #[serde(rename = "realtime.session")]
    RealtimeSession,
    /// `realtime.turn`.
    #[serde(rename = "realtime.turn")]
    RealtimeTurn,
    /// `realtime.tool_call`.
    #[serde(rename = "realtime.tool_call")]
    RealtimeToolCall,
    /// `realtime.tool_recovery`.
    #[serde(rename = "realtime.tool_recovery")]
    RealtimeToolRecovery,
    /// `hook.sync`: a sync hook's run.
    #[serde(rename = "hook.sync")]
    HookSync,
    /// `hook.async`: an async hook's run.
    #[serde(rename = "hook.async")]
    HookAsync,
    /// `framework.inbound`: an inbound call and its steps, an injected
    /// event's.
    #[serde(rename = "framework.inbound")]
    InboundPipeline,
    /// `framework.broadcast`: an event's delivery to the room's channels.
    #[serde(rename = "framework.broadcast")]
    Broadcast,
    /// `framework.delivery`: a transport's send.
    #[serde(rename = "framework.delivery")]
    Delivery,
    /// `voice.session`.
    #[serde(rename = "voice.session")]
    VoiceSession,
    /// `pipeline.speech_segment`.
    #[serde(rename = "pipeline.speech_segment")]
    PipelineSpeechSegment,
    /// `store.query`.
    #[serde(rename = "store.query")]
    StoreQuery,
    /// `backend.connect`.
    #[serde(rename = "backend.connect")]
    BackendConnect,
    /// `delegation`: a delegated task.
    #[serde(rename = "delegation")]
    Delegation,
    /// `delegation.worker`.
    #[serde(rename = "delegation.worker")]
    DelegationWorker,
    /// `custom`, a span's kind by default.
    #[serde(rename = "custom")]
    Custom,
}

impl SpanKind {
    /// The kind's value, as Python's `str(kind)`.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::PipelineInbound => "pipeline.inbound",
            Self::PipelineOutbound => "pipeline.outbound",
            Self::SttTranscribe => "stt.transcribe",
            Self::SttStream => "stt.stream",
            Self::TtsSynthesize => "tts.synthesize",
            Self::LlmGenerate => "llm.generate",
            Self::LlmToolCall => "llm.tool_call",
            Self::RealtimeSession => "realtime.session",
            Self::RealtimeTurn => "realtime.turn",
            Self::RealtimeToolCall => "realtime.tool_call",
            Self::RealtimeToolRecovery => "realtime.tool_recovery",
            Self::HookSync => "hook.sync",
            Self::HookAsync => "hook.async",
            Self::InboundPipeline => "framework.inbound",
            Self::Broadcast => "framework.broadcast",
            Self::Delivery => "framework.delivery",
            Self::VoiceSession => "voice.session",
            Self::PipelineSpeechSegment => "pipeline.speech_segment",
            Self::StoreQuery => "store.query",
            Self::BackendConnect => "backend.connect",
            Self::Delegation => "delegation",
            Self::DelegationWorker => "delegation.worker",
            Self::Custom => "custom",
        }
    }
}

impl fmt::Display for SpanKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Python `Attr`: the well-known attribute keys of the kit's spans and
/// metrics, each an associated constant.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct Attr;

impl Attr {
    /// `provider`.
    pub const PROVIDER: &'static str = "provider";
    /// `model`.
    pub const MODEL: &'static str = "model";
    /// `room_id`.
    pub const ROOM_ID: &'static str = "room_id";
    /// `session_id`.
    pub const SESSION_ID: &'static str = "session_id";
    /// `channel_id`.
    pub const CHANNEL_ID: &'static str = "channel_id";
    /// `ttfb_ms`.
    pub const TTFB_MS: &'static str = "ttfb_ms";
    /// `duration_ms`.
    pub const DURATION_MS: &'static str = "duration_ms";
    /// `stt.text`.
    pub const STT_TEXT: &'static str = "stt.text";
    /// `stt.text_length`.
    pub const STT_TEXT_LENGTH: &'static str = "stt.text_length";
    /// `stt.confidence`.
    pub const STT_CONFIDENCE: &'static str = "stt.confidence";
    /// `stt.is_final`.
    pub const STT_IS_FINAL: &'static str = "stt.is_final";
    /// `stt.mode`.
    pub const STT_MODE: &'static str = "stt.mode";
    /// `tts.voice`.
    pub const TTS_VOICE: &'static str = "tts.voice";
    /// `tts.char_count`.
    pub const TTS_CHAR_COUNT: &'static str = "tts.char_count";
    /// `tts.text_length`.
    pub const TTS_TEXT_LENGTH: &'static str = "tts.text_length";
    /// `llm.input_tokens`.
    pub const LLM_INPUT_TOKENS: &'static str = "llm.input_tokens";
    /// `llm.output_tokens`.
    pub const LLM_OUTPUT_TOKENS: &'static str = "llm.output_tokens";
    /// `llm.tool_count`.
    pub const LLM_TOOL_COUNT: &'static str = "llm.tool_count";
    /// `llm.streaming`.
    pub const LLM_STREAMING: &'static str = "llm.streaming";
    /// `hook.name`.
    pub const HOOK_NAME: &'static str = "hook.name";
    /// `hook.trigger`.
    pub const HOOK_TRIGGER: &'static str = "hook.trigger";
    /// `hook.result`.
    pub const HOOK_RESULT: &'static str = "hook.result";
    /// `pipeline.frame_count`.
    pub const PIPELINE_FRAME_COUNT: &'static str = "pipeline.frame_count";
    /// `pipeline.bytes_processed`.
    pub const PIPELINE_BYTES_PROCESSED: &'static str = "pipeline.bytes_processed";
    /// `pipeline.stages`.
    pub const PIPELINE_STAGES: &'static str = "pipeline.stages";
    /// `pipeline.frames`.
    pub const PIPELINE_FRAMES: &'static str = "pipeline.frames";
    /// `realtime.provider`.
    pub const REALTIME_PROVIDER: &'static str = "realtime.provider";
    /// `realtime.tool_name`.
    pub const REALTIME_TOOL_NAME: &'static str = "realtime.tool_name";
    /// `realtime.tool_denied`.
    pub const REALTIME_TOOL_DENIED: &'static str = "realtime.tool_denied";
    /// `delivery.channel_type`.
    pub const DELIVERY_CHANNEL_TYPE: &'static str = "delivery.channel_type";
    /// `delivery.recipient`.
    pub const DELIVERY_RECIPIENT: &'static str = "delivery.recipient";
    /// `delivery.success`.
    pub const DELIVERY_SUCCESS: &'static str = "delivery.success";
    /// `delivery.error`.
    pub const DELIVERY_ERROR: &'static str = "delivery.error";
    /// `delivery.message_id`.
    pub const DELIVERY_MESSAGE_ID: &'static str = "delivery.message_id";
    /// `store.operation`.
    pub const STORE_OPERATION: &'static str = "store.operation";
    /// `store.table`.
    pub const STORE_TABLE: &'static str = "store.table";
    /// `backend.type`.
    pub const BACKEND_TYPE: &'static str = "backend.type";
    /// `delegation.task_id`.
    pub const DELEGATION_TASK_ID: &'static str = "delegation.task_id";
    /// `delegation.worker_id`.
    pub const DELEGATION_WORKER_ID: &'static str = "delegation.worker_id";
    /// `delegation.child_room_id`.
    pub const DELEGATION_CHILD_ROOM_ID: &'static str = "delegation.child_room_id";
    /// `delegation.parent_room_id`.
    pub const DELEGATION_PARENT_ROOM_ID: &'static str = "delegation.parent_room_id";
    /// `delegation.status`.
    pub const DELEGATION_STATUS: &'static str = "delegation.status";
    /// `delegation.mode`.
    pub const DELEGATION_MODE: &'static str = "delegation.mode";
}

#[cfg(test)]
mod tests {
    use super::*;

    // Python: tests/test_telemetry.py::TestSpanKind::test_span_kind_values
    #[test]
    fn a_kind_s_value_is_python_s() {
        assert_eq!(SpanKind::SttTranscribe.to_string(), "stt.transcribe");
        assert_eq!(SpanKind::InboundPipeline.to_string(), "framework.inbound");
        let json = serde_json::to_value(SpanKind::LlmToolCall).unwrap();
        assert_eq!(json, "llm.tool_call");
        let read: SpanKind = serde_json::from_value(json).unwrap();
        assert_eq!(read, SpanKind::LlmToolCall);
    }
}
