# roomkit-rs

A Rust implementation of [RoomKit](https://github.com/roomkit-live/roomkit), the
multi-channel conversation framework: rooms, channels, hooks and permissions
for conversations between humans, AI agents and programs.

> **Status: pre-alpha.** Step 1 is done: the text path of the RFC's inbound
> and broadcast pipelines, with hooks, permissions, transcoding and the
> chain-depth limit, on a tokio runtime and an in-memory store. Step 2, the
> rest of Level 0, is in progress: the room lifecycle and its timers, a
> message naming its room, routing a message that names no room to its
> conversation, edits and deletes, instructions, and off-lock checks with
> the processing timeout are done. No provider yet, and the API will
> change.

## Why a Rust implementation

The Python implementation is the reference. This one exists for two reasons:

- **Embedding.** Its core performs no I/O and depends on no async runtime, so
  one core can run inside a Rust service, a desktop application, Python, and
  later mobile and WebAssembly hosts.
- **Testing the specification.** It is written from the
  [RoomKit RFC](https://github.com/roomkit-live/roomkit-specs), not ported from
  the Python code. Every place where the RFC does not say enough is recorded in
  [docs/rfc-gaps.md](docs/rfc-gaps.md).

## A first room

```rust
use roomkit::RoomKit;
use roomkit::model::{ChannelId, ChannelType, EventContent, InboundMessage, RoomId};
use roomkit::testing::{EchoAgent, RecordingTransport};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let kit = RoomKit::new();
    let sms = RecordingTransport::new(ChannelId::new("sms")?, ChannelType::Sms);
    let agent = EchoAgent::new(ChannelId::new("ai")?, "You said: ").quoting();
    kit.register_channel(sms.clone())?;
    kit.register_channel(agent.clone())?;

    let room = RoomId::new("room_1")?;
    kit.create_room(&room).await?;
    kit.attach(&room, sms.id()).await?;
    kit.attach(&room, agent.id()).await?;

    let message = InboundMessage::new(sms.id().clone(), "+15551234567", EventContent::text("Bonjour"));
    kit.process_inbound(message, Some(&room)).await?;
    assert_eq!(sms.texts(), ["You said: Bonjour"]);
    Ok(())
}
```

The examples run the RFC's Appendix B flows and check their own result:

```sh
cargo run -p roomkit --example echo_room         # B.1: an agent answers on SMS
cargo run -p roomkit --example new_conversation  # B.1: two senders on one number, a room each
cargo run -p roomkit --example block_and_inject  # B.2: a hook blocks and injects
cargo run -p roomkit --example agent_chain       # B.4: the chain-depth limit
cargo run -p roomkit --example advisor_joins     # B.5: mute, whisper to an advisor, unmute
cargo run -p roomkit --example edit_and_delete   # B.6, B.7: an edit and a delete, native or as text
```

## Documentation

- [Architecture](docs/architecture.md): the sans-IO core, the effect protocol,
  the crates and their rules.
- [Roadmap](docs/roadmap.md): the steps, from the text path to voice.
- [Step 1](docs/steps/step-01.md): the text path, done.
- [Step 2](docs/steps/step-02.md): the rest of Level 0, the current step.
- [RFC gaps](docs/rfc-gaps.md): what the specification leaves open, and the
  decisions taken here.

## License

MIT. See [LICENSE](LICENSE).
