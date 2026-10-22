# roomkit-rs

A Rust implementation of [RoomKit](https://github.com/roomkit-live/roomkit), the
multi-channel conversation framework: rooms, channels, hooks and permissions
for conversations between humans, AI agents and programs.

> **Status: pre-alpha.** Step 1 is in progress. Nothing here is usable yet.

## Why a Rust implementation

The Python implementation is the reference. This one exists for two reasons:

- **Embedding.** Its core performs no I/O and depends on no async runtime, so
  one core can run inside a Rust service, a desktop application, Python, and
  later mobile and WebAssembly hosts.
- **Testing the specification.** It is written from the
  [RoomKit RFC](https://github.com/roomkit-live/roomkit-specs), not ported from
  the Python code. Every place where the RFC does not say enough is recorded in
  [docs/rfc-gaps.md](docs/rfc-gaps.md).

## Documentation

- [Architecture](docs/architecture.md): the sans-IO core, the effect protocol,
  the crates and their rules.
- [Roadmap](docs/roadmap.md): the steps, from the text path to voice.
- [Step 1](docs/steps/step-01.md): the current step.
- [RFC gaps](docs/rfc-gaps.md): what the specification leaves open, and the
  decisions taken here.

## License

MIT. See [LICENSE](LICENSE).
