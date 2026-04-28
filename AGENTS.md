# Agent instructions

Repo role: **the engine** — the validator pipeline + sema host. Receives signal frames from nexus, dispatches verbs through a ractor supervision tree, applies accepted mutations to sema.

Read [ARCHITECTURE.md](ARCHITECTURE.md) — *the project-wide canonical doc*. Invariants A–D, the request flow, the three-daemon shape, the two-stores split. Project-wide architecture lives here, not duplicated elsewhere.

Workspace conventions live in [mentci/AGENTS.md](https://github.com/LiGoldragon/mentci/blob/main/AGENTS.md) — beauty, methods on types, full-English naming, `-daemon` binary suffix, S-expression commit messages, jj + always-push.

Ractor patterns (one actor per file, four-piece template, mailbox semantics, supervision) live in [tools-documentation/rust/ractor.md](https://github.com/LiGoldragon/tools-documentation/blob/main/rust/ractor.md). criome is the canonical example.

The supervision tree lives in [src/lib.rs](src/lib.rs)'s doc comment.

## Carve-outs worth knowing

- **`engine::State` carries the sync façade** ([`State::handle_frame`](src/engine.rs)). The actor wraps it for async use; the [`criome-handle-frame`](src/bin/handle_frame.rs) one-shot binary and the integration tests construct `State::new(sema)` directly. Don't duplicate the dispatch — every verb flows through `State::handle_*` whether async or sync.
- **`Reader` is a worker pool** sized by `sema::Sema::reader_count()` (default 4). Round-robin via `Arc<AtomicUsize>`. Don't replace with a factory — uncontended atomics + a flat `Vec<ActorRef>` is the right shape here.
- **Schema-as-data lives in `KindDecl` records** (defined in [signal](https://github.com/LiGoldragon/signal)). The closed Rust enum dispatch is rsc's projection of those records, not a separate truth.
