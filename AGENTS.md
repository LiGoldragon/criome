# Agent instructions — criome

You **MUST** read AGENTS.md at `github:ligoldragon/lore` — the workspace contract.

This repo's `ARCHITECTURE.md` is the **project-wide canonical doc**. Invariants A–D, request flow, three-daemon shape, two-stores split. Project-wide architecture lives here, not duplicated elsewhere.

## Repo role

**The engine.** Validator pipeline + sema host. Receives signal frames from nexus, dispatches verbs through a ractor supervision tree, applies accepted mutations to sema.

The supervision tree lives in `src/lib.rs`'s doc comment. Ractor patterns are in lore (`rust/ractor.md`); criome is the canonical example.

---

## Carve-outs worth knowing

- **`engine::State` carries the sync façade** ([`State::handle_frame`](src/engine.rs)). The actor wraps it for async use; the [`criome-handle-frame`](src/bin/handle_frame.rs) one-shot binary and the integration tests construct `State::new(sema)` directly. Every verb flows through `State::handle_*` whether async or sync — single canonical dispatch path.
- **`Reader` is a worker pool** sized by `sema::Sema::reader_count()` (default 4). Round-robin via `Arc<AtomicUsize>`. Uncontended atomics + a flat `Vec<ActorRef>` is the right shape.
- **The closed Rust enum is the authoritative type system today.** New record kinds land by adding the typed struct + the closed-enum variant in signal, then propagating through the hand-coded dispatch here. Records-driven schema (the eventual `prism` self-host loop) is post-M0 work.
