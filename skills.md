# Skill — working in criome

*What an agent needs to know to be effective in this repo.*

---

## What criome is

Criome is the **state engine** around sema. It receives typed
requests (signal frames), validates them, writes to sema,
forwards effect-bearing work to dedicated executors. **Criome
runs nothing** — every effect (subprocess spawn, blob write,
external tool invocation, code emission) is dispatched as a
typed verb to a dedicated component (forge, arca-daemon, prism)
and never performed in-process.

Read `ARCHITECTURE.md` for the apex map: the three runtime
clusters, the flow-graph-is-the-program model, the rules table
in §10. This skill captures intent that isn't fully there.

---

## Intent

**Criome and sema are meant to be eventually impossible to
improve.**

> *"I am much more interested in a good design than in producing
> it quickly — criome and sema are meant to be eventually
> impossible to improve, so I value clarity, correctness, and
> introspection above production volume, speed, and time to
> market."*
>
> — Li, 2026-04-29

What this commits the project to:

- **Clarity** — the design reads cleanly to a careful reader.
  The structure of the system is the documentation of itself.
- **Correctness** — every typed boundary names exactly what
  flows through it; nothing accidental survives the type
  system. The type system is the hallucination wall, not just
  the validator.
- **Introspection** — the engine reveals itself to those
  building it. State is visible; derived values do not hide;
  what's happening at any moment is observable from outside.
- **Beauty** — beauty in the operative sense: not pretty, but
  right. Ugliness is evidence the underlying problem is
  unsolved.

When two of these conflict, the earlier wins. When deadline
pressure pulls toward "small/quick" instead of "the durable
shape," intent wins. The right shape now is worth more than a
wrong shape sooner; unbuilding a wrong shape costs more than
the speed it bought.

---

## Hard invariants for an agent working here

These are non-negotiable. Any change that violates one is
wrong.

- **Criome runs nothing.** Effect-bearing work is dispatched as
  typed verbs; never spawned in-process. If you find yourself
  about to spawn a subprocess from criome, stop — the work
  belongs in forge or arca-daemon.
- **Sema is the only state.** Every concept the engine reasons
  about is a sema record. No parallel datastore.
- **Text crosses only at nexus's boundary.** Internal wire is
  rkyv. If text appears anywhere except nexus's parser/renderer,
  it's a bug.
- **Content-addressing is non-negotiable.** Identity is the
  blake3 of canonical rkyv encoding. Slots provide
  follow-this-thing semantics on top of the immutable identity.
- **Closed enums at every typed boundary.** No `Unknown`
  variant, no string-tagged dispatch, no generic-record
  fallback.
- **Sigil budget is closed.** New features land as
  delimiter-matrix slots or PascalCase records — never new
  sigils.
- **Skeleton-as-design.** New design lands as compiled types +
  trait signatures + `todo!()`, not as prose blocks claiming
  "here's what the type would look like."

---

## What this repo is canonical for

Criome owns:

- The validator pipeline.
- The slot-allocation policy (slot ranges, freelist, content-
  hash binding semantics).
- The dispatch from signal verbs to executor components.
- The §10 rules table (the project-wide invariants table).

Criome does **not** own:

- Record types (those live in signal).
- Artifact bytes (those live in arca).
- Build/runtime execution (that's forge).
- Text parsing/rendering (that's nexus).

When a question crosses one of these boundaries, the answer
lives in the owning repo's `skills.md` / `ARCHITECTURE.md`.

---

## See also

- `ARCHITECTURE.md` — the apex map of the sema-ecosystem.
- `AGENTS.md` — repo-specific carve-outs.
- sema's `skills.md` — what sema is and what it owns.
- nexus's `skills.md` — text↔signal gateway.
- signal's `skills.md` — the rkyv envelope shape.
- forge's `skills.md` — the executor.
- arca's `skills.md` — the content-addressed store.
- prism's `skills.md` — sema → Rust projection.
- lore's `programming/abstractions.md`,
  `programming/beauty.md`,
  `programming/push-not-pull.md`,
  `programming/micro-components.md` — cross-language
  discipline.
- this workspace's `skills/autonomous-agent.md` — how to act
  on routine obstacles.
- this workspace's `skills/skill-editor.md` — how to edit and
  cross-reference skills.
