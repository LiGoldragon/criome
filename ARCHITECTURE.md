# Criome — architecture

*Canonical reference for the engine's shape. Edited with extreme
care.*

Criome is the project. **Sema is its heart** — the typed,
content-addressed records that hold every concept the engine
reasons about. **Nexus is the bridge** that lets the legacy
untyped-text world create and interact with sema — humans and
LLMs author nexus text; nexus parses it into signal rkyv;
criome validates and commits to sema. **Lojix is the compiler
infrastructure** — build, store, deploy of artifacts referenced
from sema by hash.

Criome runs on top of [CriomOS](https://github.com/LiGoldragon/CriomOS).
Development happens in [mentci](https://github.com/LiGoldragon/mentci).

---

## 1 · The engine in one paragraph

**Sema is all we are concerned with.** Sema is the records —
the canonical, content-addressed, evaluated state of the
engine. Every concept the engine reasons about (code, schema,
rules, plans, authz, history, world data) is expressed as
records in sema. The records are stored in rkyv, content-
addressed by blake3. The rest of the engine exists to serve
sema:

- **sema** is the **database** — the records' home. Typed,
  content-addressed, redb-backed; the canonical evaluated
  state of the engine.
- **criome** is the **state-engine** — the engine *around*
  sema. It receives every request, validates it (schema,
  references, permissions, invariants), and applies the
  change to sema. Rules and derivations are themselves
  records; cascades settle inside sema. Nothing "lives above"
  sema holding derived values. **criome communicates; it
  never runs.** It does not spawn subprocesses, write files
  outside sema, invoke external tools, or link code-emission
  libraries. Effect-bearing work is dispatched as typed verbs
  to other components — `lojix` for filesystem and nix
  execution, `prism` (via lojix) for code emission, and so
  on. See §10 and
  [tools-documentation/programming/micro-components.md](https://github.com/LiGoldragon/tools-documentation/blob/main/programming/micro-components.md).
- **nexus** is the text front-end — the bridge to the legacy
  untyped-text world. A text request language (structured,
  controlled, permissioned) that parses to **signal**, criome's
  rkyv request protocol. Envelopes: `Assert`, `Mutate`,
  `Retract`, `AtomicBatch`, `Query`, `Subscribe`, `Validate`
  today; `Compile` is planned post-MVP. Replies serialise back
  to text the same way. Two faces of one language; the
  translation is mechanical. Future clients (the GUI editor
  being the first) speak signal directly and never go through
  nexus.
- **lojix** is the hands. It performs effects sema can't
  (spawning `nix` subprocesses; reading and writing
  filesystem paths; materialising files). Inputs are plan
  records read from sema; outputs become outcome records
  written back.
- **prism** projects sema records → `.rs` source files. Used
  by lojix-daemon's runtime-creation pipeline as the code-
  emission phase (lojix-daemon orchestrates the surrounding
  work — directory assembly, dependency resolution, compiler
  invocation). One-way emission: sema → Rust source.
- **lojix-store** is a content-addressed filesystem (nix-store
  analogue) holding real unix files, referenced from sema by
  hash. Canonical from day one — see §5 for how it relates to
  `/nix/store` during the bootstrap era.

**Signal is the messaging system of the whole sema-ecosystem.**
**criome speaks only signal.** Nexus is one signal speaker —
the text↔signal gateway for humans, agents, and scripts. Future
clients (the GUI editor being the first) connect to criome by
speaking signal directly, the same way nexus does. Anything that
wants to talk to criome speaks signal; nexus is one front-end
among many that may exist over time, not a required intermediary.

**Build backend for this era**: **nix via crane + fenix**.
fenix pins the Rust toolchain; crane builds packages. prism
emits the source files; lojix-daemon assembles the workdir
(`.rs` from prism + `Cargo.toml` + `flake.nix`) and invokes
the nix build. Direct `rustc` orchestration is a post-nix-
replacement concern.

**Macro philosophy** (current era → eventual state). In the
eventual self-hosting state — when sema holds the full
specification of every program as a typed graph of records,
and prism projects those records to Rust source — there are no
*authored* macros. Code-generation patterns live as sema rules
that prism emits as plain Rust; the macro-like behaviour happens
at the sema-to-Rust boundary, not as proc-macro expansion
inside rustc.

In the current bootstrap era, the engine itself is still
written in Rust by hand. Authored macros (`macro_rules!` and
proc-macro crates) are fine when they're the right tool —
they will be migrated to sema-rules + prism-projection later,
the same way every other piece of hand-written Rust will be.
Per Li 2026-04-27: *"right now we are writing the engine in
Rust, so we can write Rust macros."*

We **freely call** third-party macros — `#[derive(Serialize)]`,
`#[tokio::main]`, `format!`, `println!`, etc. — in both eras,
and prism emits those invocations verbatim for rustc to expand.

**The code category in sema is named *machina*** — the subset
of records that compiles to Rust in v1. The native checker
over machina records is *machina-chk*. World-fact records,
operational-state records, and authz records are separate
categories.

**Bootstrap is rung by rung.** The engine bootstraps using
its own primitives starting from rung 0. There is no "before
the engine runs" mode; criome runs from the first instant,
sema starts empty, nexus messages populate it. Each rung's
capability comes from the data already loaded; that
capability is what populates the next rung. See §10.

---

## 2 · Three invariants

These are load-bearing. Everything downstream depends on them.

### Invariant A — Rust is only an output

Sema changes **only** in response to nexus requests. There is
**no** `.rs` → sema parsing path. No ingester. prism projects
sema → `.rs` one-way for rustc/cargo; nothing in the engine
ever reads that text back. External tools may do whatever they
want in user-space, but only nexus requests reach the engine.

### Invariant B — Nexus is a language, not a record format

Sema is rkyv (binary, content-addressed). **Signal is criome's
rkyv request protocol** — every client speaks signal to criome.
**Nexus is one front-end** — the text request language whose
parser produces signal envelopes (the parse is mechanical).
Future clients may speak signal directly without going through
nexus. Parsing nexus produces signal envelopes; it does not
produce sema directly. There are no "nexus records." There is
sema (rkyv records of typed kinds defined in signal), and there
are signal messages (rkyv envelopes carrying language IR). nexus
text is never persisted as records; signal is never rendered to
text outside nexus. The analogy is SQL-and-a-DB: SQL is one
text request language; stored rows are in the DB's on-disk
format. No one calls a row a "SQL record."

**Criome's wire is signal, end-to-end.** Text never crosses
criome's boundary in either direction. The nexus daemon owns
all text translation — text-in becomes signal before the
request reaches criome; signal-out becomes text after the reply
leaves criome. Failure modes involving text streams (truncation,
mid-render crashes, partial sequences in flight) live entirely
at the daemon ↔ client leg; criome itself only emits complete
signal frames.

**Nexus is a request language, not a programming language.** It
has no variables, no scoping, no evaluation, no cross-request
state. Each top-level expression is one self-describing request
with literal values. Pattern binds (`@x` inside `(\| ... \|)`)
are the only form of name in the language and exist for matching
during querying — they never appear in assertion positions, and
they never carry state across requests. Dependent edits — where
request N+1 needs the slot assigned by request N's reply — are
the *client's* orchestration concern: the client captures the
reply value in its host language and substitutes it into the next
request text. The grammar stays small.

**The "no parser keywords" rule does not preclude schema
enums.** The nexus parser has no reserved words like `SELECT` or
`IF` that it dispatches on; the verb system is sigil × delimiter
composition. But the **schema** is strongly typed and grows by
adding new typed kinds and enum variants —
`RelationKind { DependsOn, Contains, … }`,
`OutcomeMessage { Ok, Diagnostic }`, etc. Schema-level closed
enums are exactly what signal is for. The two scopes (parser /
schema) are distinct.

**Slots are user-facing identity, hashes are version-locking.**
Records reference each other by `Slot` (mutable identity that
follows the current version of the referenced record). When
content changes, the slot binding rebinds to the new content;
references via slot keep working — no Merkle-DAG ripple. Hashes
exist for cases where the client wants to lock onto a specific
version (snapshots, audits, distributed sync); they are an
optional verification mechanism, not the primary reference type.
`Edge.from: Slot` is the correct shape; do not try to make it
`Hash`.

### Invariant C — Sema is the concern; everything orbits

If a component does not serve sema directly, it is not core.
criome = sema's engine / guardian. nexus = sema's
text-request translator. lojix = executor for effects sema
can't perform directly — outcomes return as sema. prism = sema →
`.rs` projector. lojix-store = artifact files, referenced
*from* sema.

### Invariant D — Perfect specificity

Every typed boundary in the system names exactly what flows
through it. No wrapper enums that mix concerns; no string-
tagged dynamic dispatch; no generic-record fallback. Each
verb's payload type is the precise shape it operates on; each
record kind is a closed Rust type defined in signal — the
authoritative type system today. Once `prism` lands, those
typed structs will be projected from records; until then,
new kinds land by hand.

The engine speaks in narrow, named types —
`AssertOperation::Node`,
`MutateOperation::Edge { slot, new, expected_rev }`,
`Records::Graph(Vec<Graph>)` — never `Request(GenericRecord)`
or `Records(Vec<AnyKind>)`. A query is its own kind paired
with the instance kind it queries; an `Unknown` escape hatch
does not exist; reply payloads are typed per query.

This is the property that makes criome a *guardian* of sema
rather than a generic record store: the type system is the
hallucination wall, not just the validator. Things that don't
have a name don't pass the boundary.

The principle generates concrete rules:
- **Closed enums at the wire.** Adding a kind = adding the
  typed struct + the closed-enum variant in signal +
  recompiling. No string kind-name lookup at runtime.
- **Per-verb payload types.** `AssertOperation` ≠
  `MutateOperation` ≠ `QueryOperation`; each is its own enum
  because each carries a different shape per kind.
- **Typed query results.** A Node-query reply is
  `Records::Node(Vec<Node>)`, not a heterogeneous list.
- **No `Unknown` variant.** Closed-enum exhaustiveness is
  load-bearing; rebuilds bring the world forward together.

---

## 3 · The request flow

```
  user writes nexus text
      │
      ▼
  nexus ─────── parses text → signal (rkyv)
      │           (CriomeRequest::Assert / Mutate / Retract /
      │            Query / Compile / Subscribe / …)
      ▼
  criome ─────── validates:
      │            • schema conformance
      │            • reference resolution (slot-refs exist)
      │            • invariant preservation (Rule records with `is_must_hold`)
      │            • authorization (capability tokens; BLS quorum post-MVP)
      │
      │          if valid → apply to sema; otherwise → reject
      │
      ▼
  criome replies via signal rkyv
      │
      ▼
  nexus ─────── rkyv → nexus text
      │
      ▼
  user reads reply
```

Current signal::Request verbs (per
[signal/src/request.rs](https://github.com/LiGoldragon/signal/blob/main/src/request.rs)):
`Handshake`, `Assert`, `Mutate`, `Retract`, `AtomicBatch`,
`Query`, `Subscribe`, `Validate`. `Compile` (referenced
elsewhere in this doc) is a **planned** post-MVP verb that
criome forwards to lojix once accepted; it is not in the M0
wire today.

**Every edit is a request.** criome is the arbiter; assertions,
mutations, retractions can all be rejected. This is the
hallucination wall: unknown names, broken references,
schema-invalid shapes, unauthorised actions all fail here.

**Genesis runs the same flow.** At first boot, criome
dispatches a `genesis.nexus` text file (shipping with the
criome binary) through the same path: nexus parses it,
signal envelopes flow to criome, the validator runs,
records land in sema. Validation runs against the built-in
Rust types in signal (the closed `AssertOperation` /
`MutateOperation` / data-kind enums compiled into the binary
are the authoritative type system today). Once the
`SemaGenesis` marker lands, normal mode begins.

---

## 4 · The three daemons (expanded)

```
     nexus text (humans, LLMs, nexus-cli)
        ▲ │
        │ ▼
     ┌─────────┐
     │ nexus  │ messenger: text ↔ rkyv only; validates syntax +
     │         │ protocol version; forwards requests to criome;
     │         │ serialises replies back to text. Stateless modulo
     │         │ in-flight request correlations.
     └────┬────┘
          │ rkyv (signal contract)
          ▼
     ┌─────────┐
     │ criome │ sema's engine — validates, applies, cascades.
     │         │ • receives every request; checks validity
     │         │ • writes accepted mutations to sema
     │         │ • rules cascade as records update (nothing
     │         │   lives outside sema)
     │         │ • resolves RawPattern → PatternExpr
     │         │ • fires subscriptions on commits
     │         │ • reads plan records from sema; dispatches
     │         │   execution verbs to lojix
     │         │ • signs capability tokens; tracks reachability
     │         │   for lojix-store GC
     │         │ • never touches binary bytes itself
     └────┬────┘
          │ signal (rkyv) — effect-bearing verbs forwarded to lojix
          ▼
     ┌──────────┐   owns lojix-store directory
     │  lojix  │   (lojix family; thin executor; no evaluation)
     │          │ internal actors:
     │          │   • NixRunner (spawns nix/nixos-rebuild;
     │          │     cargo runs inside via crane, not directly)
     │          │   • StoreWriter + StoreReaderPool (store-entry
     │          │     placement + path lookup + index updates)
     │          │   • FileMaterialiser (store entries → workdir)
     │          │ • receives effect-bearing signal verbs from
     │          │   criome — build (records → bundle), deploy
     │          │   (nixos-rebuild), store-entry operations
     │          │   (get/put/materialize/delete)
     │          │ • links prism (records → .rs source) and
     │          │   invokes nix (crane + fenix) against the
     │          │   emitted workdir; output lands in /nix/store
     │          │   during the bootstrap era
     │          │ • replies {output-hash, warnings, wall_ms}
     └──────────┘
```

**Invariants**:

- Text crosses only at nexus's boundary. Internal daemon-
  to-daemon messages are rkyv.
- No daemon-to-daemon path routes bulk data through criome —
  when forge work inside lojix writes to lojix-store, it does
  so in-process under a criome-signed capability token; no
  bytes ever cross criome.
- Criomed never sees compiled binary bytes; it only records
  their hashes (as slot-refs resolved to blake3 via sema) in
  sema.
- There is no `Launch` protocol verb. Store entries are real
  files at hash-derived paths; you `exec` them from a shell.

---

## 5 · The two stores

### sema — records database

- **Owner**: criome.
- **Backend**: redb-backed, content-addressed records keyed
  by blake3 of their canonical rkyv encoding.
- **Reference model**: records store **slot-refs** (`Slot(u64)`),
  not content hashes. Sema's index maps each slot to its
  current content hash plus a bitemporal display-name binding
  (`SlotBinding` records). Content edits update the slot's
  current-hash (no ripple-rehash of dependents). Renames
  update the slot's display-name (no record rewrites
  anywhere). Display-name is global — one name per slot; prism
  projections pick it up everywhere.
- **Change log**: per-kind. Each record-kind has its own redb
  table keyed by `(Slot, seq)` carrying `ChangeLogEntry`
  records (rev, op, content hashes, principal, sig-proof for
  quorum-authored changes). Per-kind logs are ground truth;
  per-kind index tables and a global revision index are
  derivable views.
- **Scope**: slots are **global** (not opus-scoped); one name
  per slot, globally consistent.

### lojix-store — canonical artifact store (built on nix)

lojix-store is the **canonical artifact store from day one**.
It's an analogue to the nix-store, hashed by blake3. It holds
**actual unix files and directory trees**, not blobs. A
compiled binary lives at a hash-derived path; you `exec` it
directly.

nix produces artifacts into `/nix/store` during the build.
lojix immediately bundles them into `~/.lojix/store/` (copy
closure with RPATH rewrite) and returns the lojix-store hash.
**sema records reference lojix-store hashes as canonical
identity** — `/nix/store` is a transient build-intermediate,
not a destination.

Why not defer lojix-store: dogfooding the real interface now
reveals what it actually needs; deferred implementations rot.
The gradualist path "nix builds; lojix-store stores; loosen
dep on nix over time" is strictly safer than "nix forever
until Big Bang replace."

- **Owner**: lojix.
- **Layout**: hash-keyed subdirectory per store entry, close
  to nix's `/nix/store/<hash>-<name>/` tree.
- **Index DB**: lojix-owned redb table mapping
  `blake3 → { path, metadata, reachability }`. The index does
  not contain the files; it maps to them.
- **Holds**: compiled binaries and their runtime trees;
  user file attachments referenced by sema. Always real files
  on disk.
- **No typing**. The type of a store entry is known only
  through the sema record that references its hash.
- **Access control**: capability tokens, signed by criome.

### Relationship

Sema records carry `StoreEntryRef` (blake3) fields pointing at
lojix-store entries. Criomed maintains the reachability view
and drives GC; lojix resolves hashes to filesystem paths;
binaries are `exec`'d directly from their store path (no
extraction, no copy, no `Launch` verb).

---

## 6 · Key type families (named, not specified)

Concrete field lists live in skeleton code (per-repo `src/`)
and in mentci's reports; this file only names.

- **Opus** — pure-Rust artifact specification. User-authored
  sema record. Toolchain pinned by derivation reference,
  outputs enumerated, every build-affecting input a field so
  the record's hash captures the full closure.
- **Derivation** — escape hatch for non-pure deps. Wraps a nix
  flake output or inline nix expression.
- **OpusDep** — opus → {opus | derivation} link.
- **Slot** — `u64` content-agnostic identity. Counter-minted
  by criome with freelist-reuse. Seed range `[0, 1024)`
  reserved.
- **SlotBinding** — slot-keyed binding to current content
  hash and global display name. Bitemporal; slot-reuse is
  safe for historical queries.
- **MemberEntry** — opus-membership record declaring which
  slots an opus contributes and at what visibility.
- **RawPattern** — wire form of a nexus pattern, carrying
  user-facing names. Transient on signal.
- **PatternExpr** — resolved form, carrying slot-refs. Pinned
  to a sema snapshot. Internal to criome.
- **Frame / Body / Request / Reply** — signal envelope and
  protocol verbs (lives in [signal](https://github.com/LiGoldragon/signal)).
- **lojix-bound signal verbs** — effect-bearing requests
  criome forwards to lojix: **build** (records →
  `CompiledBinary` outcome via crane + fenix +
  RPATH-rewrite-into-lojix-store), **deploy**
  (nixos-rebuild), **store-entry operations**
  (get / put / materialize / delete). No `CompileRequest {
  opus: OpusId }` at the wire — criome forwards records
  directly; lojix runs prism + nix + bundle internally.

---

## 7 · Data flow

### Single query

```
 human nexus text: (| Fn @name |)
        ▼
  nexus parses → RawPattern; wraps as signal::Query
        ▼
  criome validates; resolver(RawPattern, sema snapshot) → PatternExpr
        ▼
  matcher runs; records returned
        ▼
  criome replies via signal (rkyv)
        ▼
  nexus serialises reply to nexus text
        ▼
 human
```

### Mutation request (validation + apply)

```
 user nexus text: ~(| Fn @id _ |) (Fn @id (Block …))
        ▼
 nexus → criome (signal::Mutate, one per matched record)
        ▼
 criome validates:
   • kind well-formed?
   • all slot-refs in the new content resolve to existing slots?
   • author authorised? (caps / BLS post-MVP)
   • rule engine permits? (e.g., not mutating a seed-protected
     record)
        ▼ (if any check fails → reject with Diagnostic)
 criome writes new content to sema:
   • per-kind ChangeLogEntry appended
   • SlotBinding updated with new current-hash
   • subscriptions on the affected slots fire → downstream
     cascades re-derive
        ▼
 criome replies (Ok) per affected slot
```

### Compile + self-host loop

Edit-time (requests accumulate):
- User issues nexus requests (Assert / Mutate / Retract /
  AtomicBatch) that change code records in sema. Each is
  validated; cascades settle; sema reflects the new state.

Run-time (plan dispatch):
- User issues a Compile request against an Opus record.
- criome reads the Opus + transitive OpusDeps from sema.
- criome **forwards the records to lojix** as a signal verb
  (criome itself runs nothing — see §10 "criome communicates;
  it never runs").
- lojix-daemon links `prism` and runs the full pipeline
  internally: prism emits `.rs` from the records → lojix
  assembles the scratch workdir (`.rs` + `Cargo.toml` +
  `flake.nix` + crane glue) → NixRunner spawns `nix build`
  (nix/crane run cargo + rustc with the fenix-pinned
  toolchain; proc-macros expand in rustc; output lands in
  `/nix/store`) → StoreWriter copies the closure into
  lojix-store with RPATH rewrite (patchelf), deterministic
  bundle, blake3 hash, writes tree under
  `~/.lojix/store/<blake3>/`.
- lojix replies with `{ store_entry_hash, narhash,
  wall_ms }`.
- criome asserts `CompiledBinary { opus, store_entry_hash,
  narhash, toolchain_pin, … }` to sema. The canonical
  identity is `store_entry_hash`; narhash is kept for nix
  cache lookup.

The signal verb that carries the records from criome to
lojix lands when `lojix-daemon` is wired. The load-bearing
constraint: criome's role is **forward + await**; lojix runs
prism + nix + bundle internally.

Self-host close:
- User runs the new binary directly from its lojix-store path.
- New binary connects to nexus; asserts records; cascades fire
  against the live sema. Loop closes.

---

## 8 · Repo layout

Canonical inventory lives in [mentci's
docs/workspace-manifest.md](https://github.com/LiGoldragon/mentci/blob/main/docs/workspace-manifest.md);
this section is the architectural roles.

- **Layer 0 — text grammars + codec**: nota (spec), nexus
  (spec), [nota-codec](https://github.com/LiGoldragon/nota-codec)
  (typed Decoder + Encoder runtime; lexer; trait surface;
  blanket impls for primitives + standard containers
  including BTreeMap/HashMap/HashSet/BTreeSet/Box/tuples;
  `PatternField<T>` lives here),
  [nota-derive](https://github.com/LiGoldragon/nota-derive)
  (proc-macro derives — `NotaRecord`, `NotaEnum`,
  `NotaTransparent`, `NotaTryTransparent`, `NexusPattern`,
  `NexusVerb`; re-exported through nota-codec).
- **Layer 1 — schema vocabulary**: lives inside
  [signal](https://github.com/LiGoldragon/signal) — the
  data-kind structs `Node` / `Edge` / `Graph` and their
  paired `*Query` types, plus the IR types
  (`AssertOperation` / `MutateOperation` / `RetractOperation` /
  `QueryOperation` / `BatchOperation` / `AtomicBatch` /
  `Records`, `Diagnostic`). New record kinds land here as the
  closed enum grows.
- **Layer 2 — contract crate**: signal — the workspace's
  typed wire protocol (requests + replies + handshake +
  record kinds). Spoken on every leg: front-ends to criome,
  and criome to lojix.
- **Layer 3 — storage**: sema (records DB — redb-backed;
  owned by criome), lojix-store (content-addressed
  filesystem — owned by lojix; includes a reader library).
- **Layer 4 — daemons**: nexus (translator), criome (sema's
  engine), lojix (executor).
- **Layer 5 — clients + projectors**: nexus-cli (the text
  client), prism (sema → `.rs` projector; linked by lojix).
- **Spec-only (terminal state)**: lojix (namespace README).

Currently `lojix` is CANON-MISSING (not yet scaffolded).
`criome` is scaffolded; criome has its
M0 daemon body shipped (ractor-hosted; see `criome/src/lib.rs`
for the supervision tree). See workspace-manifest in mentci
for the full per-repo status.

> Some repos in this layout are not yet at terminal shape;
> see workspace-manifest for current vs. terminal status
> (e.g., `lojix` is currently a working monolith and must not
> be rewritten — its own AGENTS.md carries the binding
> warning).

### Three-pillar framing

- **criome** — the runtime (nexus, criome, lojix; the
  daemon graph).
- **sema** — the records (the heart).
- **lojix** — the artifacts pillar (build, compile, store,
  deploy; the compiler infrastructure).

criome ⊇ {sema, lojix}. nexus is the bridge to legacy text
(spanning all of criome); not a fourth pillar.

**Lojix family membership** is orthogonal to layer. A crate is
lojix-family iff it participates in the content-addressed
typed build/store/deploy pipeline. `lojix` is the only
current lojix-family daemon.

**Shelved**: `arbor` (prolly-tree versioning) — post-MVP.

---

## 9 · Grammar shape

Nota is a strict subset of nexus. A single lexer (in
nota-codec) handles both, gated by a dialect knob. The
grammar is organised as a **delimiter-family matrix**:

- Outer character picks the family — records `( )`, composites
  `{ }`, evaluation `[ ]`, flow `< >`.
- Pipe count inside picks the abstraction level — none for
  concrete, one for abstracted/pattern, two for
  committed/scoped.

**Every top-level nexus expression is a request.** The head of
a top-level `( )`-form is a request verb (`Assert`, `Mutate`,
`Retract`, `Query`, `Subscribe`, `Validate` today; `Compile`
post-MVP). Nested expressions are record constructions that
the request refers to. Parsing rejects top-level expressions
that aren't requests.

**Sigil budget is closed.** Six total: `;;` (comment), `#`
(byte-literal prefix), `~` (mutate), `@` (bind), `!` (negate),
`=` (bind-alias, narrow use). New features land as delimiter-
matrix slots or Pascal-named records — **never new sigils**.

Detailed grammar shape lives in
[nexus](https://github.com/LiGoldragon/nexus) and
[nota](https://github.com/LiGoldragon/nota).

---

## 10 · Project-wide rules

Foundational rules. Every session follows these.

- **Rust is only an output.** No `.rs` → sema parsing. prism
  emits one-way.
- **Nix is the build backend until we replace it.** Compile
  plans become `RunNix` invocations (crane + fenix); lojix
  spawns `nix build`. Direct rustc orchestration is a post-
  nix-replacement concern. prism emits `.rs` source; lojix-
  daemon assembles the workdir with `Cargo.toml` + `flake.nix`;
  nix drives the rest.
- **Authored macros are transitional.** In the eventual
  self-hosting state, code-gen patterns are sema rules
  emitted by prism and there are no authored macros. In the
  current bootstrap era we may author macros where useful,
  understanding that they're transitional code that will be
  replaced by sema-projection later. We freely **call**
  third-party macros (derive, attribute, function-like) in
  both eras.
- **Skeleton-as-design.** New concrete design starts as
  compiled skeleton code (types + trait signatures + `todo!()`
  bodies) in the relevant repo. Reports (in mentci) are for
  WHY (philosophy, invariants, decision-journey); skeleton
  code is for WHAT (types, traits, enums, verbs). rustc checks
  consistency; prose can't drift. Example: `lojix-store/src/`.
- **Per-repo `ARCHITECTURE.md` at root.** Every canonical repo
  carries its own ARCHITECTURE.md describing role + boundaries
  + code map. Points at this file for cross-cutting context;
  does not duplicate.
- **AGENTS.md/CLAUDE.md shim.** In every canonical repo:
  `AGENTS.md` holds real content; `CLAUDE.md` is a one-line
  shim. Codex reads AGENTS.md; Claude Code reads CLAUDE.md;
  both converge.
- **Delete wrong reports; don't banner.** When a report's
  thesis is wrong or the content is absorbed elsewhere,
  delete it. Banners invite agents to relitigate. Mentci
  carries the reports; trim discipline lives there.
- **Nexus is a request language.** Sema is rkyv. There are no
  "nexus records."
- **Sema is all we are concerned with.** Everything else
  orbits sema.
- **Text only crosses nexus.** All internal traffic is rkyv.
- **All-rkyv except nexus text.** The only non-rkyv messaging
  surface is the nexus *text* payload (carried inside a
  client-msg `Send`). Every other wire / storage format —
  signal, future criome-net, sema records, lojix-store index
  entries — is rkyv. No compromise. All
  rkyv-using crates pin the *same* feature set so archived
  types interop:
  `default-features = false, features = ["std", "bytecheck",
  "little_endian", "pointer_width_32", "unaligned"]`. Pinned
  to rkyv 0.8.x. Discipline documented in
  [tools-documentation/rust/rkyv.md](https://github.com/LiGoldragon/tools-documentation/blob/main/rust/rkyv.md).
- **Push, not pull.** Producers push, consumers subscribe. No
  polling, ever. Real-time consumers (the GUI editor; future
  alternative UIs; any agent reflecting criome state) use
  `Subscribe` once it ships (M2+) and **defer their real-time
  feature** until then — they do not poll while waiting.
  Discipline documented in
  [tools-documentation/programming/push-not-pull.md](https://github.com/LiGoldragon/tools-documentation/blob/main/programming/push-not-pull.md).
- **criome communicates; it never runs.** sema is the
  database; criome is the engine around it — receives,
  validates, persists to sema, and forwards typed
  instructions to other components. criome never spawns
  subprocesses, writes files outside sema, invokes external
  tools, or links libraries that do those things.
  Effect-bearing work (nix builds, file writes, code
  emission, deployment) lives in dedicated components
  dispatched via typed verbs — `lojix` for filesystem/nix,
  `prism` (via lojix) for code emission. The workspace is
  composed of micro-components per
  [tools-documentation/programming/micro-components.md](https://github.com/LiGoldragon/tools-documentation/blob/main/programming/micro-components.md);
  criome is one of them — the state-engine — not the
  do-everything box. The failure mode this rule closes:
  agents bundling new features into criome (or any existing
  crate) until the result is a monolith no LLM can hold in
  context.
- **One capability, one crate, one repo.** Every functional
  capability lives in its own repo with its own `Cargo.toml`,
  `flake.nix`, and tests. Components communicate through
  typed protocols; each fits in a single LLM context window.
  Adding a feature defaults to a *new* crate, not editing an
  existing one — the burden of proof is on the contributor
  who wants to grow a crate. Discipline + the case in
  [tools-documentation/programming/micro-components.md](https://github.com/LiGoldragon/tools-documentation/blob/main/programming/micro-components.md).
- **Every edit is a request.** criome validates; requests can
  be rejected; this is the hallucination wall.
- **Bootstrap rung by rung.** The engine bootstraps using its
  own primitives, starting from rung 0. There is no "before
  the engine runs" mode; criome runs from the first instant,
  with sema initially empty. Nexus messages populate the
  initial versions of the database — including seed records
  via `genesis.nexus`. Each rung's capability comes from the
  data already loaded; that capability is what populates the
  next rung. No internal-assert paths, no baked-in-rkyv
  shortcuts, no special bootstrap inputs that bypass nexus.
  If a proposed mechanism cannot be explained step by step,
  the framing is wrong.
- **References are slot-refs.** Records store `Slot(u64)`;
  the index resolves slot → current hash + display name.
- **Content-addressing is non-negotiable.** Record identity is
  the blake3 of its canonical rkyv encoding.
- **A binary is just a path.** No `Launch` verb; store entries
  are real files.
- **Criomed is the overlord** of lojix-store. Tracks
  reachability; signs tokens; directs GC.
- **lojix is for effects sema can't do.** Its inputs are plan
  records; its outputs are outcome records. It never sees an
  Opus directly.
- **No backward compat.** The engine is being born. Rename,
  move, restructure freely until Li declares a compatibility
  boundary.
- **No ETAs.** Describe the work; don't schedule it.
- **Sigils as last resort.** New features are delimiter-matrix
  slots or Pascal-named records.
- **One artifact per repo** (per rust/style.md rule 1).

### Rejected framings (reject-loud)

Agents repeatedly rediscover wrong framings when the docs
say only what is true. These explicit rejections block
recurrence. Add to this list when Li rejects a new framing.

- **Aski is retired.** mentci / criome does not treat aski as
  a design input. Do not reason from aski axioms (II-L,
  v0.21 syntax, synth.md, compile-pipeline framing) to current
  sema architecture. Shared surface features (delimiter-family
  matrix, case rules) are coincidence, not lineage.
- **Scope is world-supersession, not personal-scale.** CriomOS
  + criome aim to supersede proprietary operating systems and
  computing stacks globally; mentci is intended to become the
  universal UI replacing today's fragmented software
  interfaces. Framings like "personal-scale," "craftsperson
  workshop," or "self-hosted-self" underestimate the project.
- **Sema is local; reality is subjective.** There is no global
  sema, no federated-global database, no single logical truth.
  Each criome holds a subjective view; instances communicate,
  agree, disagree, and negotiate to reach agreement. "Global
  database," "global blockchain," and "federated global sema"
  are wrong framings.
- **Categories are intrinsic.** Code records and world-fact
  records cannot share a category — the separation is a fact
  of reality, not a schema choice. The code category is named
  **machina** (the subset of sema that compiles to Rust in
  v1). The native checker over machina records is
  **machina-chk** (not "semachk" — the check is not over all
  of sema). Names for world-fact, operational, and authz
  categories are still open.
- **Self-hosting close is normal software engineering.** The
  engine works correctly, canonical crates authored as
  records. Bit-for-bit identity with the bootstrap version is
  not a bar — new rustc versions aren't byte-identical to
  predecessors either.
- **Nexus is the agent interface.** "Legibility to agents" is
  not a separate design axis. Nexus is how agents (LLMs,
  humans, scripts) interact with criome; text in, criome-
  validated records out.

### Reject-loud rule

When a framing is considered and rejected, state the
rejection here — not just the acceptance elsewhere. Past
recurring wrong frames: aski-as-input, personal-scale,
global-database, federation, boundary-as-tension,
bit-for-bit-identity, legibility-axis, sema-as-data-store,
four-daemon topology, ingester-for-Rust, lojix-store-as-
blob-DB, banner-wrong-reports.

---

## 11 · Update policy

This file is the golden document. Edits are deliberate and
surgical.

1. **Cross-repo report links are sparing.** Decision histories
   and research syntheses live in [mentci's reports/](https://github.com/LiGoldragon/mentci/tree/main/reports);
   they may be cited from this file when load-bearing for a
   reader, but never as required reading. The architecture
   stands on its own.
2. **Prose + diagrams only.** Type sketches, field lists,
   enum variants belong in skeleton code (compiler-checked)
   in the relevant repo, or in mentci's reports.
3. **Update this file first**, then update implementation
   in the affected repos, then write a report (in mentci) only
   if the decision carries a journey worth recording.
4. **If a framing is rejected, name the rejection in §10
   "Rejected framings."** Stating only the acceptance lets
   agents rediscover the wrong frame.
5. **If a report is superseded, delete it.** Don't banner.
   Mentci's AGENTS.md carries the rollover discipline.
6. **Skeleton-as-design over prose-as-design.** Prefer
   compiler-checked types in the relevant repo over prose
   here.

---

*End criome/ARCHITECTURE.md.*
