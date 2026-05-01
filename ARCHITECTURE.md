# Criome — architecture

*Canonical reference for the engine's shape. Edited with extreme care.*

> **🚨 REQUIRED READING for every agent and human working in any
> sema-ecosystem repo. 🚨**
>
> Read this file in full before touching any component (criome, signal,
> signal-forge, signal-arca, sema, nexus, nexus-cli, forge, arca, prism,
> mentci-lib, lojix-cli, the GUI repo, or any future canonical crate).
> Per-repo `ARCHITECTURE.md` files describe each repo's niche; this file
> describes how the niches fit. Both layers are needed; this file is the
> apex.

Criome runs on top of CriomOS.
Development happens in mentci;
deployment composes from there too.

---

## 0 · TL;DR

**Sema is the database** — typed, content-addressed records.
**Criome is the state-engine around sema** — validates, persists,
communicates. Everything else orbits.

- **criome** runs nothing — receives signal, validates, writes to sema,
  forwards typed verbs.
- **forge** is the executor — links prism, runs nix, bundles outputs.
- **arca-daemon** is the privileged store writer — verifies tokens,
  computes hashes, atomic-moves into the canonical store.
- **nexus** is the text↔signal gateway. Future clients (GUI editor,
  mentci-lib consumers, agents) speak signal directly.
- **signal** is the wire on every leg, with `signal-forge` and
  `signal-arca` layered atop for audience-scoped isolation.

The flow-graph IS the program: a `Graph` record holding `Node` records
linked by `Edge` records (`Contains` for membership, `DependsOn` for
deps). prism projects graphs to Rust; nix builds; arca holds.

---

## 1 · The engine in one map

Three runtime clusters speak via typed protocols. Wire-type and library
crates sit underneath, consumed by every participant.

```
                ┌─────────────────────────────────────┐
                │           STATE CLUSTER             │
                │                                     │
                │   ┌─────────────────────────────┐   │
                │   │           criome            │   │
                │   │       (state-engine)        │   │
                │   │  validates · forwards ·     │   │
                │   │  persists · communicates    │   │
                │   │  ─────────────────────────  │   │
                │   │       runs nothing          │   │
                │   └──────────────┬──────────────┘   │
                │                  │ writes/reads     │
                │                  ▼                  │
                │   ┌─────────────────────────────┐   │
                │   │            sema             │   │
                │   │     (records DB; redb)      │   │
                │   └─────────────────────────────┘   │
                └──────────────────┬──────────────────┘
                                   │
                signal  ───────────┼─── + signal-forge
                (front-end verbs)  │     (effect-bearing)
                                   │
            ┌──────────────────────┼──────────────────────┐
            │                      │                      │
            ▼                      ▼                      ▼
    ┌──────────────┐   ┌────────────────────────┐   ┌────────────┐
    │  FRONT-ENDS  │   │   EXECUTOR CLUSTER     │   │  DIRECT    │
    │              │   │                        │   │  SIGNAL    │
    │  nexus       │   │   ┌────────────────┐   │   │  SPEAKERS  │
    │  daemon      │   │   │     forge      │   │   │            │
    │  (text↔sig)  │   │   │ links prism;   │   │   │  agents,   │
    │      ▲       │   │   │ runs nix;      │   │   │  scripts,  │
    │      │ text  │   │   │ bundles to     │   │   │  CI,       │
    │      ▼       │   │   │ ~/.arca/       │   │   │  harnesses │
    │  nexus-cli   │   │   │ _staging/      │   │   └────────────┘
    │              │   │   └────────┬───────┘   │
    │  GUI repo    │   │            │ signal-   │
    │  (egui)      │   │            │ arca      │
    │      ▲       │   │            ▼           │
    │      │ uses  │   │   ┌────────────────┐   │
    │      ▼       │   │   │  arca-daemon   │   │
    │  mentci-lib  │   │   │ verifies token │   │
    │  (gesture→   │   │   │ computes hash  │   │
    │   signal)    │   │   │ atomic-move    │   │
    │              │   │   │ into store     │   │
    │  + alt UIs   │   │   └────────┬───────┘   │
    └──────────────┘   │            │           │
                       │            ▼           │
                       │   ┌────────────────┐   │
                       │   │ ~/.arca/       │   │
                       │   │ _staging/      │   │
                       │   │ <store>/       │   │
                       │   │  <blake3>/...  │   │
                       │   │  index.redb    │   │
                       │   └────────────────┘   │
                       └────────────────────────┘

    ┌─── wire-type crates ────┐    ┌─── library crates ────┐
    │   signal                │    │   prism               │
    │   signal-forge          │    │   arca (reader lib)   │
    │   signal-arca           │    │   mentci-lib          │
    │   nota / -codec / -derive│   │   sema (consumed by   │
    │                         │    │             criome)   │
    └─────────────────────────┘    └───────────────────────┘
```

**Build backend (this era):** nix via crane + fenix. fenix pins the
toolchain; crane builds. **Deploy:** nix flakes aggregated from mentci
(`nixos-rebuild --flake mentci#<host>`); see §8.

---

## 2 · Four invariants

Load-bearing. Everything downstream depends on them.

### Invariant A — Rust is only an output

Sema changes **only** in response to nexus requests. There is **no**
`.rs` → sema parsing path. No ingester. prism projects sema → `.rs`
one-way for rustc/cargo; nothing in the engine ever reads that text
back. External tools may do whatever they want in user-space, but only
nexus requests reach the engine.

### Invariant B — Nexus is a language, not a record format

Sema is rkyv (binary, content-addressed). **Signal is criome's rkyv
request protocol** — every client speaks signal to criome. **Nexus is
one front-end** — the text request language whose parser produces
signal envelopes (the parse is mechanical). Future clients may speak
signal directly without going through nexus. There are no "nexus
records." There is sema (rkyv records of typed kinds defined in
signal), and there are signal messages (rkyv envelopes carrying
language IR). The analogy is SQL-and-a-DB: SQL is one text request
language; stored rows are in the DB's on-disk format. No one calls a
row a "SQL record."

**Criome's wire is signal, end-to-end.** Text never crosses criome's
boundary in either direction. The nexus daemon owns all text
translation. Failure modes involving text streams (truncation,
mid-render crashes) live entirely at the daemon ↔ client leg; criome
itself only emits complete signal frames.

**Nexus is a request language, not a programming language.** No
variables, no scoping, no evaluation, no cross-request state. Each
top-level expression is one self-describing request with literal
values. Pattern binds (`@x` inside `(\| ... \|)`) exist for matching
during querying — they never appear in assertion positions, never
carry state across requests. Dependent edits are the *client's*
orchestration concern.

**Slots are user-facing identity, hashes are version-locking.** Records
reference each other by `Slot` (mutable identity that follows the
current version). Content edits update the slot's binding; references
keep working — no Merkle-DAG ripple. Hashes are an optional
verification mechanism for snapshots/audits, not the primary reference
type. `Edge.from: Slot` is the correct shape; do not try to make it
`Hash`.

### Invariant C — Sema is the concern; everything orbits

If a component does not serve sema directly, it is not core.
criome = sema's engine / guardian. nexus = sema's text-request
translator. forge = executor for effects sema can't perform directly —
outcomes return as sema. prism = sema → `.rs` projector. arca =
artifact files, referenced *from* sema.

### Invariant D — Perfect specificity

Every typed boundary in the system names exactly what flows through
it. No wrapper enums that mix concerns; no string-tagged dynamic
dispatch; no generic-record fallback. Each verb's payload type is the
precise shape it operates on; each record kind is a closed Rust type
defined in signal — the authoritative type system today. Once `prism`
lands, those typed structs will be projected from records; until then,
new kinds land by hand.

The engine speaks in narrow, named types — `AssertOperation::Node`,
`MutateOperation::Edge { slot, new, expected_rev }`,
`Records::Graph(Vec<Graph>)` — never `Request(GenericRecord)` or
`Records(Vec<AnyKind>)`. A query is its own kind paired with the
instance kind it queries; an `Unknown` escape hatch does not exist;
reply payloads are typed per query. The type system is the
hallucination wall, not just the validator.

The principle generates concrete rules:
- **Closed enums at the wire.** Adding a kind = adding the typed
  struct + the closed-enum variant in signal + recompiling.
- **Per-verb payload types.** `AssertOperation` ≠ `MutateOperation` ≠
  `QueryOperation`; each carries a different shape per kind.
- **Typed query results.** A Node-query reply is
  `Records::Node(Vec<Node>)`, not a heterogeneous list.
- **No `Unknown` variant.** Closed-enum exhaustiveness is load-bearing;
  rebuilds bring the world forward together.

---

## 3 · The wire protocol family

Signal is the messaging system of the workspace. Layered protocols
re-use signal's envelope/handshake/auth and contribute their own
typed verbs.

### 3.1 Layering

```
       ┌─────────────────────┐    ┌──────────────────────┐
       │    signal-forge     │    │     signal-arca      │
       │  criome  ↔  forge   │    │  writers ↔ arca-d    │
       │ Build · Deploy ·    │    │  Deposit · Release   │
       │ store-entry verbs   │    │  Token · ...         │
       └──────────┬──────────┘    └──────────┬───────────┘
                  │                          │
                  └────────────┬─────────────┘
                               │
                ┌──────────────▼──────────────┐
                │           signal            │
                │  Frame · handshake · auth   │
                │  records · front-end verbs  │
                │      (every wire leg)       │
                └─────────────────────────────┘
```

**Why layered, not parallel.** Front-ends depend only on `signal`.
forge depends on signal + signal-forge + signal-arca. arca-daemon
depends on signal + signal-arca. Builder-internal field churn (refining
`BuildOutcome`, evolving capability tokens) recompiles only the
audience that depends on it. A unified single-crate signal would force
every front-end to recompile on every protocol tweak.

The Frame envelope, handshake, auth, and capability-token encoding
live once in signal; only the verbs differ per layer.

### 3.2 signal::Request — verbs every client speaks to criome

```
signal::Request
│
├─ Handshake(HandshakeRequest)         ── must be first on the conn
│
├── EDIT (mutating sema) ──
├─ Assert(AssertOperation)
├─ Mutate(MutateOperation)
├─ Retract(RetractOperation)
├─ AtomicBatch(AtomicBatch)
│
├── READ ──
├─ Query(QueryOperation)               ── one-shot read
├─ Subscribe(QueryOperation)           ── push-subscription [M2+]
│
├── DRY-RUN ──
├─ Validate(ValidateOperation)         ── would-be outcome, no commit
│
└── DISPATCH ──
   └─ BuildRequest(BuildRequestOp)     ── compile a graph [post-MVP]


signal::Reply
│
├─ HandshakeAccepted / HandshakeRejected
├─ Outcome(OutcomeMessage)             ── one OutcomeMessage per edit
├─ Outcomes(Vec<OutcomeMessage>)       ── per-position for batches
└─ Records(Records)                    ── typed per-kind result
```

Replies pair to requests by **position** on the connection (FIFO). No
correlation IDs.

### 3.3 signal-forge::Request — criome → forge

```
signal-forge::Request
│
├─ Build(BuildSpec)                    ── records → CompiledBinary
│   └─ BuildSpec {
│        target: Slot,                 ── Graph the user named
│        graph:  Graph,                ── the actual records
│        nodes:  Vec<Node>,
│        edges:  Vec<Edge>,
│        capability_token: Token,      ── criome-signed
│        ... (TBD)
│     }
│
├─ Deploy(DeploySpec)                  ── nixos-rebuild on host
│
└─ store-entry control plane           ── put / materialize / delete
                                          (reads do not need a verb;
                                           consumers open arca's
                                           index DB directly)


signal-forge::Reply
│
├─ BuildOk { arca_hash, narhash, wall_ms }
├─ DeployOk { generation, wall_ms }
└─ Failed { code, message }
```

### 3.4 signal-arca::Request — writers → arca-daemon

```
signal-arca::Request
│
├─ Deposit(DepositSpec)                ── take ownership of staged
│   └─ DepositSpec {                      content
│        staging_id:       StagingId,
│        target_store:     StoreId,
│        capability_token: Token,      ── criome-signed
│     }
│
└─ ReleaseToken(TokenId)               ── relinquish a capability


signal-arca::Reply
│
├─ DepositOk { blake3, bytes }
└─ Failed { code, message }
```

forge is the most active writer of these verbs today; future writers
(uploads, document ingestion, anything blob-shaped) speak the same
protocol.

---

## 4 · The four daemons

```
       nexus text (humans, LLMs, nexus-cli, scripts)
         ▲ │
         │ ▼
      ┌─────────┐
      │  nexus  │  text ↔ rkyv translator. Validates syntax +
      │ daemon  │  protocol version; forwards requests to criome;
      │         │  serialises replies back to text. Stateless modulo
      │         │  in-flight request correlations.
      └────┬────┘
           │ signal (rkyv)
           ▼
      ┌─────────┐
      │ criome  │  sema's engine — validates · applies · cascades.
      │         │  • receives every request; checks validity
      │         │  • writes accepted mutations to sema
      │         │  • cascades within sema (no derived state outside)
      │         │  • fires subscriptions on commits
      │         │  • forwards effect-bearing verbs to forge
      │         │  • signs capability tokens; tracks reachability
      │         │  • never touches binary bytes itself
      └────┬────┘
           │ signal-forge (rkyv)
           ▼
      ┌─────────┐
      │  forge  │  build + deploy executor. Thin; no evaluation.
      │ daemon  │  • links prism (records → .rs)
      │         │  • spawns nix (crane + fenix)
      │         │  • bundles closure (RPATH rewrite, det. timestamps)
      │         │  • writes bundled tree into ~/.arca/_staging/
      │         │  • performs nixos-rebuild for Deploy
      │         │  • replies { arca_hash, narhash, wall_ms }
      └────┬────┘
           │ signal-arca (rkyv)
           ▼
      ┌──────────────┐
      │ arca-daemon  │  privileged writer for arca stores.
      │              │  • verifies criome-signed capability tokens
      │              │  • computes blake3 of staged content
      │              │  • atomic-moves into ~/.arca/<store>/<blake3>/
      │              │  • updates per-store redb index
      │              │  • manages multi-store ACL
      │              │  • replies { blake3 }
      └──────────────┘
```

**Invariants (text/data flow):**

- Text crosses only at nexus's boundary. All internal traffic is rkyv.
- No daemon-to-daemon path routes bulk data through criome — bytes
  travel forge → arca's `_staging/` → arca-daemon's atomic move.
- criome never sees compiled binary bytes; only their hashes (as
  slot-refs resolved to blake3 via sema).
- There is no `Launch` protocol verb. Store entries are real files at
  hash-derived paths; you `exec` them from a shell.

Per-actor wiring (NixRunner, StoreWriter, ArcaDepositor,
FileMaterialiser, ...) lives in each repo's own `ARCHITECTURE.md` and
src/.

---

## 5 · Two stores

```
   ┌──────────────────────────┐         ┌──────────────────────────┐
   │           sema           │         │           arca           │
   │      records database    │         │  content-addressed FS    │
   │                          │         │                          │
   │  Owner: criome           │         │  Owner: arca-daemon      │
   │  Backend: redb           │         │  Backend: real files     │
   │  Keying: blake3 of       │         │  Keying: blake3 of       │
   │    canonical rkyv        │         │    canonical encoding    │
   │  Reference: Slot(u64)    │         │  Reference: blake3 hash  │
   │                          │         │  Stores: multi (system / │
   │  ┌────────────────────┐  │         │    user-X / project-Y)   │
   │  │ Graph              │  │         │                          │
   │  │ Node               │  │         │  ┌────────────────────┐  │
   │  │ Edge               │  │ ──ref──▶│  │ <blake3>/<files>   │  │
   │  │ Derivation         │  │         │  │ <blake3>/<files>   │  │
   │  │ CompiledBinary ────┼──┼─hash──▶ │  │ ...                │  │
   │  │ SlotBinding        │  │         │  └────────────────────┘  │
   │  │ ChangeLogEntry     │  │         │                          │
   │  └────────────────────┘  │         │  index.redb (per store)  │
   │                          │         │   blake3 → { path,       │
   │  Per-kind change-logs    │         │              metadata,   │
   │  are ground truth;       │         │              reachability}│
   │  index tables derivable. │         │                          │
   └──────────────────────────┘         └──────────────────────────┘
```

### sema — records database

- **Backend:** redb-backed, content-addressed records keyed by blake3
  of canonical rkyv encoding.
- **Reference model:** records store **slot-refs**, not content
  hashes. Sema's index maps each slot to its current content hash plus
  a bitemporal display-name binding (`SlotBinding` records). Content
  edits update the slot's current-hash (no ripple-rehash). Renames
  update the slot's display-name (no record rewrites).
- **Change log:** per-kind. Each record-kind has its own redb table
  keyed by `(Slot, seq)`. Per-kind logs are ground truth; index tables
  and global revision index are derivable.
- **Scope:** slots are **global** (not graph-scoped); one name per slot.

### arca — content-addressed filesystem

- **Multi-store:** arca-daemon manages multiple stores under
  `~/.arca/<store-name>/<blake3>/` for access control. Stores are
  filesystem-read-only to consumers; only arca-daemon writes.
- **Write-only staging:** writers deposit into
  `~/.arca/_staging/<deposit-id>/` and cannot read or modify
  afterwards. arca-daemon hashes exactly what's there (no TOCTOU race).
- **Capability tokens:** every deposit carries a criome-signed token
  referencing a sema authz record + target store + validity window.
  arca-daemon verifies signature; rejects expired or malformed tokens.
- **Index DB:** per-store redb mapping `blake3 → { path, metadata,
  reachability }`. arca-daemon writes; readers open read-only.
- **No typing.** The type of a store entry is known only through the
  sema record that references its hash.
- **Reads are local.** Consumers open arca's index DB directly under
  filesystem read permissions — no daemon round-trip, no protocol verb.

### Why arca from day one

Dogfooding the real interface now reveals what it actually needs;
deferred implementations rot. The gradualist path "nix builds; arca
stores; loosen dep on nix over time" is strictly safer than "nix
forever until Big Bang replace."

### Relationship

Sema records carry `StoreEntryRef` (blake3) fields pointing at arca
entries. criome maintains the reachability view and signs the
capability tokens that authorise GC; arca-daemon enforces. Binaries
are `exec`'d directly from their store path (no extraction, no copy,
no `Launch` verb).

---

## 6 · Type families

The flow-graph IS the program. A `Graph` record holds `Node` records
via `Contains` edges and depends on other graphs via `DependsOn` edges.

```
                      ┌─────────────┐
                      │    Graph    │
                      │ (program /  │
                      │  build      │
                      │   target)   │
                      └──┬───┬──────┘
                         │   │
              Contains   │   │   DependsOn
                edges    │   │   edges
                         │   │
              ┌──────────┘   └──────────┐
              ▼                         ▼
      ┌─────────────┐            ┌─────────────┐
      │    Node     │            │    Graph    │     (or)
      │ Source/     │            └─────────────┘
      │ Transformer/│                  │
      │ Sink/       │                  │ DependsOn
      │ Junction/   │                  ▼
      │ Supervisor/ │            ┌─────────────┐
      │  ...        │            │ Derivation  │
      └─────────────┘            │ (nix escape │
                                 │  hatch)     │
                                 └─────────────┘
```

| Kind | What it is | Where defined |
|---|---|---|
| `Graph` | flow-graph; user-authored program / build-target | signal/flow.rs |
| `Node` | one computational unit (Source/Transformer/Sink/Junction/Supervisor + future kinds) | signal/flow.rs |
| `Edge` | typed connection carrying `RelationKind` | signal/flow.rs |
| `RelationKind` | Flow / DependsOn / Contains / References / Produces / Consumes / Calls / Implements / IsA | signal/flow.rs |
| `Derivation` | non-pure escape hatch (nix flake output / inline expression) | signal (planned) |
| `CompiledBinary` | outcome record asserted after a Build flow | signal (planned) |
| `Slot` / `SlotBinding` | identity + current-hash + display-name index | signal/slot.rs + sema |
| `RawPattern` / `PatternExpr` | wire form (user names) / resolved form (slot-refs) | signal / criome internal |
| `Frame` / `Body` / `Request` / `Reply` | wire envelope + verbs | signal |

Concrete field lists live in skeleton code per repo, not here.

---

## 7 · Flows

Four canonical flows. All show messages between components; internal
actor wiring is per-repo.

### 7.1 Edit (M0)

```
USER         NEXUS-CLI      NEXUS DAEMON       CRIOME           SEMA
 │              │                │               │                │
 │ (Assert      │                │               │                │
 │   (Node "X"))│                │               │                │
 │ ── text ────▶│                │               │                │
 │              │ ── UDS text ──▶│               │                │
 │              │                │ parse →       │                │
 │              │                │ signal::      │                │
 │              │                │  Request::    │                │
 │              │                │  Assert(Node…)│                │
 │              │                │ ── UDS rkyv ─▶│                │
 │              │                │               │ validate:      │
 │              │                │               │  schema/refs/  │
 │              │                │               │  perms/inv.    │
 │              │                │               │ ── write ─────▶│
 │              │                │               │ ◀── ack ───────│
 │              │                │ ◀── Reply ────│                │
 │              │                │   Outcome(Ok) │                │
 │              │ ◀── UDS text ──│               │                │
 │ ◀── text ────│                │               │                │
```

mentci-lib clients skip nexus daemon — they speak signal directly to
criome.

### 7.2 Query (M0)

```
CLIENT          CRIOME             SEMA
 │                │                 │
 │ Query(NodeQuery│                 │
 │   { name: ?* })│                 │
 │ ── UDS rkyv ──▶│                 │
 │                │ scan Node table │
 │                │ filter by name  │
 │                │ ── read ───────▶│
 │                │ ◀── Vec<Node> ──│
 │ ◀── Reply ─────│                 │
 │  Records::Node │                 │
 │   (Vec<Node>)  │                 │
```

### 7.3 Build (post-MVP — the milestone flow)

```
USER  NEXUS    CRIOME            FORGE                  ARCA-DAEMON     SEMA
 │      │        │                  │                        │           │
 │Build │        │                  │                        │           │
 │Reqst │        │                  │                        │           │
 │─text▶│        │                  │                        │           │
 │      │parse → │                  │                        │           │
 │      │signal::│                  │                        │           │
 │      │ Build- │                  │                        │           │
 │      │ Request│                  │                        │           │
 │      │  {Slot}│                  │                        │           │
 │      │─rkyv──▶│                  │                        │           │
 │      │        │ validate target  │                        │           │
 │      │        │ resolve: Graph?  │                        │           │
 │      │        │ ◀──── read graph + transitive ────────────────────────│
 │      │        │       (DependsOn graphs + Contains nodes + edges)     │
 │      │        │ sign capability  │                        │           │
 │      │        │  token (target   │                        │           │
 │      │        │  store + scope)  │                        │           │
 │      │        │                  │                        │           │
 │      │        │ signal-forge::   │                        │           │
 │      │        │  Build{records,  │                        │           │
 │      │        │   cap_token}     │                        │           │
 │      │        │── UDS rkyv ─────▶│                        │           │
 │      │        │                  │ ┌── inside forge ────┐ │           │
 │      │        │                  │ │ prism: emit .rs    │ │           │
 │      │        │                  │ │ FileMaterialiser:  │ │           │
 │      │        │                  │ │  workdir to disk   │ │           │
 │      │        │                  │ │ NixRunner:         │ │           │
 │      │        │                  │ │  spawn nix build   │ │           │
 │      │        │                  │ │ StoreWriter:       │ │           │
 │      │        │                  │ │  RPATH-rewrite +   │ │           │
 │      │        │                  │ │  det. timestamps   │ │           │
 │      │        │                  │ │  → ~/.arca/        │ │           │
 │      │        │                  │ │     _staging/<id>/ │ │           │
 │      │        │                  │ └────────┬───────────┘ │           │
 │      │        │                  │          │             │           │
 │      │        │                  │ ArcaDepositor:         │           │
 │      │        │                  │ signal-arca::          │           │
 │      │        │                  │  Deposit{staging_id,   │           │
 │      │        │                  │   target_store,        │           │
 │      │        │                  │   cap_token}           │           │
 │      │        │                  │── UDS rkyv ───────────▶│           │
 │      │        │                  │                        │ verify    │
 │      │        │                  │                        │  token    │
 │      │        │                  │                        │ scan      │
 │      │        │                  │                        │  staging  │
 │      │        │                  │                        │ blake3    │
 │      │        │                  │                        │ atomic    │
 │      │        │                  │                        │  move →   │
 │      │        │                  │                        │  <store>/ │
 │      │        │                  │                        │  <blake3>/│
 │      │        │                  │                        │ index ++  │
 │      │        │                  │ ◀─ DepositOk{blake3} ──│           │
 │      │        │ ◀── BuildOk ─────│                        │           │
 │      │        │   { arca_hash,   │                        │           │
 │      │        │     narhash,     │                        │           │
 │      │        │     wall_ms }    │                        │           │
 │      │        │ assert           │                        │           │
 │      │        │ CompiledBinary{  │                        │           │
 │      │        │   graph, store,  │                        │           │
 │      │        │   arca_hash, ...}│                        │           │
 │      │        │ ── write ────────────────────────────────────────────▶│
 │      │        │ ◀── ack ─────────────────────────────────────────────  │
 │      │ ◀── Re-│                  │                        │           │
 │      │ ply Ok │                  │                        │           │
 │ ◀text│        │                  │                        │           │
```

**Roles:**

- **criome:** validate, read records, sign capability token, forward
  to forge over signal-forge, await, assert outcome record, reply.
  No subprocess, no file write, no external tool, no prism link.
- **forge:** receive records, link prism, write workdir, run nix,
  bundle to arca's `_staging/`, ask arca-daemon to take ownership.
  Does NOT compute the canonical blake3 — arca-daemon does.
- **arca-daemon:** verify token, blake3 of exactly-what-was-staged,
  atomic move, update per-store index, reply with the hash.

### 7.4 Subscribe (M2+ — push, never pull)

```
CLIENT             CRIOME                                       SEMA
 │                   │                                            │
 │ Subscribe(...)    │                                            │
 │ ── UDS rkyv ─────▶│                                            │
 │                   │ register subscription                      │
 │                   │ ◀── any matching write ────────────────────│
 │ ◀── push: Records │                                            │
 │ ◀── push: Records │ ◀── any matching write ────────────────────│
 │     ...           │                                            │
 │ (close socket)    │                                            │
 │ ─── EOF ─────────▶│ subscription dies with the connection      │
```

No initial snapshot — issue a `Query` first if you want current state.
Per push-not-pull discipline, clients **defer** their real-time
feature until Subscribe ships rather than poll while waiting.

---

## 8 · Repo layout

```
   ┌─────────────────────────────────────────────────────────┐
 5 │  CLIENTS + PROJECTORS                                   │
   │  nexus-cli   lojix-cli (transitional)   prism (lib)     │
   └────▲──────────────▲────────────────────▲────────────────┘
        │              │                    │ linked by forge
   ┌────┴──────────────┴────────────────────┴────────────────┐
 4 │  DAEMONS                                                │
   │  nexus     criome     forge     arca-daemon             │
   │  (peers — none contains the others; see §10 table)      │
   └────▲──────────▲──────────▲──────────▲───────────────────┘
        │          │          │          │
   ┌────┴──────────┴──────────┴──────────┴───────────────────┐
 3 │  STORAGE                                                │
   │  sema (criome-owned)        arca (arca-daemon-owned)    │
   └────▲────────────────────────▲───────────────────────────┘
        │                        │
   ┌────┴────────────────────────┴───────────────────────────┐
 2 │  CONTRACT CRATES                                        │
   │  signal       signal-forge       signal-arca            │
   │  (every leg) (criome↔forge)      (writers↔arca-d)       │
   └────▲────────────────────────────────────────────────────┘
        │
   ┌────┴────────────────────────────────────────────────────┐
 1 │  SCHEMA VOCABULARY                                      │
   │  signal — typed record kinds (Node / Edge / Graph / …)  │
   │  + IR (AssertOperation / MutateOperation / …)           │
   └────▲────────────────────────────────────────────────────┘
        │
   ┌────┴────────────────────────────────────────────────────┐
 0 │  TEXT GRAMMARS + CODEC                                  │
   │  nota (spec)  nexus (spec)  nota-codec  nota-derive     │
   └─────────────────────────────────────────────────────────┘
```

Layer N depends on layers below it. Per-repo status (current vs.
terminal shape) lives in workspace's `docs/workspace-manifest.md`.

**Deployment is nix-based, aggregated from mentci.** Each canonical
crate publishes its own flake; `workspace/flake.nix` defines NixOS modules
+ service specs composing the four daemons.
`nixos-rebuild --flake mentci#<host>` is the deploy. lojix-cli covers
this path during the transitional phase; eventually criome drives
deploys via signal-forge `Deploy` verbs.

**Shelved:** `arbor` (prolly-tree versioning) — post-MVP.

---

## 9 · Grammar shape

Nota is a strict subset of nexus. A single lexer (in nota-codec)
handles both, gated by a dialect knob. The grammar is a
**delimiter-family matrix**:

- Outer character picks the family — records `( )`, composites `{ }`,
  evaluation `[ ]`, flow `< >`.
- Pipe count picks the abstraction level — none for concrete, one for
  abstracted/pattern, two for committed/scoped.

**Every top-level nexus expression is a request.** The head of a
top-level `( )`-form is a request verb (`Assert`, `Mutate`, `Retract`,
`Query`, `Subscribe`, `Validate` today; `BuildRequest` post-MVP).
Parsing rejects top-level expressions that aren't requests.

**Sigil budget is closed.** Six total: `;;` (comment), `#` (byte-literal
prefix), `~` (mutate), `@` (bind), `!` (negate), `=` (bind-alias,
narrow use). New features land as delimiter-matrix slots or
Pascal-named records — **never new sigils**.

Detailed grammar shape lives in
nexus and
nota.

---

## 10 · Rules

Foundational rules — every session follows these.

| Rule | Why |
|---|---|
| Rust is only an output | No `.rs` → sema parsing path. prism emits one-way. |
| Nix is the build backend until we replace it | `BuildRequest` flows become `nix build` invocations (crane + fenix). Direct rustc is post-replacement. |
| Authored macros are transitional | Eventual self-hosting state has no authored macros; bootstrap era may use them. Third-party macros call freely in both eras. |
| Skeleton-as-design | New design starts as compiled types + trait signatures + `todo!()`. Reports are for WHY; skeleton code is for WHAT. |
| Per-repo `ARCHITECTURE.md` at root | matklad pattern. Points at this file, doesn't duplicate. |
| AGENTS.md / CLAUDE.md shim | One source of truth, read by both Codex and Claude Code. |
| Delete wrong reports; don't banner | Banners invite agents to relitigate. |
| Sema is all we are concerned with | Everything else orbits sema. |
| Text only crosses nexus | All internal traffic is rkyv. |
| All-rkyv except nexus text | Same pinned feature set workspace-wide (rkyv 0.8, std + bytecheck + little_endian + pointer_width_32 + unaligned). See lore/rust/rkyv.md. |
| Push, not pull | Producers expose subscriptions; consumers subscribe. No polling fallback ever. See lore/programming/push-not-pull.md. |
| criome communicates; it never runs | Effect-bearing work lives in dedicated components dispatched via typed verbs. The failure mode this rule closes: agents bundling features into criome until it's a monolith no LLM can hold in context. |
| One capability, one crate, one repo | Adding a feature defaults to a *new* crate. See lore/programming/micro-components.md. |
| Every edit is a request | criome validates; requests can be rejected. The hallucination wall. |
| Bootstrap rung by rung | No "before the engine runs" mode; criome runs from the first instant, sema starts empty, nexus messages populate it (including seed records via `genesis.nexus`, fed through nexus by the launcher). |
| References are slot-refs | Records store `Slot(u64)`; index resolves to current hash + display name. |
| Content-addressing is non-negotiable | Record identity is the blake3 of its canonical rkyv encoding. |
| A binary is just a path | No `Launch` verb; store entries are real files. |
| criome is the overlord of arca | Tracks reachability, signs capability tokens; arca-daemon enforces. |
| forge is for effects sema can't do | Inputs: records criome forwards (Graphs + Nodes + Edges + Derivations). Outputs: outcome records criome asserts back. |
| No backward compat | Rename, move, restructure freely until Li declares a boundary. |
| No ETAs | Describe the work; don't schedule it. |
| Sigils as last resort | New features are delimiter-matrix slots or Pascal-named records. |
| One artifact per repo | Per lore/rust/style.md. |

### 10.1 Categories of records

Records in sema split by **category** — the separation is intrinsic
to the records' nature, not a schema choice.

- **machina** — the code category. The subset of sema that compiles
  to Rust in v1. The native checker over machina records is
  `machina-chk`. machina records are what prism reads and emits.
- **world-fact, operational, access-control** — categories whose
  names are still open (see §11).

### 10.2 Sema's string discipline

Schema identifiers (kind, field, variant) in sema are slot ids —
integers in per-kind indexes. Display names and per-language
translations live in a **localization store** distinct from sema
and arca, loaded by whichever components render or parse human-
facing text. Sema records carry numerical references (slot ids),
primitive scalars, and user-data text content (e.g. a free-form
`Label` field on a domain record); the schema layer stays
string-free. The localization store's owner (daemon, library,
parallel record-engine instance) is open — see §11.

### 10.3 Bootstrap and runtime data flow

criome runs from the first instant of execution. Sema starts
empty; criome's init constructs the bootstrap kinds (Struct,
Enum, Field, Variant, TypeExpression, Localization, Language,
Slot, primitives) directly as records, populating sema before
opening the UDS listener.

After init, every record entering sema arrives as a signal
Assert frame from a connected client (mentci-egui, agents,
scripts, nexus-cli). The wire is signal end-to-end; no on-disk
text file feeds sema as a source of truth.

### 10.4 Responsibilities table — criome / forge / arca-daemon

The criome-runs-nothing rule made concrete. Each row is one concern;
columns mark which daemon owns it.

| Concern | criome | forge | arca-d |
|---|:---:|:---:|:---:|
| Validates request (schema / refs / perms / invariants) | ✓ | — | — |
| Reads from sema | ✓ | — | — |
| Writes to sema | ✓ | — | — |
| Forwards typed signal verbs to other components | ✓ | — | — |
| Awaits replies from forge / arca-daemon | ✓ | — | — |
| Signs capability tokens (criome holds the key) | ✓ | — | — |
| Persists outcome records (e.g. `CompiledBinary`) | ✓ | — | — |
| Spawns subprocesses (nix, nixos-rebuild) | — | ✓ | — |
| Links `prism` (the code-emission library) | — | ✓ | — |
| Runs `nix build` via crane + fenix | — | ✓ | — |
| Bundles closures (RPATH rewrite + deterministic timestamps) | — | ✓ | — |
| Performs `nixos-rebuild` (deploy) | — | ✓ | — |
| Writes the bundled tree into arca's `_staging/` | — | ✓ | — |
| Verifies criome-signed capability tokens | — | — | ✓ |
| Computes blake3 of staged content | — | — | ✓ |
| Atomic move from `_staging/` into `<store>/<blake3>/` | — | — | ✓ |
| Updates per-store redb index | — | — | ✓ |
| Manages multi-store ACL (sole writer of canonical store dirs) | — | — | ✓ |

If a future contributor finds themselves adding "spawn", "write file
into a store", "link prism", "run X" to criome, **that's the failure
mode this rule closes**. Add the capability to forge or arca-daemon —
or, if it's a new concern with its own bounded context, start a new
component.

---

## 11 · Open shapes

Known unknowns the architecture leaves open. Not blockers — each can
be settled when the relevant component is wired.

| Item | Open question |
|---|---|
| `signal::BuildRequest` payload | beyond `target: Slot` — nix-attr override, target-platform, env knobs |
| `signal-forge::Build` payload | precise field set including the capability-token field criome signs for forge to present to arca-daemon |
| `signal-arca::Deposit` payload | precise field set; how staging IDs are minted; whether multiple deposits batch |
| `signal-arca` repo | needs creation as a peer to signal-forge; same layered shape (depends on signal for envelope/auth) |
| Capability tokens | criome-signed BLS G1 token shape; one token covers (depositor, target store, validity window); verification logic in arca-daemon |
| Write-only staging mechanism | filesystem-level (chmod 1733 + per-deposit subdirs?) or process-boundary (SCM_RIGHTS, namespace)? |
| Multi-store registry | how arca-daemon learns which stores exist and their ACL — sema records read at startup, or pushed via signal-arca? |
| criome → forge connection module | re-use criome's `Connection` actor for the forge leg, or introduce a `ForgeLink`? |
| Node-kind enum landing | the 5 first kinds (Source / Transformer / Sink / Junction / Supervisor) need to land in `signal/src/flow.rs` |
| `RelationKind` control-plane variants | `Supervises`, `EscalatesTo` — exact set when the Supervisor kind lands |
| Per-kind sema tables | physical layout in redb (replaces M0's 1-byte discriminator) |
| Subscribe payload format | what arrives on the stream — snapshot delta or full record? |
| `mentci-lib`'s exact API | precise type names + connection lifecycle (auto-reconnect, handshake retry) |
| GUI repo name | "mentci" remains the working name in design docs until that repo is created |
| mentci flake structure | per-host NixOS module surface composing all four daemons |
| World-fact / operational / authz category names | machina is the code category; the others are still open |
| Localization store owner | a separate component (daemon/library/parallel record-engine instance) holds per-language display names mapped from slot ids. Distinct from sema (string-free) and arca (blob-only). Owner shape, naming, and protocol are open |

---

## 12 · What's NOT here (intentionally)

- **No deployment topology.** Whether components compile into one
  binary, many binaries, or talk over a network is left open. The
  architecture is *source-organization*, not deployment (per
  lore/programming/micro-components.md).
- **No nexus-text grammar additions.** The sigil for `BuildRequest` is
  TBD; nexus parser+renderer wire-in is a thin layer covered in
  nexus/ARCHITECTURE.md.
- **No M6 self-host close.** That's the next layer — criome's own
  request flow expressed as records, prism emits criome from them,
  recompile, loop closes. Mechanism shown here is the prerequisite.
- **No mentci UI screens.** The UI's visual design (egui widgets,
  theming) is out of scope here. mentci's role as workspace umbrella +
  meta-deploy aggregator is in workspace/ARCHITECTURE.md.
- **No CriomOS / horizon-rs / lojix-cli deploy flow internals.** Those
  are an existing parallel track; lojix-cli migrates to a thin
  signal-speaking client of forge during phases B–E.
- **No actor-level wiring inside any daemon.** Per-actor structure
  (NixRunner, StoreWriter, ArcaDepositor, FileMaterialiser; criome's
  Connection / Listener / Dispatcher) lives in each repo's own
  `ARCHITECTURE.md` and src/.
- **No field lists.** Per §13, type sketches live in skeleton code
  in the relevant repo, not here.

---

## 13 · Update policy

This file is the golden document. Edits are deliberate and surgical.

1. **Cross-repo report links are sparing.** Decision histories live in
   mentci's reports/;
   they may be cited when load-bearing for a reader, but never as
   required reading. The architecture stands on its own.
2. **Prose + diagrams only.** Type sketches, field lists, enum
   variants belong in skeleton code (compiler-checked) in the relevant
   repo, or in mentci's reports.
3. **Update this file first**, then the affected repos, then a report
   only if the decision carries a journey worth recording.
4. **If a framing is rejected, name the rejection in §10.1.** Stating
   only the acceptance lets agents rediscover the wrong frame.
5. **If a report is superseded, delete it.** Don't banner. Mentci's
   AGENTS.md carries the rollover discipline.
6. **Skeleton-as-design over prose-as-design.** Prefer compiler-checked
   types in the relevant repo over prose here.

---

*End criome/ARCHITECTURE.md.*
