# Skill — working in criome

*What an agent needs to know to be effective in this repo.*


## What criome is

Criome is the workspace's **BLS-signature authentication and
attestation substrate**. A single Kameo daemon holding criome's
own root keypair, an identity registry, and an attestation
audit log. It signs typed attestations over content records
(channel grants, archive fingerprints, authorization decisions,
privilege elevations); it verifies signatures against the
registry; it serves identity lookups via a push subscription; and it
is the authorization topology for Lojix deploy requests.

Read `ARCHITECTURE.md` for this repo's shape.


## Today vs eventually

Today's criome is **narrow and Spartan**. The eventual `Criome`
(per `~/primary/ESSENCE.md` §"Today and eventually") subsumes
everything — validation, identity, programming, version control,
network identity, web request handling — expressed in Sema.
Today's Spartan criome is one step toward that eventual shape,
bringing forward the auth/identity slice and nothing else.

When working here, hold the distinction: today's code serves
today's narrow scope; the eventual encompassment is described in
ESSENCE, not in this repo.


## Hard invariants for an agent working here

- **Out-of-band attestations only.** Attestations are separate
  records in `signal-criome` that reference content records;
  they never embed proof fields inside the content records. This
  preserves the origin-context discipline.
- **Closed enums at every typed boundary.** No `Unknown`
  variant; no string-tagged dispatch; no generic-record
  fallback. Per `~/primary/ESSENCE.md` §"Perfect specificity at
  boundaries".
- **One store, one writer.** `StoreKernel` owns `criome.sema`;
  every other store actor routes through it.
- **Push, not poll.** Identity-update consumers subscribe; they
  do not poll. Per `~/primary/skills/push-not-pull.md`.
- **Kameo, not ractor.** Direct Kameo per
  `~/primary/skills/kameo.md`. `Self IS the actor`; no ZST
  marker types; data lives on the actor.
- **No private keypair custody other than criome's root.**
  Personas, agents, developers, hosts custody their own
  private keys. Criome holds only the *public* halves in its
  registry.
- **Criome permission comes from policy plus signatures.** Lojix
  submits the exact canonical `signal-lojix` request digest and
  requested scope; criome policy names which signatures count; criome
  routes signature solicitations, records pending/granted/denied
  authorization state, and issues the authorization envelope when the
  required signatures satisfy policy.
- **Authorization request slots are store-minted identities.** The
  request digest is signed payload content. Do not derive
  `AuthorizationRequestSlot` from a digest; ask `StoreKernel` to
  create authorization state and return the durable slot.
- **Authorization replay and expiry are enforced before signing
  state.** `AuthorizeSignalCall` with an expired `expires_at` records
  an expired authorization state; reuse of the same requester/nonce
  is rejected as `ReplayAttempted` before a second slot is minted.
- **Pending authorization is pushed.** Signature gathering may take
  time; clients observe `AuthorizationObservationStream` updates.
  Do not add polling loops for authorization completion.
- **Owner-class operations use `owner-signal-criome`.** The `criome`
  CLI and `tui-criome` are owner clients of the user's own
  `criome-daemon`; they do not use ordinary `signal-criome` for
  passphrase submission, policy mutation, peer-route mutation, or
  escalation-to-approve replies. `tui-criome` is the long-running
  owner client for approval prompts, not a separate triad daemon.
- **Owner sessions are encrypted.** When owner-signal-criome lands,
  the owner client and daemon perform an ECDH handshake, derive a
  symmetric session key, and exchange AEAD-encrypted frames before
  passphrase submission or any owner-class operation. Do not add a
  plaintext passphrase or owner-command path to the current
  signal-criome socket skeleton.
- **Skeleton-as-design.** New design lands as compiled
  types + trait signatures + `todo!()`, not as prose in this
  repo.


## What this repo is canonical for

Criome owns:

- The `Identity` enum vocabulary (`Persona`, `Agent`, `Host`,
  `Developer`, `Cluster`) — closed.
- The attestation envelope format and signing/verification API.
- The identity registry storage shape (in `criome.sema`).
- The signing/verification API contract surface.
- The `criome.pub` public-material publication conventions.
- Authorization request state, signature solicitation state,
  submitted signature state, authorization grant issuance, expiry,
  replay policy, policy state, and peer-routing state.

Criome does **not** own:

- Content record types (those live in `signal-mind`,
  `signal-persona`, `signal-forge`, etc.).
- Per-persona / per-agent private keys.
- Audit-policy decisions (the audit-policy engine is a
  separate component, to be designed in a follow-up report).
- Sema-ecosystem records validation (deferred to eventual
  Criome).
- ClaviFaber's per-host key generation (ClaviFaber remains a
  narrow per-host shim; it feeds criome but criome doesn't own
  it).


## See also

- `ARCHITECTURE.md` — this repo's shape.
- `~/primary/ESSENCE.md` §"Today and eventually" — the scope
  discipline.
- `~/primary/skills/kameo.md` — actor runtime.
- `~/primary/skills/actor-systems.md` — actor discipline.
- `~/primary/skills/contract-repo.md` — `signal-criome`'s
  shape.
- `~/primary/skills/architectural-truth-tests.md` — witnesses
  for `ARCHITECTURE.md` §8 constraints.
- `~/primary/skills/push-not-pull.md` — subscription discipline.
- `~/primary/skills/rust/storage-and-wire.md` — storage + rkyv
  discipline.
- `~/primary/skills/rust/crate-layout.md` — CLIs are daemon
  clients; one NOTA record in, one out.
- `~/primary/reports/system-assistant/21-criome-routed-authorization-and-thin-cli-shape-2026-05-17.md`
  — routed authorization and thin caller shape.
- `~/primary/reports/system-specialist/140-lojix-criome-mediated-authorization-decision-2026-05-17.md`
  — Lojix consequences and required authorization actor.
- `/git/github.com/LiGoldragon/clavifaber/ARCHITECTURE.md` —
  feeds criome's identity registry.
- This repo at commit `a3f4173` — archaeology of the prior
  sema-records-validator skeleton.
