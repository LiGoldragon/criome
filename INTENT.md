# INTENT — criome

*What the psyche wants this project to be, and its most important
design constraints. Synthesised from workspace-backed intent that
applies to this repo plus the repo's specific purpose. Backed by the
repo's real purpose and applicable workspace constraints; not
embellished. Companion to `ARCHITECTURE.md` and `skills.md`.
Maintenance: `primary/skills/repo-intent.md`.*

## Why this repo exists

Today's `criome` is a **minimal Spartan BLS-signature authentication
and attestation substrate** for the Persona ecosystem: a single
Kameo-based daemon holding an identity registry, sign/verify
primitives, delegation grants, a replay guard, and a typed audit log
in `criome.sema` via `sema-engine` over the `sema` kernel. It is paired with
the `signal-criome` contract and the target `meta-signal-criome` meta
contract.

This repo is "today, not eventually." The **eventual** `Criome` is the
universal computing paradigm expressed in Sema — replacing Git, the
editor, SSH, and the web — encompassing programming, version control,
network identity, validation, and auth/security across the stack.
Today's Spartan daemon is one realization step that brings forward the
auth/identity slice; it does not carry the eventual scope.

The internal policy-language slice lives on that realization path: it models
Criome contracts as a finite typed rule tree over signatures, quorum /
timelock evidence, and reconciliation facts, with contracts admitted into
the component-local SEMA store by content digest. A contract may also return an
explicit `EscalateToPsyche` decision. That is a typed outcome, not a hidden
side effect: Criome says the policy requires psyche judgment; another layer
performs that judgment and supplies any later signed verdict.

Policy quorum signatures are stamped with crystallized time. The public
contract carries `StampedSignatureEnvelope` for operation evidence,
adjudicator agreement facts, routed signature submissions, and authorization
grants. The daemon verifies the attached `AttestedMoment` and binds policy /
agreement signature bytes to that stamp; the only bare signature envelopes in
the time path are the `TimeSignature` values that create an `AttestedMoment`.

Time-driven pulses are contract-programmed, not an ambient global
heartbeat. Accepting a contract with an after-time condition schedules a
later check of that contract against related events; when the crystallized
time condition matures, criome checks whether those events happened, and if
they did not, triggers a new acceptance for the resulting time-based state
to be quorum-signed.

## Load-bearing constraints

*Criome verifies; Persona decides.* This is the operative principle.
Criome answers "is this signature valid for this principal under this
grant for these bytes?" Persona answers "should this prompt be
delivered, should this work be executed?" The boundary is sharp:
prompt-audit and delivery policy live in `mind` / Persona, never in
criome.

*BLS12-381 from day one.* The closed `SignatureScheme` enum carries the
BLS variants from the first milestone, so every Spartan attestation is
already a quorum candidate when eventual-Criome's quorum-signature
multi-sig lands — no future scheme migration. There is no string-tagged
or open-ended scheme dispatch.

*One daemon, one meta authority — a single Unix user.* Only that user can write
to the daemon's meta socket; single-ownership is what gives the daemon
authority to sign with its master key. There are many criome daemons,
one per Unix user; new trust boundaries spawn new daemons, and complex
quorum policies find peers by predictable socket names. Permission for
a request is constituted by signatures over the canonical request
digest that satisfy criome's policy for that exact request; a grant for
one request cannot authorize another.

*Attestations are out-of-band only; content records carry no embedded
proof.* An attestation lives as a separate record that references a
content record (a `ChannelGrantAttestation` references a
`ChannelGrant`). Content records do not carry embedded proof fields —
the "origin context, not proof material" discipline stays inviolate.

*One NOTA record in, one NOTA record out at the CLI boundary; the
daemon takes one signal-encoded rkyv configuration file.* The CLI
accepts exactly one NOTA request and prints exactly one reply. The
daemon accepts exactly one rkyv `CriomeDaemonConfiguration` file and
does not parse NOTA. The wire is the `signal-criome` contract (closed
`CriomeRequest` / `CriomeReply` enums over `signal-frame`) over
length-prefixed rkyv frames between components. No flags.

*Wire vocabulary is closed and typed.* Request and reply enums are
closed — no `Unknown` escape hatch, no stringly-typed dispatch. The
contract crate (`signal-criome`) owns the wire vocabulary; this daemon
owns the runtime: actors, sockets, the durable authority state, and the
verify/sign/register logic.

## See also

- `ARCHITECTURE.md` — the authorization model, policy classes,
  escalation kinds, peer discovery, and actor topology.
- `skills.md` — repo-specific required reading.
- `../signal-criome/ARCHITECTURE.md` — the wire contract.
- This file's "Why this repo exists" section — the scope discipline
  separating today's criome from the eventual Criome.
- `primary/skills/component-triad.md` — repo triad structure.
