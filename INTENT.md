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
in `criome.sema` via the `sema` library. It is paired with the
`signal-criome` contract and the `owner-signal-criome` owner contract.

This repo is "today, not eventually." The **eventual** `Criome` is the
universal computing paradigm expressed in Sema — replacing Git, the
editor, SSH, and the web — encompassing programming, version control,
network identity, validation, and auth/security across the stack.
Today's Spartan daemon is one realization step that brings forward the
auth/identity slice; it does not carry the eventual scope.

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

*One daemon, one owner — a single Unix user.* Only that user can write
to the daemon's owner socket; single-ownership is what gives the daemon
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
daemon takes one argument.* The CLI accepts exactly one NOTA request
and prints exactly one reply. The wire is the `signal-criome` contract
(closed `CriomeRequest` / `CriomeReply` enums over `signal-frame`)
over length-prefixed rkyv frames between components. No flags;
configuration arrives as a typed record.

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
- `primary/ESSENCE.md` §"Today and eventually" — the scope discipline
  separating today's criome from the eventual Criome.
- `primary/skills/component-triad.md` — repo triad structure.
