# Agent instructions — criome

You **MUST** read AGENTS.md at `github:ligoldragon/lore` — the workspace contract.

This repo's `ARCHITECTURE.md` describes **today's Spartan criome**
— a minimal BLS-signature authentication and attestation substrate
for the Persona ecosystem.

## Repo role

**Authentication and attestation substrate.** A single Kameo daemon
owning criome's root BLS keypair, an identity registry, and an
attestation audit log. Signs typed attestations over channel
grants, archive fingerprints, authorization decisions, and
privilege elevations. Verifies signatures against the identity
registry. Does **not** run effects, validate sema records, or
hold any private keys other than its own root.

---

## Repo state — pre-rewrite

This repo currently holds the **prior sema-records-validator
skeleton** (validator pipeline, ractor supervision tree,
sema-records tables) at commit `a3f4173`. That code is the
archaeology of the previous shape; the rewrite to the Spartan
shape is **operator's first track**.

Until the rewrite lands, the code in `src/` does not match this
ARCH. Read the ARCH for the *target*; consult commit `a3f4173`
for the *current* code if you need to mine the prior validator
skeleton for any reason.

---

## Carve-outs worth knowing

- **Kameo, not ractor.** The new daemon is direct Kameo per
  `~/primary/skills/kameo.md`. The prior shape used ractor; that
  vocabulary is retired.
- **Out-of-band attestations.** Attestations live in separate
  `signal-criome` records that reference content records
  (`signal-persona-mind::ChannelGrant`, etc.). Content records
  do not carry embedded proof fields. `signal-persona-auth`'s
  discipline (origin context, not proof material) stays
  inviolate.
- **One redb, one writer.** `StoreKernel` is the only actor that
  opens `criome.redb`. Other store actors route through it (per
  `~/primary/skills/rust/storage-and-wire.md`).
- **Blocking belongs in plane actors.** BLS signature
  generation/verification is blocking work; it lives behind
  `DelegatedReply` or a dedicated thread per
  `~/primary/skills/kameo.md` §"Blocking-plane templates".
- **One NOTA record at the CLI.** The `criome` CLI accepts
  exactly one NOTA request record and prints exactly one NOTA
  reply record, per
  `~/primary/skills/rust/crate-layout.md` §"CLIs are daemon
  clients".
