# Agent instructions — criome

This repo's `ARCHITECTURE.md` describes **today's Spartan criome**
— a minimal BLS-signature authentication and attestation substrate
for the Persona ecosystem.

## Repo role

**Authentication and attestation substrate.** A single Kameo daemon
owning criome's local key store, identity registry, authorization
state, and attestation audit log. Signs typed attestations over
channel grants, archive fingerprints, authorization decisions, and
privilege elevations. Verifies signatures against the identity
registry. Does **not** run effects or validate sema records.


## Repo state

This repo holds today's Spartan Criome daemon. The older
sema-records-validator skeleton (validator pipeline, ractor
supervision tree, sema-records tables) is archaeology preserved at
commit `a3f4173`; today's code is the Kameo authentication and
attestation daemon described in `ARCHITECTURE.md`.


## Carve-outs worth knowing

- **Kameo, not ractor.** The daemon is direct Kameo. The prior shape used
  ractor; that vocabulary is retired.
- **Out-of-band attestations.** Attestations live in separate
  `signal-criome` records that reference content records
  (`signal-mind` channel-grant records, etc.). Content records
  do not carry embedded proof fields. `signal-persona-origin`'s
  discipline (origin context, not proof material) stays
  inviolate.
- **One store, one writer.** `StoreKernel` is the only actor that
  opens `criome.sema`. Other store actors route through it.
- **Blocking belongs in plane actors.** BLS signature
  generation/verification is blocking work; it lives behind
  `DelegatedReply` or a dedicated thread.
- **One NOTA record at the CLI.** The `criome` CLI accepts
  exactly one NOTA request record and prints exactly one NOTA
  reply record.
