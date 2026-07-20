---
Title: criome
---

# criome

*Today's `criome` is a minimal Spartan **BLS-signature
authentication and attestation substrate** for the Persona
ecosystem. A single Kameo daemon: identity registry,
sign/verify primitives, typed attestations.*

The **eventual** `Criome` is the universal computing paradigm
expressed in Sema — replacing Git, the editor, SSH, and the
web; encompassing programming, version control, network
identity, validation, and auth/security across the stack.
Today's Spartan criome is one realization step toward that
eventual shape, bringing forward the auth/identity slice.

See `ARCHITECTURE.md` for this repo's shape.

`criome` is the one-argument NOTA client. `criome-daemon` is the
one-argument daemon and accepts only a signal-encoded rkyv
`CriomeDaemonConfiguration` file; it has no flag/subcommand
configuration surface.

## v0.11 compatibility family

This release pins the runtime contract family to immutable producer revisions.
It retains one Nota, signal-frame, and signal-criome runtime family while
preserving criome's source, schema, storage, and wire behavior.
