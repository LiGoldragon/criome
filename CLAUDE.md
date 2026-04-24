# criome

The runtime for sema worlds. What an OS is to processes, the criome is
to agents: the substrate that provides content-addressed storage, identity,
and persistence without dictating what runs on it.

## What the Criome Is

A universal computing paradigm. The environment in which sema-typed agents
operate. Agents own their own sema worlds within the criome. The criome
doesn't know about schemas, domains, or typed relations — that's the
agent's concern. The criome knows about bytes, hashes, and identity.

## The Stack

Current (MVP-era, aligned with
`mentci-next/docs/architecture.md`):

```
criome          runtime — hosts sema worlds; three daemons
                nexusd (text↔rkyv), criomed (sema's engine),
                lojixd (effects executor)
sema            records DB — content-addressed logical code records
                (Fn, Struct, Expr, Type, …); owned by criomed
                backed by redb
lojix-store     blob DB — append-only file + hash→offset index;
                opaque bytes (compiled binaries, attachments);
                owned by lojixd
nexus           protocol — how clients talk to criomed (parsed
                by nexusd; rkyv to criomed)
nota            language — canonical text grammar nota ⊂ nexus
```

**Shelved for MVP**: `arbor` (prolly-tree versioning).
`aski` is no longer in the stack — its role as "how types are
specified" is now played by `nexus-schema` records.

**Earlier framing (historical)**: an earlier vision called the
persistence layer `criome-store` as a single universal store.
The MVP splits this into two stores — `sema` (records, redb)
and `lojix-store` (blobs, append-only) — so that records and
opaque bytes can evolve independently and GC differently.

## The Vision

Sema is the eternal format — typed binary all the way down, from knowledge
bases to multimedia. The criome is where sema worlds run. As sema grows to
enumerate audio, video, and spatial composition, the criome becomes the
runtime for experiences that science fiction depicts today: semantic-level
editing of media, zero-copy structural sharing across versions, meaning
that is independent of any natural language.

## VCS

Jujutsu (`jj`) is mandatory. Always pass `-m`.
