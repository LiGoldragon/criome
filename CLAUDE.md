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

```
criome          runtime — hosts sema worlds, provides identity
criome-store    persistence — content-addressed bytes (blake3 → bytes)
arbor           versioning — prolly trees over the store
nexus           protocol — how agents talk to sema worlds
aski            language — how sema types are specified
sema            the format — the universal typed binary that everything is
```

Each layer knows only the one below it. The contracts between layers are
the only coupling.

## The Vision

Sema is the eternal format — typed binary all the way down, from knowledge
bases to multimedia. The criome is where sema worlds run. As sema grows to
enumerate audio, video, and spatial composition, the criome becomes the
runtime for experiences that science fiction depicts today: semantic-level
editing of media, zero-copy structural sharing across versions, meaning
that is independent of any natural language.

## VCS

Jujutsu (`jj`) is mandatory. Always pass `-m`.
