//! `criome-cluster-root` — the offline cluster-root admission-signing ceremony.
//!
//! This one-shot tool mints a cluster-root-signed admission so criome's
//! already-tested `ClusterRoot::admits` gate accepts a router/member identity.
//! It reads the single NOTA argument — an unadmitted `IdentityRegistration`
//! (the identity↔key↔purpose binding, `admission None`), inline or as a file
//! path per the one-argument rule — signs the canonical registration statement
//! with the cluster-root key, and prints the same registration carrying the
//! cluster-root `SignatureEnvelope` as NOTA on stdout.
//!
//! It is an OFFLINE signer: it never opens the daemon socket and never builds a
//! `CriomeClient`. The cluster-root secret key is loaded out-of-band from the
//! path named by the `CRIOME_CLUSTER_ROOT_KEY` environment variable, defaulting
//! to `/var/lib/criome/cluster-root.secret`; on first run a fresh key is
//! generated and persisted there at mode `0600`, and every later run reloads
//! the same key so all admissions verify under one stable trust anchor. The one
//! NOTA argument is the registration to admit, never the key path.
//!
//! Operator hand-off: the printed registration is the admitted binding to feed
//! back into criome's registry; the cluster-root public key (the trust anchor
//! configured as criome's `cluster_root`) is the public half of the key file.

use criome::ceremony::ClusterRootCeremonyCommand;

fn main() -> criome::Result<()> {
    ClusterRootCeremonyCommand::from_environment().run()
}
