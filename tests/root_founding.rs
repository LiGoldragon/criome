//! THE ROOT FOUNDING CEREMONY: a node exposes its Criome master public key on the
//! public socket, an explicit owner accept on the owner-only meta socket founds
//! the root (the master key signs the founding statement — no auto-approval), the
//! founded root is persisted, and a later boot verifies the anchor and never
//! re-founds.
//!
//!   - `an_owner_accept_founds_the_root_and_returns_a_verifiable_signature`: the
//!     public read-op returns the node's master key; the meta accept returns this
//!     node's `FoundingSignature`, which verifies under that key over the founding
//!     statement — the willing establishment, minted only on the explicit accept.
//!   - `an_anchor_that_mismatches_the_cohort_is_refused`: the owner's stated anchor
//!     must equal the cohort's self-certifying anchor, else `CohortMismatch`.
//!   - `a_second_accept_after_founding_is_refused`: a founded (unanimous) root is
//!     immutable within a boot — a re-accept is `AlreadyFounded`.
//!   - `a_reboot_verifies_the_founded_anchor_and_never_refounds`: a fresh daemon on
//!     the SAME store reads the persisted founded root, verifies it, adopts it, and
//!     refuses to re-found — closing the "haywire trust on every boot" hazard.

use std::path::{Path, PathBuf};
use std::thread;

use criome::daemon::{BoundCriomeDaemon, CriomeDaemon};
use criome::founding::FoundingStatementBytes;
use criome::master_key::VerifyBls;
use criome::tables::StoreLocation;
use criome::transport::{CriomeClient, CriomeMetaClient};
use meta_signal_criome::{
    Input as MetaInput, Output as MetaOutput, RootFoundingAcceptance, RootFoundingRejectionReason,
};
use signal_criome::{
    BlsPublicKey, Contract, CriomeReply, CriomeRequest, FoundingMember, GenesisDomainTag, Identity,
    IdentityLookup, NodePublicKeyObservation, PolicyMember, ReplayNonce,
    RequiredSignatureThreshold, RootAnchorDigest, RootFoundingStatement, RootGenesis, Rule,
    Threshold,
};

fn nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos()
}

fn fixture(tag: &str) -> (PathBuf, StoreLocation) {
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "criome-founding-{tag}-{}-{}",
        std::process::id(),
        nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create founding fixture dir");
    (dir.clone(), StoreLocation::new(dir.join("criome.sema")))
}

fn founder() -> Identity {
    Identity::host("founder".to_string())
}

fn bind(working: &Path, store: StoreLocation) -> (BoundCriomeDaemon, PathBuf) {
    let daemon = CriomeDaemon::new(working, store).with_node_identity(founder());
    let meta = daemon.meta_socket().clone();
    (daemon.bind().expect("bind founding daemon"), meta)
}

fn send_working(bound: &BoundCriomeDaemon, socket: &Path, request: CriomeRequest) -> CriomeReply {
    thread::scope(|scope| {
        let server = scope.spawn(|| bound.serve_next().expect("serve working request"));
        let reply = CriomeClient::new(socket)
            .send(request)
            .expect("submit working request");
        assert_eq!(server.join().expect("join working server"), reply);
        reply
    })
}

fn send_meta(bound: &BoundCriomeDaemon, socket: &Path, request: MetaInput) -> MetaOutput {
    thread::scope(|scope| {
        let server = scope.spawn(|| bound.serve_next_meta().expect("serve meta request"));
        let reply = CriomeMetaClient::new(socket)
            .send(request)
            .expect("submit meta request");
        assert_eq!(server.join().expect("join meta server"), reply);
        reply
    })
}

fn observe_node_public_key(bound: &BoundCriomeDaemon, working: &Path) -> BlsPublicKey {
    match send_working(
        bound,
        working,
        CriomeRequest::observe_node_public_key(NodePublicKeyObservation::new()),
    ) {
        CriomeReply::NodePublicKey(key) => key.public_key().clone(),
        other => panic!("expected NodePublicKey, got {other:?}"),
    }
}

/// A single-node cohort genesis: root Threshold over the founder as key member,
/// `founding_keys` = the founder's master key, `parent = Root`.
fn single_node_genesis(public_key: &BlsPublicKey, nonce: &str) -> RootGenesis {
    let root_contract = Contract::root(Rule::Threshold(Threshold::new(
        RequiredSignatureThreshold::new(1),
        vec![PolicyMember::KeyMember(founder())],
    )));
    RootGenesis::new(
        root_contract,
        vec![FoundingMember::new(founder(), public_key.clone())],
        GenesisDomainTag::CriomeRootFoundingV1,
        ReplayNonce::new(nonce),
    )
}

fn accept(
    bound: &BoundCriomeDaemon,
    meta: &Path,
    anchor: RootAnchorDigest,
    cohort: RootGenesis,
) -> MetaOutput {
    send_meta(
        bound,
        meta,
        MetaInput::accept_root_founding(RootFoundingAcceptance::new(anchor, cohort)),
    )
}

fn identity_is_registered(bound: &BoundCriomeDaemon, working: &Path, identity: Identity) -> bool {
    matches!(
        send_working(
            bound,
            working,
            CriomeRequest::LookupIdentity(IdentityLookup::new(identity)),
        ),
        CriomeReply::IdentityReceipt(_)
    )
}

#[test]
fn an_owner_accept_founds_the_root_and_returns_a_verifiable_signature() {
    let (dir, store) = fixture("verifiable-signature");
    let working = dir.join("criome.sock");
    let (bound, meta) = bind(&working, store);

    let public_key = observe_node_public_key(&bound, &working);
    let genesis = single_node_genesis(&public_key, "found-verifiable");
    let anchor = genesis.anchor().expect("genesis anchor");

    match accept(&bound, &meta, anchor.clone(), genesis) {
        MetaOutput::RootFoundingAccepted(accepted) => {
            assert_eq!(
                accepted.root_anchor_digest, anchor,
                "the founded anchor is echoed back"
            );
            assert_eq!(
                accepted.founding_signature.identity,
                founder(),
                "this node signs the founding as its own identity"
            );
            // The returned founding signature verifies against the node's master
            // key over the founding-statement preimage — a real, willing sign.
            let statement =
                RootFoundingStatement::new(anchor.clone(), GenesisDomainTag::CriomeRootFoundingV1);
            let bytes = statement.signing_bytes().expect("statement encodes");
            assert!(
                public_key.verify_bls(
                    &accepted.founding_signature.signature_envelope.bls_signature,
                    &bytes
                ),
                "the founding signature verifies under the node's master key"
            );
        }
        other => panic!("expected RootFoundingAccepted, got {other:?}"),
    }

    assert!(
        identity_is_registered(&bound, &working, founder()),
        "the founded cohort seeds the registry"
    );
    bound.shutdown().expect("shutdown founding daemon");
}

#[test]
fn an_anchor_that_mismatches_the_cohort_is_refused() {
    let (dir, store) = fixture("cohort-mismatch");
    let working = dir.join("criome.sock");
    let (bound, meta) = bind(&working, store);

    let public_key = observe_node_public_key(&bound, &working);
    let genesis = single_node_genesis(&public_key, "real-cohort");
    // An anchor from a DIFFERENT cohort (distinct nonce) never matches this one.
    let foreign_anchor = single_node_genesis(&public_key, "foreign-cohort")
        .anchor()
        .expect("foreign anchor");

    match accept(&bound, &meta, foreign_anchor, genesis) {
        MetaOutput::RootFoundingRejected(rejected) => assert_eq!(
            rejected.payload(),
            &RootFoundingRejectionReason::CohortMismatch,
        ),
        other => panic!("expected RootFoundingRejected(CohortMismatch), got {other:?}"),
    }
    bound.shutdown().expect("shutdown founding daemon");
}

#[test]
fn a_second_accept_after_founding_is_refused() {
    let (dir, store) = fixture("already-founded");
    let working = dir.join("criome.sock");
    let (bound, meta) = bind(&working, store);

    let public_key = observe_node_public_key(&bound, &working);
    let genesis = single_node_genesis(&public_key, "found-once");
    let anchor = genesis.anchor().expect("genesis anchor");

    // A single-node cohort reaches unanimity on the first accept.
    assert!(matches!(
        accept(&bound, &meta, anchor.clone(), genesis.clone()),
        MetaOutput::RootFoundingAccepted(_)
    ));
    // The founded root is immutable — a re-accept is refused, never re-founded.
    match accept(&bound, &meta, anchor, genesis) {
        MetaOutput::RootFoundingRejected(rejected) => assert_eq!(
            rejected.payload(),
            &RootFoundingRejectionReason::AlreadyFounded,
        ),
        other => panic!("expected RootFoundingRejected(AlreadyFounded), got {other:?}"),
    }
    bound.shutdown().expect("shutdown founding daemon");
}

#[test]
fn a_reboot_verifies_the_founded_anchor_and_never_refounds() {
    let (dir, store) = fixture("reboot");
    let working_one = dir.join("criome-1.sock");

    // First boot: found the root, then shut down. The founded root and its
    // attached signature persist in the store (and the master key beside it).
    let (bound_one, meta_one) = bind(&working_one, store.clone());
    let public_key = observe_node_public_key(&bound_one, &working_one);
    let genesis = single_node_genesis(&public_key, "reboot-cohort");
    let anchor = genesis.anchor().expect("genesis anchor");
    assert!(matches!(
        accept(&bound_one, &meta_one, anchor.clone(), genesis.clone()),
        MetaOutput::RootFoundingAccepted(_)
    ));
    bound_one.shutdown().expect("shutdown first boot");

    // Second boot on the SAME store: on_start verifies the persisted anchor and
    // its founding signature, adopts the founded root (re-seeds the registry),
    // and never re-founds.
    let working_two = dir.join("criome-2.sock");
    let (bound_two, meta_two) = bind(&working_two, store);
    assert!(
        identity_is_registered(&bound_two, &working_two, founder()),
        "the adopted founded cohort is present in the registry after reboot"
    );
    // Re-founding is refused: the node trusts its verified founded anchor.
    match accept(&bound_two, &meta_two, anchor, genesis) {
        MetaOutput::RootFoundingRejected(rejected) => assert_eq!(
            rejected.payload(),
            &RootFoundingRejectionReason::AlreadyFounded,
            "a rebooted node never re-founds its verified root"
        ),
        other => panic!("expected RootFoundingRejected(AlreadyFounded), got {other:?}"),
    }
    bound_two.shutdown().expect("shutdown second boot");
}
