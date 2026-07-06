//! THE TRUST-ANCHOR WITNESS: two independent criome daemons with DISTINCT
//! signing identities cross-verify by registered key, and refuse an
//! unregistered or foreign key fail-closed.
//!
//! The psyche chose distinct criome identities over a shared cluster key: two
//! genuinely separate parties, each signing as its own `Host(...)` identity,
//! cross-trusting only via keys registered into the peer's registry. This test
//! is the load-bearing proof that the configurable `node_identity` actually
//! delivers that:
//!
//!   - criome A signs as `Host("node-a")` with its own master key;
//!   - criome B, which has registered A's public key under `Host("node-a")`,
//!     verifies A's attestation as `Valid` — cross-identity ACCEPT;
//!   - a criome that B has NOT registered is `UnknownSigner` — REFUSE;
//!   - a FOREIGN criome that also calls itself `Host("node-a")` but signs with a
//!     different key is `InvalidSignature` against B's registered key — REFUSE,
//!     and crucially this refusal bites even though B's `cluster_root` is `None`
//!     (loose admission lets any key be REGISTERED, but the signature still has
//!     to verify against the key B holds for that identity).
//!
//! Every signature is real `blst` BLS12-381 over criome's canonical attestation
//! preimage (which binds the signer identity). There is no shared key and no
//! fixed-identity always-yes; the accept and the refusals run against the same
//! verifying criome B in one test, so a degenerate verifier could not pass.

use criome::actors::root::{Arguments as RootArguments, CriomeRoot, SubmitRequest};
use criome::tables::StoreLocation;
use signal_criome::{
    AuditContext, BlsPublicKey, ContentPurpose, ContentReference, CriomeReply, CriomeRequest,
    Identity, IdentityRegistration, KeyPurpose, ObjectDigest, PrincipalName, PublicKeyFingerprint,
    ReplayNonce, SignRequest, VerificationDecision, VerifyRequest,
};

fn node_store(name: &str) -> StoreLocation {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "criome-distinct-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("create node fixture dir");
    StoreLocation::new(path.join("criome.sema"))
}

/// A criome instance configured to sign as `node_identity`. Each instance owns
/// its own store directory, hence its own master key — two instances are two
/// independent trust roots.
async fn start_node(name: &str, node_identity: Identity) -> kameo::actor::ActorRef<CriomeRoot> {
    CriomeRoot::start(RootArguments {
        store: node_store(name),
        cluster_root: None,
        authorization_mode: signal_criome::AuthorizationMode::Quorum,
        node_identity,
        conveyance: std::sync::Arc::new(criome::conveyance::NoConveyance),
        clock: criome::master_key::SystemClock::system(),
    })
    .await
    .unwrap_or_else(|error| panic!("start criome {name}: {error}"))
}

/// A sign request from `requester` over fixed fixture content. The requester
/// must be an Active identity in the signing criome; a node's own identity is
/// self-registered Active at startup, so a node can always ask itself to sign.
fn sign_request(requester: Identity) -> SignRequest {
    SignRequest::new(
        ContentReference {
            digest: ObjectDigest::from_bytes(b"distinct-identity-fixture"),
            purpose: ContentPurpose::SignedObject,
            schema_version: PrincipalName::new("distinct-identity-schema".to_string()),
        },
        requester,
        AuditContext {
            purpose: ContentPurpose::SignedObject,
            audience: PrincipalName::new("distinct-identity-audience".to_string()),
            policy_version: PrincipalName::new("distinct-identity-policy".to_string()),
            nonce: ReplayNonce::new("distinct-identity-nonce".to_string()),
        },
        None,
    )
}

/// Ask a criome to sign as itself (requester = its own identity) and return the
/// resulting attestation. The attestation's `signer` is the criome's configured
/// `node_identity`; its envelope carries that criome's master public key.
async fn attest_as(
    node: &kameo::actor::ActorRef<CriomeRoot>,
    node_identity: Identity,
) -> signal_criome::Attestation {
    let reply = node
        .ask(SubmitRequest::new(CriomeRequest::Sign(sign_request(
            node_identity,
        ))))
        .await
        .expect("submit sign request")
        .into_reply();
    match reply {
        CriomeReply::SignReceipt(receipt) => receipt.attestation,
        other => panic!("expected SignReceipt, got {other:?}"),
    }
}

/// Register `identity` -> `public_key` into a criome's registry. With
/// `cluster_root = None` this is admitted unconditionally — the point of the
/// refusal cases below is that admission being loose does NOT make verification
/// loose.
async fn register_peer_key(
    node: &kameo::actor::ActorRef<CriomeRoot>,
    identity: Identity,
    public_key: BlsPublicKey,
) {
    let reply = node
        .ask(SubmitRequest::new(CriomeRequest::RegisterIdentity(
            IdentityRegistration::new(
                identity.clone(),
                public_key,
                PublicKeyFingerprint::new(format!("{identity:?}-fingerprint")),
                KeyPurpose::CriomeRoot,
                None,
            ),
        )))
        .await
        .expect("submit register identity")
        .into_reply();
    match reply {
        CriomeReply::IdentityReceipt(_) => {}
        other => panic!("expected IdentityReceipt for peer registration, got {other:?}"),
    }
}

async fn verify_on(
    node: &kameo::actor::ActorRef<CriomeRoot>,
    attestation: signal_criome::Attestation,
) -> VerificationDecision {
    let content = attestation.content.clone();
    let reply = node
        .ask(SubmitRequest::new(CriomeRequest::VerifyAttestation(
            VerifyRequest {
                attestation,
                content,
            },
        )))
        .await
        .expect("submit verify request")
        .into_reply();
    match reply {
        CriomeReply::VerificationResult(result) => result.decision,
        other => panic!("expected VerificationResult, got {other:?}"),
    }
}

/// A criome configured as `Host("node-a")` produces an attestation that VERIFIES
/// on a separate criome `Host("node-b")` which has registered A's key under
/// `Host("node-a")`; an attestation whose signer B never registered is refused
/// `UnknownSigner`; and a foreign criome that also calls itself `Host("node-a")`
/// but signs with a different key is refused `InvalidSignature` against the key
/// B holds for `node-a` — all against the SAME verifying criome B in one run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_criome_identities_cross_verify_and_refuse_foreign_keys() {
    let identity_a = Identity::host("node-a".to_string());
    let identity_b = Identity::host("node-b".to_string());

    let node_a = start_node("node-a", identity_a.clone()).await;
    let node_b = start_node("node-b", identity_b.clone()).await;
    let foreign = start_node("foreign", identity_a.clone()).await;

    // criome A signs as Host("node-a") with key Ka. The envelope carries Ka.
    let attestation_a = attest_as(&node_a, identity_a.clone()).await;
    let key_a = attestation_a.envelope.public_key.clone();

    // The foreign criome ALSO calls itself Host("node-a"), but signs with its
    // own independent key Kf (different store -> different master key).
    let attestation_foreign = attest_as(&foreign, identity_a.clone()).await;
    assert_ne!(
        attestation_foreign.envelope.public_key, key_a,
        "the foreign criome must have an independent key for the refusal to be meaningful"
    );

    // ACCEPT: B registers A's real key under Host("node-a") and verifies A's
    // attestation as Valid — distinct identities, cross-trust by registered key.
    register_peer_key(&node_b, identity_a.clone(), key_a).await;
    assert_eq!(
        verify_on(&node_b, attestation_a.clone()).await,
        VerificationDecision::Valid,
        "criome B must accept an attestation from the registered distinct identity node-a"
    );

    // REFUSE (foreign key): the same B, holding node-a -> Ka, verifies the
    // FOREIGN node-a attestation (signed with Kf). Even though B's cluster_root
    // is None (any key could be REGISTERED), the signature does not verify
    // against the key B actually holds for node-a -> InvalidSignature.
    assert_eq!(
        verify_on(&node_b, attestation_foreign.clone()).await,
        VerificationDecision::InvalidSignature,
        "criome B must refuse a foreign key impersonating a registered identity"
    );

    // REFUSE (unregistered): a fresh criome B' that never registered node-a
    // cannot resolve the signer at all -> UnknownSigner, fail-closed.
    let node_b_prime = start_node("node-b-prime", identity_b.clone()).await;
    assert_eq!(
        verify_on(&node_b_prime, attestation_a.clone()).await,
        VerificationDecision::UnknownSigner,
        "a criome that never registered node-a must refuse its attestation"
    );

    CriomeRoot::stop(node_a).await.expect("stop node-a");
    CriomeRoot::stop(node_b).await.expect("stop node-b");
    CriomeRoot::stop(foreign).await.expect("stop foreign");
    CriomeRoot::stop(node_b_prime)
        .await
        .expect("stop node-b-prime");
}
