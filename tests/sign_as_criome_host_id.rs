//! THE CRIOME-HOST-ID WITNESS (primary-79z1.18, slice A1): a node signs an
//! attestation as its own Criome host ID — `Host(<master public key>)` — and a
//! peer that has registered that host ID by key verifies it, while a stranger
//! host ID the peer never registered is refused fail-closed.
//!
//! This is the criome side of making the router fabric's identity the Criome
//! host ID. The router asks its co-resident criome to attest a session proof or
//! forward object as the router's own host ID (the pubkey it sourced via
//! `ObserveNodePublicKey`); criome authorizes that self-signing without a
//! registry gate and mints the attestation under `Host(<pubkey>)`. The receiving
//! router hands the attestation to ITS criome, which resolves the signer — whose
//! name IS the Criome host ID — against its registry: a registered host ID
//! verifies `Valid`, an unregistered one is `UnknownSigner`. No OS host name is
//! involved; the identity is the master public key end-to-end.
//!
//! Every signature is real `blst` BLS12-381 over criome's canonical attestation
//! preimage, which binds the signer identity — here the host ID. The accept and
//! the refusal run against the same verifying criome in one test, so a
//! degenerate always-yes verifier could not pass.

use criome::actors::root::{Arguments as RootArguments, CriomeRoot, SubmitRequest};
use criome::tables::StoreLocation;
use signal_criome::{
    AuditContext, BlsPublicKey, ContentPurpose, ContentReference, CriomeReply, CriomeRequest,
    Identity, IdentityRegistration, KeyPurpose, NodePublicKeyObservation, ObjectDigest,
    PrincipalName, PublicKeyFingerprint, ReplayNonce, SignRequest, VerificationDecision,
    VerifyRequest,
};

fn node_store(name: &str) -> StoreLocation {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "criome-host-id-{name}-{}-{}",
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

/// A criome instance. Each owns its own store, hence its own master key — two
/// instances are two independent Criome host IDs.
async fn start_node(name: &str, node_identity: Identity) -> kameo::actor::ActorRef<CriomeRoot> {
    CriomeRoot::start(RootArguments {
        store: node_store(name),
        cluster_root: None,
        authorization_mode: signal_criome::AuthorizationMode::Quorum,
        quorum_window: RootArguments::DEFAULT_QUORUM_WINDOW,
        node_identity,
        conveyance: std::sync::Arc::new(criome::conveyance::NoConveyance),
        clock: criome::master_key::SystemClock::system(),
    })
    .await
    .unwrap_or_else(|error| panic!("start criome {name}: {error}"))
}

/// This node's Criome host ID: its master public key, read from the public
/// `ObserveNodePublicKey` read-op — exactly the value the router sources to key
/// the fabric on.
async fn host_id_key(node: &kameo::actor::ActorRef<CriomeRoot>) -> BlsPublicKey {
    let reply = node
        .ask(SubmitRequest::new(CriomeRequest::ObserveNodePublicKey(
            NodePublicKeyObservation::new(),
        )))
        .await
        .expect("submit observe node public key")
        .into_reply();
    match reply {
        CriomeReply::NodePublicKey(observed) => observed.public_key().clone(),
        other => panic!("expected NodePublicKey, got {other:?}"),
    }
}

/// The `Host(<public key>)` identity — the Criome host ID as a signing identity.
fn host_id_identity(key: &BlsPublicKey) -> Identity {
    Identity::host(key.as_str().to_string())
}

/// Ask `node` to attest fixed content AS ITS OWN CRIOME HOST ID. The request's
/// signer is `Host(<this node's master key>)`; criome authorizes the self-sign
/// and mints the attestation under that host ID.
async fn attest_as_host_id(
    node: &kameo::actor::ActorRef<CriomeRoot>,
    host_id: Identity,
) -> signal_criome::Attestation {
    let request = SignRequest::new(
        ContentReference {
            object_digest: ObjectDigest::from_bytes(b"criome-host-id-fixture"),
            content_purpose: ContentPurpose::SignedObject,
            principal_name: PrincipalName::new("criome-host-id-schema".to_string()),
        },
        host_id,
        AuditContext {
            content_purpose: ContentPurpose::SignedObject,
            audience: PrincipalName::new("criome-host-id-audience".to_string()),
            policy_version: PrincipalName::new("criome-host-id-policy".to_string()),
            replay_nonce: ReplayNonce::new("criome-host-id-nonce".to_string()),
        },
        None,
    );
    let reply = node
        .ask(SubmitRequest::new(CriomeRequest::Sign(request)))
        .await
        .expect("submit sign-as-host-id request")
        .into_reply();
    match reply {
        CriomeReply::SignReceipt(receipt) => receipt.attestation,
        other => panic!("expected SignReceipt, got {other:?}"),
    }
}

/// Register `host_id -> key` — the owner-authored trust seed a receiving criome
/// holds for a known peer, keyed on the Criome host ID.
async fn register_host_id(
    node: &kameo::actor::ActorRef<CriomeRoot>,
    host_id: Identity,
    key: BlsPublicKey,
) {
    let reply = node
        .ask(SubmitRequest::new(CriomeRequest::RegisterIdentity(
            IdentityRegistration::new(
                host_id.clone(),
                key,
                PublicKeyFingerprint::new(format!("{host_id:?}-fingerprint")),
                KeyPurpose::CriomeRoot,
                None,
            ),
        )))
        .await
        .expect("submit register host id")
        .into_reply();
    match reply {
        CriomeReply::IdentityReceipt(_) => {}
        other => panic!("expected IdentityReceipt, got {other:?}"),
    }
}

async fn verify_on(
    node: &kameo::actor::ActorRef<CriomeRoot>,
    attestation: signal_criome::Attestation,
) -> VerificationDecision {
    let content = attestation.content_reference.clone();
    let reply = node
        .ask(SubmitRequest::new(CriomeRequest::VerifyAttestation(
            VerifyRequest {
                attestation,
                content_reference: content,
            },
        )))
        .await
        .expect("submit verify request")
        .into_reply();
    match reply {
        CriomeReply::VerificationResult(result) => result.verification_decision,
        other => panic!("expected VerificationResult, got {other:?}"),
    }
}

/// A node signs an attestation as its own Criome host ID; a peer that registered
/// that host ID by key verifies it `Valid`; a stranger host ID the peer never
/// registered is `UnknownSigner` — all against the same verifying criome.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_attests_as_its_criome_host_id_and_a_peer_verifies_by_that_key() {
    let node_a = start_node("node-a", Identity::host("os-host-a".to_string())).await;
    let verifier = start_node("verifier", Identity::host("os-host-v".to_string())).await;
    let stranger = start_node("stranger", Identity::host("os-host-s".to_string())).await;

    let key_a = host_id_key(&node_a).await;
    let host_id_a = host_id_identity(&key_a);
    let key_stranger = host_id_key(&stranger).await;
    let host_id_stranger = host_id_identity(&key_stranger);
    assert_ne!(
        key_a, key_stranger,
        "independent nodes must have independent Criome host IDs"
    );

    // A attests as its own host ID Host(key_a) — no OS name in the signer.
    let attestation_a = attest_as_host_id(&node_a, host_id_a.clone()).await;
    assert_eq!(
        attestation_a.identity, host_id_a,
        "the attestation is signed under the Criome host ID, not the OS node identity"
    );
    assert_eq!(
        attestation_a.signature_envelope.bls_public_key, key_a,
        "the signing key is the master public key that IS the host ID"
    );

    // The stranger attests as its own host ID Host(key_stranger).
    let attestation_stranger = attest_as_host_id(&stranger, host_id_stranger.clone()).await;

    // ACCEPT: the verifier registered A's host ID by key and verifies Valid.
    register_host_id(&verifier, host_id_a, key_a).await;
    assert_eq!(
        verify_on(&verifier, attestation_a).await,
        VerificationDecision::Valid,
        "a registered Criome host ID's attestation must verify"
    );

    // REFUSE: the verifier never registered the stranger's host ID -> fail-closed.
    assert_eq!(
        verify_on(&verifier, attestation_stranger).await,
        VerificationDecision::UnknownSigner,
        "an unregistered Criome host ID must be refused fail-closed"
    );

    CriomeRoot::stop(node_a).await.expect("stop node-a");
    CriomeRoot::stop(verifier).await.expect("stop verifier");
    CriomeRoot::stop(stranger).await.expect("stop stranger");
}
