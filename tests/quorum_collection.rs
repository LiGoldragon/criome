//! THE QUORUM-COLLECTION WITNESS: two independent criome daemons gather a real
//! 2-of-2 BLS quorum across the voice, withhold the change until a true majority
//! co-sign, and refuse a below-majority Evidence fail-closed.
//!
//! This is the security heart of the persistent Spirit mirror. Each daemon is a
//! genuinely separate party: its own store, its own `blst` master key, its own
//! `Host(...)` signing identity, cross-trusting only via keys mutually registered
//! into the peer's registry (the same shape `distinct_node_identities` proves for
//! attestation). The gather runs OVER a real Unix-socket voice
//! ([`DirectDialQuorumVoice`], the single-host multi-user transport):
//!
//!   - A proposes an operation under the admitted 2-of-2 Threshold contract and
//!     casts its own BLS vote (operation signature + moment time-signature). One
//!     vote is one short of the 2-of-2 majority, so the round is WITHHELD
//!     (`Gathering`) — A's own change is not valid on its own say-so.
//!   - A solicits B across the voice; B independently re-validates, casts vote #2,
//!     and conveys it back. A assembles the Evidence and the EXISTING majority
//!     judge (`ContractStore::evaluate`) returns `Authorized` — 2-of-2 gathered.
//!   - That Evidence is judge-valid: fed back through `EvaluateAuthorization` it
//!     verifies as `Authorized` (real per-signer BLS, both members).
//!   - Drop one member's operation signature and the same judge returns
//!     `QuorumShort` — below majority is refused fail-closed.
//!
//! A second scenario proves the "unreachable peer ⇒ waits" rule: with the peer
//! never bound, the round stays `Gathering` and never becomes valid.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use criome::daemon::CriomeDaemon;
use criome::tables::StoreLocation;
use criome::transport::{CriomeClient, CriomeFrameCodec};
use criome::voice::{DirectDialQuorumVoice, PeerSocketRoute, RouterQuorumVoice};
use signal_criome::{
    AuditContext, AuthorizationEvaluation, AuthorizedObjectKind, AuthorizedObjectReference,
    BlsPublicKey, BlsSignature, ComponentKind, ContentPurpose, ContentReference, Contract,
    ContractDigest, CriomeReply, CriomeRequest, EvaluationDecision, EvaluationRejectionReason,
    Evidence, Identity, IdentityRegistration, KeyPurpose, ObjectDigest, OperationDigest,
    PolicyMember, PrincipalName, PublicKeyFingerprint, QuorumProposal, QuorumRoundIdentifier,
    QuorumRoundState, QuorumRoundStatus, QuorumVote, ReplayNonce, RequiredSignatureThreshold,
    RoundPhase, Rule, SignRequest, SignatureEnvelope, SignatureScheme, Threshold, TimeWindow,
    TimestampNanos,
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
        "criome-quorum-{tag}-{}-{}",
        std::process::id(),
        nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create quorum fixture dir");
    (
        dir.join("criome.sock"),
        StoreLocation::new(dir.join("criome.sema")),
    )
}

fn host(name: &str) -> Identity {
    Identity::host(name.to_string())
}

/// Bind `daemon` and serve it forever on a background thread; the process exit
/// reaps the thread at test end.
fn serve(daemon: CriomeDaemon) {
    std::thread::spawn(move || {
        let _ = daemon.run();
    });
}

fn wait_for_socket(socket: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !socket.exists() {
        assert!(
            Instant::now() < deadline,
            "criome socket never appeared: {socket:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn ask(socket: &Path, request: CriomeRequest) -> CriomeReply {
    CriomeClient::new(socket)
        .send(request)
        .unwrap_or_else(|error| panic!("criome round-trip on {socket:?}: {error}"))
}

/// Discover a node's master public key by asking it to sign a fixture as itself;
/// the attestation envelope carries that node's key (mirrors the trust-anchor
/// witness).
fn node_public_key(socket: &Path, identity: Identity) -> BlsPublicKey {
    let request = SignRequest::new(
        ContentReference {
            digest: ObjectDigest::from_bytes(b"quorum-key-probe"),
            purpose: ContentPurpose::SignedObject,
            schema_version: PrincipalName::new("quorum-probe"),
        },
        identity,
        AuditContext {
            purpose: ContentPurpose::SignedObject,
            audience: PrincipalName::new("quorum-probe-audience"),
            policy_version: PrincipalName::new("quorum-probe-policy"),
            nonce: ReplayNonce::new("quorum-probe-nonce"),
        },
        None,
    );
    match ask(socket, CriomeRequest::Sign(request)) {
        CriomeReply::SignReceipt(receipt) => receipt.attestation.envelope.public_key,
        other => panic!("expected SignReceipt, got {other:?}"),
    }
}

fn register_peer(socket: &Path, identity: Identity, public_key: BlsPublicKey) {
    let registration = IdentityRegistration::new(
        identity.clone(),
        public_key,
        PublicKeyFingerprint::new(format!("{identity:?}-fingerprint")),
        KeyPurpose::CriomeRoot,
        None,
    );
    match ask(socket, CriomeRequest::RegisterIdentity(registration)) {
        CriomeReply::IdentityReceipt(_) => {}
        other => panic!("expected IdentityReceipt, got {other:?}"),
    }
}

fn admit(socket: &Path, contract: Contract) -> ContractDigest {
    match ask(socket, CriomeRequest::AdmitContract(contract)) {
        CriomeReply::ContractAdmitted(admitted) => admitted.into_payload(),
        other => panic!("expected ContractAdmitted, got {other:?}"),
    }
}

fn propose(socket: &Path, proposal: QuorumProposal) -> QuorumRoundState {
    match ask(socket, CriomeRequest::ProposeQuorumAuthorization(proposal)) {
        CriomeReply::QuorumRoundOpened(state) => state,
        other => panic!("expected QuorumRoundOpened, got {other:?}"),
    }
}

fn observe(socket: &Path, round: &QuorumRoundIdentifier) -> QuorumRoundState {
    match ask(socket, CriomeRequest::observe_quorum_round(round.clone())) {
        CriomeReply::QuorumRoundObserved(state) => state,
        other => panic!("expected QuorumRoundObserved, got {other:?}"),
    }
}

fn submit(socket: &Path, vote: QuorumVote) -> QuorumRoundState {
    match ask(socket, CriomeRequest::submit_quorum_vote(vote)) {
        CriomeReply::QuorumVoteAccepted(state) => state,
        other => panic!("expected QuorumVoteAccepted, got {other:?}"),
    }
}

/// A signature envelope that is present and well-formed on the wire but carries a
/// foreign key + garbage signature — it can never satisfy a member whose admitted
/// key differs, so the judge does not count it.
fn forged_envelope() -> SignatureEnvelope {
    SignatureEnvelope {
        scheme: SignatureScheme::Bls12_381MinPk,
        public_key: BlsPublicKey::new("forged-foreign-key"),
        signature: BlsSignature::new("forged-signature"),
    }
}

fn evaluate(
    socket: &Path,
    contract: ContractDigest,
    object: AuthorizedObjectReference,
    evidence: Evidence,
) -> EvaluationDecision {
    let evaluation = AuthorizationEvaluation {
        contract,
        object,
        evidence,
    };
    match ask(socket, CriomeRequest::EvaluateAuthorization(evaluation)) {
        CriomeReply::AuthorizationEvaluated(evaluated) => evaluated.decision,
        other => panic!("expected AuthorizationEvaluated, got {other:?}"),
    }
}

fn mirror_contract(alpha: &Identity, beta: &Identity) -> Contract {
    Contract::root(Rule::threshold(Threshold::new(
        RequiredSignatureThreshold::new(2),
        vec![
            PolicyMember::key_member(alpha.clone()),
            PolicyMember::key_member(beta.clone()),
        ],
    )))
}

fn mirror_object() -> AuthorizedObjectReference {
    AuthorizedObjectReference {
        component: ComponentKind::Spirit,
        digest: ObjectDigest::from_bytes(b"mirror-head-operation"),
        kind: AuthorizedObjectKind::Head,
    }
}

fn open_window() -> TimeWindow {
    TimeWindow {
        opens_at: TimestampNanos::new(1),
        closes_at: TimestampNanos::new(4_000_000_000_000_000_000),
    }
}

#[test]
fn two_criomes_gather_a_real_bls_quorum_and_withhold_until_majority() {
    let alpha = host("mirror-alpha");
    let beta = host("mirror-beta");

    let (socket_a, store_a) = fixture("alpha");
    let (socket_b, store_b) = fixture("beta");

    let daemon_a = CriomeDaemon::new(&socket_a, store_a)
        .with_node_identity(alpha.clone())
        .with_quorum_voice(Arc::new(DirectDialQuorumVoice::new(vec![
            PeerSocketRoute::new(beta.clone(), socket_b.clone()),
        ])));
    let daemon_b = CriomeDaemon::new(&socket_b, store_b)
        .with_node_identity(beta.clone())
        .with_quorum_voice(Arc::new(DirectDialQuorumVoice::new(vec![
            PeerSocketRoute::new(alpha.clone(), socket_a.clone()),
        ])));

    serve(daemon_a);
    serve(daemon_b);
    wait_for_socket(&socket_a);
    wait_for_socket(&socket_b);

    // Mutual identity seed: each node holds the peer's identity → key.
    let key_a = node_public_key(&socket_a, alpha.clone());
    let key_b = node_public_key(&socket_b, beta.clone());
    register_peer(&socket_a, beta.clone(), key_b);
    register_peer(&socket_b, alpha.clone(), key_a);

    // Admit the 2-of-2 mirror contract on BOTH nodes.
    let contract = mirror_contract(&alpha, &beta);
    let contract_digest = admit(&socket_a, contract.clone());
    let contract_digest_b = admit(&socket_b, contract);
    assert_eq!(
        contract_digest, contract_digest_b,
        "the same content-addressed contract admits identically on both nodes"
    );

    // Propose on A. The self-vote alone is one short of the 2-of-2 majority.
    let object = mirror_object();
    // The round-id is bound to the change's fingerprint (the operation digest);
    // the originator derives it and the criome ingress enforces the binding.
    let round = QuorumRoundIdentifier::for_operation(&object.digest);
    let opened = propose(
        &socket_a,
        QuorumProposal {
            phase: RoundPhase::Request,
            round: round.clone(),
            contract: contract_digest.clone(),
            object: object.clone(),
            window: open_window(),
        },
    );
    assert_eq!(
        opened.status,
        QuorumRoundStatus::Gathering,
        "the originator's own change is WITHHELD until the peer co-signs"
    );
    assert_eq!(opened.gathered.into_u16(), 1);
    assert_eq!(opened.required.into_u16(), 2);
    assert!(opened.authorized_evidence.is_none());

    // The solicitation crosses the voice, B votes, the vote comes back, and the
    // gathered 2-of-2 quorum authorizes.
    let authorized = wait_until_authorized(&socket_a, &round);
    assert_eq!(authorized.status, QuorumRoundStatus::Authorized);
    assert_eq!(
        authorized.gathered.into_u16(),
        2,
        "both members' votes gathered"
    );
    let evidence = authorized
        .authorized_evidence
        .expect("an authorized round carries its assembled Evidence");
    assert_eq!(
        evidence.signatures().len(),
        2,
        "the Evidence carries both members' operation signatures"
    );
    assert_eq!(
        evidence.stamp.signatures().len(),
        2,
        "the shared moment carries both members' time signatures"
    );

    // The gathered Evidence is judge-valid on its own terms: the reused evaluator
    // returns Authorized (real per-signer BLS over both members).
    assert_eq!(
        evaluate(
            &socket_a,
            contract_digest.clone(),
            object.clone(),
            evidence.clone(),
        ),
        EvaluationDecision::Authorized,
        "the gathered quorum Evidence must independently re-judge as Authorized"
    );

    // Below-majority refused fail-closed: drop one member's operation signature
    // (both time signatures stay in the stamp, so the moment is still proven) and
    // the same judge returns QuorumShort.
    let short = Evidence::new(
        ComponentKind::Spirit,
        OperationDigest::new(object.digest.clone()),
        evidence.stamp.clone(),
        evidence.signatures()[..1].to_vec(),
        Vec::new(),
    );
    let decision = evaluate(&socket_a, contract_digest, object, short);
    assert!(
        matches!(
            decision,
            EvaluationDecision::Rejected(EvaluationRejectionReason::QuorumShort(_))
        ),
        "one operation signature short of the majority must be refused QuorumShort, got {decision:?}"
    );
}

#[test]
fn a_proposal_waits_when_the_peer_cannot_be_reached() {
    let alpha = host("mirror-alpha");
    let beta = host("mirror-beta");

    let (socket_a, store_a) = fixture("lonely-alpha");
    // A peer socket path that is never bound — the voice cannot reach it.
    let (dead_socket, _dead_store) = fixture("dead-beta");

    let daemon_a = CriomeDaemon::new(&socket_a, store_a)
        .with_node_identity(alpha.clone())
        .with_quorum_voice(Arc::new(DirectDialQuorumVoice::new(vec![
            PeerSocketRoute::new(beta.clone(), dead_socket),
        ])));
    serve(daemon_a);
    wait_for_socket(&socket_a);

    let contract_digest = admit(&socket_a, mirror_contract(&alpha, &beta));
    let object = mirror_object();
    let round = QuorumRoundIdentifier::for_operation(&object.digest);
    let opened = propose(
        &socket_a,
        QuorumProposal {
            phase: RoundPhase::Request,
            round: round.clone(),
            contract: contract_digest,
            object,
            window: open_window(),
        },
    );
    assert_eq!(opened.status, QuorumRoundStatus::Gathering);

    // The peer never answers; the round must stay pending, never becoming valid.
    let deadline = Instant::now() + Duration::from_millis(1500);
    while Instant::now() < deadline {
        let state = observe(&socket_a, &round);
        assert_eq!(
            state.status,
            QuorumRoundStatus::Gathering,
            "an unreachable peer must leave the round WITHHELD — never last-writer-wins"
        );
        assert!(state.authorized_evidence.is_none());
        std::thread::sleep(Duration::from_millis(150));
    }
}

#[test]
fn a_forged_member_vote_is_recorded_but_not_counted() {
    // The attack: a vote arrives claiming to be from member `beta` (a real member
    // of the admitted 2-of-2 contract, so it survives the non-member ingress drop)
    // but carries a present-but-invalid signature — a foreign key, not beta's
    // admitted key. The row is recorded (so `gathered` reflects it), yet the judge
    // (`has_valid_signature_from`) does not COUNT it, because the envelope's key is
    // not beta's admitted key. The round therefore stays WITHHELD: a below-majority
    // set of VALID signatures is refused fail-closed even when the vote count
    // reaches the threshold.
    let alpha = host("mirror-alpha");
    let beta = host("mirror-beta");

    let (socket_a, store_a) = fixture("forged-alpha");
    let (dead_socket, _dead_store) = fixture("forged-dead-beta");

    let daemon_a = CriomeDaemon::new(&socket_a, store_a)
        .with_node_identity(alpha.clone())
        .with_quorum_voice(Arc::new(DirectDialQuorumVoice::new(vec![
            PeerSocketRoute::new(beta.clone(), dead_socket),
        ])));
    serve(daemon_a);
    wait_for_socket(&socket_a);

    let contract_digest = admit(&socket_a, mirror_contract(&alpha, &beta));
    let object = mirror_object();
    let round = QuorumRoundIdentifier::for_operation(&object.digest);
    let opened = propose(
        &socket_a,
        QuorumProposal {
            phase: RoundPhase::Request,
            round: round.clone(),
            contract: contract_digest,
            object: object.clone(),
            window: open_window(),
        },
    );
    assert_eq!(opened.status, QuorumRoundStatus::Gathering);
    assert_eq!(opened.gathered.into_u16(), 1);

    // Inject the forged vote for member beta.
    let forged = QuorumVote {
        phase: RoundPhase::Request,
        round: round.clone(),
        voter: beta.clone(),
        operation_signature: forged_envelope(),
        time_signature: forged_envelope(),
    };
    let after = submit(&socket_a, forged);
    assert_eq!(
        after.gathered.into_u16(),
        2,
        "the forged member vote IS recorded — gathered reflects the row"
    );
    assert_eq!(
        after.status,
        QuorumRoundStatus::Gathering,
        "a present-but-invalid member signature is NOT counted — the round stays WITHHELD"
    );
    assert!(
        after.authorized_evidence.is_none(),
        "no Evidence is surfaced while the only valid signature is below the majority"
    );

    // And it stays withheld under an independent re-read.
    let observed = observe(&socket_a, &round);
    assert_eq!(observed.status, QuorumRoundStatus::Gathering);
    assert!(observed.authorized_evidence.is_none());
}

#[test]
fn router_voice_frames_a_criome_request_the_working_socket_reads() {
    // The router carries the routed object's octets opaquely and re-prefixes them
    // with its own length before delivering [len][octets] to the peer criome
    // working socket. So the octets must be exactly the frame body the criome
    // codec reads. Round-trip through the daemon's OWN codec to prove the peer
    // criome decodes a router-carried vote unchanged — no router source needed.
    let request = CriomeRequest::submit_quorum_vote(QuorumVote {
        phase: RoundPhase::Request,
        round: QuorumRoundIdentifier::new("framing-round-1"),
        voter: host("mirror-beta"),
        operation_signature: SignatureEnvelope {
            scheme: SignatureScheme::Bls12_381MinPk,
            public_key: BlsPublicKey::new("operation-key"),
            signature: BlsSignature::new("operation-signature"),
        },
        time_signature: SignatureEnvelope {
            scheme: SignatureScheme::Bls12_381MinPk,
            public_key: BlsPublicKey::new("time-key"),
            signature: BlsSignature::new("time-signature"),
        },
    });

    let octets = RouterQuorumVoice::request_octets(request.clone()).expect("frame the request");
    // Simulate the router's length-prefix framing (triad LengthPrefixedCodec: u32 BE).
    let mut delivered = (octets.len() as u32).to_be_bytes().to_vec();
    delivered.extend(octets);

    let mut reader = delivered.as_slice();
    let decoded = CriomeFrameCodec::default()
        .read_request(&mut reader)
        .expect("the criome working socket decodes the router-carried octets");
    assert_eq!(
        decoded, request,
        "a router-carried criome vote must decode byte-for-byte on the peer's working socket"
    );
}

fn wait_until_authorized(socket: &Path, round: &QuorumRoundIdentifier) -> QuorumRoundState {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let state = observe(socket, round);
        if state.status == QuorumRoundStatus::Authorized {
            return state;
        }
        assert!(
            Instant::now() < deadline,
            "the gathered quorum never authorized (last gathered {} of {})",
            state.gathered.into_u16(),
            state.required.into_u16()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}
