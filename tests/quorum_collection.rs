//! THE QUORUM-COLLECTION WITNESS: two independent criome daemons gather a real
//! 2-of-2 BLS quorum across the conveyance, withhold the change until a true majority
//! co-sign, and refuse a below-majority Evidence fail-closed.
//!
//! This is the security heart of the persistent Spirit mirror. Each daemon is a
//! genuinely separate party: its own store, its own `blst` master key, its own
//! `Host(...)` signing identity, cross-trusting only via keys mutually registered
//! into the peer's registry (the same shape `distinct_node_identities` proves for
//! attestation). The gather runs OVER a real Unix-socket conveyance
//! ([`DirectDialConveyance`], the single-host multi-user transport):
//!
//!   - A proposes an operation under the admitted 2-of-2 Threshold contract and
//!     casts its own BLS vote (operation signature + moment time-signature). One
//!     vote is one short of the 2-of-2 majority, so the round is WITHHELD
//!     (`Gathering`) — A's own change is not valid on its own say-so.
//!   - A solicits B across the conveyance; B independently re-validates, casts vote #2,
//!     and conveys it back. A assembles the Evidence and the EXISTING majority
//!     judge (`ContractStore::evaluate`) returns `Authorized` — 2-of-2 gathered.
//!   - That Evidence is judge-valid: fed back through `EvaluateAuthorization` it
//!     verifies as `Authorized` (real per-signer BLS, both members).
//!   - Drop one member's operation signature and the same judge returns
//!     `QuorumShort` — below majority is refused fail-closed.
//!
//! A second scenario proves the "unreachable peer ⇒ waits" rule: with the peer
//! never bound, the round stays `Gathering` and never becomes valid.

use std::io::{Read as _, Write as _};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

use criome::conveyance::{
    DirectDialConveyance, PeerActorRoute, PeerConveyance, PeerSocketRoute, RouterSubmission,
};
use criome::daemon::CriomeDaemon;
use criome::tables::StoreLocation;
use criome::transport::{CriomeClient, CriomeFrameCodec};
use signal_criome::{
    AuditContext, AuthorizationEvaluation, AuthorizedObjectKind, AuthorizedObjectReference,
    BlsPublicKey, BlsSignature, ComponentKind, ContentPurpose, ContentReference, Contract,
    ContractDigest, CriomeReply, CriomeRequest, EvaluationDecision, EvaluationRejectionReason,
    Evidence, FoundingConveyance, FoundingSignature, FoundingSignatureReturn, Identity,
    IdentityRegistration, KeyPurpose, ObjectDigest, OperationDigest, PolicyMember, PrincipalName,
    PublicKeyFingerprint, QuorumProposal, QuorumRoundIdentifier, QuorumRoundState,
    QuorumRoundStatus, QuorumVote, ReplayNonce, RequiredSignatureThreshold, RootAnchorDigest,
    RoundPhase, Rule, SignRequest, SignatureEnvelope, SignatureScheme, Threshold, TimeWindow,
    TimestampNanos,
};
use signal_frame::{NonEmpty, Reply, SubReply};
use signal_router::{
    ActorIdentifier, ForwardRefusalReason, Frame as RouterFrame, FrameBody as RouterFrameBody,
    Input as RouterInput, MessageSlot, Output as RouterOutput, RouterForwardRefusalReason,
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
        .with_peer_conveyance(Arc::new(DirectDialConveyance::new(vec![
            PeerSocketRoute::new(beta.clone(), socket_b.clone()),
        ])));
    let daemon_b = CriomeDaemon::new(&socket_b, store_b)
        .with_node_identity(beta.clone())
        .with_peer_conveyance(Arc::new(DirectDialConveyance::new(vec![
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

    // The solicitation crosses the conveyance, B votes, the vote comes back, and the
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
    // A peer socket path that is never bound — the conveyance cannot reach it.
    let (dead_socket, _dead_store) = fixture("dead-beta");

    let daemon_a = CriomeDaemon::new(&socket_a, store_a)
        .with_node_identity(alpha.clone())
        .with_peer_conveyance(Arc::new(DirectDialConveyance::new(vec![
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
        .with_peer_conveyance(Arc::new(DirectDialConveyance::new(vec![
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
fn router_submission_frames_a_criome_request_the_working_socket_reads() {
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

    let octets = RouterSubmission::request_octets(request.clone()).expect("frame the request");
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

/// A `FoundingConveyance` carrying a peer's signature return — the shape
/// `RouterSubmission::submit` conveys cross-node once a founding is under
/// way. Content is a fixture; the round-trip proof does not depend on it
/// being a live founding's real signature.
fn founding_signature_conveyance(voter: Identity) -> CriomeRequest {
    CriomeRequest::convey_founding(FoundingConveyance::Signature(FoundingSignatureReturn {
        anchor: RootAnchorDigest::new(ObjectDigest::from_bytes(b"router-origination-anchor")),
        signature: FoundingSignature {
            signer: voter,
            envelope: SignatureEnvelope {
                scheme: SignatureScheme::Bls12_381MinPk,
                public_key: BlsPublicKey::new("origination-key"),
                signature: BlsSignature::new("origination-signature"),
            },
        },
    }))
}

/// A minimal stand-in for `router::apply_routed_object_submission`'s wire
/// surface — the router's real working-socket handler
/// (`router::daemon::RouterEngine::handle_working_connection`) without any
/// router runtime. It decodes exactly what `RouterClient::send` writes,
/// reports the routed criome octets decoded back into a `CriomeRequest` (the
/// round-trip proof) over `sender`, and replies with `outcome` — `Ok` mirrors
/// `RoutedObjectsAccepted`, `Err` mirrors `RoutedObjectsRefused`.
fn serve_stub_router(
    socket: PathBuf,
    outcome: std::result::Result<(), RouterForwardRefusalReason>,
) -> Receiver<CriomeRequest> {
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let listener = UnixListener::bind(&socket).expect("bind stub router working socket");
        let (mut stream, _) = listener.accept().expect("accept the criome connection");

        let mut prefix = [0_u8; 4];
        stream.read_exact(&mut prefix).expect("read length prefix");
        let length = u32::from_be_bytes(prefix) as usize;
        let mut framed = prefix.to_vec();
        framed.resize(4 + length, 0);
        stream
            .read_exact(&mut framed[4..])
            .expect("read frame body");

        let (exchange, submission) = match RouterFrame::decode_length_prefixed(&framed)
            .expect("decode the SubmitRoutedObjects working-socket frame")
            .into_body()
        {
            RouterFrameBody::Request { exchange, request } => match request.payloads.into_head() {
                RouterInput::SubmitRoutedObjects(submission) => (exchange, submission),
                other => panic!("expected SubmitRoutedObjects, got {other:?}"),
            },
            other => panic!("expected a working-socket Request frame, got {other:?}"),
        };

        let object = submission
            .routed_objects()
            .first()
            .expect("submission carries the one criome routed object")
            .clone();
        assert_eq!(object.contract_name.payload(), "signal-criome");

        let mut body: Vec<u8> = object
            .payload_octets()
            .iter()
            .map(|byte| *byte as u8)
            .collect();
        let mut delivered = (body.len() as u32).to_be_bytes().to_vec();
        delivered.append(&mut body);
        let mut reader = delivered.as_slice();
        let decoded = CriomeFrameCodec::default()
            .read_request(&mut reader)
            .expect("the router-carried octets decode as a criome working-socket frame");
        sender.send(decoded).expect("report the decoded request");

        let output = match outcome {
            Ok(()) => RouterOutput::routed_objects_accepted(MessageSlot::new(1)),
            Err(reason) => RouterOutput::routed_objects_refused(ForwardRefusalReason::new(reason)),
        };
        let reply = RouterFrame::new(RouterFrameBody::Reply {
            exchange,
            reply: Reply::committed(NonEmpty::single(SubReply::Ok(output))),
        });
        let bytes = reply
            .encode_length_prefixed()
            .expect("encode the router reply frame");
        stream.write_all(&bytes).expect("write the router reply");
        stream.flush().expect("flush the router reply");
    });
    receiver
}

/// Like [`serve_stub_router`], but serves every connection it receives — not
/// just one — always refusing with `reason` and forwarding each decoded
/// request onto the returned channel before replying. Used to prove how many
/// steps of an ordered conveyance actually reached the wire (M2,
/// primary-79z1.22): `convey_ordered` must stop firing the rest of an
/// ordering-dependent sequence once an earlier step is refused, so a correct
/// fix produces exactly one decoded request here, never the whole sequence.
fn serve_stub_router_always_refusing(
    socket: PathBuf,
    reason: RouterForwardRefusalReason,
) -> Receiver<CriomeRequest> {
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let listener = UnixListener::bind(&socket).expect("bind stub router working socket");
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else {
                break;
            };

            let mut prefix = [0_u8; 4];
            if stream.read_exact(&mut prefix).is_err() {
                continue;
            }
            let length = u32::from_be_bytes(prefix) as usize;
            let mut framed = prefix.to_vec();
            framed.resize(4 + length, 0);
            if stream.read_exact(&mut framed[4..]).is_err() {
                continue;
            }

            let (exchange, submission) = match RouterFrame::decode_length_prefixed(&framed)
                .expect("decode the SubmitRoutedObjects working-socket frame")
                .into_body()
            {
                RouterFrameBody::Request { exchange, request } => {
                    match request.payloads.into_head() {
                        RouterInput::SubmitRoutedObjects(submission) => (exchange, submission),
                        other => panic!("expected SubmitRoutedObjects, got {other:?}"),
                    }
                }
                other => panic!("expected a working-socket Request frame, got {other:?}"),
            };

            let object = submission
                .routed_objects()
                .first()
                .expect("submission carries the one criome routed object")
                .clone();
            let mut body: Vec<u8> = object
                .payload_octets()
                .iter()
                .map(|byte| *byte as u8)
                .collect();
            let mut delivered = (body.len() as u32).to_be_bytes().to_vec();
            delivered.append(&mut body);
            let mut reader = delivered.as_slice();
            let decoded = CriomeFrameCodec::default()
                .read_request(&mut reader)
                .expect("the router-carried octets decode as a criome working-socket frame");
            let _ = sender.send(decoded);

            let output = RouterOutput::routed_objects_refused(ForwardRefusalReason::new(reason));
            let reply = RouterFrame::new(RouterFrameBody::Reply {
                exchange,
                reply: Reply::committed(NonEmpty::single(SubReply::Ok(output))),
            });
            let bytes = reply
                .encode_length_prefixed()
                .expect("encode the router reply frame");
            let _ = stream.write_all(&bytes);
            let _ = stream.flush();
        }
    });
    receiver
}

#[test]
fn router_submission_submits_through_the_router_working_socket_and_is_accepted() {
    // The full origination path: `RouterSubmission::submit` frames the
    // request, wraps it as a `RoutedContractObject`, hands it to a
    // `SubmitRoutedObjects` origination, and dials the local router's working
    // socket over the new `RouterClient`. The stub router proves the carried
    // octets decode to the SAME criome request on the far side, and the
    // `RoutedObjectsAccepted` reply maps to `Ok(())`.
    let peer = host("router-peer");
    let source_actor = ActorIdentifier::new("criome-alpha");
    let destination_actor = ActorIdentifier::new("criome-beta");
    let (router_socket, _store) = fixture("router-stub-accept");

    let received = serve_stub_router(router_socket.clone(), Ok(()));
    wait_for_socket(&router_socket);

    let conveyance = RouterSubmission::new(
        router_socket,
        source_actor,
        vec![PeerActorRoute::new(peer.clone(), destination_actor.clone())],
    );

    let request = founding_signature_conveyance(peer);
    conveyance
        .submit(destination_actor, request.clone())
        .expect("the stub router accepts the origination");

    let decoded = received
        .recv_timeout(Duration::from_secs(5))
        .expect("the stub router decoded the routed criome request");
    assert_eq!(
        decoded, request,
        "the octets the router carries must decode to the SAME criome request byte-for-byte"
    );
}

#[test]
fn router_submission_maps_a_router_refusal_to_a_delivery_error() {
    // Same origination path, but the stub router refuses. The refusal must
    // surface as an `Err`, not a swallowed success, and must name the reason.
    let peer = host("router-peer");
    let source_actor = ActorIdentifier::new("criome-alpha");
    let destination_actor = ActorIdentifier::new("criome-beta");
    let (router_socket, _store) = fixture("router-stub-refuse");

    let received = serve_stub_router(
        router_socket.clone(),
        Err(RouterForwardRefusalReason::MirrorDisabled),
    );
    wait_for_socket(&router_socket);

    let conveyance = RouterSubmission::new(
        router_socket,
        source_actor,
        vec![PeerActorRoute::new(peer.clone(), destination_actor.clone())],
    );

    let request = founding_signature_conveyance(peer);
    let error = conveyance
        .submit(destination_actor, request.clone())
        .expect_err("a router refusal must surface as a delivery error, not a silent success");
    assert!(
        format!("{error}").contains("MirrorDisabled"),
        "the refusal reason must be visible in the mapped error: {error}"
    );

    let decoded = received
        .recv_timeout(Duration::from_secs(5))
        .expect("the stub router still decoded the routed criome request before refusing");
    assert_eq!(decoded, request);
}

#[test]
fn router_submission_convey_still_attempts_delivery_and_does_not_panic_on_refusal() {
    // M2 (primary-79z1.22): `convey` stays fire-and-forget by trait contract —
    // an unreachable peer must still leave the round `Gathering` rather than
    // aborting it — but a router refusal on the founding path must no longer
    // vanish with `let _ = self.submit(...)`. This proves going through the
    // `PeerConveyance` trait object still lands the delivery attempt on the wire
    // (no regression to the fire-and-forget behavior) and does not panic now
    // that the refusal is loud-logged instead of silently discarded.
    let peer = host("router-peer");
    let source_actor = ActorIdentifier::new("criome-alpha");
    let destination_actor = ActorIdentifier::new("criome-beta");
    let (router_socket, _store) = fixture("router-stub-convey-refuse");

    let received = serve_stub_router(
        router_socket.clone(),
        Err(RouterForwardRefusalReason::MirrorDisabled),
    );
    wait_for_socket(&router_socket);

    let conveyance: Arc<dyn PeerConveyance> = Arc::new(RouterSubmission::new(
        router_socket,
        source_actor,
        vec![PeerActorRoute::new(peer.clone(), destination_actor)],
    ));

    let request = founding_signature_conveyance(peer.clone());
    conveyance.convey(&peer, request.clone());

    let decoded = received
        .recv_timeout(Duration::from_secs(5))
        .expect("convey still attempts delivery even though the router refuses it");
    assert_eq!(decoded, request);
}

#[test]
fn router_submission_convey_ordered_stops_after_the_first_refusal() {
    // The two-round commit driver relies on the ordering: sending a LATER
    // step (e.g. the commit solicitation) after an EARLIER step (the round-1
    // evidence) failed to deliver would race a peer that never received the
    // evidence the later step depends on. `convey_ordered` must stop at the
    // first refusal rather than keep firing the rest of the sequence
    // regardless — the "propagate it where the signature allows" half of the
    // M2 fix (primary-79z1.22): the `()` trait signature carries nothing back
    // to the caller, but the failure still governs the REST of this call.
    let peer = host("router-peer");
    let source_actor = ActorIdentifier::new("criome-alpha");
    let destination_actor = ActorIdentifier::new("criome-beta");
    let (router_socket, _store) = fixture("router-stub-convey-ordered-refuse");

    let received = serve_stub_router_always_refusing(
        router_socket.clone(),
        RouterForwardRefusalReason::MirrorDisabled,
    );
    wait_for_socket(&router_socket);

    let conveyance: Arc<dyn PeerConveyance> = Arc::new(RouterSubmission::new(
        router_socket,
        source_actor,
        vec![PeerActorRoute::new(peer.clone(), destination_actor)],
    ));

    let sequence = vec![
        founding_signature_conveyance(peer.clone()),
        founding_signature_conveyance(peer.clone()),
        founding_signature_conveyance(peer.clone()),
    ];
    conveyance.convey_ordered(&peer, sequence);

    // Exactly one step reaches the stub router: the first refusal must stop
    // the rest of the ordering-dependent sequence from being sent at all.
    received
        .recv_timeout(Duration::from_secs(5))
        .expect("the first step of the sequence is still attempted");
    assert!(
        received.recv_timeout(Duration::from_millis(300)).is_err(),
        "no further steps should be sent once an earlier one is refused"
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
