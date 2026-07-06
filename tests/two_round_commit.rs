//! THE TWO-ROUND COMMIT WITNESS: real approval lands only on round 2. A round-1
//! (Request) majority is not authorization — it merely opens the commit round; a
//! round-2 (Commit) signer independently re-judges a REAL round-1 majority within
//! the window before it co-signs, and only the round-2 majority publishes the
//! authorized-object update. Non-equivocation rides alongside: an honest node
//! co-signs at most one successor per (Criome, head), refusing a conflicting
//! second successor with the typed `QuorumConflict` reply.
//!
//! These witnesses pin each daemon's clock ([`SystemClock::pinned`]) so BOTH
//! rounds are exercised against a fixed instant, never the wall clock:
//!
//!   - `found_then_two_round_commit_authorizes_on_round_two`: the happy path at
//!     2-of-2 (both rounds unanimous) — propose → round-1 majority → round-2
//!     majority → Authorized on the COMMIT round.
//!   - `a_commit_is_refused_when_round_one_is_short` /
//!     `..._is_forged`: a round-2 signer refuses to commit unless a real round-1
//!     majority is Authorized locally — a short or forged round-1 (rows present,
//!     but not a counted majority) is refused.
//!   - `a_conflicting_second_successor_is_refused_with_quorum_conflict`: a second,
//!     different successor from the same (Criome, head) is refused the loser's
//!     "refused, resubmit" `QuorumConflict`.
//!   - `both_rounds_are_window_gated_an_out_of_window_peer_commits_neither`: a peer
//!     whose clock is outside the shared window co-signs NEITHER round, so the
//!     commit round never authorizes — both rounds fall in the window or nothing
//!     commits.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use criome::conveyance::{DirectDialConveyance, PeerSocketRoute};
use criome::daemon::CriomeDaemon;
use criome::master_key::SystemClock;
use criome::tables::StoreLocation;
use criome::transport::CriomeClient;
use signal_criome::{
    AttestedMomentProposition, AuditContext, AuthorizedObjectKind, AuthorizedObjectReference,
    BlsPublicKey, BlsSignature, ComponentKind, ContentPurpose, ContentReference, Contract,
    ContractDigest, CriomeReply, CriomeRequest, Identity, IdentityRegistration, KeyPurpose,
    ObjectDigest, PolicyMember, PrincipalName, PublicKeyFingerprint, QuorumProposal,
    QuorumRoundIdentifier, QuorumRoundState, QuorumRoundStatus, QuorumVote, QuorumVoteSolicitation,
    RejectionReason, ReplayNonce, RequiredSignatureThreshold, RoundPhase, Rule, SignRequest,
    SignatureEnvelope, SignatureScheme, Threshold, TimeWindow, TimestampNanos,
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
        "criome-tworound-{tag}-{}-{}",
        std::process::id(),
        nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create two-round fixture dir");
    (
        dir.join("criome.sock"),
        StoreLocation::new(dir.join("criome.sema")),
    )
}

fn host(name: &str) -> Identity {
    Identity::host(name.to_string())
}

fn pinned_clock(instant: u64) -> SystemClock {
    SystemClock::pinned(TimestampNanos::new(instant))
}

fn window(opens_at: u64, closes_at: u64) -> TimeWindow {
    TimeWindow {
        opens_at: TimestampNanos::new(opens_at),
        closes_at: TimestampNanos::new(closes_at),
    }
}

/// A window wide enough that a pinned clock at 1_500 or 1_800 sits inside it.
fn shared_window() -> TimeWindow {
    window(1_000, 2_000)
}

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

fn node_public_key(socket: &Path, identity: Identity) -> BlsPublicKey {
    let request = SignRequest::new(
        ContentReference {
            digest: ObjectDigest::from_bytes(b"two-round-key-probe"),
            purpose: ContentPurpose::SignedObject,
            schema_version: PrincipalName::new("two-round-probe"),
        },
        identity,
        AuditContext {
            purpose: ContentPurpose::SignedObject,
            audience: PrincipalName::new("two-round-probe-audience"),
            policy_version: PrincipalName::new("two-round-probe-policy"),
            nonce: ReplayNonce::new("two-round-probe-nonce"),
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

fn mirror_contract(alpha: &Identity, beta: &Identity) -> Contract {
    Contract::root(Rule::threshold(Threshold::new(
        RequiredSignatureThreshold::new(2),
        vec![
            PolicyMember::key_member(alpha.clone()),
            PolicyMember::key_member(beta.clone()),
        ],
    )))
}

/// A successor object (a head advance) fingerprinted by `tag`, so distinct tags
/// are distinct successors from the same head.
fn successor(tag: &[u8]) -> AuthorizedObjectReference {
    AuthorizedObjectReference {
        component: ComponentKind::Spirit,
        digest: ObjectDigest::from_bytes(tag),
        kind: AuthorizedObjectKind::Head,
    }
}

fn two_of_two_moment(
    alpha: &Identity,
    beta: &Identity,
    window: TimeWindow,
) -> AttestedMomentProposition {
    AttestedMomentProposition::new(
        window,
        RequiredSignatureThreshold::new(2),
        vec![alpha.clone(), beta.clone()],
    )
}

fn forged_envelope() -> SignatureEnvelope {
    SignatureEnvelope {
        scheme: SignatureScheme::Bls12_381MinPk,
        public_key: BlsPublicKey::new("forged-foreign-key"),
        signature: BlsSignature::new("forged-signature"),
    }
}

fn propose_request(
    socket: &Path,
    contract: ContractDigest,
    object: AuthorizedObjectReference,
    window: TimeWindow,
) -> CriomeReply {
    let round = QuorumRoundIdentifier::for_phase(&object.digest, RoundPhase::Request);
    ask(
        socket,
        CriomeRequest::ProposeQuorumAuthorization(QuorumProposal {
            round,
            phase: RoundPhase::Request,
            contract,
            object,
            window,
        }),
    )
}

fn try_observe(socket: &Path, round: &QuorumRoundIdentifier) -> Option<QuorumRoundState> {
    match ask(socket, CriomeRequest::observe_quorum_round(round.clone())) {
        CriomeReply::QuorumRoundObserved(state) => Some(state),
        CriomeReply::Rejection(_) => None,
        other => panic!("expected QuorumRoundObserved or Rejection, got {other:?}"),
    }
}

fn wait_until_authorized(socket: &Path, round: &QuorumRoundIdentifier) -> QuorumRoundState {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(state) = try_observe(socket, round)
            && state.status == QuorumRoundStatus::Authorized
        {
            return state;
        }
        assert!(
            Instant::now() < deadline,
            "round {:?} never authorized",
            round.as_str()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn found_then_two_round_commit_authorizes_on_round_two() {
    // The happy path at 2-of-2, both rounds unanimous: A proposes, round 1 gathers
    // both members (Request), which OPENS but does not authorize; the initiator
    // then drives round 2 (Commit), each signer independently re-judges the round-1
    // majority within the window, and the round-2 majority is the real approval.
    let alpha = host("tworound-alpha");
    let beta = host("tworound-beta");
    let (socket_a, store_a) = fixture("commit-alpha");
    let (socket_b, store_b) = fixture("commit-beta");

    // Both clocks sit inside the shared window, so both rounds are in-window.
    let daemon_a = CriomeDaemon::new(&socket_a, store_a)
        .with_node_identity(alpha.clone())
        .with_clock(pinned_clock(1_500))
        .with_peer_conveyance(Arc::new(DirectDialConveyance::new(vec![
            PeerSocketRoute::new(beta.clone(), socket_b.clone()),
        ])));
    let daemon_b = CriomeDaemon::new(&socket_b, store_b)
        .with_node_identity(beta.clone())
        .with_clock(pinned_clock(1_800))
        .with_peer_conveyance(Arc::new(DirectDialConveyance::new(vec![
            PeerSocketRoute::new(alpha.clone(), socket_a.clone()),
        ])));

    serve(daemon_a);
    serve(daemon_b);
    wait_for_socket(&socket_a);
    wait_for_socket(&socket_b);

    let key_a = node_public_key(&socket_a, alpha.clone());
    let key_b = node_public_key(&socket_b, beta.clone());
    register_peer(&socket_a, beta.clone(), key_b);
    register_peer(&socket_b, alpha.clone(), key_a);

    let contract = admit(&socket_a, mirror_contract(&alpha, &beta));
    let _ = admit(&socket_b, mirror_contract(&alpha, &beta));

    let object = successor(b"two-round-head-advance");
    let request_round = QuorumRoundIdentifier::for_phase(&object.digest, RoundPhase::Request);
    let commit_round = QuorumRoundIdentifier::for_phase(&object.digest, RoundPhase::Commit);

    match propose_request(&socket_a, contract, object, shared_window()) {
        CriomeReply::QuorumRoundOpened(state) => {
            assert_eq!(state.phase, RoundPhase::Request);
            assert_eq!(
                state.status,
                QuorumRoundStatus::Gathering,
                "the lone self-vote is one short of the 2-of-2 round-1 majority"
            );
        }
        other => panic!("propose must open the Request round, got {other:?}"),
    }

    // Round 1 (Request) reaches a majority — but this is NOT approval; it only
    // opens the commit round.
    let request_authorized = wait_until_authorized(&socket_a, &request_round);
    assert_eq!(request_authorized.phase, RoundPhase::Request);
    assert_eq!(request_authorized.gathered.into_u16(), 2);

    // Round 2 (Commit) reaches a majority — THIS is the real approval. Each signer
    // verified the round-1 majority within the window before co-signing.
    let commit_authorized = wait_until_authorized(&socket_a, &commit_round);
    assert_eq!(
        commit_authorized.phase,
        RoundPhase::Commit,
        "real approval lands on the COMMIT round"
    );
    assert_eq!(
        commit_authorized.gathered.into_u16(),
        2,
        "both members co-signed the commit after verifying the round-1 majority"
    );
    assert!(
        commit_authorized.authorized_evidence.is_some(),
        "the authorized commit round carries its assembled Evidence"
    );
}

#[test]
fn a_commit_is_refused_when_round_one_is_short() {
    // A round-2 signer must verify a REAL round-1 majority before committing. Here
    // node A holds only its own round-1 vote (one short of the 2-of-2 majority), so
    // a commit solicitation for the same object is refused — a short round-1 is not
    // a verified majority.
    let alpha = host("tworound-alpha");
    let beta = host("tworound-beta");
    let (socket_a, store_a) = fixture("short-alpha");

    let daemon_a = CriomeDaemon::new(&socket_a, store_a)
        .with_node_identity(alpha.clone())
        .with_clock(pinned_clock(1_500));
    serve(daemon_a);
    wait_for_socket(&socket_a);

    let contract = admit(&socket_a, mirror_contract(&alpha, &beta));
    let object = successor(b"short-round-one");

    // Open a round-1 that never gathers the peer — it stays one short of majority.
    match propose_request(&socket_a, contract.clone(), object.clone(), shared_window()) {
        CriomeReply::QuorumRoundOpened(state) => {
            assert_eq!(state.status, QuorumRoundStatus::Gathering);
            assert_eq!(state.gathered.into_u16(), 1);
        }
        other => panic!("propose must open the Request round, got {other:?}"),
    }

    // A commit solicitation for that same object must be refused: round 1 is short.
    let commit_round = QuorumRoundIdentifier::for_phase(&object.digest, RoundPhase::Commit);
    let reply = ask(
        &socket_a,
        CriomeRequest::solicit_quorum_vote(QuorumVoteSolicitation {
            round: commit_round,
            phase: RoundPhase::Commit,
            contract,
            object,
            proposition: two_of_two_moment(&alpha, &beta, shared_window()),
            originator: beta.clone(),
        }),
    );
    match reply {
        CriomeReply::Rejection(rejection) => assert_eq!(
            rejection.payload(),
            &RejectionReason::MalformedRequest,
            "a commit without a verified round-1 majority must be refused"
        ),
        other => panic!("a short round-1 must refuse the commit, got {other:?}"),
    }
}

#[test]
fn a_commit_is_refused_when_round_one_is_forged() {
    // The forged-round-1 attack: a forged B vote arrives for round 1. The vote
    // ingress verification gate refuses it outright (it would otherwise occupy
    // B's slot, where the one-vote-per-member replacement rule could clobber a
    // valid vote), so round 1 holds only A's real vote — no majority. A commit
    // solicitation is therefore refused: a round-2 signer re-runs the REUSED
    // judge over what actually verified, never a row count.
    let alpha = host("tworound-alpha");
    let beta = host("tworound-beta");
    let (socket_a, store_a) = fixture("forged-alpha");

    let daemon_a = CriomeDaemon::new(&socket_a, store_a)
        .with_node_identity(alpha.clone())
        .with_clock(pinned_clock(1_500));
    serve(daemon_a);
    wait_for_socket(&socket_a);

    let contract = admit(&socket_a, mirror_contract(&alpha, &beta));
    let object = successor(b"forged-round-one");
    let request_round = QuorumRoundIdentifier::for_phase(&object.digest, RoundPhase::Request);

    match propose_request(&socket_a, contract.clone(), object.clone(), shared_window()) {
        CriomeReply::QuorumRoundOpened(state) => assert_eq!(state.gathered.into_u16(), 1),
        other => panic!("propose must open the Request round, got {other:?}"),
    }

    // Inject a forged member vote into round 1 — refused at ingress, never
    // recorded.
    let forged = QuorumVote {
        round: request_round.clone(),
        phase: RoundPhase::Request,
        voter: beta.clone(),
        operation_signature: forged_envelope(),
        time_signature: forged_envelope(),
    };
    let refused = ask(&socket_a, CriomeRequest::submit_quorum_vote(forged));
    assert!(
        matches!(refused, CriomeReply::Rejection(_)),
        "an unverifiable member vote is refused at ingress, got {refused:?}"
    );
    match try_observe(&socket_a, &request_round) {
        Some(state) => {
            assert_eq!(state.gathered.into_u16(), 1, "only the real self-vote stands");
            assert_eq!(state.status, QuorumRoundStatus::Gathering);
        }
        None => panic!("the proposed round exists"),
    }

    let commit_round = QuorumRoundIdentifier::for_phase(&object.digest, RoundPhase::Commit);
    let reply = ask(
        &socket_a,
        CriomeRequest::solicit_quorum_vote(QuorumVoteSolicitation {
            round: commit_round,
            phase: RoundPhase::Commit,
            contract,
            object,
            proposition: two_of_two_moment(&alpha, &beta, shared_window()),
            originator: beta.clone(),
        }),
    );
    match reply {
        CriomeReply::Rejection(rejection) => assert_eq!(
            rejection.payload(),
            &RejectionReason::MalformedRequest,
            "a forged (uncounted) round-1 must refuse the commit"
        ),
        other => panic!("a forged round-1 must refuse the commit, got {other:?}"),
    }
}

#[test]
fn a_conflicting_second_successor_is_refused_with_quorum_conflict() {
    // Non-equivocation: an honest node co-signs at most one successor per (Criome,
    // head). Having co-signed successor S1 from the contract's genesis head, node A
    // refuses a different successor S2 from the SAME head with the typed
    // QuorumConflict "refused, resubmit" reply naming the already-co-signed S1.
    let alpha = host("tworound-alpha");
    let beta = host("tworound-beta");
    let (socket_a, store_a) = fixture("conflict-alpha");

    let daemon_a = CriomeDaemon::new(&socket_a, store_a)
        .with_node_identity(alpha.clone())
        .with_clock(pinned_clock(1_500));
    serve(daemon_a);
    wait_for_socket(&socket_a);

    let contract = admit(&socket_a, mirror_contract(&alpha, &beta));
    let successor_one = successor(b"successor-one");
    let successor_two = successor(b"successor-two");

    // Co-sign S1 (round 1 opens, one short of the 2-of-2 majority — it never
    // commits, so the head stays at genesis).
    match propose_request(
        &socket_a,
        contract.clone(),
        successor_one.clone(),
        shared_window(),
    ) {
        CriomeReply::QuorumRoundOpened(state) => {
            assert_eq!(state.status, QuorumRoundStatus::Gathering)
        }
        other => panic!("the first successor must open its round, got {other:?}"),
    }

    // A different successor from the same head is refused.
    let reply = propose_request(&socket_a, contract.clone(), successor_two, shared_window());
    match reply {
        CriomeReply::QuorumConflict(conflict) => {
            assert_eq!(
                conflict.contract, contract,
                "the conflict names the contract it protects"
            );
            assert_eq!(
                conflict.existing_successor.digest, successor_one.digest,
                "the loser is told which successor already holds the state-point"
            );
        }
        other => panic!(
            "a conflicting second successor must be refused with QuorumConflict, got {other:?}"
        ),
    }
}

#[test]
fn both_rounds_are_window_gated_an_out_of_window_peer_commits_neither() {
    // Both rounds must fall in the window. Node A's clock is inside the shared
    // window; peer B's clock is OUTSIDE it. B refuses to co-sign round 1 (its
    // witness-clock gate), so round 1 never reaches a majority, the commit round is
    // never driven, and no round authorizes — the window gates the whole two-round
    // flow, not merely round 1.
    let alpha = host("tworound-alpha");
    let beta = host("tworound-beta");
    let (socket_a, store_a) = fixture("window-alpha");
    let (socket_b, store_b) = fixture("window-beta");

    let daemon_a = CriomeDaemon::new(&socket_a, store_a)
        .with_node_identity(alpha.clone())
        .with_clock(pinned_clock(1_500))
        .with_peer_conveyance(Arc::new(DirectDialConveyance::new(vec![
            PeerSocketRoute::new(beta.clone(), socket_b.clone()),
        ])));
    let daemon_b = CriomeDaemon::new(&socket_b, store_b)
        .with_node_identity(beta.clone())
        .with_clock(pinned_clock(9_000))
        .with_peer_conveyance(Arc::new(DirectDialConveyance::new(vec![
            PeerSocketRoute::new(alpha.clone(), socket_a.clone()),
        ])));

    serve(daemon_a);
    serve(daemon_b);
    wait_for_socket(&socket_a);
    wait_for_socket(&socket_b);

    let key_a = node_public_key(&socket_a, alpha.clone());
    let key_b = node_public_key(&socket_b, beta.clone());
    register_peer(&socket_a, beta.clone(), key_b);
    register_peer(&socket_b, alpha.clone(), key_a);

    let contract = admit(&socket_a, mirror_contract(&alpha, &beta));
    let _ = admit(&socket_b, mirror_contract(&alpha, &beta));

    let object = successor(b"window-gated-advance");
    let request_round = QuorumRoundIdentifier::for_phase(&object.digest, RoundPhase::Request);
    let commit_round = QuorumRoundIdentifier::for_phase(&object.digest, RoundPhase::Commit);

    match propose_request(&socket_a, contract, object, shared_window()) {
        CriomeReply::QuorumRoundOpened(state) => assert_eq!(state.gathered.into_u16(), 1),
        other => panic!("propose must open the Request round, got {other:?}"),
    }

    // B is outside the window: neither round ever authorizes.
    let deadline = Instant::now() + Duration::from_millis(1_500);
    while Instant::now() < deadline {
        if let Some(state) = try_observe(&socket_a, &request_round) {
            assert_eq!(
                state.status,
                QuorumRoundStatus::Gathering,
                "an out-of-window peer leaves round 1 short"
            );
        }
        assert!(
            try_observe(&socket_a, &commit_round)
                .map(|state| state.status != QuorumRoundStatus::Authorized)
                .unwrap_or(true),
            "the commit round must never authorize when round 1 cannot gather a majority"
        );
        std::thread::sleep(Duration::from_millis(150));
    }
}

#[test]
fn two_committed_successors_converge_both_heads() {
    // HEAD-CONVERGENCE WITNESS (F1): a cluster must authorize more than one change
    // per contract. After successor S1 commits, BOTH nodes advance to the SAME head
    // (S1), so a second successor S2 — proposed from that head — commits too. If the
    // peer's head stayed stale at genesis (the wedge this fixes: the initiator cast
    // its commit vote locally and never conveyed it, so the peer's commit round
    // never reached a majority and never advanced), S2 would be refused as a
    // `QuorumConflict` from genesis and would never commit. The proof is that S1 AND
    // S2 each reach a 2-of-2 commit majority on BOTH nodes.
    let alpha = host("tworound-alpha");
    let beta = host("tworound-beta");
    let (socket_a, store_a) = fixture("converge-alpha");
    let (socket_b, store_b) = fixture("converge-beta");

    let daemon_a = CriomeDaemon::new(&socket_a, store_a)
        .with_node_identity(alpha.clone())
        .with_clock(pinned_clock(1_500))
        .with_peer_conveyance(Arc::new(DirectDialConveyance::new(vec![
            PeerSocketRoute::new(beta.clone(), socket_b.clone()),
        ])));
    let daemon_b = CriomeDaemon::new(&socket_b, store_b)
        .with_node_identity(beta.clone())
        .with_clock(pinned_clock(1_800))
        .with_peer_conveyance(Arc::new(DirectDialConveyance::new(vec![
            PeerSocketRoute::new(alpha.clone(), socket_a.clone()),
        ])));

    serve(daemon_a);
    serve(daemon_b);
    wait_for_socket(&socket_a);
    wait_for_socket(&socket_b);

    let key_a = node_public_key(&socket_a, alpha.clone());
    let key_b = node_public_key(&socket_b, beta.clone());
    register_peer(&socket_a, beta.clone(), key_b);
    register_peer(&socket_b, alpha.clone(), key_a);

    let contract = admit(&socket_a, mirror_contract(&alpha, &beta));
    let _ = admit(&socket_b, mirror_contract(&alpha, &beta));

    // S1: a successor from genesis. Commit it, and confirm BOTH commit rounds reach
    // the 2-of-2 majority — the peer's commit round authorizing is the proof it
    // received the initiator's commit vote and advanced its head to S1.
    let s1 = successor(b"successor-one-converge");
    let s1_commit = QuorumRoundIdentifier::for_phase(&s1.digest, RoundPhase::Commit);
    match propose_request(&socket_a, contract.clone(), s1.clone(), shared_window()) {
        CriomeReply::QuorumRoundOpened(_) => {}
        other => panic!("S1 propose must open the Request round, got {other:?}"),
    }
    assert_eq!(
        wait_until_authorized(&socket_a, &s1_commit)
            .gathered
            .into_u16(),
        2,
        "the initiator committed S1"
    );
    assert_eq!(
        wait_until_authorized(&socket_b, &s1_commit)
            .gathered
            .into_u16(),
        2,
        "the peer's commit round reached the majority — it advanced its head to S1"
    );

    // S2: a second successor, proposed AFTER S1 committed, from the S1 head both
    // nodes now hold. It commits only if BOTH advanced to S1; a stale peer head
    // would refuse it as a conflict from genesis and it would never authorize.
    let s2 = successor(b"successor-two-converge");
    let s2_commit = QuorumRoundIdentifier::for_phase(&s2.digest, RoundPhase::Commit);
    match propose_request(&socket_a, contract.clone(), s2.clone(), shared_window()) {
        CriomeReply::QuorumRoundOpened(_) => {}
        CriomeReply::QuorumConflict(conflict) => panic!(
            "S2 must not conflict after S1 committed, but the initiator refused it \
             naming already-co-signed {:?}",
            conflict.existing_successor.digest
        ),
        other => panic!("S2 propose must open the Request round, got {other:?}"),
    }
    assert_eq!(
        wait_until_authorized(&socket_a, &s2_commit)
            .gathered
            .into_u16(),
        2,
        "the initiator committed the SECOND successor from the new head"
    );
    assert_eq!(
        wait_until_authorized(&socket_b, &s2_commit)
            .gathered
            .into_u16(),
        2,
        "both nodes committed S2 from the shared S1 head — repeated deploys converge"
    );
}
