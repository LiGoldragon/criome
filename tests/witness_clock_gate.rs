//! THE WITNESS-CLOCK GATE: each quorum signer consults its OWN clock and emits
//! its time-signature only when the present is inside the request's window, so a
//! signature testifies "now is inside this window" — not merely "a quorum
//! co-signed this window value."
//!
//! The load-bearing property is anti-forgery of "now": a proposer cannot
//! manufacture the present by choosing a convenient window, because every honest
//! signer refuses a window its own clock is not inside. These witnesses pin each
//! daemon's clock to a fixed instant ([`SystemClock::pinned`]) so the gate is
//! exercised deterministically, never against the real wall clock.
//!
//!   - `a_signer_refuses_the_convenient_window_its_clock_is_outside`: the
//!     originator's OWN self-vote is refused when its pinned clock is not inside
//!     the proposed window (the "proposer picked a convenient future window"
//!     attack — it cannot even sign its own convenient window).
//!   - `a_signer_signs_the_window_its_clock_is_inside`: the same node, proposing a
//!     window that DOES contain its pinned clock, signs — the round opens.
//!   - `a_peer_refuses_the_window_its_own_clock_is_outside`: a solicited peer
//!     whose clock is outside the window refuses to co-sign even though the
//!     originator's clock was inside it (the peer re-check — each honest node
//!     gates on its OWN clock), so the round never reaches a majority.
//!   - `two_peers_whose_clocks_are_inside_the_window_co_sign_to_authorized`: the
//!     control — with both clocks inside, the identical setup DOES gather the
//!     2-of-2 majority, so the stall above is caused by the clock gate, not by an
//!     unreachable peer.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use criome::daemon::CriomeDaemon;
use criome::master_key::SystemClock;
use criome::tables::StoreLocation;
use criome::transport::CriomeClient;
use criome::voice::{DirectDialQuorumVoice, PeerSocketRoute};
use signal_criome::{
    AuditContext, AuthorizedObjectKind, AuthorizedObjectReference, BlsPublicKey, ComponentKind,
    ContentPurpose, ContentReference, Contract, ContractDigest, CriomeReply, CriomeRequest,
    Identity, IdentityRegistration, KeyPurpose, ObjectDigest, PolicyMember, PrincipalName,
    PublicKeyFingerprint, QuorumProposal, QuorumRoundIdentifier, QuorumRoundState,
    QuorumRoundStatus, RejectionReason, ReplayNonce, RequiredSignatureThreshold, RoundPhase, Rule,
    SignRequest, Threshold, TimeWindow, TimestampNanos,
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
        "criome-clock-{tag}-{}-{}",
        std::process::id(),
        nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create clock-gate fixture dir");
    (
        dir.join("criome.sock"),
        StoreLocation::new(dir.join("criome.sema")),
    )
}

fn host(name: &str) -> Identity {
    Identity::host(name.to_string())
}

/// A clock pinned to a fixed instant, so the witness-clock gate is deterministic:
/// its verdict on a window is a pure function of these fixed nanos, never the
/// real wall clock.
fn pinned_clock(instant: u64) -> SystemClock {
    SystemClock::pinned(TimestampNanos::new(instant))
}

fn window(opens_at: u64, closes_at: u64) -> TimeWindow {
    TimeWindow {
        opens_at: TimestampNanos::new(opens_at),
        closes_at: TimestampNanos::new(closes_at),
    }
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
/// the attestation envelope carries that node's key.
fn node_public_key(socket: &Path, identity: Identity) -> BlsPublicKey {
    let request = SignRequest::new(
        ContentReference {
            digest: ObjectDigest::from_bytes(b"clock-gate-key-probe"),
            purpose: ContentPurpose::SignedObject,
            schema_version: PrincipalName::new("clock-probe"),
        },
        identity,
        AuditContext {
            purpose: ContentPurpose::SignedObject,
            audience: PrincipalName::new("clock-probe-audience"),
            policy_version: PrincipalName::new("clock-probe-policy"),
            nonce: ReplayNonce::new("clock-probe-nonce"),
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

fn mirror_object() -> AuthorizedObjectReference {
    AuthorizedObjectReference {
        component: ComponentKind::Spirit,
        digest: ObjectDigest::from_bytes(b"clock-gate-head-operation"),
        kind: AuthorizedObjectKind::Head,
    }
}

fn observe(socket: &Path, round: &QuorumRoundIdentifier) -> QuorumRoundState {
    match ask(socket, CriomeRequest::observe_quorum_round(round.clone())) {
        CriomeReply::QuorumRoundObserved(state) => state,
        other => panic!("expected QuorumRoundObserved, got {other:?}"),
    }
}

#[test]
fn a_signer_refuses_the_convenient_window_its_clock_is_outside() {
    // The attack: a proposer picks a convenient window (here, far in the future)
    // that its own clock is NOT inside, hoping the co-signed window will pass off
    // as "now." The witness-clock gate defeats it at the source — the originator's
    // OWN self-vote is refused, so the proposal never even opens.
    let alpha = host("clock-alpha");
    let beta = host("clock-beta");
    let (socket, store) = fixture("convenient-window");

    // Clock pinned at 1_500; the proposed window opens far later and never
    // contains 1_500.
    let daemon = CriomeDaemon::new(&socket, store)
        .with_node_identity(alpha.clone())
        .with_clock(pinned_clock(1_500));
    serve(daemon);
    wait_for_socket(&socket);

    let contract = admit(&socket, mirror_contract(&alpha, &beta));
    let object = mirror_object();
    let round = QuorumRoundIdentifier::for_operation(&object.digest);
    let reply = ask(
        &socket,
        CriomeRequest::ProposeQuorumAuthorization(QuorumProposal {
            phase: RoundPhase::Request,
            round,
            contract,
            object,
            window: window(1_000_000, 2_000_000),
        }),
    );

    match reply {
        CriomeReply::Rejection(rejection) => assert_eq!(
            rejection.payload(),
            &RejectionReason::MalformedRequest,
            "a window the signer's clock is outside must be refused, not opened"
        ),
        other => panic!(
            "the originator's own convenient window must be REFUSED (no self-vote), got {other:?}"
        ),
    }
}

#[test]
fn a_signer_signs_the_window_its_clock_is_inside() {
    // The same node, proposing a window that DOES contain its pinned clock, signs:
    // the round opens with the lone self-vote (one short of the 2-of-2 majority).
    let alpha = host("clock-alpha");
    let beta = host("clock-beta");
    let (socket, store) = fixture("inside-window");

    let daemon = CriomeDaemon::new(&socket, store)
        .with_node_identity(alpha.clone())
        .with_clock(pinned_clock(1_500));
    serve(daemon);
    wait_for_socket(&socket);

    let contract = admit(&socket, mirror_contract(&alpha, &beta));
    let object = mirror_object();
    let round = QuorumRoundIdentifier::for_operation(&object.digest);
    let reply = ask(
        &socket,
        CriomeRequest::ProposeQuorumAuthorization(QuorumProposal {
            phase: RoundPhase::Request,
            round,
            contract,
            object,
            window: window(1_000, 2_000),
        }),
    );

    match reply {
        CriomeReply::QuorumRoundOpened(state) => {
            assert_eq!(
                state.status,
                QuorumRoundStatus::Gathering,
                "the lone self-vote is one short of the 2-of-2 majority"
            );
            assert_eq!(
                state.gathered.into_u16(),
                1,
                "the signer's clock is inside the window, so its time-signature is emitted"
            );
        }
        other => panic!("a window the signer's clock is inside must OPEN the round, got {other:?}"),
    }
}

#[test]
fn a_peer_refuses_the_window_its_own_clock_is_outside() {
    // The peer re-check: node A (clock inside the window) proposes and self-signs,
    // then solicits node B across the voice. B's own clock is OUTSIDE the window,
    // so B refuses to co-sign — even though A's clock was inside. The round can
    // therefore never reach the 2-of-2 majority: an honest peer gates on its OWN
    // clock, defeating a convenient window a proposer's clock happened to fit.
    let alpha = host("clock-alpha");
    let beta = host("clock-beta");
    let (socket_a, store_a) = fixture("peer-refuse-alpha");
    let (socket_b, store_b) = fixture("peer-refuse-beta");

    // Window [1_000, 2_000]: A's clock 1_500 is inside, B's clock 9_000 is outside.
    let daemon_a = CriomeDaemon::new(&socket_a, store_a)
        .with_node_identity(alpha.clone())
        .with_clock(pinned_clock(1_500))
        .with_quorum_voice(Arc::new(DirectDialQuorumVoice::new(vec![
            PeerSocketRoute::new(beta.clone(), socket_b.clone()),
        ])));
    let daemon_b = CriomeDaemon::new(&socket_b, store_b)
        .with_node_identity(beta.clone())
        .with_clock(pinned_clock(9_000))
        .with_quorum_voice(Arc::new(DirectDialQuorumVoice::new(vec![
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

    let object = mirror_object();
    let round = QuorumRoundIdentifier::for_operation(&object.digest);
    let opened = match ask(
        &socket_a,
        CriomeRequest::ProposeQuorumAuthorization(QuorumProposal {
            phase: RoundPhase::Request,
            round: round.clone(),
            contract,
            object,
            window: window(1_000, 2_000),
        }),
    ) {
        CriomeReply::QuorumRoundOpened(state) => state,
        other => panic!("A's clock is inside the window; the round must open, got {other:?}"),
    };
    assert_eq!(opened.status, QuorumRoundStatus::Gathering);
    assert_eq!(opened.gathered.into_u16(), 1);

    // B is solicited across the voice but refuses on its own clock. The round must
    // stay WITHHELD — it never gathers B's vote, never authorizes.
    let deadline = Instant::now() + Duration::from_millis(1_500);
    while Instant::now() < deadline {
        let state = observe(&socket_a, &round);
        assert_eq!(
            state.status,
            QuorumRoundStatus::Gathering,
            "a peer whose clock is outside the window must refuse; the round never authorizes"
        );
        assert_eq!(
            state.gathered.into_u16(),
            1,
            "B contributes no vote — its witness-clock gate refuses the window"
        );
        assert!(state.authorized_evidence.is_none());
        std::thread::sleep(Duration::from_millis(150));
    }
}

#[test]
fn two_peers_whose_clocks_are_inside_the_window_co_sign_to_authorized() {
    // The control that isolates the clock gate as the cause of the stall above:
    // the IDENTICAL two-node setup, but with BOTH clocks inside the window, DOES
    // gather the 2-of-2 majority. So the refusal above is the witness-clock gate,
    // not an unreachable peer.
    let alpha = host("clock-alpha");
    let beta = host("clock-beta");
    let (socket_a, store_a) = fixture("peer-inside-alpha");
    let (socket_b, store_b) = fixture("peer-inside-beta");

    // Window [1_000, 2_000]: both clocks (1_500 and 1_800) are inside.
    let daemon_a = CriomeDaemon::new(&socket_a, store_a)
        .with_node_identity(alpha.clone())
        .with_clock(pinned_clock(1_500))
        .with_quorum_voice(Arc::new(DirectDialQuorumVoice::new(vec![
            PeerSocketRoute::new(beta.clone(), socket_b.clone()),
        ])));
    let daemon_b = CriomeDaemon::new(&socket_b, store_b)
        .with_node_identity(beta.clone())
        .with_clock(pinned_clock(1_800))
        .with_quorum_voice(Arc::new(DirectDialQuorumVoice::new(vec![
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

    let object = mirror_object();
    let round = QuorumRoundIdentifier::for_operation(&object.digest);
    let opened = match ask(
        &socket_a,
        CriomeRequest::ProposeQuorumAuthorization(QuorumProposal {
            phase: RoundPhase::Request,
            round: round.clone(),
            contract,
            object,
            window: window(1_000, 2_000),
        }),
    ) {
        CriomeReply::QuorumRoundOpened(state) => state,
        other => panic!("both clocks inside; the round must open, got {other:?}"),
    };
    assert_eq!(opened.gathered.into_u16(), 1);

    let authorized = wait_until_authorized(&socket_a, &round);
    assert_eq!(authorized.status, QuorumRoundStatus::Authorized);
    assert_eq!(
        authorized.gathered.into_u16(),
        2,
        "both peers' clocks are inside the window, so both time-signatures are emitted"
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
            "the in-window quorum never authorized (last gathered {} of {})",
            state.gathered.into_u16(),
            state.required.into_u16()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}
