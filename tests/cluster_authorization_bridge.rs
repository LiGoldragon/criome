//! THE CLUSTER-AUTHORIZATION BRIDGE WITNESSES (§3.3): in Quorum mode every
//! `AuthorizeSignalCall` is cluster-authorized — the bridge originates the
//! existing two-round commit over the operational quorum contract (the
//! founded root at this prototype stage) and pushes the terminal verdict to
//! the held observation session.
//!
//!   - `founded_single_node_bridge_grants_with_quorum_evidence`: the
//!     degenerate 1-of-1 quorum — a founded single-member root authorizes a
//!     head-advance ask through the FULL bridge (Request self-majority →
//!     driven commit → Granted), and the Granted state carries the signed
//!     grant (ComplexQuorum satisfaction, typed authorized object) plus the
//!     assembled quorum Evidence hand-off.
//!   - `window_expiry_pushes_expired_fail_closed`: a founded 2-member root
//!     whose peer member runs no criome — the Request round can never reach
//!     its majority, so the window-close timer marks the ask Expired and the
//!     session receives the terminal push. Quorum can't complete ⇒ operation
//!     refused; nothing is granted.
//!
//! Falsification: if the bridge granted on the Request round alone, the
//! expiry witness would grant instead of expire; if status were trusted
//! without machinery, the grant witness would carry no evidence.

use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use criome::daemon::CriomeDaemon;
use criome::founding::FoundingStatementBytes;
use criome::master_key::MasterKey;
use criome::tables::StoreLocation;
use criome::transport::{CriomeClient, CriomeMetaClient};
use meta_signal_criome::{
    Input as MetaInput, Output as MetaOutput, RootFoundingAcceptance, RootFoundingInitiation,
};
use signal_criome::{
    AuthorizationPolicyClass, AuthorizationStatus, AuthorizedObjectKind, AuthorizedObjectReference,
    BlsPublicKey, ComponentKind, Contract, CriomeReply, CriomeRequest, FoundingConveyance,
    FoundingMember, FoundingSignature, FoundingSignatureReturn, GenesisDomainTag, Identity,
    NodePublicKeyObservation, ObjectDigest, PolicyMember, ReplayNonce, RequiredSignatureThreshold,
    RootGenesis, Rule, SignalCallAuthorization, SignatureEnvelope, SignatureScheme, Threshold,
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
        "criome-bridge-{tag}-{}-{}",
        std::process::id(),
        nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create bridge fixture dir");
    (
        dir.join("criome.sock"),
        StoreLocation::new(dir.join("criome.sema")),
    )
}

fn host(name: &str) -> Identity {
    Identity::host(name.to_string())
}

fn serve(daemon: CriomeDaemon) {
    thread::spawn(move || {
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
        thread::sleep(Duration::from_millis(20));
    }
}

fn meta_socket_for(working: &Path) -> PathBuf {
    working.with_file_name("criome.sock.meta")
}

fn node_public_key(socket: &Path) -> BlsPublicKey {
    let reply = CriomeClient::new(socket)
        .send(CriomeRequest::observe_node_public_key(
            NodePublicKeyObservation::new(),
        ))
        .expect("observe node public key");
    match reply {
        CriomeReply::NodePublicKey(key) => key.public_key().clone(),
        other => panic!("expected NodePublicKey, got {other:?}"),
    }
}

fn genesis(members: Vec<FoundingMember>, nonce: &str) -> RootGenesis {
    let policy_members = members
        .iter()
        .map(|member| PolicyMember::key_member(member.identity.clone()))
        .collect();
    let root_contract = Contract::root(Rule::threshold(Threshold::new(
        RequiredSignatureThreshold::new(members.len() as u64),
        policy_members,
    )));
    RootGenesis::new(
        root_contract,
        members,
        GenesisDomainTag::CriomeRootFoundingV1,
        ReplayNonce::new(nonce),
    )
}

fn meta(socket: &Path, request: MetaInput) -> MetaOutput {
    CriomeMetaClient::new(socket)
        .send(request)
        .expect("meta round-trip")
}

fn found(meta_socket: &Path, cohort: &RootGenesis) {
    let anchor = cohort.anchor().expect("cohort anchor");
    match meta(
        meta_socket,
        MetaInput::AcceptRootFounding(RootFoundingAcceptance::new(anchor, cohort.clone())),
    ) {
        MetaOutput::RootFoundingAccepted(_) => {}
        other => panic!("expected RootFoundingAccepted, got {other:?}"),
    }
}

fn head_advance(seed: &[u8], nonce: &str) -> SignalCallAuthorization {
    SignalCallAuthorization::new(
        AuthorizedObjectReference {
            component: ComponentKind::Spirit,
            digest: ObjectDigest::from_bytes(seed),
            kind: AuthorizedObjectKind::Head,
        },
        host("spirit"),
        ReplayNonce::new(nonce),
        None,
    )
}

/// Drive one authorization to its terminal state over the streaming session:
/// consume the snapshot, then drain pushed updates until a terminal status.
fn terminal_state(
    socket: &Path,
    authorization: SignalCallAuthorization,
    deadline: Duration,
) -> signal_criome::AuthorizationStateRecord {
    let submitted_digest = authorization.object.digest.clone();
    let mut session = CriomeClient::new(socket)
        .authorize_signal_call(authorization)
        .expect("open authorization session");
    session
        .set_read_timeout(Some(deadline))
        .expect("set session read deadline");
    let token_slot = session.token().payload().clone();
    let snapshot_terminal = session
        .snapshot()
        .states()
        .iter()
        .find(|state| {
            state.request_slot == token_slot
                && state.request_digest == submitted_digest
                && matches!(
                    state.status,
                    AuthorizationStatus::Granted
                        | AuthorizationStatus::Denied
                        | AuthorizationStatus::Expired
                        | AuthorizationStatus::Unavailable
                )
        })
        .cloned();
    if let Some(state) = snapshot_terminal {
        return state;
    }
    loop {
        let state = session.next_update().expect("session pushes an update");
        assert_eq!(state.request_digest, submitted_digest);
        if matches!(
            state.status,
            AuthorizationStatus::Granted
                | AuthorizationStatus::Denied
                | AuthorizationStatus::Expired
                | AuthorizationStatus::Unavailable
        ) {
            return state;
        }
    }
}

#[test]
fn founded_single_node_bridge_grants_with_quorum_evidence() {
    let founder = host("bridge-founder");
    let (socket, store) = fixture("single-grant");
    let meta_socket = meta_socket_for(&socket);
    serve(
        CriomeDaemon::new(&socket, store)
            .with_node_identity(founder.clone())
            .with_quorum_window(Duration::from_secs(10)),
    );
    wait_for_socket(&socket);
    wait_for_socket(&meta_socket);

    // Found the single-member root: the operational quorum contract at this
    // prototype stage. The bridge resolves it criome-side.
    let key = node_public_key(&socket);
    let cohort = genesis(
        vec![FoundingMember::new(founder.clone(), key)],
        "bridge-single-grant",
    );
    found(&meta_socket, &cohort);

    let authorization = head_advance(b"bridge single-node head advance", "bridge-grant-nonce");
    let requested = authorization.object.clone();
    let state = terminal_state(&socket, authorization, Duration::from_secs(20));

    assert_eq!(
        state.status,
        AuthorizationStatus::Granted,
        "the founded 1-of-1 quorum grants through the full bridge, got {state:?}"
    );
    let grant = state.grant().expect("a Granted state carries its grant");
    assert_eq!(
        grant.authorized_object, requested,
        "the grant binds the requested typed object"
    );
    assert_eq!(
        grant.policy_satisfaction.policy_class,
        AuthorizationPolicyClass::ComplexQuorum,
        "a cluster grant attests the quorum policy class, not self-signed"
    );
    assert!(
        !grant.signatures().is_empty(),
        "the grant carries the criome master-key signature envelope"
    );
    let evidence = state
        .granted_evidence()
        .expect("the Granted state carries the assembled quorum Evidence hand-off");
    assert_eq!(evidence.object, requested);
    assert!(
        !evidence.evidence.evidence_signatures.is_empty(),
        "the hand-off Evidence carries the commit round's stamped signatures"
    );
}

#[test]
fn window_expiry_pushes_expired_fail_closed() {
    let founder = host("bridge-expiry-founder");
    let absent_member = host("bridge-expiry-absent");
    let (socket, store) = fixture("window-expiry");
    let meta_socket = meta_socket_for(&socket);
    serve(
        CriomeDaemon::new(&socket, store)
            .with_node_identity(founder.clone())
            // Seconds in tests: the window must expire quickly, and the
            // conveyance is silent so the peer's vote never arrives.
            .with_quorum_window(Duration::from_secs(2)),
    );
    wait_for_socket(&socket);
    wait_for_socket(&meta_socket);

    // A 2-member root whose second member is a key this test controls and no
    // running criome serves: the founding completes (the test returns the
    // member's valid signature), but no quorum round can ever reach 2-of-2.
    let key = node_public_key(&socket);
    let absent_key = MasterKey::generate().expect("absent member key");
    let cohort = genesis(
        vec![
            FoundingMember::new(founder.clone(), key),
            FoundingMember::new(absent_member.clone(), absent_key.public_key()),
        ],
        "bridge-window-expiry",
    );
    let anchor = cohort.anchor().expect("cohort anchor");
    match meta(
        &meta_socket,
        MetaInput::InitiateRootFounding(RootFoundingInitiation::new(cohort.clone())),
    ) {
        MetaOutput::RootFoundingStatus(_status) => {}
        other => panic!("initiate must report status, got {other:?}"),
    }
    found(&meta_socket, &cohort);
    let statement = criome::founding::RootFounding::found(cohort.clone())
        .expect("cohort founds")
        .statement();
    let statement_bytes = statement.signing_bytes().expect("statement encodes");
    let returned = FoundingSignatureReturn {
        anchor,
        signature: FoundingSignature::new(
            absent_member,
            SignatureEnvelope {
                scheme: SignatureScheme::Bls12_381MinPk,
                public_key: absent_key.public_key(),
                signature: absent_key.sign(&statement_bytes),
            },
        ),
    };
    let reply = CriomeClient::new(&socket)
        .send(CriomeRequest::convey_founding(
            FoundingConveyance::Signature(returned),
        ))
        .expect("convey the absent member's founding signature");
    assert!(
        matches!(reply, CriomeReply::FoundingConveyed(_)),
        "the valid conveyed signature completes the founding, got {reply:?}"
    );

    // The ask: the round gathers only the self-vote (1 of 2, the peer runs no
    // criome), so the window-close timer must push the terminal Expired.
    let started = Instant::now();
    let authorization = head_advance(b"bridge expiring head advance", "bridge-expiry-nonce");
    let state = terminal_state(&socket, authorization, Duration::from_secs(30));
    assert_eq!(
        state.status,
        AuthorizationStatus::Expired,
        "a quorum that cannot complete expires fail-closed at window close, got {state:?}"
    );
    assert!(
        state.grant().is_none(),
        "an expired ask carries no grant — status alone is never proof"
    );
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "the expiry is the window timer, not an immediate refusal"
    );
}

/// Open one authorization observation session WITHOUT draining it, so a
/// second ask can be submitted while the first is still in flight.
fn open_session(
    socket: &Path,
    authorization: SignalCallAuthorization,
    deadline: Duration,
) -> criome::transport::CriomeAuthorizationObservationSession {
    let session = CriomeClient::new(socket)
        .authorize_signal_call(authorization)
        .expect("open authorization session");
    session
        .set_read_timeout(Some(deadline))
        .expect("set session read deadline");
    session
}

/// Drain an already-open session to its terminal state (snapshot first, then
/// pushed updates), binding by the session's own slot.
fn drain_to_terminal(
    mut session: criome::transport::CriomeAuthorizationObservationSession,
    submitted_digest: &ObjectDigest,
) -> signal_criome::AuthorizationStateRecord {
    let token_slot = session.token().payload().clone();
    let terminal = |status: AuthorizationStatus| {
        matches!(
            status,
            AuthorizationStatus::Granted
                | AuthorizationStatus::Denied
                | AuthorizationStatus::Expired
                | AuthorizationStatus::Unavailable
        )
    };
    if let Some(state) = session
        .snapshot()
        .states()
        .iter()
        .find(|state| {
            state.request_slot == token_slot
                && state.request_digest == *submitted_digest
                && terminal(state.status)
        })
        .cloned()
    {
        return state;
    }
    loop {
        let state = session.next_update().expect("session pushes an update");
        assert_eq!(state.request_digest, *submitted_digest);
        if terminal(state.status) {
            return state;
        }
    }
}

/// AUDIT F1 — the standing-head wedge, witnessed at the daemon boundary. The
/// spirit drain re-asks an already-committed head on every idle or coalesced
/// mail pass, and again after a grant-then-ship failure. Each re-ask must
/// RE-GRANT from the stored committed round (with the full grant + evidence
/// hand-off), record NO self-loop veto row, and leave every later successor
/// grantable. Pre-fix, the first re-ask durably recorded `(contract, D) → D`
/// and every successor expired as a false QuorumConflict, forever.
#[test]
fn re_ask_of_a_standing_committed_head_re_grants_and_the_successor_still_grants() {
    let founder = host("bridge-re-ask-founder");
    let (socket, store) = fixture("standing-head-re-ask");
    let meta_socket = meta_socket_for(&socket);
    serve(
        CriomeDaemon::new(&socket, store)
            .with_node_identity(founder.clone())
            .with_quorum_window(Duration::from_secs(10)),
    );
    wait_for_socket(&socket);
    wait_for_socket(&meta_socket);
    let key = node_public_key(&socket);
    let cohort = genesis(
        vec![FoundingMember::new(founder.clone(), key)],
        "bridge-standing-head-re-ask",
    );
    found(&meta_socket, &cohort);

    // The committed head D.
    let first = terminal_state(
        &socket,
        head_advance(b"re-ask head D", "re-ask-d-1"),
        Duration::from_secs(20),
    );
    assert_eq!(first.status, AuthorizationStatus::Granted, "got {first:?}");

    // The drain's re-ask of the STANDING head (fresh nonce, same digest):
    // re-granted from the committed round, never re-proposed.
    let re_ask = terminal_state(
        &socket,
        head_advance(b"re-ask head D", "re-ask-d-2"),
        Duration::from_secs(20),
    );
    assert_eq!(
        re_ask.status,
        AuthorizationStatus::Granted,
        "a re-ask of the committed head re-grants, got {re_ask:?}"
    );
    let grant = re_ask.grant().expect("the re-grant carries its grant");
    assert_eq!(
        grant.authorized_object_digest().as_str(),
        ObjectDigest::from_bytes(b"re-ask head D").as_str(),
        "the re-grant binds the standing head digest"
    );
    assert!(
        re_ask
            .granted_evidence()
            .is_some_and(|evidence| !evidence.evidence.evidence_signatures.is_empty()),
        "the re-grant hands off the committed round's quorum Evidence"
    );

    // The grant-then-ship-failure retry: a THIRD identical ask, still granted
    // idempotently.
    let ship_failure_retry = terminal_state(
        &socket,
        head_advance(b"re-ask head D", "re-ask-d-3"),
        Duration::from_secs(20),
    );
    assert_eq!(
        ship_failure_retry.status,
        AuthorizationStatus::Granted,
        "the grant-then-ship-failure re-ask re-grants, got {ship_failure_retry:?}"
    );

    // THE WEDGE PROBE: the successor H must still grant. Pre-fix this
    // expired forever on the self-loop veto row.
    let successor = terminal_state(
        &socket,
        head_advance(b"re-ask successor H", "re-ask-h-1"),
        Duration::from_secs(20),
    );
    assert_eq!(
        successor.status,
        AuthorizationStatus::Granted,
        "the successor after a standing-head re-ask still grants — no poison row, got {successor:?}"
    );
    // And the successor's own re-ask re-grants at the NEW head.
    let successor_re_ask = terminal_state(
        &socket,
        head_advance(b"re-ask successor H", "re-ask-h-2"),
        Duration::from_secs(20),
    );
    assert_eq!(
        successor_re_ask.status,
        AuthorizationStatus::Granted,
        "got {successor_re_ask:?}"
    );
}

/// AUDIT F3 — no ask is silently orphaned. Two in-flight asks for the SAME
/// digest (a founded 2-member root whose peer is dark, so neither can grant):
/// the second JOINS the first's drive, and BOTH sessions receive the terminal
/// Expired at window close. Pre-fix, the second ask overwrote the first's
/// pending slot; the first slot's timer no-opped on the slot mismatch and its
/// session never turned terminal.
#[test]
fn every_pending_slot_for_one_digest_settles_at_window_close() {
    let founder = host("bridge-join-founder");
    let absent_member = host("bridge-join-absent");
    let (socket, store) = fixture("duplicate-ask-join");
    let meta_socket = meta_socket_for(&socket);
    serve(
        CriomeDaemon::new(&socket, store)
            .with_node_identity(founder.clone())
            .with_quorum_window(Duration::from_secs(3)),
    );
    wait_for_socket(&socket);
    wait_for_socket(&meta_socket);
    let key = node_public_key(&socket);
    let absent_key = MasterKey::generate().expect("absent member key");
    let cohort = genesis(
        vec![
            FoundingMember::new(founder.clone(), key),
            FoundingMember::new(absent_member.clone(), absent_key.public_key()),
        ],
        "bridge-duplicate-ask-join",
    );
    let anchor = cohort.anchor().expect("cohort anchor");
    match meta(
        &meta_socket,
        MetaInput::InitiateRootFounding(RootFoundingInitiation::new(cohort.clone())),
    ) {
        MetaOutput::RootFoundingStatus(_status) => {}
        other => panic!("initiate must report status, got {other:?}"),
    }
    found(&meta_socket, &cohort);
    let statement = criome::founding::RootFounding::found(cohort.clone())
        .expect("cohort founds")
        .statement();
    let statement_bytes = statement.signing_bytes().expect("statement encodes");
    let returned = FoundingSignatureReturn {
        anchor,
        signature: FoundingSignature::new(
            absent_member,
            SignatureEnvelope {
                scheme: SignatureScheme::Bls12_381MinPk,
                public_key: absent_key.public_key(),
                signature: absent_key.sign(&statement_bytes),
            },
        ),
    };
    let reply = CriomeClient::new(&socket)
        .send(CriomeRequest::convey_founding(
            FoundingConveyance::Signature(returned),
        ))
        .expect("convey the absent member's founding signature");
    assert!(matches!(reply, CriomeReply::FoundingConveyed(_)));

    // Two in-flight asks for the SAME digest, distinct nonces, both submitted
    // inside the window.
    let digest = ObjectDigest::from_bytes(b"join twin head");
    let session_one = open_session(
        &socket,
        head_advance(b"join twin head", "join-nonce-1"),
        Duration::from_secs(30),
    );
    let session_two = open_session(
        &socket,
        head_advance(b"join twin head", "join-nonce-2"),
        Duration::from_secs(30),
    );
    let first = drain_to_terminal(session_one, &digest);
    let second = drain_to_terminal(session_two, &digest);
    assert_eq!(
        first.status,
        AuthorizationStatus::Expired,
        "the FIRST slot settles at window close — never orphaned, got {first:?}"
    );
    assert_eq!(
        second.status,
        AuthorizationStatus::Expired,
        "the joined second slot settles too, got {second:?}"
    );
    assert_ne!(
        first.request_slot, second.request_slot,
        "two asks occupy two observable slots"
    );
}
