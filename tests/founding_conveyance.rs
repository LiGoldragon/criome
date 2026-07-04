//! THE CROSS-NODE FOUNDING WITNESS: two nodes assemble a UNANIMOUS root across
//! the router voice, end-to-end. The initiator conveys a founding proposal to the
//! peer, each owner explicitly accepts on their own meta socket (no
//! auto-approval), the peer's signature is conveyed back to the initiator, and on
//! unanimity the finished root is distributed to both nodes — which each verify,
//! persist, and adopt the SAME anchor by seeding their registries.
//!
//! This is the in-process, direct-dial analogue of the live 2-node proof
//! (primary-79z1.15): the `DirectDialQuorumVoice` carries the conveyance to each
//! peer's working socket, exactly as the router voice does across nodes.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use criome::daemon::CriomeDaemon;
use criome::tables::StoreLocation;
use criome::transport::{CriomeClient, CriomeMetaClient};
use criome::voice::{DirectDialQuorumVoice, PeerSocketRoute};
use meta_signal_criome::{
    Input as MetaInput, Output as MetaOutput, RootFoundingAcceptance, RootFoundingInitiation,
    RootFoundingObservation, RootFoundingState, RootFoundingStatus,
};
use signal_criome::{
    BlsPublicKey, Contract, CriomeReply, CriomeRequest, FoundingMember, GenesisDomainTag, Identity,
    IdentityLookup, NodePublicKeyObservation, PolicyMember, ReplayNonce,
    RequiredSignatureThreshold, RootAnchorDigest, RootGenesis, Rule, Threshold,
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
        "criome-founding-conveyance-{tag}-{}-{}",
        std::process::id(),
        nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create founding fixture dir");
    (
        dir.join("criome.sock"),
        StoreLocation::new(dir.join("criome.sema")),
    )
}

fn meta_socket_for(working: &Path) -> PathBuf {
    let name = working
        .file_name()
        .and_then(|name| name.to_str())
        .expect("working socket file name");
    working.with_file_name(format!("{name}.meta"))
}

fn host(name: &str) -> Identity {
    Identity::host(name.to_string())
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

fn node_public_key(socket: &Path) -> BlsPublicKey {
    let reply = CriomeClient::new(socket)
        .send(CriomeRequest::observe_node_public_key(
            NodePublicKeyObservation::new(),
        ))
        .unwrap_or_else(|error| panic!("observe node key on {socket:?}: {error}"));
    match reply {
        CriomeReply::NodePublicKey(key) => key.public_key().clone(),
        other => panic!("expected NodePublicKey, got {other:?}"),
    }
}

fn cohort(
    alpha: &Identity,
    beta: &Identity,
    key_alpha: &BlsPublicKey,
    key_beta: &BlsPublicKey,
) -> RootGenesis {
    let root_contract = Contract::root(Rule::threshold(Threshold::new(
        RequiredSignatureThreshold::new(2),
        vec![
            PolicyMember::key_member(alpha.clone()),
            PolicyMember::key_member(beta.clone()),
        ],
    )));
    RootGenesis::new(
        root_contract,
        vec![
            FoundingMember::new(alpha.clone(), key_alpha.clone()),
            FoundingMember::new(beta.clone(), key_beta.clone()),
        ],
        GenesisDomainTag::CriomeRootFoundingV1,
        ReplayNonce::new("cross-node-founding"),
    )
}

fn meta(socket: &Path, request: MetaInput) -> MetaOutput {
    CriomeMetaClient::new(socket)
        .send(request)
        .unwrap_or_else(|error| panic!("meta round-trip on {socket:?}: {error}"))
}

fn observe_status(socket: &Path) -> RootFoundingStatus {
    match meta(
        socket,
        MetaInput::ObserveRootFounding(RootFoundingObservation::new()),
    ) {
        MetaOutput::RootFoundingStatus(status) => status,
        other => panic!("expected RootFoundingStatus, got {other:?}"),
    }
}

fn accept(socket: &Path, anchor: &RootAnchorDigest, cohort: &RootGenesis) {
    match meta(
        socket,
        MetaInput::AcceptRootFounding(RootFoundingAcceptance::new(anchor.clone(), cohort.clone())),
    ) {
        MetaOutput::RootFoundingAccepted(_) => {}
        other => panic!("expected RootFoundingAccepted on {socket:?}, got {other:?}"),
    }
}

fn identity_registered(socket: &Path, identity: &Identity) -> bool {
    matches!(
        CriomeClient::new(socket)
            .send(CriomeRequest::LookupIdentity(IdentityLookup::new(
                identity.clone()
            )))
            .expect("lookup identity"),
        CriomeReply::IdentityReceipt(_)
    )
}

fn wait_until<Predicate>(what: &str, predicate: Predicate)
where
    Predicate: Fn() -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(15);
    while !predicate() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn two_nodes_found_a_unanimous_root_end_to_end() {
    let alpha = host("founder-alpha");
    let beta = host("founder-beta");
    let (socket_a, store_a) = fixture("alpha");
    let (socket_b, store_b) = fixture("beta");
    let meta_a = meta_socket_for(&socket_a);
    let meta_b = meta_socket_for(&socket_b);

    // Each node dials the OTHER'S working socket over the direct-dial voice —
    // exactly the carriage the founding conveyance rides.
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
    for socket in [&socket_a, &socket_b, &meta_a, &meta_b] {
        wait_for_socket(socket);
    }

    // The owner reads each node's master public key out-of-band and builds the
    // self-certifying cohort (its anchor commits to these exact keys).
    let key_a = node_public_key(&socket_a);
    let key_b = node_public_key(&socket_b);
    let genesis = cohort(&alpha, &beta, &key_a, &key_b);
    let anchor = genesis.anchor().expect("cohort anchor");

    // Initiate on A: A records its gathering and conveys the proposal to B. No
    // root is founded yet — founding is owner-accepted with no auto-approval.
    match meta(
        &meta_a,
        MetaInput::InitiateRootFounding(RootFoundingInitiation::new(genesis.clone())),
    ) {
        MetaOutput::RootFoundingStatus(status) => {
            assert_eq!(status.state, RootFoundingState::Unfounded);
            assert!(
                status
                    .pending
                    .iter()
                    .any(|pending| pending.anchor == anchor),
                "the initiator queues its own founding as pending its accept"
            );
        }
        other => panic!("initiate must report status, got {other:?}"),
    }

    // The proposal reaches B over the voice: it appears in B's pending queue,
    // awaiting B's owner accept.
    wait_until("the proposal to reach B's pending queue", || {
        observe_status(&meta_b)
            .pending
            .iter()
            .any(|pending| pending.anchor == anchor)
    });

    // Both owners explicitly accept on their own meta socket. A signs first (one
    // of two — not unanimous). B signs and its criome conveys that signature back
    // to A, which completes the cohort and distributes the finished root.
    accept(&meta_a, &anchor, &genesis);
    assert_eq!(
        observe_status(&meta_a).state,
        RootFoundingState::Gathering,
        "the initiator's lone signature is one short of unanimity"
    );
    accept(&meta_b, &anchor, &genesis);

    // The conveyed signature makes A unanimous; the distributed root makes B
    // unanimous. Both nodes adopt the same founded anchor.
    wait_until("A to reach a founded root", || {
        observe_status(&meta_a).state == RootFoundingState::Founded
    });
    wait_until(
        "B to reach a founded root (from the distributed root)",
        || observe_status(&meta_b).state == RootFoundingState::Founded,
    );

    // Adoption seeds each node's registry from the founded cohort, on BOTH nodes —
    // the durable proof that each persisted and adopted the same root.
    for socket in [&socket_a, &socket_b] {
        assert!(
            identity_registered(socket, &alpha),
            "alpha is seeded into {socket:?}"
        );
        assert!(
            identity_registered(socket, &beta),
            "beta is seeded into {socket:?}"
        );
    }
}
