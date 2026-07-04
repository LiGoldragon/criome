//! ANTI-EQUIVOCATION-ACROSS-RESTART WITNESS (F2): a node's single-successor veto
//! survives a restart. The co-signed-successor ledger is DURABLE, so a node that
//! co-signed successor S1 from a head cannot, after a reboot that rebuilds its
//! actor state through `on_start`, co-sign a CONFLICTING S2 from the same head.
//!
//! The audit flagged that `co_signed_successors` was an in-memory-only map,
//! cleared on every boot: a node could co-sign S1, restart, then co-sign a
//! conflicting S2 from the same head — equivocating across the restart and
//! defeating the very veto the safety argument depends on. This test drives a real
//! stop/start against ONE store to prove the ledger is reconstructed: the second,
//! conflicting successor is still refused with the typed `QuorumConflict` after
//! the actor state was rebuilt from durable state alone.

use std::sync::Arc;

use criome::actors::root::{Arguments as RootArguments, CriomeRoot, SubmitRequest};
use criome::master_key::SystemClock;
use criome::tables::StoreLocation;
use criome::voice::SilentVoice;
use kameo::actor::ActorRef;
use signal_criome::{
    AuthorizationMode, AuthorizedObjectKind, AuthorizedObjectReference, ComponentKind, Contract,
    ContractDigest, CriomeReply, CriomeRequest, Identity, ObjectDigest, PolicyMember,
    QuorumProposal, QuorumRoundIdentifier, RequiredSignatureThreshold, RoundPhase, Rule, Threshold,
    TimeWindow, TimestampNanos,
};

fn nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos()
}

/// One store shared by both actor lifetimes: the restart reopens the SAME path, so
/// `on_start` sees exactly the durable state the first lifetime wrote.
fn shared_store() -> StoreLocation {
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "criome-ledger-restart-{}-{}",
        std::process::id(),
        nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create ledger-restart fixture dir");
    StoreLocation::new(dir.join("criome.sema"))
}

fn host(name: &str) -> Identity {
    Identity::host(name.to_string())
}

/// A window wide enough that the pinned clock at 1_500 sits inside it, so both
/// lifetimes are deterministically in-window.
fn shared_window() -> TimeWindow {
    TimeWindow {
        opens_at: TimestampNanos::new(1_000),
        closes_at: TimestampNanos::new(2_000),
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

/// A distinct successor object per `tag` — same shape the two-round witnesses use.
fn successor(tag: &[u8]) -> AuthorizedObjectReference {
    AuthorizedObjectReference {
        component: ComponentKind::Spirit,
        digest: ObjectDigest::from_bytes(tag),
        kind: AuthorizedObjectKind::Head,
    }
}

/// Start a criome root as `identity` over `store`, with a pinned in-window clock so
/// the propose path's witness-clock gate is deterministic.
async fn start(store: StoreLocation, identity: Identity) -> ActorRef<CriomeRoot> {
    CriomeRoot::start(RootArguments {
        store,
        cluster_root: None,
        authorization_mode: AuthorizationMode::Quorum,
        node_identity: identity,
        voice: Arc::new(SilentVoice),
        clock: SystemClock::pinned(TimestampNanos::new(1_500)),
    })
    .await
    .expect("start criome root")
}

async fn submit(root: &ActorRef<CriomeRoot>, request: CriomeRequest) -> CriomeReply {
    root.ask(SubmitRequest::new(request))
        .await
        .expect("submit request")
        .into_reply()
}

async fn admit(root: &ActorRef<CriomeRoot>, contract: Contract) -> ContractDigest {
    match submit(root, CriomeRequest::AdmitContract(contract)).await {
        CriomeReply::ContractAdmitted(admitted) => admitted.into_payload(),
        other => panic!("expected ContractAdmitted, got {other:?}"),
    }
}

/// Propose a Request-phase round for `object` — the propose path casts this node's
/// vote and RECORDS the co-sign for `(contract, head)`, even at a lone self-vote.
async fn propose(
    root: &ActorRef<CriomeRoot>,
    contract: ContractDigest,
    object: AuthorizedObjectReference,
) -> CriomeReply {
    let round = QuorumRoundIdentifier::for_phase(&object.digest, RoundPhase::Request);
    submit(
        root,
        CriomeRequest::ProposeQuorumAuthorization(QuorumProposal {
            round,
            phase: RoundPhase::Request,
            contract,
            object,
            window: shared_window(),
        }),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_co_signed_successor_veto_survives_a_restart() {
    let alpha = host("restart-alpha");
    let beta = host("restart-beta");
    let store = shared_store();

    // Lifetime one: co-sign S1 from the genesis head (round 1 opens one short of the
    // 2-of-2 majority, so it never commits and the head stays at genesis), then stop
    // the actor — releasing the store so the second lifetime can reopen it.
    let root = start(store.clone(), alpha.clone()).await;
    let contract = admit(&root, mirror_contract(&alpha, &beta)).await;
    let successor_one = successor(b"restart-successor-one");
    let successor_two = successor(b"restart-successor-two");

    match propose(&root, contract.clone(), successor_one.clone()).await {
        CriomeReply::QuorumRoundOpened(state) => {
            assert_eq!(
                state.gathered.into_u16(),
                1,
                "the lone self-vote co-signs S1"
            );
        }
        other => panic!("S1 propose must open its round, got {other:?}"),
    }
    CriomeRoot::stop(root).await.expect("stop first lifetime");

    // Lifetime two: rebuild the actor state through `on_start` on the SAME store —
    // no in-memory carry-over, only durable state. A conflicting S2 from the same
    // genesis head must still be refused with the typed QuorumConflict naming S1.
    let restarted = start(store.clone(), alpha.clone()).await;
    let reply = propose(&restarted, contract.clone(), successor_two).await;
    match reply {
        CriomeReply::QuorumConflict(conflict) => {
            assert_eq!(
                conflict.contract, contract,
                "the reconstructed conflict names the contract it protects"
            );
            assert_eq!(
                conflict.existing_successor.digest, successor_one.digest,
                "the durable ledger still holds S1 as the one co-signed successor from this head"
            );
        }
        other => panic!(
            "after a restart the co-signed-successor veto must still refuse a conflicting \
             second successor with QuorumConflict, got {other:?}"
        ),
    }
    CriomeRoot::stop(restarted)
        .await
        .expect("stop second lifetime");
}
