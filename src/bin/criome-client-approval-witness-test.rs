//! The criome ClientApproval park/approve/reject witness-test (report 705).
//!
//! Proves the `ClientApproval` authorization mode end-to-end against a REAL
//! `criome-daemon`, exercising BOTH halves of the park flow over the two sockets
//! the daemon exposes:
//!
//!   * the WORKING socket (`CriomeClient`) is the requester half — it submits an
//!     `EvaluateAuthorization` that, under `ClientApproval`, is PARKED rather than
//!     evaluated, and later reads the settled verdict back via
//!     `ObserveAuthorization`.
//!   * the META socket (`CriomeMetaClient`) is the policy/operator half — it
//!     reconfigures the daemon into `ClientApproval`, lists the parked requests,
//!     and decides each one with `SubmitAuthorizationApproval`.
//!
//! The witness plays both roles, proving the full lifecycle twice:
//!
//!   (1) CONFIGURE   — meta `Configure(ClientApproval)` -> `Configured`.
//!   (2) PARK        — working `EvaluateAuthorization` -> `AuthorizationPending`
//!                     (a parked slot, NOT an immediate verdict).
//!   (3) LIST        — meta `ObserveParkedAuthorizations` -> snapshot containing
//!                     the parked slot.
//!   (4) APPROVE     — meta `SubmitAuthorizationApproval(Approve)` -> recorded.
//!   (5) GRANTED     — working `ObserveAuthorization(slot)` -> state `Granted`.
//!   (6) REJECT PATH — a SECOND, different request parks under a different slot,
//!                     is decided `Reject`, and settles `Denied`.
//!
//! Exit 0 only when every assertion holds; any socket fault, off-contract reply,
//! or wrong state panics (nonzero exit). Sockets: `CRIOME_SOCKET` /
//! `CRIOME_META_SOCKET` (defaults `/run/criome/criome.sock` + `<socket>.meta`).

use std::path::PathBuf;

use criome::transport::{CriomeClient, CriomeMetaClient};
use meta_signal_criome::{AuthorizationApproval, AuthorizationApprovalDecision};
use signal_criome::{
    AttestedMoment, AttestedMomentProposition, AuthorizationEvaluation, AuthorizationMode,
    AuthorizationObservation, AuthorizationRequestSlot, AuthorizationStatus, AuthorizedObjectKind,
    AuthorizedObjectReference, ComponentKind, ContractDigest, CriomeDaemonConfiguration,
    CriomeReply, CriomeRequest, Evidence, ObjectDigest, OperationDigest,
    ParkedAuthorizationObservation, RequiredSignatureThreshold, TimeWindow, TimestampNanos,
};

/// The witness run: a working-socket client (the requester half), a meta-socket
/// client (the policy/operator half), and the daemon paths it reconfigures into
/// `ClientApproval`. Every step is a method on the data it acts through.
struct Witness {
    working: CriomeClient,
    meta: CriomeMetaClient,
    socket_path: String,
    store_path: String,
}

impl Witness {
    /// A synthetic spirit head digest `D` (32 bytes), parameterised by `salt` so
    /// two distinct requests park under two distinct slots. Object and operation
    /// derive from the same bytes so the daemon's digest-consistency guard (which
    /// runs before the park branch) passes.
    fn head_bytes(salt: u8) -> [u8; 32] {
        let mut bytes = [0u8; 32];
        let mut index = 0u8;
        while (index as usize) < bytes.len() {
            bytes[index as usize] = index
                .wrapping_mul(11)
                .wrapping_add(5)
                .wrapping_add(salt.wrapping_mul(31));
            index += 1;
        }
        bytes
    }

    fn from_environment() -> Self {
        let socket = std::env::var_os("CRIOME_SOCKET")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/run/criome/criome.sock"));
        let meta = std::env::var_os("CRIOME_META_SOCKET")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(format!("{}.meta", socket.display())));
        eprintln!(
            "criome-client-approval-witness-test: socket={} meta={}",
            socket.display(),
            meta.display()
        );
        Self {
            working: CriomeClient::new(&socket),
            meta: CriomeMetaClient::new(&meta),
            socket_path: socket.display().to_string(),
            store_path: format!("{}.sema", socket.display()),
        }
    }

    /// (1) Reconfigure the running daemon to `ClientApproval` over the meta socket.
    fn configure_client_approval(&self) {
        let configuration = CriomeDaemonConfiguration::new(&self.socket_path, &self.store_path)
            .with_authorization_mode(AuthorizationMode::ClientApproval);
        let reply = self
            .meta
            .send(meta_signal_criome::Input::Configure(configuration))
            .expect("Configure reaches criome over the meta socket");
        assert!(
            matches!(reply, meta_signal_criome::Output::Configured(_)),
            "meta Configure(ClientApproval) is applied, got {reply:?}"
        );
        eprintln!(
            "criome-client-approval-witness-test: PROOF (1) meta Configure(ClientApproval) -> Configured"
        );
    }

    /// A well-formed, signature-less evaluation over a `salt`-derived head. Under
    /// `ClientApproval` the verdict is deferred to the client, so the evidence
    /// need carry no signatures — the daemon parks it regardless.
    fn evaluation(salt: u8) -> AuthorizationEvaluation {
        let bytes = Self::head_bytes(salt);
        let object = AuthorizedObjectReference {
            component: ComponentKind::Spirit,
            digest: ObjectDigest::from_bytes(&bytes),
            kind: AuthorizedObjectKind::Head,
        };
        let stamp = AttestedMoment::new(
            AttestedMomentProposition::new(
                TimeWindow {
                    opens_at: TimestampNanos::new(10),
                    closes_at: TimestampNanos::new(20),
                },
                RequiredSignatureThreshold::new(1),
                Vec::new(),
            ),
            Vec::new(),
        );
        let evidence = Evidence::new(
            ComponentKind::Spirit,
            OperationDigest::from_bytes(&bytes),
            stamp,
            Vec::new(),
            Vec::new(),
        );
        AuthorizationEvaluation {
            contract: ContractDigest::from_bytes(&bytes),
            object,
            evidence,
        }
    }

    /// (2) Submit the evaluation over the working socket; in `ClientApproval` the
    /// reply is `AuthorizationPending`, carrying the parked slot. Returns it.
    fn park(&self, salt: u8) -> AuthorizationRequestSlot {
        let reply = self
            .working
            .send(CriomeRequest::EvaluateAuthorization(Self::evaluation(salt)))
            .expect("EvaluateAuthorization reaches criome over the working socket");
        let CriomeReply::AuthorizationPending(pending) = reply else {
            panic!(
                "ClientApproval parks the request, expected AuthorizationPending, got {reply:?}"
            );
        };
        let slot = pending.request_slot;
        eprintln!(
            "criome-client-approval-witness-test: PROOF (2) salt={salt} EvaluateAuthorization -> AuthorizationPending (parked)"
        );
        slot
    }

    /// (3) List parked authorizations over the meta socket and assert `slot` is
    /// present.
    fn assert_parked(&self, slot: &AuthorizationRequestSlot) {
        let reply = self
            .meta
            .send(meta_signal_criome::Input::ObserveParkedAuthorizations(
                ParkedAuthorizationObservation::new(),
            ))
            .expect("ObserveParkedAuthorizations reaches criome over the meta socket");
        let meta_signal_criome::Output::ParkedAuthorizationSnapshot(snapshot) = reply else {
            panic!("expected ParkedAuthorizationSnapshot, got {reply:?}");
        };
        assert!(
            snapshot
                .parked()
                .iter()
                .any(|parked| &parked.request_slot == slot),
            "the parked snapshot lists the slot just parked, got {:?}",
            snapshot.parked()
        );
        eprintln!(
            "criome-client-approval-witness-test: PROOF (3) parked snapshot contains the slot"
        );
    }

    /// (4) Decide the parked request over the meta socket and assert the daemon
    /// echoes the decision.
    fn decide(&self, slot: &AuthorizationRequestSlot, decision: AuthorizationApprovalDecision) {
        let reply = self
            .meta
            .send(meta_signal_criome::Input::SubmitAuthorizationApproval(
                AuthorizationApproval {
                    request_slot: slot.clone(),
                    decision,
                },
            ))
            .expect("SubmitAuthorizationApproval reaches criome over the meta socket");
        let meta_signal_criome::Output::AuthorizationApprovalRecorded(recorded) = reply else {
            panic!("expected AuthorizationApprovalRecorded, got {reply:?}");
        };
        assert_eq!(
            recorded.decision, decision,
            "the daemon records the submitted decision"
        );
        eprintln!(
            "criome-client-approval-witness-test: PROOF (4) SubmitAuthorizationApproval({decision:?}) -> recorded"
        );
    }

    /// (5) Read the settled verdict over the working socket and assert its status.
    fn assert_status(&self, slot: &AuthorizationRequestSlot, expected: AuthorizationStatus) {
        let reply = self
            .working
            .send(CriomeRequest::ObserveAuthorization(
                AuthorizationObservation::new(slot.clone()),
            ))
            .expect("ObserveAuthorization reaches criome over the working socket");
        let CriomeReply::AuthorizationObservationSnapshot(snapshot) = reply else {
            panic!("expected AuthorizationObservationSnapshot, got {reply:?}");
        };
        let state = snapshot
            .states()
            .iter()
            .find(|state| &state.request_slot == slot)
            .unwrap_or_else(|| {
                panic!(
                    "the observation snapshot carries the slot, got {:?}",
                    snapshot.states()
                )
            });
        assert_eq!(
            state.status, expected,
            "the settled authorization status matches the decision"
        );
        eprintln!(
            "criome-client-approval-witness-test: PROOF (5) ObserveAuthorization -> status {expected:?}"
        );
    }

    /// One full lifecycle: park, list, decide, verify the settled status.
    fn prove_lifecycle(
        &self,
        salt: u8,
        decision: AuthorizationApprovalDecision,
        expected: AuthorizationStatus,
    ) {
        let slot = self.park(salt);
        self.assert_parked(&slot);
        self.decide(&slot, decision);
        self.assert_status(&slot, expected);
    }

    fn run(&self) {
        self.configure_client_approval();
        // Approve path: parked -> Approve -> Granted.
        self.prove_lifecycle(
            0,
            AuthorizationApprovalDecision::Approve,
            AuthorizationStatus::Granted,
        );
        // Reject path: a different request parks under a different slot, is
        // rejected, and settles Denied.
        self.prove_lifecycle(
            1,
            AuthorizationApprovalDecision::Reject,
            AuthorizationStatus::Denied,
        );
        println!(
            "criome-client-approval-witness-test: OK (ClientApproval park/approve->Granted, park/reject->Denied)"
        );
    }
}

fn main() {
    Witness::from_environment().run();
}
