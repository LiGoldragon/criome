//! The criome auto-approve + meta-Configure witness-test (report 704, Spirit
//! `t00s`/`da5i`).
//!
//! Proves the meta socket does its job and the auto-approve verdict mode works,
//! against a REAL `criome-daemon` started in the default `Quorum` mode:
//!
//!   (1) META CONFIGURE — over the meta socket (`CriomeMetaClient`), send
//!       `Configure(config.with_authorization_mode(AutoApprove))`; criome applies
//!       it and replies `Configured` (runtime reconfiguration over the meta
//!       socket — "all components need a meta socket, even if just to configure
//!       them", `da5i`).
//!   (2) AUTO-APPROVE — over the working socket (`CriomeClient`), send an
//!       `EvaluateAuthorization` carrying evidence with NO signatures (which the
//!       quorum path would reject as threshold-short); in AutoApprove the verdict
//!       is `Authorized`.
//!
//! Exit 0 only when both hold. Sockets: `CRIOME_SOCKET` / `CRIOME_META_SOCKET`
//! (defaults `/run/criome/criome.sock` + `<socket>.meta`).

use std::path::PathBuf;

use criome::transport::{CriomeClient, CriomeMetaClient};
use signal_criome::{
    AttestedMoment, AttestedMomentProposition, AuthorizationEvaluation, AuthorizationMode,
    AuthorizedObjectKind, AuthorizedObjectReference, ComponentKind, ContractDigest,
    CriomeDaemonConfiguration, CriomeReply, CriomeRequest, EvaluationDecision, Evidence,
    ObjectDigest, OperationDigest, RequiredSignatureThreshold, TimeWindow, TimestampNanos,
};

/// The witness run: a working-socket client + a meta-socket client + the daemon
/// paths it reconfigures. Every step is a method on the data it acts through.
struct Witness {
    working: CriomeClient,
    meta: CriomeMetaClient,
    socket_path: String,
    store_path: String,
}

impl Witness {
    /// A synthetic spirit head digest `D` (32 bytes); object and operation derive
    /// from the same bytes so the structural integrity check passes.
    fn head_bytes() -> [u8; 32] {
        let mut bytes = [0u8; 32];
        let mut index = 0u8;
        while (index as usize) < bytes.len() {
            bytes[index as usize] = index.wrapping_mul(11).wrapping_add(5);
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
            "criome-auto-approve-witness-test: socket={} meta={}",
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

    /// (1) Reconfigure the running daemon to AutoApprove over the meta socket.
    fn configure_auto_approve(&self) {
        let configuration = CriomeDaemonConfiguration::new(&self.socket_path, &self.store_path)
            .with_authorization_mode(AuthorizationMode::AutoApprove);
        let reply = self
            .meta
            .send(meta_signal_criome::Input::Configure(configuration))
            .expect("Configure reaches criome over the meta socket");
        assert!(
            matches!(reply, meta_signal_criome::Output::Configured(_)),
            "meta Configure(AutoApprove) is applied, got {reply:?}"
        );
        eprintln!(
            "criome-auto-approve-witness-test: PROOF (1) meta Configure(AutoApprove) -> Configured"
        );
    }

    /// (2) An evidence-less evaluation is Authorized under AutoApprove.
    fn evaluate_evidence_less(&self) {
        let bytes = Self::head_bytes();
        let object = AuthorizedObjectReference {
            component: ComponentKind::Spirit,
            digest: ObjectDigest::from_bytes(&bytes),
            kind: AuthorizedObjectKind::Head,
        };
        // A well-formed but signature-less evidence (the quorum path rejects it as
        // threshold-short; AutoApprove authorizes it).
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
        let evaluation = AuthorizationEvaluation {
            contract: ContractDigest::from_bytes(&bytes),
            object,
            evidence,
        };
        let reply = self
            .working
            .send(CriomeRequest::EvaluateAuthorization(evaluation))
            .expect("EvaluateAuthorization reaches criome over the working socket");
        let CriomeReply::AuthorizationEvaluated(evaluated) = reply else {
            panic!("expected AuthorizationEvaluated, got {reply:?}");
        };
        assert!(
            matches!(evaluated.decision, EvaluationDecision::Authorized),
            "AutoApprove authorizes an evidence-less request, got {:?}",
            evaluated.decision
        );
        eprintln!(
            "criome-auto-approve-witness-test: PROOF (2) evidence-less evaluation -> Authorized"
        );
    }

    fn run(&self) {
        self.configure_auto_approve();
        self.evaluate_evidence_less();
        println!(
            "criome-auto-approve-witness-test: OK (meta Configure applied, auto-approve authorizes)"
        );
    }
}

fn main() {
    Witness::from_environment().run();
}
