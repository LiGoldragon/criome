use std::io::BufReader;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::thread;

use criome::actors::root::{Arguments as RootArguments, CriomeRoot, ReadTopology, SubmitRequest};
use criome::actors::store::{InterceptSpiritAuthorization, StoreInterceptPolicy, StoreKernel};
use criome::command::CriomeDaemonCommand;
#[cfg(feature = "nota-text")]
use criome::command::CriomeRequestArgument;
use criome::daemon::{BoundCriomeDaemon, CriomeDaemon};
use criome::daemon::{CriomeDaemonConfiguration, CriomeDaemonConfigurationFile};
use criome::language::{AttestedMomentStatement, OperationStatement};
use criome::master_key::MasterKey;
use criome::tables::StoreLocation;
use criome::transport::{CriomeClient, CriomeFrameCodec, CriomeMetaClient};
use kameo::actor::Spawn;
use meta_signal_criome::{AuthorizationApproval, AuthorizationApprovalDecision};
#[cfg(feature = "nota-text")]
use nota_next::NotaEncode;
use signal_criome::{
    ApprovalAuditSource, AttestedMoment, AttestedMomentProposition, AuditContext,
    AuthorizationDenialReason, AuthorizationDenialSource, AuthorizationEvaluation,
    AuthorizationExpired, AuthorizationGrant, AuthorizationObservation, AuthorizationPolicyClass,
    AuthorizationPolicySatisfaction, AuthorizationRejection, AuthorizationRequestSlot,
    AuthorizationScope, AuthorizationStatus, AuthorizedObjectInterest, AuthorizedObjectKind,
    AuthorizedObjectObservation, AuthorizedObjectUpdateToken, BlsPublicKey, BlsSignature,
    ComponentKind, ComponentObjectInterest, ContentPurpose, ContentReference, Contract,
    ContractName, ContractOperationHead, ContractTimeCheck, CriomeFrame, CriomeFrameBody,
    CriomeReply, CriomeRequest, EscalationTarget, EvaluationDecision, Evidence, ExpiryAction,
    Identity, IdentityLookup, IdentityRegistration, InterceptPolicyCancellation,
    InterceptPolicyProposal, InterceptTargetSelector, KeyPurpose, MentciSessionSlot, ObjectDigest,
    OperationDigest, ParkedRequestAnswer, ParkedRequestDecision, ParkedRequestOutcome,
    ParkedRequestQuery, PolicyDurationNanos, PolicyOverlapMode, PolicyPriority, PrincipalName,
    PrincipalStatus, PublicKeyFingerprint, RawSpiritOperationPayload, RejectionReason, ReplayNonce,
    RequiredSignatureThreshold, Rule, SignRequest, SignalCallAuthorization,
    SignatureAuthorizationResult, SignatureEnvelope, SignatureScheme, SpiritAuthorizationContext,
    SpiritOperationName, SpiritOperationNames, SpiritProcessKey, StampedSignatureEnvelope,
    TimeSignature, TimeWindow, TimestampNanos, WorkflowDigest, WorkflowGuard,
    WorkflowProvenanceDigest, WorkflowReceipt,
};
use signal_frame::{ExchangeIdentifier, ExchangeLane, LaneSequence, RequestPayload, SessionEpoch};

fn synthetic_exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(1),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

fn fixture_path(name: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("criome-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("create fixture dir");
    path
}

fn store_location(name: &str) -> StoreLocation {
    StoreLocation::new(fixture_path(name).join("criome.sema"))
}

fn daemon_configuration(name: &str) -> CriomeDaemonConfiguration {
    let workspace = fixture_path(name);
    CriomeDaemonConfiguration::new(
        workspace.join("criome.sock").display().to_string(),
        workspace.join("criome.sema").display().to_string(),
    )
}

fn send_meta_request(
    daemon: &BoundCriomeDaemon,
    meta_socket: &std::path::Path,
    request: meta_signal_criome::Input,
) -> meta_signal_criome::Output {
    thread::scope(move |scope| {
        let server = scope.spawn(|| daemon.serve_next_meta().expect("serve meta request"));
        let reply = CriomeMetaClient::new(meta_socket)
            .send(request)
            .expect("submit meta request");
        assert_eq!(server.join().expect("join meta server"), reply);
        reply
    })
}

fn intercept_policy_proposal(
    session: &str,
    target: &str,
    operation: &str,
    priority: u64,
) -> InterceptPolicyProposal {
    InterceptPolicyProposal {
        session_slot: MentciSessionSlot::new(session),
        target: InterceptTargetSelector::new(SpiritProcessKey::new(target)),
        spirit_operation_names: SpiritOperationNames::from_names(vec![SpiritOperationName::new(
            operation,
        )]),
        duration: PolicyDurationNanos::new(u64::MAX),
        expiry_action: ExpiryAction::AutoApprove,
        priority: PolicyPriority::new(priority),
        overlap_mode: PolicyOverlapMode::RejectSamePriorityOverlap,
    }
}

fn spirit_authorization_context(
    target: &str,
    operation: &str,
    raw_payload: &str,
) -> SpiritAuthorizationContext {
    SpiritAuthorizationContext {
        operation_name: SpiritOperationName::new(operation),
        raw_payload: RawSpiritOperationPayload::new(raw_payload),
        target_key: SpiritProcessKey::new(target),
    }
}

fn registration(name: &str) -> IdentityRegistration {
    IdentityRegistration::new(
        Identity::developer((name).to_string()),
        BlsPublicKey::new((format!("{name}-public-key")).to_string()),
        PublicKeyFingerprint::new((format!("{name}-fingerprint")).to_string()),
        KeyPurpose::ReleaseAuthorization,
        None,
    )
}

fn registration_with_key(name: &str, public_key: BlsPublicKey) -> IdentityRegistration {
    IdentityRegistration::new(
        Identity::developer((name).to_string()),
        public_key,
        PublicKeyFingerprint::new((format!("{name}-fingerprint")).to_string()),
        KeyPurpose::ReleaseAuthorization,
        None,
    )
}

fn sign_request(name: &str) -> SignRequest {
    SignRequest::new(
        ContentReference {
            digest: ObjectDigest::from_bytes(b"fixture"),
            purpose: ContentPurpose::SignedObject,
            schema_version: PrincipalName::new(("fixture-schema").to_string()),
        },
        Identity::developer((name).to_string()),
        AuditContext {
            purpose: ContentPurpose::SignedObject,
            audience: PrincipalName::new(("fixture-audience").to_string()),
            policy_version: PrincipalName::new(("fixture-policy").to_string()),
            nonce: ReplayNonce::new(("fixture-nonce").to_string()),
        },
        None,
    )
}

fn authorization_scope() -> AuthorizationScope {
    AuthorizationScope::new(("deploy-zeus-full-os").to_string())
}

fn contract_name() -> ContractName {
    ContractName::new(("signal-lojix").to_string())
}

fn contract_operation_head() -> ContractOperationHead {
    ContractOperationHead::new(("Deploy").to_string())
}

fn signal_call_authorization(seed: &[u8]) -> SignalCallAuthorization {
    signal_call_authorization_with_nonce(seed, "authorization-nonce")
}

fn signal_call_authorization_with_nonce(seed: &[u8], nonce: &str) -> SignalCallAuthorization {
    SignalCallAuthorization::new(
        ObjectDigest::from_bytes(seed),
        contract_name(),
        contract_operation_head(),
        authorization_scope(),
        Identity::developer(("operator").to_string()),
        ReplayNonce::new((nonce).to_string()),
        None,
    )
}

fn signature_envelope() -> SignatureEnvelope {
    SignatureEnvelope {
        scheme: SignatureScheme::Bls12_381MinPk,
        public_key: BlsPublicKey::new(("public-key").to_string()),
        signature: BlsSignature::new(("signature").to_string()),
    }
}

fn stamped_signature_envelope() -> StampedSignatureEnvelope {
    StampedSignatureEnvelope {
        stamp: AttestedMoment::new(
            AttestedMomentProposition::new(
                TimeWindow {
                    opens_at: TimestampNanos::new(1),
                    closes_at: TimestampNanos::new(2),
                },
                RequiredSignatureThreshold::new(1),
                vec![Identity::cluster(("timekeeper").to_string())],
            ),
            vec![TimeSignature {
                signer: Identity::cluster(("timekeeper").to_string()),
                envelope: signature_envelope(),
            }],
        ),
        envelope: signature_envelope(),
    }
}

fn operation_digest(seed: &[u8]) -> OperationDigest {
    OperationDigest::from_bytes(seed)
}

fn workflow_digest(seed: &[u8]) -> WorkflowDigest {
    WorkflowDigest::new(ObjectDigest::from_bytes(seed))
}

fn workflow_provenance_digest(seed: &[u8]) -> WorkflowProvenanceDigest {
    WorkflowProvenanceDigest::new(ObjectDigest::from_bytes(seed))
}

fn authorization_grant(seed: &[u8]) -> AuthorizationGrant {
    AuthorizationGrant::new(
        AuthorizationRequestSlot::new(("authorization-grant-slot").to_string()),
        ObjectDigest::from_bytes(seed),
        contract_name(),
        contract_operation_head(),
        authorization_scope(),
        AuthorizationPolicySatisfaction::new(
            AuthorizationPolicyClass::SimpleSelfSigned,
            RequiredSignatureThreshold::new(1),
            vec![Identity::cluster(("criome-master").to_string())],
        ),
        SignatureAuthorizationResult::SingleSignature,
        vec![stamped_signature_envelope()],
        Identity::cluster(("criome-master").to_string()),
        TimestampNanos::new(1),
        None,
    )
}

fn unproven_evidence(seed: &[u8]) -> Evidence {
    let operation = operation_digest(seed);
    Evidence::new(
        ComponentKind::Spirit,
        operation,
        AttestedMoment::new(
            AttestedMomentProposition::new(
                TimeWindow {
                    opens_at: TimestampNanos::new(1),
                    closes_at: TimestampNanos::new(2),
                },
                RequiredSignatureThreshold::new(1),
                Vec::new(),
            ),
            Vec::new(),
        ),
        Vec::new(),
        Vec::new(),
    )
}

fn pending_authorization(reply: CriomeReply) -> signal_criome::AuthorizationPending {
    let CriomeReply::AuthorizationPending(pending) = reply else {
        panic!("expected AuthorizationPending, got {reply:?}");
    };
    pending
}

fn expired_authorization(reply: CriomeReply) -> AuthorizationExpired {
    let CriomeReply::AuthorizationExpired(expired) = reply else {
        panic!("expected AuthorizationExpired, got {reply:?}");
    };
    expired
}

#[tokio::test]
async fn criome_root_starts_data_bearing_kameo_children() {
    let root = CriomeRoot::start(RootArguments::new(store_location("topology")))
        .await
        .expect("start criome root");

    let topology = root.ask(ReadTopology).await.expect("read topology");
    assert!(topology.registry());
    assert!(topology.signer());
    assert!(topology.verifier());
    assert!(topology.authorization());
    assert!(topology.subscription());

    CriomeRoot::stop(root).await.expect("stop criome root");
}

#[tokio::test]
async fn sign_with_unregistered_identity_returns_rejection() {
    let root = CriomeRoot::start(RootArguments::new(store_location("unregistered-sign")))
        .await
        .expect("start criome root");

    let reply = root
        .ask(SubmitRequest::new(CriomeRequest::Sign(sign_request(
            "unknown",
        ))))
        .await
        .expect("submit sign request")
        .into_reply();

    assert_eq!(
        reply,
        CriomeReply::Rejection(signal_criome::Rejection::new(
            RejectionReason::UnknownIdentity
        ))
    );

    CriomeRoot::stop(root).await.expect("stop criome root");
}

#[tokio::test]
async fn authorize_signal_call_records_observable_signing_state() {
    let root = CriomeRoot::start(RootArguments::new(store_location("authorization-pending")))
        .await
        .expect("start criome root");
    let authorization = signal_call_authorization(b"authorize observable request");
    let request_digest = authorization.request_digest.clone();

    let reply = root
        .ask(SubmitRequest::new(CriomeRequest::AuthorizeSignalCall(
            authorization,
        )))
        .await
        .expect("submit authorization request")
        .into_reply();
    let pending = pending_authorization(reply);
    assert_eq!(pending.request_digest, request_digest);
    assert!(pending.missing_authorities().is_empty());

    let snapshot = root
        .ask(SubmitRequest::new(CriomeRequest::ObserveAuthorization(
            AuthorizationObservation::new(pending.request_slot.clone()),
        )))
        .await
        .expect("observe authorization")
        .into_reply();
    let CriomeReply::AuthorizationObservationSnapshot(snapshot) = snapshot else {
        panic!("expected AuthorizationObservationSnapshot, got {snapshot:?}");
    };
    assert_eq!(snapshot.states().len(), 1);
    assert_eq!(snapshot.states()[0].request_slot, pending.request_slot);
    assert_eq!(snapshot.states()[0].request_digest, request_digest);
    assert_eq!(snapshot.states()[0].status, AuthorizationStatus::Signing);

    CriomeRoot::stop(root).await.expect("stop criome root");
}

#[tokio::test]
async fn authorization_slots_are_store_minted_not_request_digest_derived() {
    let root = CriomeRoot::start(RootArguments::new(store_location("authorization-slots")))
        .await
        .expect("start criome root");
    let authorization =
        signal_call_authorization_with_nonce(b"same authorization request", "first-nonce");
    let request_digest = authorization.request_digest.clone();

    let first = pending_authorization(
        root.ask(SubmitRequest::new(CriomeRequest::AuthorizeSignalCall(
            authorization.clone(),
        )))
        .await
        .expect("submit first authorization")
        .into_reply(),
    );
    let second = pending_authorization(
        root.ask(SubmitRequest::new(CriomeRequest::AuthorizeSignalCall(
            SignalCallAuthorization::new(
                authorization.request_digest.clone(),
                authorization.contract.clone(),
                authorization.operation.clone(),
                authorization.scope.clone(),
                authorization.requester.clone(),
                ReplayNonce::new(("second-nonce").to_string()),
                authorization.expires_at(),
            ),
        )))
        .await
        .expect("submit second authorization")
        .into_reply(),
    );

    assert_eq!(first.request_digest, request_digest);
    assert_eq!(second.request_digest, request_digest);
    assert_ne!(first.request_slot, second.request_slot);
    assert_ne!(first.request_slot.as_str(), request_digest.as_str());
    assert_ne!(second.request_slot.as_str(), request_digest.as_str());

    CriomeRoot::stop(root).await.expect("stop criome root");
}

#[tokio::test]
async fn expired_authorization_records_expired_state_instead_of_signing() {
    let root = CriomeRoot::start(RootArguments::new(store_location("authorization-expired")))
        .await
        .expect("start criome root");
    let authorization = SignalCallAuthorization::new(
        ObjectDigest::from_bytes(b"expired authorization request"),
        contract_name(),
        contract_operation_head(),
        authorization_scope(),
        Identity::developer(("operator").to_string()),
        ReplayNonce::new(("expired-nonce").to_string()),
        Some(TimestampNanos::new(0)),
    );
    let request_digest = authorization.request_digest.clone();

    let expired = expired_authorization(
        root.ask(SubmitRequest::new(CriomeRequest::AuthorizeSignalCall(
            authorization,
        )))
        .await
        .expect("submit expired authorization")
        .into_reply(),
    );
    assert_eq!(expired.expired_at, TimestampNanos::new(0));

    let snapshot = root
        .ask(SubmitRequest::new(CriomeRequest::ObserveAuthorization(
            AuthorizationObservation::new(expired.request_slot.clone()),
        )))
        .await
        .expect("observe expired authorization")
        .into_reply();
    let CriomeReply::AuthorizationObservationSnapshot(snapshot) = snapshot else {
        panic!("expected AuthorizationObservationSnapshot, got {snapshot:?}");
    };
    assert_eq!(snapshot.states().len(), 1);
    assert_eq!(snapshot.states()[0].request_slot, expired.request_slot);
    assert_eq!(snapshot.states()[0].request_digest, request_digest);
    assert_eq!(snapshot.states()[0].status, AuthorizationStatus::Expired);

    CriomeRoot::stop(root).await.expect("stop criome root");
}

#[tokio::test]
async fn authorization_replay_nonce_rejects_changed_digest_reuse() {
    let root = CriomeRoot::start(RootArguments::new(store_location("authorization-replay")))
        .await
        .expect("start criome root");
    let first =
        signal_call_authorization_with_nonce(b"first authorization request", "replayed-nonce");
    let second =
        signal_call_authorization_with_nonce(b"second authorization request", "replayed-nonce");

    let first_reply = root
        .ask(SubmitRequest::new(CriomeRequest::AuthorizeSignalCall(
            first,
        )))
        .await
        .expect("submit first authorization")
        .into_reply();
    let _pending = pending_authorization(first_reply);

    let second_reply = root
        .ask(SubmitRequest::new(CriomeRequest::AuthorizeSignalCall(
            second,
        )))
        .await
        .expect("submit replayed authorization")
        .into_reply();
    assert_eq!(
        second_reply,
        CriomeReply::Rejection(signal_criome::Rejection::new(
            RejectionReason::ReplayAttempted
        ))
    );

    CriomeRoot::stop(root).await.expect("stop criome root");
}

#[tokio::test]
async fn verify_authorization_rejects_digest_mismatch() {
    let root = CriomeRoot::start(RootArguments::new(store_location("authorization-mismatch")))
        .await
        .expect("start criome root");

    let reply = root
        .ask(SubmitRequest::new(CriomeRequest::VerifyAuthorization(
            signal_criome::AuthorizationVerification {
                request_digest: ObjectDigest::from_bytes(b"request-b"),
                authorization: authorization_grant(b"request-a"),
            },
        )))
        .await
        .expect("verify authorization")
        .into_reply();
    let CriomeReply::AuthorizationDenied(denied) = reply else {
        panic!("expected AuthorizationDenied, got {reply:?}");
    };
    assert_eq!(denied.denial.source, AuthorizationDenialSource::Policy);
    assert_eq!(
        denied.denial.reason,
        AuthorizationDenialReason::RequestDigestMismatch,
    );

    CriomeRoot::stop(root).await.expect("stop criome root");
}

#[tokio::test]
async fn registered_signer_attestation_verifies_under_real_bls() {
    let root = CriomeRoot::start(RootArguments::new(store_location("real-bls-roundtrip")))
        .await
        .expect("start criome root");

    // The requesting identity must be registered and active to be allowed an
    // attestation; criome then signs as itself with its master key.
    root.ask(SubmitRequest::new(CriomeRequest::RegisterIdentity(
        registration("operator"),
    )))
    .await
    .expect("register operator identity");

    let sign_reply = root
        .ask(SubmitRequest::new(CriomeRequest::Sign(sign_request(
            "operator",
        ))))
        .await
        .expect("submit sign request")
        .into_reply();
    let CriomeReply::SignReceipt(receipt) = sign_reply else {
        panic!("expected SignReceipt, got {sign_reply:?}");
    };
    let attestation = receipt.attestation;
    let content = attestation.content.clone();

    // A real BLS signature over the canonical preimage verifies as Valid.
    let verify_reply = root
        .ask(SubmitRequest::new(CriomeRequest::VerifyAttestation(
            signal_criome::VerifyRequest {
                attestation: attestation.clone(),
                content,
            },
        )))
        .await
        .expect("submit verify request")
        .into_reply();
    let CriomeReply::VerificationResult(result) = verify_reply else {
        panic!("expected VerificationResult, got {verify_reply:?}");
    };
    assert_eq!(result.decision, signal_criome::VerificationDecision::Valid);

    // A tampered attestation (different content digest) must not verify.
    let mut tampered = attestation;
    tampered.content.digest = ObjectDigest::from_bytes(b"tampered");
    let tampered_content = tampered.content.clone();
    let tampered_reply = root
        .ask(SubmitRequest::new(CriomeRequest::VerifyAttestation(
            signal_criome::VerifyRequest {
                attestation: tampered,
                content: tampered_content,
            },
        )))
        .await
        .expect("submit tampered verify request")
        .into_reply();
    let CriomeReply::VerificationResult(tampered_result) = tampered_reply else {
        panic!("expected VerificationResult, got {tampered_reply:?}");
    };
    assert_eq!(
        tampered_result.decision,
        signal_criome::VerificationDecision::InvalidSignature
    );

    CriomeRoot::stop(root).await.expect("stop criome root");
}

#[tokio::test]
async fn criome_root_admits_and_evaluates_policy_contracts() {
    let store = store_location("policy-contracts");
    let root = CriomeRoot::start(RootArguments::new(store.clone()))
        .await
        .expect("start criome root");
    let signer = MasterKey::generate().expect("policy signer key");
    let timekeeper = MasterKey::generate().expect("timekeeper key");
    let identity = Identity::developer(("operator").to_string());
    let timekeeper_identity = Identity::cluster(("timekeeper").to_string());

    root.ask(SubmitRequest::new(CriomeRequest::RegisterIdentity(
        registration_with_key("operator", signer.public_key()),
    )))
    .await
    .expect("register policy signer")
    .into_reply();
    root.ask(SubmitRequest::new(CriomeRequest::RegisterIdentity(
        IdentityRegistration::new(
            timekeeper_identity.clone(),
            timekeeper.public_key(),
            PublicKeyFingerprint::new(("timekeeper-fingerprint").to_string()),
            KeyPurpose::ReleaseAuthorization,
            None,
        ),
    )))
    .await
    .expect("register timekeeper")
    .into_reply();

    let contract = Contract::new(Rule::SignedBy(identity.clone()));
    let expected_contract = contract.clone();
    let admitted = root
        .ask(SubmitRequest::new(CriomeRequest::AdmitContract(contract)))
        .await
        .expect("admit contract")
        .into_reply();
    let CriomeReply::ContractAdmitted(admitted) = admitted else {
        panic!("expected ContractAdmitted, got {admitted:?}");
    };
    let digest = admitted.into_payload();
    let operation = operation_digest(b"policy-evaluation");
    let proposition = AttestedMomentProposition::new(
        TimeWindow {
            opens_at: TimestampNanos::new(10),
            closes_at: TimestampNanos::new(20),
        },
        RequiredSignatureThreshold::new(1),
        vec![timekeeper_identity.clone()],
    );
    let stamp = AttestedMoment::new(
        proposition.clone(),
        vec![TimeSignature {
            signer: timekeeper_identity.clone(),
            envelope: SignatureEnvelope {
                scheme: SignatureScheme::Bls12_381MinPk,
                public_key: timekeeper.public_key(),
                signature: timekeeper.sign(
                    AttestedMomentStatement::new(&proposition)
                        .to_signing_bytes()
                        .expect("moment statement")
                        .as_slice(),
                ),
            },
        }],
    );
    let statement = OperationStatement::new(&identity, &operation, &stamp)
        .to_signing_bytes()
        .expect("operation statement");
    let evidence = Evidence::new(
        ComponentKind::Spirit,
        operation,
        stamp.clone(),
        vec![StampedSignatureEnvelope {
            stamp,
            envelope: SignatureEnvelope {
                scheme: SignatureScheme::Bls12_381MinPk,
                public_key: signer.public_key(),
                signature: signer.sign(&statement),
            },
        }],
        Vec::new(),
    );
    let authorized_head = signal_criome::AuthorizedObjectReference {
        component: ComponentKind::Spirit,
        digest: evidence.operation.object_digest().clone(),
        kind: AuthorizedObjectKind::Head,
    };

    let evaluation = AuthorizationEvaluation {
        contract: digest.clone(),
        object: authorized_head.clone(),
        evidence: evidence.clone(),
    };
    let evaluated = root
        .ask(SubmitRequest::new(CriomeRequest::EvaluateAuthorization(
            evaluation.clone(),
        )))
        .await
        .expect("evaluate contract")
        .into_reply();
    let CriomeReply::AuthorizationEvaluated(evaluated) = evaluated else {
        panic!("expected AuthorizationEvaluated, got {evaluated:?}");
    };
    assert_eq!(evaluated.decision, EvaluationDecision::Authorized);
    let observer = Identity::agent("component-observer".to_string());
    let snapshot = root
        .ask(SubmitRequest::new(CriomeRequest::ObserveAuthorizedObjects(
            AuthorizedObjectObservation {
                subscriber: observer.clone(),
                interest: AuthorizedObjectInterest::Component(ComponentKind::Spirit),
            },
        )))
        .await
        .expect("observe authorized objects")
        .into_reply();
    let CriomeReply::AuthorizedObjectUpdateSnapshot(snapshot) = snapshot else {
        panic!("expected AuthorizedObjectUpdateSnapshot, got {snapshot:?}");
    };
    let updates = snapshot.into_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].object, authorized_head);
    assert_eq!(updates[0].contract, digest);
    assert_eq!(updates[0].decision, EvaluationDecision::Authorized);
    assert_eq!(updates[0].stamp, evidence.stamp);

    let rejected_mismatch = root
        .ask(SubmitRequest::new(CriomeRequest::EvaluateAuthorization(
            AuthorizationEvaluation {
                contract: digest.clone(),
                object: signal_criome::AuthorizedObjectReference {
                    component: ComponentKind::Spirit,
                    digest: ObjectDigest::from_bytes(b"not-the-signed-operation"),
                    kind: AuthorizedObjectKind::Head,
                },
                evidence: evidence.clone(),
            },
        )))
        .await
        .expect("reject mismatched authorization object")
        .into_reply();
    assert_eq!(
        rejected_mismatch,
        CriomeReply::Rejection(signal_criome::Rejection::new(
            RejectionReason::MalformedRequest
        ))
    );

    let mirror_snapshot = root
        .ask(SubmitRequest::new(CriomeRequest::ObserveAuthorizedObjects(
            AuthorizedObjectObservation {
                subscriber: Identity::agent("mirror-observer".to_string()),
                interest: AuthorizedObjectInterest::Component(ComponentKind::Mirror),
            },
        )))
        .await
        .expect("observe mirror authorized objects")
        .into_reply();
    let CriomeReply::AuthorizedObjectUpdateSnapshot(mirror_snapshot) = mirror_snapshot else {
        panic!("expected AuthorizedObjectUpdateSnapshot, got {mirror_snapshot:?}");
    };
    assert!(
        mirror_snapshot.into_updates().is_empty(),
        "component filters keep unrelated pulses out of snapshots"
    );

    let time_result = signal_criome::AuthorizedObjectReference {
        component: ComponentKind::Spirit,
        digest: ObjectDigest::from_bytes(b"timeout-result"),
        kind: AuthorizedObjectKind::Time,
    };
    let check = ContractTimeCheck {
        contract: digest.clone(),
        due_at: TimestampNanos::new(25),
        result: time_result.clone(),
        absent: AuthorizedObjectInterest::ComponentObject(ComponentObjectInterest {
            component: ComponentKind::Mirror,
            kind: AuthorizedObjectKind::Operation,
        }),
    };
    let scheduled = root
        .ask(SubmitRequest::new(
            CriomeRequest::ScheduleContractTimeCheck(check.clone()),
        ))
        .await
        .expect("schedule contract time check")
        .into_reply();
    assert_eq!(
        scheduled,
        CriomeReply::ContractTimeCheckScheduled(signal_criome::ContractTimeCheckScheduled::new(
            check
        ))
    );

    let later_proposition = AttestedMomentProposition::new(
        TimeWindow {
            opens_at: TimestampNanos::new(30),
            closes_at: TimestampNanos::new(40),
        },
        RequiredSignatureThreshold::new(1),
        vec![timekeeper_identity.clone()],
    );
    let later_stamp = AttestedMoment::new(
        later_proposition.clone(),
        vec![TimeSignature {
            signer: timekeeper_identity,
            envelope: SignatureEnvelope {
                scheme: SignatureScheme::Bls12_381MinPk,
                public_key: timekeeper.public_key(),
                signature: timekeeper.sign(
                    AttestedMomentStatement::new(&later_proposition)
                        .to_signing_bytes()
                        .expect("later moment statement")
                        .as_slice(),
                ),
            },
        }],
    );
    let due = root
        .ask(SubmitRequest::new(CriomeRequest::RunDueContractChecks(
            later_stamp.clone(),
        )))
        .await
        .expect("run due contract checks")
        .into_reply();
    let CriomeReply::DueContractChecksEvaluated(due) = due else {
        panic!("expected DueContractChecksEvaluated, got {due:?}");
    };
    let triggered = due.into_triggered();
    assert_eq!(triggered.len(), 1);
    assert_eq!(triggered[0].object, time_result);
    assert_eq!(triggered[0].contract, digest);
    assert_eq!(triggered[0].stamp, later_stamp);

    let time_snapshot = root
        .ask(SubmitRequest::new(CriomeRequest::ObserveAuthorizedObjects(
            AuthorizedObjectObservation {
                subscriber: observer.clone(),
                interest: AuthorizedObjectInterest::ObjectKind(AuthorizedObjectKind::Time),
            },
        )))
        .await
        .expect("observe time authorized objects")
        .into_reply();
    let CriomeReply::AuthorizedObjectUpdateSnapshot(time_snapshot) = time_snapshot else {
        panic!("expected AuthorizedObjectUpdateSnapshot, got {time_snapshot:?}");
    };
    assert_eq!(time_snapshot.into_updates().len(), 1);

    let retracted = root
        .ask(SubmitRequest::new(
            CriomeRequest::AuthorizedObjectUpdateRetraction(AuthorizedObjectUpdateToken {
                subscriber: observer.clone(),
                interest: AuthorizedObjectInterest::Component(ComponentKind::Spirit),
            }),
        ))
        .await
        .expect("close authorized object observation")
        .into_reply();
    assert!(matches!(
        retracted,
        CriomeReply::AuthorizedObjectUpdateRetracted(_)
    ));

    let time_retracted = root
        .ask(SubmitRequest::new(
            CriomeRequest::AuthorizedObjectUpdateRetraction(AuthorizedObjectUpdateToken {
                subscriber: observer,
                interest: AuthorizedObjectInterest::ObjectKind(AuthorizedObjectKind::Time),
            }),
        ))
        .await
        .expect("close time authorized object observation")
        .into_reply();
    assert!(
        matches!(
            time_retracted,
            CriomeReply::AuthorizedObjectUpdateRetracted(_)
        ),
        "authorized-object retraction is scoped to one subscriber interest"
    );

    CriomeRoot::stop(root).await.expect("stop criome root");

    let restarted = CriomeRoot::start(RootArguments::new(store))
        .await
        .expect("restart criome root");
    let found = restarted
        .ask(SubmitRequest::new(CriomeRequest::LookupContract(
            digest.clone(),
        )))
        .await
        .expect("lookup persisted contract")
        .into_reply();
    assert_eq!(
        found,
        CriomeReply::ContractFound(signal_criome::ContractFound {
            digest: digest.clone(),
            contract: expected_contract,
        })
    );
    let evaluated_after_restart = restarted
        .ask(SubmitRequest::new(CriomeRequest::EvaluateAuthorization(
            evaluation,
        )))
        .await
        .expect("evaluate persisted contract")
        .into_reply();
    let CriomeReply::AuthorizationEvaluated(evaluated_after_restart) = evaluated_after_restart
    else {
        panic!("expected AuthorizationEvaluated, got {evaluated_after_restart:?}");
    };
    assert_eq!(
        evaluated_after_restart.decision,
        EvaluationDecision::Authorized
    );

    CriomeRoot::stop(restarted)
        .await
        .expect("stop restarted criome root");
}

#[tokio::test]
async fn criome_root_evaluates_workflow_rule_from_local_receipt() {
    let root = CriomeRoot::start(RootArguments::new(store_location("workflow-receipts")))
        .await
        .expect("start criome root");
    let timekeeper = MasterKey::generate().expect("timekeeper key");
    let timekeeper_identity = Identity::cluster(("timekeeper").to_string());

    root.ask(SubmitRequest::new(CriomeRequest::RegisterIdentity(
        IdentityRegistration::new(
            timekeeper_identity.clone(),
            timekeeper.public_key(),
            PublicKeyFingerprint::new(("timekeeper-fingerprint").to_string()),
            KeyPurpose::ReleaseAuthorization,
            None,
        ),
    )))
    .await
    .expect("register timekeeper")
    .into_reply();

    let workflow = workflow_digest(b"spirit guardian workflow");
    let contract = Contract::new(Rule::Workflow(WorkflowGuard {
        workflow: workflow.clone(),
        executor: Identity::host(("orchestrate").to_string()),
    }));
    let admitted = root
        .ask(SubmitRequest::new(CriomeRequest::AdmitContract(contract)))
        .await
        .expect("admit workflow contract")
        .into_reply();
    let CriomeReply::ContractAdmitted(admitted) = admitted else {
        panic!("expected ContractAdmitted, got {admitted:?}");
    };
    let contract_digest = admitted.into_payload();
    let operation = operation_digest(b"spirit-head-that-would-be-guarded");
    let proposition = AttestedMomentProposition::new(
        TimeWindow {
            opens_at: TimestampNanos::new(10),
            closes_at: TimestampNanos::new(20),
        },
        RequiredSignatureThreshold::new(1),
        vec![timekeeper_identity.clone()],
    );
    let stamp = AttestedMoment::new(
        proposition.clone(),
        vec![TimeSignature {
            signer: timekeeper_identity,
            envelope: SignatureEnvelope {
                scheme: SignatureScheme::Bls12_381MinPk,
                public_key: timekeeper.public_key(),
                signature: timekeeper.sign(
                    AttestedMomentStatement::new(&proposition)
                        .to_signing_bytes()
                        .expect("moment statement")
                        .as_slice(),
                ),
            },
        }],
    );
    let evidence = Evidence::new(
        ComponentKind::Spirit,
        operation.clone(),
        stamp,
        Vec::new(),
        Vec::new(),
    );
    let object = signal_criome::AuthorizedObjectReference {
        component: ComponentKind::Spirit,
        digest: operation.object_digest().clone(),
        kind: AuthorizedObjectKind::Head,
    };

    let absent_receipt = root
        .ask(SubmitRequest::new(CriomeRequest::EvaluateAuthorization(
            AuthorizationEvaluation {
                contract: contract_digest.clone(),
                object: object.clone(),
                evidence: evidence.clone(),
            },
        )))
        .await
        .expect("evaluate workflow without receipt")
        .into_reply();
    let CriomeReply::AuthorizationEvaluated(absent_receipt) = absent_receipt else {
        panic!("expected AuthorizationEvaluated, got {absent_receipt:?}");
    };
    assert_eq!(
        absent_receipt.decision,
        EvaluationDecision::Escalate(EscalationTarget::Workflow(workflow.clone()))
    );

    let receipt = WorkflowReceipt {
        workflow,
        operation,
        outcome: EvaluationDecision::Authorized,
        provenance: workflow_provenance_digest(b"fixture-workflow-run-log"),
    };
    let accepted_receipt = root
        .ask(SubmitRequest::new(CriomeRequest::EvaluateAuthorization(
            AuthorizationEvaluation {
                contract: contract_digest,
                object,
                evidence: evidence.with_workflow_receipts(vec![receipt]),
            },
        )))
        .await
        .expect("evaluate workflow with receipt")
        .into_reply();
    let CriomeReply::AuthorizationEvaluated(accepted_receipt) = accepted_receipt else {
        panic!("expected AuthorizationEvaluated, got {accepted_receipt:?}");
    };
    assert_eq!(accepted_receipt.decision, EvaluationDecision::Authorized);

    CriomeRoot::stop(root).await.expect("stop criome root");
}

#[tokio::test]
async fn parked_authorization_snapshot_sorts_slots_numerically() {
    let root = CriomeRoot::start(RootArguments {
        store: store_location("parked-numeric-order"),
        cluster_root: None,
        authorization_mode: signal_criome::AuthorizationMode::ClientApproval,
        node_identity: RootArguments::default_node_identity(),
    })
    .await
    .expect("start criome root");

    let mut expected = Vec::new();
    for index in 0..11 {
        let evidence = unproven_evidence(format!("parked-{index}").as_bytes());
        let object = signal_criome::AuthorizedObjectReference {
            component: ComponentKind::Spirit,
            digest: evidence.operation.object_digest().clone(),
            kind: AuthorizedObjectKind::Head,
        };
        let reply = root
            .ask(SubmitRequest::new(CriomeRequest::EvaluateAuthorization(
                AuthorizationEvaluation {
                    contract: signal_criome::ContractDigest::from_bytes(
                        format!("contract-{index}").as_bytes(),
                    ),
                    object,
                    evidence,
                },
            )))
            .await
            .expect("park authorization")
            .into_reply();
        let CriomeReply::AuthorizationPending(pending) = reply else {
            panic!("expected AuthorizationPending, got {reply:?}");
        };
        expected.push(pending.request_slot);
    }

    let snapshot = root
        .ask(SubmitRequest::new(
            CriomeRequest::ObserveParkedAuthorizations(
                signal_criome::ParkedAuthorizationObservation::new(),
            ),
        ))
        .await
        .expect("observe parked")
        .into_reply();
    let CriomeReply::ParkedAuthorizationSnapshot(snapshot) = snapshot else {
        panic!("expected ParkedAuthorizationSnapshot, got {snapshot:?}");
    };
    let actual: Vec<_> = snapshot
        .parked()
        .iter()
        .map(|parked| parked.request_slot.clone())
        .collect();
    assert_eq!(actual, expected);

    CriomeRoot::stop(root).await.expect("stop criome root");
}

#[tokio::test]
async fn expired_attestation_verifies_as_expired() {
    let root = CriomeRoot::start(RootArguments::new(store_location("expired-attestation")))
        .await
        .expect("start criome root");
    root.ask(SubmitRequest::new(CriomeRequest::RegisterIdentity(
        registration("operator"),
    )))
    .await
    .expect("register operator identity");

    // Sign with an expiry one nanosecond after the epoch — long past.
    let request = SignRequest::new(
        ContentReference {
            digest: ObjectDigest::from_bytes(b"fixture"),
            purpose: ContentPurpose::SignedObject,
            schema_version: PrincipalName::new(("fixture-schema").to_string()),
        },
        Identity::developer(("operator").to_string()),
        AuditContext {
            purpose: ContentPurpose::SignedObject,
            audience: PrincipalName::new(("fixture-audience").to_string()),
            policy_version: PrincipalName::new(("fixture-policy").to_string()),
            nonce: ReplayNonce::new(("fixture-nonce").to_string()),
        },
        Some(TimestampNanos::new(1)),
    );
    let sign_reply = root
        .ask(SubmitRequest::new(CriomeRequest::Sign(request)))
        .await
        .expect("submit sign request")
        .into_reply();
    let CriomeReply::SignReceipt(receipt) = sign_reply else {
        panic!("expected SignReceipt, got {sign_reply:?}");
    };
    let attestation = receipt.attestation;
    let content = attestation.content.clone();

    let verify_reply = root
        .ask(SubmitRequest::new(CriomeRequest::VerifyAttestation(
            signal_criome::VerifyRequest {
                attestation,
                content,
            },
        )))
        .await
        .expect("submit verify request")
        .into_reply();
    let CriomeReply::VerificationResult(result) = verify_reply else {
        panic!("expected VerificationResult, got {verify_reply:?}");
    };
    assert_eq!(
        result.decision,
        signal_criome::VerificationDecision::Expired
    );

    CriomeRoot::stop(root).await.expect("stop criome root");
}

#[tokio::test]
async fn unsupported_signature_scheme_is_rejected() {
    let root = CriomeRoot::start(RootArguments::new(store_location("scheme-mismatch")))
        .await
        .expect("start criome root");
    root.ask(SubmitRequest::new(CriomeRequest::RegisterIdentity(
        registration("operator"),
    )))
    .await
    .expect("register operator identity");

    let sign_reply = root
        .ask(SubmitRequest::new(CriomeRequest::Sign(sign_request(
            "operator",
        ))))
        .await
        .expect("submit sign request")
        .into_reply();
    let CriomeReply::SignReceipt(receipt) = sign_reply else {
        panic!("expected SignReceipt, got {sign_reply:?}");
    };
    let mut attestation = receipt.attestation;
    // Relabel the envelope with an unsupported scheme; it must be rejected, not
    // parsed as min-pk bytes.
    attestation.envelope.scheme = SignatureScheme::Bls12_381MinSig;
    let content = attestation.content.clone();

    let verify_reply = root
        .ask(SubmitRequest::new(CriomeRequest::VerifyAttestation(
            signal_criome::VerifyRequest {
                attestation,
                content,
            },
        )))
        .await
        .expect("submit verify request")
        .into_reply();
    let CriomeReply::VerificationResult(result) = verify_reply else {
        panic!("expected VerificationResult, got {verify_reply:?}");
    };
    assert_eq!(
        result.decision,
        signal_criome::VerificationDecision::InvalidSignature
    );

    CriomeRoot::stop(root).await.expect("stop criome root");
}

#[tokio::test]
async fn restored_store_with_mismatched_master_key_fails_startup() {
    let workspace = fixture_path("reconcile-mismatch");
    let store_path = workspace.join("criome.sema");
    let key_path = workspace.join("criome.masterkey");

    // First start generates master key A and registers Host("criome") = A.
    let root = CriomeRoot::start(RootArguments::new(StoreLocation::new(store_path.clone())))
        .await
        .expect("first start generates and registers");
    CriomeRoot::stop(root).await.expect("stop criome root");

    // Simulate a restored store whose adjacent key file was lost/regenerated.
    std::fs::remove_file(&key_path).expect("remove master key file");

    // Second start regenerates key B; reconcile must reject the A/B mismatch.
    let result = CriomeRoot::start(RootArguments::new(StoreLocation::new(store_path))).await;
    assert!(
        result.is_err(),
        "a master key that does not match the registered criome identity must fail startup"
    );
}

#[tokio::test]
async fn cluster_root_gates_registration() {
    use criome::admission::RegistrationStatement;

    let workspace = fixture_path("cluster-root-gate");
    let cluster_root = MasterKey::generate().expect("cluster root key");
    let root = CriomeRoot::start(RootArguments {
        store: StoreLocation::new(workspace.join("criome.sema")),
        cluster_root: Some(cluster_root.public_key()),
        authorization_mode: signal_criome::AuthorizationMode::Quorum,
        node_identity: RootArguments::default_node_identity(),
    })
    .await
    .expect("start criome root");

    // Without a cluster-root admission, an external registration is refused.
    let unadmitted = root
        .ask(SubmitRequest::new(CriomeRequest::RegisterIdentity(
            registration("operator"),
        )))
        .await
        .expect("submit unadmitted registration")
        .into_reply();
    assert_eq!(
        unadmitted,
        CriomeReply::Rejection(signal_criome::Rejection::new(
            RejectionReason::UnauthorizedRegistration
        ))
    );

    // A registration carrying a valid cluster-root admission over its statement
    // is accepted.
    let registration = registration("operator");
    let statement = RegistrationStatement::from_registration(&registration).to_signing_bytes();
    let admitted = IdentityRegistration::new(
        registration.identity,
        registration.public_key,
        registration.fingerprint,
        registration.purpose,
        Some(SignatureEnvelope {
            scheme: SignatureScheme::Bls12_381MinPk,
            public_key: cluster_root.public_key(),
            signature: cluster_root.sign(&statement),
        }),
    );
    let accepted = root
        .ask(SubmitRequest::new(CriomeRequest::RegisterIdentity(
            admitted,
        )))
        .await
        .expect("submit admitted registration")
        .into_reply();
    assert!(matches!(accepted, CriomeReply::IdentityReceipt(_)));

    CriomeRoot::stop(root).await.expect("stop criome root");
}

#[test]
fn criome_daemon_signal_frame_registers_identity() {
    let workspace = fixture_path("daemon-registers");
    let socket = workspace.join("criome.sock");
    let store = StoreLocation::new(workspace.join("criome.sema"));
    let daemon = CriomeDaemon::new(&socket, store);
    let served = thread::spawn(move || daemon.serve_one().expect("serve one request"));

    wait_for_socket(&socket);

    let reply = CriomeClient::new(&socket)
        .send(CriomeRequest::RegisterIdentity(registration("operator")))
        .expect("send register request");

    assert_eq!(
        reply,
        CriomeReply::IdentityReceipt(signal_criome::IdentityReceipt {
            identity: Identity::developer(("operator").to_string()),
            status: PrincipalStatus::Active,
        })
    );
    assert_eq!(served.join().expect("join daemon"), reply);
}

#[test]
fn criome_daemon_meta_socket_is_user_private() {
    let workspace = fixture_path("socket-mode");
    let socket = workspace.join("criome.sock");
    let meta_socket = workspace.join("criome-meta.sock");
    let store = StoreLocation::new(workspace.join("criome.sema"));
    let daemon = CriomeDaemon::new(&socket, store)
        .with_meta_socket(&meta_socket)
        .bind()
        .expect("bind daemon");

    let mode = std::fs::metadata(daemon.socket())
        .expect("read socket metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600);
    let meta_mode = std::fs::metadata(daemon.meta_socket())
        .expect("read meta socket metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(meta_mode, 0o600);

    daemon.shutdown().expect("shutdown daemon");
}

#[test]
fn meta_socket_configure_auto_approve_authorizes_without_quorum_evidence() {
    let workspace = fixture_path("meta-configure-auto-approve");
    let socket = workspace.join("criome.sock");
    let meta_socket = workspace.join("criome-meta.sock");
    let store = StoreLocation::new(workspace.join("criome.sema"));
    let daemon = CriomeDaemon::new(&socket, store.clone())
        .with_meta_socket(&meta_socket)
        .bind()
        .expect("bind daemon");
    wait_for_socket(&socket);
    wait_for_socket(&meta_socket);

    let configured = thread::scope(|scope| {
        let server = scope.spawn(|| daemon.serve_next_meta().expect("serve meta configure"));
        let configuration = CriomeDaemonConfiguration::new(
            socket.display().to_string(),
            store.as_path().display().to_string(),
        )
        .with_meta_socket_path(meta_socket.display().to_string())
        .with_authorization_mode(signal_criome::AuthorizationMode::AutoApprove);
        let reply = CriomeMetaClient::new(&meta_socket)
            .send(meta_signal_criome::Input::Configure(configuration))
            .expect("submit meta configure");
        assert_eq!(server.join().expect("join meta configure server"), reply);
        reply
    });
    let meta_signal_criome::Output::Configured(configured) = configured else {
        panic!("expected Configured, got {configured:?}");
    };
    assert_eq!(configured.payload().value(), 1);

    let evidence = unproven_evidence(b"auto-approved-head");
    let object = signal_criome::AuthorizedObjectReference {
        component: ComponentKind::Spirit,
        digest: evidence.operation.object_digest().clone(),
        kind: AuthorizedObjectKind::Head,
    };
    let contract = signal_criome::ContractDigest::from_bytes(b"unadmitted-auto-contract");
    let evaluation = AuthorizationEvaluation {
        contract: contract.clone(),
        object: object.clone(),
        evidence: evidence.clone(),
    };

    let approved = thread::scope(|scope| {
        let server = scope.spawn(|| daemon.serve_next().expect("serve auto approve"));
        let reply = CriomeClient::new(&socket)
            .send(CriomeRequest::EvaluateAuthorization(evaluation))
            .expect("evaluate auto approve");
        assert_eq!(server.join().expect("join auto approve server"), reply);
        reply
    });
    let CriomeReply::AuthorizationEvaluated(approved) = approved else {
        panic!("expected AuthorizationEvaluated, got {approved:?}");
    };
    assert_eq!(approved.decision, EvaluationDecision::Authorized);

    let snapshot = thread::scope(|scope| {
        let server = scope.spawn(|| daemon.serve_next().expect("serve authorized observation"));
        let reply = CriomeClient::new(&socket)
            .send(CriomeRequest::ObserveAuthorizedObjects(
                AuthorizedObjectObservation {
                    subscriber: Identity::agent("auto-approve-observer".to_string()),
                    interest: AuthorizedObjectInterest::Component(ComponentKind::Spirit),
                },
            ))
            .expect("observe authorized objects");
        assert_eq!(server.join().expect("join observation server"), reply);
        reply
    });
    let CriomeReply::AuthorizedObjectUpdateSnapshot(snapshot) = snapshot else {
        panic!("expected AuthorizedObjectUpdateSnapshot, got {snapshot:?}");
    };
    let updates = snapshot.into_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].object, object);
    assert_eq!(updates[0].contract, contract);
    assert_eq!(updates[0].decision, EvaluationDecision::Authorized);
    assert_eq!(updates[0].stamp, evidence.stamp);

    daemon.shutdown().expect("shutdown daemon");
}

#[test]
fn auto_approve_signal_call_returns_signed_authorization_grant() {
    let workspace = fixture_path("auto-approve-signal-call-grant");
    let socket = workspace.join("criome.sock");
    let meta_socket = workspace.join("criome-meta.sock");
    let store = StoreLocation::new(workspace.join("criome.sema"));
    let daemon = CriomeDaemon::new(&socket, store.clone())
        .with_meta_socket(&meta_socket)
        .bind()
        .expect("bind daemon");
    wait_for_socket(&socket);
    wait_for_socket(&meta_socket);

    thread::scope(|scope| {
        let server = scope.spawn(|| daemon.serve_next_meta().expect("serve meta configure"));
        let configuration = CriomeDaemonConfiguration::new(
            socket.display().to_string(),
            store.as_path().display().to_string(),
        )
        .with_meta_socket_path(meta_socket.display().to_string())
        .with_authorization_mode(signal_criome::AuthorizationMode::AutoApprove);
        let reply = CriomeMetaClient::new(&meta_socket)
            .send(meta_signal_criome::Input::Configure(configuration))
            .expect("submit meta configure");
        assert_eq!(server.join().expect("join meta configure server"), reply);
    });

    let authorization =
        signal_call_authorization_with_nonce(b"auto-approved-signal-call", "signed-grant-nonce");
    let request_digest = authorization.request_digest.clone();
    let granted = thread::scope(|scope| {
        let server = scope.spawn(|| daemon.serve_next().expect("serve signal authorization"));
        let reply = CriomeClient::new(&socket)
            .send(CriomeRequest::AuthorizeSignalCall(authorization))
            .expect("authorize signal call");
        assert_eq!(server.join().expect("join signal authorization"), reply);
        reply
    });
    let CriomeReply::AuthorizationGranted(grant) = granted else {
        panic!("expected AuthorizationGranted, got {granted:?}");
    };
    assert_eq!(grant.authorized_object_digest, request_digest);
    assert_eq!(grant.issued_by, Identity::host("criome".to_string()));
    assert_eq!(grant.signatures().len(), 1);
    let signature = &grant.signatures()[0].envelope;
    assert_eq!(signature.scheme, SignatureScheme::Bls12_381MinPk);
    assert!(
        !signature.public_key.as_str().is_empty(),
        "criome grant signature carries the master public key"
    );
    assert!(
        !signature.signature.as_str().is_empty(),
        "criome grant signature is not a placeholder"
    );

    let snapshot = thread::scope(|scope| {
        let server = scope.spawn(|| {
            daemon
                .serve_next()
                .expect("serve authorization observation")
        });
        let reply = CriomeClient::new(&socket)
            .send(CriomeRequest::ObserveAuthorization(
                signal_criome::AuthorizationObservation::new(grant.request_slot.clone()),
            ))
            .expect("observe authorization");
        assert_eq!(
            server.join().expect("join authorization observation"),
            reply
        );
        reply
    });
    let CriomeReply::AuthorizationObservationSnapshot(snapshot) = snapshot else {
        panic!("expected AuthorizationObservationSnapshot, got {snapshot:?}");
    };
    let states = snapshot.into_states();
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].status, AuthorizationStatus::Granted);
    assert_eq!(states[0].grant(), Some(&grant));

    daemon.shutdown().expect("shutdown daemon");
}

#[test]
fn client_approval_signal_call_approval_records_signed_authorization_grant() {
    let workspace = fixture_path("client-approval-signal-call-grant");
    let socket = workspace.join("criome.sock");
    let meta_socket = workspace.join("criome-meta.sock");
    let store = StoreLocation::new(workspace.join("criome.sema"));
    let daemon = CriomeDaemon::new(&socket, store.clone())
        .with_meta_socket(&meta_socket)
        .bind()
        .expect("bind daemon");
    wait_for_socket(&socket);
    wait_for_socket(&meta_socket);

    thread::scope(|scope| {
        let server = scope.spawn(|| {
            daemon
                .serve_next_meta()
                .expect("serve client approval configure")
        });
        let configuration = CriomeDaemonConfiguration::new(
            socket.display().to_string(),
            store.as_path().display().to_string(),
        )
        .with_meta_socket_path(meta_socket.display().to_string())
        .with_authorization_mode(signal_criome::AuthorizationMode::ClientApproval);
        let reply = CriomeMetaClient::new(&meta_socket)
            .send(meta_signal_criome::Input::Configure(configuration))
            .expect("submit client approval configure");
        assert_eq!(
            server.join().expect("join client approval configure"),
            reply
        );
    });

    let authorization =
        signal_call_authorization_with_nonce(b"client-approved-signal-call", "slot-grant-nonce");
    let request_digest = authorization.request_digest.clone();
    let pending = thread::scope(|scope| {
        let server = scope.spawn(|| {
            daemon
                .serve_next()
                .expect("serve signal authorization park")
        });
        let reply = CriomeClient::new(&socket)
            .send(CriomeRequest::AuthorizeSignalCall(authorization.clone()))
            .expect("authorize signal call");
        assert_eq!(
            server.join().expect("join signal authorization park"),
            reply
        );
        reply
    });
    let CriomeReply::AuthorizationPending(pending) = pending else {
        panic!("expected AuthorizationPending, got {pending:?}");
    };
    assert_eq!(pending.request_digest, request_digest);

    let parked = thread::scope(|scope| {
        let server = scope.spawn(|| daemon.serve_next_meta().expect("serve signal parked list"));
        let reply = CriomeMetaClient::new(&meta_socket)
            .send(meta_signal_criome::Input::ObserveParkedAuthorizations(
                signal_criome::ParkedAuthorizationObservation::new(),
            ))
            .expect("observe parked signal authorization");
        assert_eq!(server.join().expect("join signal parked list"), reply);
        reply
    });
    let meta_signal_criome::Output::ParkedAuthorizationSnapshot(parked) = parked else {
        panic!("expected ParkedAuthorizationSnapshot, got {parked:?}");
    };
    assert_eq!(parked.parked().len(), 1);
    assert_eq!(parked.parked()[0].request_slot, pending.request_slot);
    assert_eq!(
        parked.parked()[0].signal_authorization(),
        Some(&authorization)
    );
    assert_eq!(parked.parked()[0].evaluation(), None);

    let approved = thread::scope(|scope| {
        let server = scope.spawn(|| daemon.serve_next_meta().expect("serve signal approval"));
        let reply = CriomeMetaClient::new(&meta_socket)
            .send(meta_signal_criome::Input::SubmitAuthorizationApproval(
                AuthorizationApproval {
                    request_slot: pending.request_slot.clone(),
                    decision: AuthorizationApprovalDecision::Approve,
                },
            ))
            .expect("approve signal authorization");
        assert_eq!(server.join().expect("join signal approval"), reply);
        reply
    });
    let meta_signal_criome::Output::AuthorizationApprovalRecorded(approved) = approved else {
        panic!("expected AuthorizationApprovalRecorded, got {approved:?}");
    };
    assert_eq!(approved.request_slot, pending.request_slot);
    assert_eq!(approved.decision, AuthorizationApprovalDecision::Approve);

    let snapshot = thread::scope(|scope| {
        let server = scope.spawn(|| {
            daemon
                .serve_next()
                .expect("serve signal authorization observation")
        });
        let reply = CriomeClient::new(&socket)
            .send(CriomeRequest::ObserveAuthorization(
                signal_criome::AuthorizationObservation::new(pending.request_slot.clone()),
            ))
            .expect("observe signal authorization");
        assert_eq!(
            server
                .join()
                .expect("join signal authorization observation"),
            reply
        );
        reply
    });
    let CriomeReply::AuthorizationObservationSnapshot(snapshot) = snapshot else {
        panic!("expected AuthorizationObservationSnapshot, got {snapshot:?}");
    };
    let states = snapshot.into_states();
    assert_eq!(states.len(), 1);
    let state = &states[0];
    assert_eq!(state.status, AuthorizationStatus::Granted);
    assert_eq!(state.signal_authorization(), Some(&authorization));
    let grant = state.grant().expect("approved signal call stores grant");
    assert_eq!(grant.authorized_object_digest, request_digest);
    assert_eq!(grant.signatures().len(), 1);
    assert!(
        !grant.signatures()[0].envelope.signature.as_str().is_empty(),
        "client approval signs the grant through criome"
    );

    daemon.shutdown().expect("shutdown daemon");
}

#[test]
fn meta_socket_configure_rejects_malformed_configuration() {
    let workspace = fixture_path("meta-configure-malformed");
    let socket = workspace.join("criome.sock");
    let meta_socket = workspace.join("criome-meta.sock");
    let daemon = CriomeDaemon::new(&socket, StoreLocation::new(workspace.join("criome.sema")))
        .with_meta_socket(&meta_socket)
        .bind()
        .expect("bind daemon");
    wait_for_socket(&meta_socket);

    let rejected = thread::scope(|scope| {
        let server = scope.spawn(|| daemon.serve_next_meta().expect("serve malformed configure"));
        let reply = CriomeMetaClient::new(&meta_socket)
            .send(meta_signal_criome::Input::Configure(
                CriomeDaemonConfiguration::new("", ""),
            ))
            .expect("submit malformed configure");
        assert_eq!(server.join().expect("join malformed configure"), reply);
        reply
    });
    let meta_signal_criome::Output::ConfigurationRejected(rejected) = rejected else {
        panic!("expected ConfigurationRejected, got {rejected:?}");
    };
    assert_eq!(
        *rejected.payload(),
        meta_signal_criome::ConfigurationRejectionReason::MalformedConfiguration
    );

    daemon.shutdown().expect("shutdown daemon");
}

#[test]
fn meta_socket_intercept_policy_lifecycle_uses_store_state() {
    let workspace = fixture_path("meta-intercept-policy-lifecycle");
    let socket = workspace.join("criome.sock");
    let meta_socket = workspace.join("criome-meta.sock");
    let store = StoreLocation::new(workspace.join("criome.sema"));
    let daemon = CriomeDaemon::new(&socket, store)
        .with_meta_socket(&meta_socket)
        .bind()
        .expect("bind daemon");
    wait_for_socket(&meta_socket);

    let created = send_meta_request(
        &daemon,
        &meta_socket,
        meta_signal_criome::Input::create_intercept_policy(intercept_policy_proposal(
            "mentci-main",
            "spirit-main",
            "Record",
            50,
        )),
    );
    let meta_signal_criome::Output::InterceptPolicyCreated(created) = created else {
        panic!("expected InterceptPolicyCreated, got {created:?}");
    };
    assert_eq!(created.session_slot.as_str(), "mentci-main");
    assert_eq!(created.spirit_operation_names.names().len(), 1);

    let overlap = send_meta_request(
        &daemon,
        &meta_socket,
        meta_signal_criome::Input::create_intercept_policy(intercept_policy_proposal(
            "mentci-overlap",
            "spirit-main",
            "Record",
            50,
        )),
    );
    let meta_signal_criome::Output::RequestUnimplemented(overlap) = overlap else {
        panic!("expected RequestUnimplemented for same-priority overlap, got {overlap:?}");
    };
    assert_eq!(
        overlap.operation,
        meta_signal_criome::OperationKind::CreateInterceptPolicy
    );

    let listed = send_meta_request(
        &daemon,
        &meta_socket,
        meta_signal_criome::Input::list_intercept_policies(
            meta_signal_criome::InterceptPolicyObservation {},
        ),
    );
    let meta_signal_criome::Output::InterceptPoliciesListed(listed) = listed else {
        panic!("expected InterceptPoliciesListed, got {listed:?}");
    };
    assert_eq!(listed.policies(), std::slice::from_ref(&created));

    let observed = send_meta_request(
        &daemon,
        &meta_socket,
        meta_signal_criome::Input::observe_intercept_policies(
            meta_signal_criome::InterceptPolicyObservation {},
        ),
    );
    let meta_signal_criome::Output::InterceptPolicyObservationOpened(observed) = observed else {
        panic!("expected InterceptPolicyObservationOpened, got {observed:?}");
    };
    assert_eq!(observed.policies(), std::slice::from_ref(&created));

    let retracted = send_meta_request(
        &daemon,
        &meta_socket,
        meta_signal_criome::Input::retract_intercept_policy_observation("stream-1".to_string()),
    );
    let meta_signal_criome::Output::InterceptPolicyObservationRetracted(retracted) = retracted
    else {
        panic!("expected InterceptPolicyObservationRetracted, got {retracted:?}");
    };
    assert_eq!(retracted.payload(), "stream-1");

    let replaced = send_meta_request(
        &daemon,
        &meta_socket,
        meta_signal_criome::Input::replace_intercept_policy(intercept_policy_proposal(
            "mentci-replacement",
            "spirit-main",
            "Record",
            50,
        )),
    );
    let meta_signal_criome::Output::InterceptPolicyReplaced(replaced) = replaced else {
        panic!("expected InterceptPolicyReplaced, got {replaced:?}");
    };
    assert_ne!(replaced.identifier, created.identifier);

    let listed_after_replace = send_meta_request(
        &daemon,
        &meta_socket,
        meta_signal_criome::Input::list_intercept_policies(
            meta_signal_criome::InterceptPolicyObservation {},
        ),
    );
    let meta_signal_criome::Output::InterceptPoliciesListed(listed_after_replace) =
        listed_after_replace
    else {
        panic!("expected InterceptPoliciesListed, got {listed_after_replace:?}");
    };
    assert_eq!(
        listed_after_replace.policies(),
        std::slice::from_ref(&replaced)
    );

    let cancelled = send_meta_request(
        &daemon,
        &meta_socket,
        meta_signal_criome::Input::cancel_intercept_policy(InterceptPolicyCancellation::new(
            replaced.identifier.clone(),
        )),
    );
    let meta_signal_criome::Output::InterceptPolicyCancelled(cancelled) = cancelled else {
        panic!("expected InterceptPolicyCancelled, got {cancelled:?}");
    };
    assert_eq!(cancelled, replaced.identifier);

    let listed_after_cancel = send_meta_request(
        &daemon,
        &meta_socket,
        meta_signal_criome::Input::list_intercept_policies(
            meta_signal_criome::InterceptPolicyObservation {},
        ),
    );
    let meta_signal_criome::Output::InterceptPoliciesListed(listed_after_cancel) =
        listed_after_cancel
    else {
        panic!("expected InterceptPoliciesListed, got {listed_after_cancel:?}");
    };
    assert!(listed_after_cancel.policies().is_empty());

    daemon.shutdown().expect("shutdown daemon");
}

#[test]
fn working_socket_spirit_authorization_uses_intercept_policy_before_authorization_mode() {
    let workspace = fixture_path("working-intercept-policy");
    let socket = workspace.join("criome.sock");
    let meta_socket = workspace.join("criome-meta.sock");
    let store = StoreLocation::new(workspace.join("criome.sema"));
    let daemon = CriomeDaemon::new(&socket, store)
        .with_meta_socket(&meta_socket)
        .with_authorization_mode(signal_criome::AuthorizationMode::AutoApprove)
        .bind()
        .expect("bind daemon");
    wait_for_socket(&socket);
    wait_for_socket(&meta_socket);

    let policy = send_meta_request(
        &daemon,
        &meta_socket,
        meta_signal_criome::Input::create_intercept_policy(intercept_policy_proposal(
            "mentci-main",
            "spirit-process-main",
            "Record",
            50,
        )),
    );
    let meta_signal_criome::Output::InterceptPolicyCreated(policy) = policy else {
        panic!("expected InterceptPolicyCreated, got {policy:?}");
    };

    let raw_payload = "(Record ([intercepted Spirit operation]))";
    let authorization = signal_call_authorization(raw_payload.as_bytes()).with_spirit_context(
        spirit_authorization_context("spirit-process-main", "Record", raw_payload),
    );
    let pending = thread::scope(|scope| {
        let server = scope.spawn(|| daemon.serve_next().expect("serve intercepted signal call"));
        let reply = CriomeClient::new(&socket)
            .send(CriomeRequest::AuthorizeSignalCall(authorization.clone()))
            .expect("send Spirit authorization");
        assert_eq!(server.join().expect("join intercepted signal call"), reply);
        reply
    });
    let CriomeReply::AuthorizationPending(pending) = pending else {
        panic!("intercepted Spirit authorization should park before AutoApprove, got {pending:?}");
    };

    let fetched = send_meta_request(
        &daemon,
        &meta_socket,
        meta_signal_criome::Input::fetch_parked_requests(ParkedRequestQuery {
            session_slot: None,
            target: None,
        }),
    );
    let meta_signal_criome::Output::ParkedRequestsFetched(fetched) = fetched else {
        panic!("expected ParkedRequestsFetched, got {fetched:?}");
    };
    assert_eq!(fetched.requests().len(), 1);
    let parked = &fetched.requests()[0];
    assert_eq!(parked.matched_policy, policy.identifier);
    assert_eq!(parked.session_slot, policy.session_slot);
    assert_eq!(
        parked.context.raw_payload.as_str(),
        "(Record ([intercepted Spirit operation]))"
    );

    let answered = send_meta_request(
        &daemon,
        &meta_socket,
        meta_signal_criome::Input::answer_parked_request(ParkedRequestAnswer {
            identifier: parked.identifier.clone(),
            decision: ParkedRequestDecision::Approve,
        }),
    );
    let meta_signal_criome::Output::ParkedRequestAnswered(answered) = answered else {
        panic!("expected ParkedRequestAnswered, got {answered:?}");
    };
    assert_eq!(answered.identifier, parked.identifier);
    assert_eq!(answered.outcome, ParkedRequestOutcome::Approved);

    let observed = thread::scope(|scope| {
        let server = scope.spawn(|| daemon.serve_next().expect("serve approved observation"));
        let reply = CriomeClient::new(&socket)
            .send(CriomeRequest::ObserveAuthorization(
                signal_criome::AuthorizationObservation::new(pending.request_slot.clone()),
            ))
            .expect("observe approved authorization");
        assert_eq!(server.join().expect("join approved observation"), reply);
        reply
    });
    let CriomeReply::AuthorizationObservationSnapshot(observed) = observed else {
        panic!("expected AuthorizationObservationSnapshot, got {observed:?}");
    };
    let states = observed.into_states();
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].status, AuthorizationStatus::Granted);
    assert_eq!(states[0].signal_authorization(), Some(&authorization));
    assert!(
        states[0].grant().is_some(),
        "approved intercepted Spirit authorization stores criome grant"
    );

    daemon.shutdown().expect("shutdown daemon");
}

#[test]
fn meta_socket_rejects_intercepted_spirit_request_into_authorization_denial() {
    let workspace = fixture_path("working-intercept-policy-reject");
    let socket = workspace.join("criome.sock");
    let meta_socket = workspace.join("criome-meta.sock");
    let store = StoreLocation::new(workspace.join("criome.sema"));
    let daemon = CriomeDaemon::new(&socket, store)
        .with_meta_socket(&meta_socket)
        .with_authorization_mode(signal_criome::AuthorizationMode::AutoApprove)
        .bind()
        .expect("bind daemon");
    wait_for_socket(&socket);
    wait_for_socket(&meta_socket);

    let _policy = send_meta_request(
        &daemon,
        &meta_socket,
        meta_signal_criome::Input::create_intercept_policy(intercept_policy_proposal(
            "mentci-main",
            "spirit-process-main",
            "Record",
            50,
        )),
    );

    let raw_payload = "(Record ([rejected intercepted Spirit operation]))";
    let authorization = signal_call_authorization_with_nonce(
        raw_payload.as_bytes(),
        "rejected-intercepted-spirit-operation",
    )
    .with_spirit_context(spirit_authorization_context(
        "spirit-process-main",
        "Record",
        raw_payload,
    ));
    let pending = thread::scope(|scope| {
        let server = scope.spawn(|| daemon.serve_next().expect("serve intercepted signal call"));
        let reply = CriomeClient::new(&socket)
            .send(CriomeRequest::AuthorizeSignalCall(authorization.clone()))
            .expect("send Spirit authorization");
        assert_eq!(server.join().expect("join intercepted signal call"), reply);
        reply
    });
    let CriomeReply::AuthorizationPending(pending) = pending else {
        panic!("expected AuthorizationPending, got {pending:?}");
    };

    let fetched = send_meta_request(
        &daemon,
        &meta_socket,
        meta_signal_criome::Input::fetch_parked_requests(ParkedRequestQuery {
            session_slot: None,
            target: None,
        }),
    );
    let meta_signal_criome::Output::ParkedRequestsFetched(fetched) = fetched else {
        panic!("expected ParkedRequestsFetched, got {fetched:?}");
    };
    let parked = &fetched.requests()[0];

    let answered = send_meta_request(
        &daemon,
        &meta_socket,
        meta_signal_criome::Input::answer_parked_request(ParkedRequestAnswer {
            identifier: parked.identifier.clone(),
            decision: ParkedRequestDecision::Reject,
        }),
    );
    let meta_signal_criome::Output::ParkedRequestAnswered(answered) = answered else {
        panic!("expected ParkedRequestAnswered, got {answered:?}");
    };
    assert_eq!(answered.identifier, parked.identifier);
    assert_eq!(answered.outcome, ParkedRequestOutcome::Rejected);

    let observed = thread::scope(|scope| {
        let server = scope.spawn(|| daemon.serve_next().expect("serve rejected observation"));
        let reply = CriomeClient::new(&socket)
            .send(CriomeRequest::ObserveAuthorization(
                signal_criome::AuthorizationObservation::new(pending.request_slot.clone()),
            ))
            .expect("observe rejected authorization");
        assert_eq!(server.join().expect("join rejected observation"), reply);
        reply
    });
    let CriomeReply::AuthorizationObservationSnapshot(observed) = observed else {
        panic!("expected AuthorizationObservationSnapshot, got {observed:?}");
    };
    let states = observed.into_states();
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].status, AuthorizationStatus::Denied);
    assert_eq!(states[0].signal_authorization(), Some(&authorization));
    assert!(
        states[0].denial().is_some(),
        "rejected intercepted Spirit authorization stores denial"
    );

    daemon.shutdown().expect("shutdown daemon");
}

#[test]
fn meta_socket_fetches_and_answers_parked_spirit_requests() {
    let workspace = fixture_path("meta-parked-spirit-requests");
    let socket = workspace.join("criome.sock");
    let meta_socket = workspace.join("criome-meta.sock");
    let store = StoreLocation::new(workspace.join("criome.sema"));

    let parked = {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build store prepopulation runtime");
        runtime.block_on(async {
            let store_actor = StoreKernel::spawn(store.clone());
            let stored_policy = store_actor
                .ask(StoreInterceptPolicy::create(
                    intercept_policy_proposal("mentci-parked", "spirit-main", "Record", 50),
                    TimestampNanos::new(10),
                ))
                .await
                .expect("store intercept policy")
                .into_policy()
                .into_policy();
            let parked = store_actor
                .ask(InterceptSpiritAuthorization::new(
                    spirit_authorization_context("spirit-main", "Record", "(Record example)"),
                    TimestampNanos::new(11),
                ))
                .await
                .expect("intercept Spirit authorization")
                .into_request()
                .expect("request parked")
                .request()
                .clone();
            assert_eq!(parked.matched_policy, stored_policy.identifier);
            store_actor
                .stop_gracefully()
                .await
                .expect("stop prepopulation store actor");
            store_actor.wait_for_shutdown().await;
            parked
        })
    };

    let daemon = CriomeDaemon::new(&socket, store)
        .with_meta_socket(&meta_socket)
        .bind()
        .expect("bind daemon");
    wait_for_socket(&meta_socket);

    let fetched = send_meta_request(
        &daemon,
        &meta_socket,
        meta_signal_criome::Input::fetch_parked_requests(ParkedRequestQuery {
            session_slot: None,
            target: None,
        }),
    );
    let meta_signal_criome::Output::ParkedRequestsFetched(fetched) = fetched else {
        panic!("expected ParkedRequestsFetched, got {fetched:?}");
    };
    assert_eq!(fetched.requests(), std::slice::from_ref(&parked));
    assert_eq!(
        fetched.requests()[0].context.raw_payload.as_str(),
        "(Record example)"
    );

    let answered = send_meta_request(
        &daemon,
        &meta_socket,
        meta_signal_criome::Input::answer_parked_request(ParkedRequestAnswer {
            identifier: parked.identifier.clone(),
            decision: ParkedRequestDecision::Approve,
        }),
    );
    let meta_signal_criome::Output::ParkedRequestAnswered(answered) = answered else {
        panic!("expected ParkedRequestAnswered, got {answered:?}");
    };
    assert_eq!(answered.identifier, parked.identifier);
    assert_eq!(answered.outcome, ParkedRequestOutcome::Approved);
    assert_eq!(answered.audit_source, ApprovalAuditSource::Manual);

    let fetched_after_answer = send_meta_request(
        &daemon,
        &meta_socket,
        meta_signal_criome::Input::fetch_parked_requests(ParkedRequestQuery {
            session_slot: None,
            target: None,
        }),
    );
    let meta_signal_criome::Output::ParkedRequestsFetched(fetched_after_answer) =
        fetched_after_answer
    else {
        panic!("expected ParkedRequestsFetched, got {fetched_after_answer:?}");
    };
    assert!(fetched_after_answer.requests().is_empty());

    let missing = send_meta_request(
        &daemon,
        &meta_socket,
        meta_signal_criome::Input::answer_parked_request(ParkedRequestAnswer {
            identifier: signal_criome::ParkedRequestIdentifier::new("missing-request"),
            decision: ParkedRequestDecision::Reject,
        }),
    );
    let meta_signal_criome::Output::RequestUnimplemented(missing) = missing else {
        panic!("expected RequestUnimplemented for missing parked request, got {missing:?}");
    };
    assert_eq!(
        missing.operation,
        meta_signal_criome::OperationKind::AnswerParkedRequest
    );

    daemon.shutdown().expect("shutdown daemon");
}

#[test]
fn meta_socket_approval_by_parked_id_records_authorized_head_update() {
    let workspace = fixture_path("meta-approval-by-id");
    let socket = workspace.join("criome.sock");
    let meta_socket = workspace.join("criome-meta.sock");
    let store = StoreLocation::new(workspace.join("criome.sema"));
    let daemon = CriomeDaemon::new(&socket, store.clone())
        .with_meta_socket(&meta_socket)
        .bind()
        .expect("bind daemon");
    wait_for_socket(&socket);
    wait_for_socket(&meta_socket);

    let configured = thread::scope(|scope| {
        let server = scope.spawn(|| {
            daemon
                .serve_next_meta()
                .expect("serve client approval mode")
        });
        let configuration = CriomeDaemonConfiguration::new(
            socket.display().to_string(),
            store.as_path().display().to_string(),
        )
        .with_meta_socket_path(meta_socket.display().to_string())
        .with_authorization_mode(signal_criome::AuthorizationMode::ClientApproval);
        let reply = CriomeMetaClient::new(&meta_socket)
            .send(meta_signal_criome::Input::Configure(configuration))
            .expect("configure client approval mode");
        assert_eq!(server.join().expect("join configure server"), reply);
        reply
    });
    let meta_signal_criome::Output::Configured(configured) = configured else {
        panic!("expected Configured, got {configured:?}");
    };
    assert_eq!(configured.payload().value(), 1);

    let evidence = unproven_evidence(b"mentci-approved-head");
    let object = signal_criome::AuthorizedObjectReference {
        component: ComponentKind::Spirit,
        digest: evidence.operation.object_digest().clone(),
        kind: AuthorizedObjectKind::Head,
    };
    let contract = signal_criome::ContractDigest::from_bytes(b"client-approval-contract");
    let evaluation = AuthorizationEvaluation {
        contract: contract.clone(),
        object: object.clone(),
        evidence: evidence.clone(),
    };

    let pending = thread::scope(|scope| {
        let server = scope.spawn(|| daemon.serve_next().expect("serve client approval park"));
        let reply = CriomeClient::new(&socket)
            .send(CriomeRequest::EvaluateAuthorization(evaluation.clone()))
            .expect("evaluate authorization");
        assert_eq!(server.join().expect("join park server"), reply);
        reply
    });
    let CriomeReply::AuthorizationPending(pending) = pending else {
        panic!("expected AuthorizationPending, got {pending:?}");
    };
    assert_eq!(pending.request_digest, object.digest);

    let parked = thread::scope(|scope| {
        let server = scope.spawn(|| daemon.serve_next_meta().expect("serve parked list"));
        let reply = CriomeMetaClient::new(&meta_socket)
            .send(meta_signal_criome::Input::ObserveParkedAuthorizations(
                signal_criome::ParkedAuthorizationObservation::new(),
            ))
            .expect("observe parked authorizations");
        assert_eq!(server.join().expect("join parked list server"), reply);
        reply
    });
    let meta_signal_criome::Output::ParkedAuthorizationSnapshot(parked) = parked else {
        panic!("expected ParkedAuthorizationSnapshot, got {parked:?}");
    };
    assert_eq!(parked.parked().len(), 1);
    assert_eq!(parked.parked()[0].request_slot, pending.request_slot);
    assert_eq!(parked.parked()[0].evaluation(), Some(&evaluation));

    let working_reject = thread::scope(|scope| {
        let server = scope.spawn(|| daemon.serve_next().expect("serve working reject"));
        let reply = CriomeClient::new(&socket)
            .send(CriomeRequest::RejectAuthorization(AuthorizationRejection {
                request_slot: pending.request_slot.clone(),
                rejector: Identity::developer("working-client".to_string()),
                reason: AuthorizationDenialReason::PolicyRefused,
            }))
            .expect("submit working reject");
        assert_eq!(server.join().expect("join working reject server"), reply);
        reply
    });
    assert!(matches!(working_reject, CriomeReply::Rejection(_)));

    let parked_after_working_reject = thread::scope(|scope| {
        let server = scope.spawn(|| {
            daemon
                .serve_next_meta()
                .expect("serve parked list after working reject")
        });
        let reply = CriomeMetaClient::new(&meta_socket)
            .send(meta_signal_criome::Input::ObserveParkedAuthorizations(
                signal_criome::ParkedAuthorizationObservation::new(),
            ))
            .expect("observe parked authorizations after working reject");
        assert_eq!(
            server.join().expect("join parked working reject server"),
            reply
        );
        reply
    });
    let meta_signal_criome::Output::ParkedAuthorizationSnapshot(parked_after_working_reject) =
        parked_after_working_reject
    else {
        panic!("expected ParkedAuthorizationSnapshot, got {parked_after_working_reject:?}");
    };
    assert_eq!(parked_after_working_reject.parked().len(), 1);
    assert_eq!(
        parked_after_working_reject.parked()[0].request_slot,
        pending.request_slot
    );

    let deferred = thread::scope(|scope| {
        let server = scope.spawn(|| daemon.serve_next_meta().expect("serve meta defer"));
        let reply = CriomeMetaClient::new(&meta_socket)
            .send(meta_signal_criome::Input::SubmitAuthorizationApproval(
                AuthorizationApproval {
                    request_slot: pending.request_slot.clone(),
                    decision: AuthorizationApprovalDecision::Defer,
                },
            ))
            .expect("submit meta defer");
        assert_eq!(server.join().expect("join meta defer server"), reply);
        reply
    });
    let meta_signal_criome::Output::AuthorizationApprovalRecorded(deferred) = deferred else {
        panic!("expected AuthorizationApprovalRecorded, got {deferred:?}");
    };
    assert_eq!(deferred.request_slot, pending.request_slot);
    assert_eq!(deferred.decision, AuthorizationApprovalDecision::Defer);

    let parked_after_defer = thread::scope(|scope| {
        let server = scope.spawn(|| {
            daemon
                .serve_next_meta()
                .expect("serve parked list after defer")
        });
        let reply = CriomeMetaClient::new(&meta_socket)
            .send(meta_signal_criome::Input::ObserveParkedAuthorizations(
                signal_criome::ParkedAuthorizationObservation::new(),
            ))
            .expect("observe parked authorizations after defer");
        assert_eq!(server.join().expect("join parked defer server"), reply);
        reply
    });
    let meta_signal_criome::Output::ParkedAuthorizationSnapshot(parked_after_defer) =
        parked_after_defer
    else {
        panic!("expected ParkedAuthorizationSnapshot, got {parked_after_defer:?}");
    };
    assert_eq!(parked_after_defer.parked().len(), 1);
    assert_eq!(
        parked_after_defer.parked()[0].request_slot,
        pending.request_slot
    );

    let missing_slot = thread::scope(|scope| {
        let server = scope.spawn(|| daemon.serve_next_meta().expect("serve missing approval"));
        let reply = CriomeMetaClient::new(&meta_socket)
            .send(meta_signal_criome::Input::SubmitAuthorizationApproval(
                AuthorizationApproval {
                    request_slot: AuthorizationRequestSlot::new("999"),
                    decision: AuthorizationApprovalDecision::Approve,
                },
            ))
            .expect("submit missing approval");
        assert_eq!(server.join().expect("join missing approval server"), reply);
        reply
    });
    let meta_signal_criome::Output::RequestUnimplemented(missing_slot) = missing_slot else {
        panic!("expected RequestUnimplemented, got {missing_slot:?}");
    };
    assert_eq!(
        missing_slot.operation,
        meta_signal_criome::OperationKind::SubmitAuthorizationApproval
    );

    let approved = thread::scope(|scope| {
        let server = scope.spawn(|| daemon.serve_next_meta().expect("serve meta approval"));
        let reply = CriomeMetaClient::new(&meta_socket)
            .send(meta_signal_criome::Input::SubmitAuthorizationApproval(
                AuthorizationApproval {
                    request_slot: pending.request_slot.clone(),
                    decision: AuthorizationApprovalDecision::Approve,
                },
            ))
            .expect("submit meta approval");
        assert_eq!(server.join().expect("join meta server"), reply);
        reply
    });
    let meta_signal_criome::Output::AuthorizationApprovalRecorded(approved) = approved else {
        panic!("expected AuthorizationApprovalRecorded, got {approved:?}");
    };
    assert_eq!(approved.request_slot, pending.request_slot);
    assert_eq!(approved.decision, AuthorizationApprovalDecision::Approve);

    let snapshot = thread::scope(|scope| {
        let server = scope.spawn(|| daemon.serve_next().expect("serve authorized observation"));
        let reply = CriomeClient::new(&socket)
            .send(CriomeRequest::ObserveAuthorizedObjects(
                AuthorizedObjectObservation {
                    subscriber: Identity::agent("mentci-status".to_string()),
                    interest: AuthorizedObjectInterest::Component(ComponentKind::Spirit),
                },
            ))
            .expect("observe authorized objects");
        assert_eq!(server.join().expect("join observation server"), reply);
        reply
    });
    let CriomeReply::AuthorizedObjectUpdateSnapshot(snapshot) = snapshot else {
        panic!("expected AuthorizedObjectUpdateSnapshot, got {snapshot:?}");
    };
    let updates = snapshot.into_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].object, object);
    assert_eq!(updates[0].contract, contract);
    assert_eq!(updates[0].decision, EvaluationDecision::Authorized);
    assert_eq!(updates[0].stamp, evidence.stamp);

    daemon.shutdown().expect("shutdown daemon");
}

#[test]
fn criome_daemon_configuration_accepts_binary_file_argument() {
    let workspace = fixture_path("daemon-config-binary");
    let configuration_path = workspace.join("criome-daemon.rkyv");
    let configuration = daemon_configuration("daemon-config-value");

    CriomeDaemonConfigurationFile::new(&configuration_path)
        .write_configuration(&configuration)
        .expect("write daemon configuration");

    let decoded = CriomeDaemonCommand::from_arguments([configuration_path.display().to_string()])
        .configuration()
        .expect("decode daemon configuration");

    assert_eq!(decoded, configuration);
}

#[test]
fn criome_daemon_configuration_rejects_nota_arguments() {
    let workspace = fixture_path("daemon-config-nota");
    let nota_path = workspace.join("criome-daemon.nota");
    std::fs::write(&nota_path, "(CriomeDaemonConfiguration)").expect("write nota fixture");

    let inline = CriomeDaemonCommand::from_arguments(["(CriomeDaemonConfiguration)"])
        .configuration()
        .expect_err("inline NOTA is rejected");
    let file = CriomeDaemonCommand::from_arguments([nota_path.display().to_string()])
        .configuration()
        .expect_err(".nota file is rejected");

    assert!(matches!(inline, criome::Error::Argument(_)));
    assert!(matches!(file, criome::Error::Argument(_)));
}

#[cfg(feature = "nota-text")]
#[test]
fn criome_cli_request_argument_accepts_inline_and_nota_file() {
    let request = CriomeRequest::LookupIdentity(IdentityLookup::new(Identity::developer(
        ("operator").to_string(),
    )));
    let text = request.to_nota();
    let workspace = fixture_path("request-argument");
    let nota_path = workspace.join("request.nota");
    std::fs::write(&nota_path, &text).expect("write request");

    let inline = CriomeRequestArgument::new(
        triad_runtime::ComponentCommand::from_arguments([text.clone()])
            .nota_argument()
            .expect("inline nota argument"),
    )
    .request()
    .expect("inline request decodes");
    let file = CriomeRequestArgument::new(
        triad_runtime::ComponentCommand::from_arguments([nota_path.display().to_string()])
            .nota_argument()
            .expect("file nota argument"),
    )
    .request()
    .expect("file request decodes");

    assert_eq!(inline, request);
    assert_eq!(file, request);
}

#[cfg(feature = "nota-text")]
#[test]
fn criome_cli_request_argument_rejects_flag_shape() {
    let error = CriomeRequestArgument::new(
        triad_runtime::ComponentCommand::from_arguments(["--socket"])
            .nota_argument()
            .expect("inline flag-shaped argument"),
    )
    .request()
    .expect_err("flag-shaped argument is rejected before NOTA parsing");

    assert!(matches!(error, criome::Error::FlagArgument(_)));
}

#[test]
fn criome_cli_cannot_reply_without_daemon_signal_frame() {
    let workspace = fixture_path("missing-daemon");
    let socket = workspace.join("missing.sock");

    let error = CriomeClient::new(&socket)
        .send(CriomeRequest::LookupIdentity(IdentityLookup::new(
            Identity::developer(("operator").to_string()),
        )))
        .expect_err("missing daemon must reject");

    assert!(format!("{error}").contains("socket does not exist"));
}

#[test]
fn criome_frame_codec_rejects_reply_on_request_path() {
    let codec = CriomeFrameCodec::default();
    let left = UnixStream::pair().expect("stream pair");
    let (mut writer, reader) = left;
    codec
        .write_reply(
            &mut writer,
            CriomeReply::Rejection(signal_criome::Rejection::new(
                RejectionReason::MalformedRequest,
            )),
        )
        .expect("write reply frame");

    let mut reader = BufReader::new(reader);
    let error = codec
        .read_request(&mut reader)
        .expect_err("reply frame must not decode as request");
    assert!(format!("{error}").contains("unexpected signal frame"));
}

#[test]
fn criome_frame_codec_reads_contract_local_request_payload() {
    let expected = CriomeRequest::LookupIdentity(IdentityLookup::new(Identity::developer(
        ("operator").to_string(),
    )));
    let frame = CriomeFrame::new(CriomeFrameBody::Request {
        exchange: synthetic_exchange(),
        request: expected.clone().into_request(),
    });
    let bytes = frame.encode_length_prefixed().expect("frame encodes");
    let mut input = bytes.as_slice();
    let decoded = CriomeFrameCodec::default()
        .read_request(&mut input)
        .expect("request payload decodes");

    assert_eq!(decoded, expected);
}

#[test]
fn cargo_manifest_removed_retired_signal_and_ractor_runtime() {
    let manifest = std::fs::read_to_string("Cargo.toml").expect("read manifest");

    assert!(manifest.contains("signal-criome"));
    assert!(manifest.contains("signal-frame"));
    assert!(manifest.contains("kameo"));
    assert!(manifest.contains("triad-runtime"));
    assert!(!manifest.contains("ractor"));
    assert!(!manifest.contains("clap"));
    assert!(!manifest.contains("signal       ="));
    assert!(!manifest.contains("signal-core"));
}

fn wait_for_socket(socket: &std::path::Path) {
    for _attempt in 0..100 {
        if socket.exists() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    panic!("socket did not appear: {}", socket.display());
}
