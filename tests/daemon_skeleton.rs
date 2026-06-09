use std::io::BufReader;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::thread;

use criome::actors::root::{Arguments as RootArguments, CriomeRoot, ReadTopology, SubmitRequest};
use criome::command::CriomeDaemonCommand;
#[cfg(feature = "nota-text")]
use criome::command::CriomeRequestArgument;
use criome::daemon::CriomeDaemon;
use criome::daemon::{CriomeDaemonConfiguration, CriomeDaemonConfigurationFile};
use criome::tables::StoreLocation;
use criome::transport::{CriomeClient, CriomeFrameCodec};
#[cfg(feature = "nota-text")]
use nota_next::NotaEncode;
use signal_criome::{
    AuditContext, AuthorizationDenialReason, AuthorizationDenialSource, AuthorizationExpired,
    AuthorizationGrant, AuthorizationObservation, AuthorizationPolicyClass,
    AuthorizationPolicySatisfaction, AuthorizationRequestSlot, AuthorizationScope,
    AuthorizationStatus, BlsPublicKey, BlsSignature, ContentPurpose, ContentReference,
    ContractName, ContractOperationHead, CriomeFrame, CriomeFrameBody, CriomeReply, CriomeRequest,
    Identity, IdentityLookup, IdentityRegistration, KeyPurpose, ObjectDigest, PrincipalName,
    PrincipalStatus, PublicKeyFingerprint, RejectionReason, ReplayNonce,
    RequiredSignatureThreshold, SignRequest, SignalCallAuthorization, SignatureAuthorizationResult,
    SignatureEnvelope, SignatureScheme, TimestampNanos,
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

fn registration(name: &str) -> IdentityRegistration {
    IdentityRegistration {
        identity: Identity::developer(name),
        public_key: BlsPublicKey::new(format!("{name}-public-key")),
        fingerprint: PublicKeyFingerprint::new(format!("{name}-fingerprint")),
        purpose: KeyPurpose::ReleaseAuthorization,
    }
}

fn sign_request(name: &str) -> SignRequest {
    SignRequest {
        content: ContentReference {
            digest: ObjectDigest::from_bytes(b"fixture"),
            purpose: ContentPurpose::SignedObject,
            schema_version: PrincipalName::new("fixture-schema"),
        },
        signer: Identity::developer(name),
        audit_context: AuditContext {
            purpose: ContentPurpose::SignedObject,
            audience: PrincipalName::new("fixture-audience"),
            policy_version: PrincipalName::new("fixture-policy"),
            nonce: ReplayNonce::new("fixture-nonce"),
        },
        expires_at: None,
    }
}

fn authorization_scope() -> AuthorizationScope {
    AuthorizationScope::new("deploy-zeus-full-os")
}

fn contract_name() -> ContractName {
    ContractName::new("signal-lojix")
}

fn contract_operation_head() -> ContractOperationHead {
    ContractOperationHead::new("Deploy")
}

fn signal_call_authorization(seed: &[u8]) -> SignalCallAuthorization {
    signal_call_authorization_with_nonce(seed, "authorization-nonce")
}

fn signal_call_authorization_with_nonce(seed: &[u8], nonce: &str) -> SignalCallAuthorization {
    SignalCallAuthorization {
        request_digest: ObjectDigest::from_bytes(seed),
        contract: contract_name(),
        operation: contract_operation_head(),
        scope: authorization_scope(),
        requester: Identity::developer("operator"),
        nonce: ReplayNonce::new(nonce),
        expires_at: None,
    }
}

fn signature_envelope() -> SignatureEnvelope {
    SignatureEnvelope {
        scheme: SignatureScheme::Bls12_381MinPk,
        public_key: BlsPublicKey::new("public-key"),
        signature: BlsSignature::new("signature"),
    }
}

fn authorization_grant(seed: &[u8]) -> AuthorizationGrant {
    AuthorizationGrant {
        request_slot: AuthorizationRequestSlot::new("authorization-grant-slot"),
        authorized_object_digest: ObjectDigest::from_bytes(seed),
        authorized_contract: contract_name(),
        authorized_operation: contract_operation_head(),
        authorization_scope: authorization_scope(),
        policy_satisfaction: AuthorizationPolicySatisfaction {
            policy_class: AuthorizationPolicyClass::SimpleSelfSigned,
            required_signature_threshold: RequiredSignatureThreshold::new(1),
            satisfied_signers: vec![Identity::cluster("criome-master")],
        },
        signature_result: SignatureAuthorizationResult::SingleSignature,
        signatures: vec![signature_envelope()],
        issued_by: Identity::cluster("criome-master"),
        issued_at: TimestampNanos::new(1),
        expires_at: None,
    }
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
        CriomeReply::Rejection(signal_criome::Rejection {
            reason: RejectionReason::UnknownIdentity
        })
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
    assert!(pending.missing_authorities.is_empty());

    let snapshot = root
        .ask(SubmitRequest::new(CriomeRequest::ObserveAuthorization(
            AuthorizationObservation {
                request_slot: pending.request_slot.clone(),
            },
        )))
        .await
        .expect("observe authorization")
        .into_reply();
    let CriomeReply::AuthorizationObservationSnapshot(snapshot) = snapshot else {
        panic!("expected AuthorizationObservationSnapshot, got {snapshot:?}");
    };
    assert_eq!(snapshot.states.len(), 1);
    assert_eq!(snapshot.states[0].request_slot, pending.request_slot);
    assert_eq!(snapshot.states[0].request_digest, request_digest);
    assert_eq!(snapshot.states[0].status, AuthorizationStatus::Signing);

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
            SignalCallAuthorization {
                nonce: ReplayNonce::new("second-nonce"),
                ..authorization
            },
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
    let authorization = SignalCallAuthorization {
        expires_at: Some(TimestampNanos::new(0)),
        ..signal_call_authorization_with_nonce(b"expired authorization request", "expired-nonce")
    };
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
            AuthorizationObservation {
                request_slot: expired.request_slot.clone(),
            },
        )))
        .await
        .expect("observe expired authorization")
        .into_reply();
    let CriomeReply::AuthorizationObservationSnapshot(snapshot) = snapshot else {
        panic!("expected AuthorizationObservationSnapshot, got {snapshot:?}");
    };
    assert_eq!(snapshot.states.len(), 1);
    assert_eq!(snapshot.states[0].request_slot, expired.request_slot);
    assert_eq!(snapshot.states[0].request_digest, request_digest);
    assert_eq!(snapshot.states[0].status, AuthorizationStatus::Expired);

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
        CriomeReply::Rejection(signal_criome::Rejection {
            reason: RejectionReason::ReplayAttempted,
        })
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
            identity: Identity::developer("operator"),
            status: PrincipalStatus::Active,
        })
    );
    assert_eq!(served.join().expect("join daemon"), reply);
}

#[test]
fn criome_daemon_meta_socket_is_user_private() {
    let workspace = fixture_path("socket-mode");
    let socket = workspace.join("criome.sock");
    let store = StoreLocation::new(workspace.join("criome.sema"));
    let daemon = CriomeDaemon::new(&socket, store)
        .bind()
        .expect("bind daemon");

    let mode = std::fs::metadata(daemon.socket())
        .expect("read socket metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600);

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
    let request = CriomeRequest::LookupIdentity(IdentityLookup {
        identity: Identity::developer("operator"),
    });
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
        .send(CriomeRequest::LookupIdentity(IdentityLookup {
            identity: Identity::developer("operator"),
        }))
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
            CriomeReply::Rejection(signal_criome::Rejection {
                reason: RejectionReason::MalformedRequest,
            }),
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
    let expected = CriomeRequest::LookupIdentity(IdentityLookup {
        identity: Identity::developer("operator"),
    });
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
