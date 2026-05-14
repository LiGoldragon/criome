use std::io::BufReader;
use std::os::unix::net::UnixStream;
use std::thread;

use criome::actors::root::{Arguments as RootArguments, CriomeRoot, ReadTopology, SubmitRequest};
use criome::daemon::CriomeDaemon;
use criome::tables::StoreLocation;
use criome::transport::{CriomeClient, CriomeFrameCodec};
use signal_core::{FrameBody, Request, SignalVerb};
use signal_criome::{
    AuditContext, BlsPublicKey, ContentPurpose, ContentReference, CriomeReply, CriomeRequest,
    Frame as CriomeFrame, Identity, IdentityLookup, IdentityRegistration, KeyPurpose, ObjectDigest,
    PrincipalName, PrincipalStatus, PublicKeyFingerprint, RejectionReason, ReplayNonce,
    SignRequest,
};

fn fixture_path(name: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("criome-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("create fixture dir");
    path
}

fn store_location(name: &str) -> StoreLocation {
    StoreLocation::new(fixture_path(name).join("criome.redb"))
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

#[tokio::test]
async fn criome_root_starts_data_bearing_kameo_children() {
    let root = CriomeRoot::start(RootArguments::new(store_location("topology")))
        .await
        .expect("start criome root");

    let topology = root.ask(ReadTopology).await.expect("read topology");
    assert!(topology.registry());
    assert!(topology.signer());
    assert!(topology.verifier());

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

#[test]
fn criome_daemon_signal_frame_registers_identity() {
    let workspace = fixture_path("daemon-registers");
    let socket = workspace.join("criome.sock");
    let store = StoreLocation::new(workspace.join("criome.redb"));
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
fn criome_frame_codec_rejects_mismatched_signal_verb() {
    let frame = CriomeFrame::new(FrameBody::Request(Request::unchecked_operation(
        SignalVerb::Assert,
        CriomeRequest::LookupIdentity(IdentityLookup {
            identity: Identity::developer("operator"),
        }),
    )));
    let bytes = frame.encode_length_prefixed().expect("frame encodes");
    let mut input = bytes.as_slice();
    let error = CriomeFrameCodec::default()
        .read_request(&mut input)
        .expect_err("mismatched verb is rejected");

    assert!(error.to_string().contains("signal verb mismatch"));
}

#[test]
fn cargo_manifest_removed_retired_signal_and_ractor_runtime() {
    let manifest = std::fs::read_to_string("Cargo.toml").expect("read manifest");

    assert!(manifest.contains("signal-criome"));
    assert!(manifest.contains("kameo"));
    assert!(!manifest.contains("ractor"));
    assert!(!manifest.contains("signal       ="));
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
