//! Handshake handler — version-negotiates the connection.
//!
//! Major-exact + minor-forward per
//! [`signal::ProtocolVersion::is_compatible_with`].

use signal::{
    HandshakeRejectionReason, HandshakeReply, HandshakeRequest, ProtocolVersion, Reply, Slot,
    SIGNAL_PROTOCOL_VERSION,
};

pub fn handle(request: HandshakeRequest) -> Reply {
    if request.client_version.is_compatible_with(SIGNAL_PROTOCOL_VERSION) {
        return Reply::HandshakeAccepted(HandshakeReply {
            server_version: SIGNAL_PROTOCOL_VERSION,
            // M0 single-instance default — multi-instance
            // criome assigns a real CriomedInstance slot here.
            server_id: Slot::from(0u64),
        });
    }
    Reply::HandshakeRejected(rejection_reason(request.client_version, SIGNAL_PROTOCOL_VERSION))
}

fn rejection_reason(client: ProtocolVersion, server: ProtocolVersion) -> HandshakeRejectionReason {
    if client.major != server.major {
        HandshakeRejectionReason::IncompatibleMajor { client, server }
    } else {
        // Major matches but client.minor > server.minor (the
        // only other reason `is_compatible_with` returns false).
        HandshakeRejectionReason::ClientMinorAhead { client, server }
    }
}
