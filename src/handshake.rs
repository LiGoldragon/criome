//! Handshake handler — `impl Daemon { handle_handshake, rejection_reason }`.
//!
//! Major-exact + minor-forward per
//! [`signal::ProtocolVersion::is_compatible_with`].

use signal::{
    HandshakeRejectionReason, HandshakeReply, HandshakeRequest, ProtocolVersion, Reply, Slot,
    SIGNAL_PROTOCOL_VERSION,
};

use crate::daemon::Daemon;

impl Daemon {
    /// Associated function (no `&self`) — the handshake reply
    /// is determined entirely by the request and the
    /// build-time `SIGNAL_PROTOCOL_VERSION`. M0 single-instance
    /// default fills `server_id` with `Slot::from(0u64)`;
    /// multi-instance criome assigns a real CriomedInstance
    /// slot here.
    pub(crate) fn handle_handshake(request: HandshakeRequest) -> Reply {
        if request.client_version.is_compatible_with(SIGNAL_PROTOCOL_VERSION) {
            return Reply::HandshakeAccepted(HandshakeReply {
                server_version: SIGNAL_PROTOCOL_VERSION,
                server_id: Slot::from(0u64),
            });
        }
        Reply::HandshakeRejected(Self::rejection_reason(
            request.client_version,
            SIGNAL_PROTOCOL_VERSION,
        ))
    }

    fn rejection_reason(
        client: ProtocolVersion,
        server: ProtocolVersion,
    ) -> HandshakeRejectionReason {
        if client.major != server.major {
            HandshakeRejectionReason::IncompatibleMajor { client, server }
        } else {
            HandshakeRejectionReason::ClientMinorAhead { client, server }
        }
    }
}
