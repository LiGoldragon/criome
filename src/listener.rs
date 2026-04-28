//! `Listener` actor — UDS accept loop.
//!
//! Binds the criome socket and accepts client connections,
//! spawning one [`Connection`](crate::connection) actor per
//! accept. Holds references to the [`Engine`](crate::engine)
//! and the [`Reader`](crate::reader) pool so it can pass them
//! to each connection at construction.
//!
//! The accept loop is modeled as a self-cast `Accept` message
//! — each tick accepts one connection, spawns the child, and
//! re-arms. Connection panics are logged and the listener
//! moves on (per [reports/103 §8 Q4](../../reports/103-ractor-migration-design-2026-04-28.md)).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use ractor::{Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
use tokio::net::UnixListener;

use crate::{connection, engine, reader};

pub struct Listener;

pub struct State {
    listener: UnixListener,
    engine: ActorRef<engine::Message>,
    readers: Vec<ActorRef<reader::Message>>,
    reader_cursor: Arc<AtomicUsize>,
}

pub struct Arguments {
    pub socket_path: PathBuf,
    pub engine: ActorRef<engine::Message>,
    pub readers: Vec<ActorRef<reader::Message>>,
}

pub enum Message {
    /// Self-cast tick that accepts one connection per
    /// invocation and re-arms.
    Accept,
}

#[ractor::async_trait]
impl Actor for Listener {
    type Msg = Message;
    type State = State;
    type Arguments = Arguments;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        arguments: Arguments,
    ) -> std::result::Result<Self::State, ActorProcessingErr> {
        let _ = std::fs::remove_file(&arguments.socket_path);
        let listener = UnixListener::bind(&arguments.socket_path)?;
        ractor::cast!(myself, Message::Accept)?;
        Ok(State {
            listener,
            engine: arguments.engine,
            readers: arguments.readers,
            reader_cursor: Arc::new(AtomicUsize::new(0)),
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Message,
        state: &mut State,
    ) -> std::result::Result<(), ActorProcessingErr> {
        match message {
            Message::Accept => {
                let (stream, _) = state.listener.accept().await?;
                let arguments = connection::Arguments {
                    stream,
                    engine: state.engine.clone(),
                    readers: state.readers.clone(),
                    reader_cursor: Arc::clone(&state.reader_cursor),
                };
                Actor::spawn_linked(None, connection::Connection, arguments, myself.get_cell())
                    .await?;
                ractor::cast!(myself, Message::Accept)?;
            }
        }
        Ok(())
    }

    async fn handle_supervisor_evt(
        &self,
        _myself: ActorRef<Self::Msg>,
        event: SupervisionEvent,
        _state: &mut State,
    ) -> std::result::Result<(), ActorProcessingErr> {
        if let SupervisionEvent::ActorFailed(actor, reason) = event {
            eprintln!("criome-daemon: connection {actor:?} failed: {reason}");
        }
        Ok(())
    }
}
