//! `Daemon` actor — root of the criome supervision tree.
//!
//! Spawns the [`Engine`](crate::engine), the
//! [`Reader`](crate::reader) pool (sized by
//! [`sema::Sema::reader_count`]), and the
//! [`Listener`](crate::listener) at startup. Holds the
//! `ActorRef`s for graceful-shutdown propagation.
//!
//! The Daemon itself receives no user messages — it only
//! exists to own the supervision relationship and respond to
//! a `Stop` request from `main` (e.g., on SIGTERM). Bring it
//! up via `Actor::spawn(Some("daemon".into()), Daemon, args)`
//! at the binary entry point — see `bin/main.rs`.

use std::path::PathBuf;
use std::sync::Arc;

use ractor::{Actor, ActorProcessingErr, ActorRef};
use sema::Sema;

use crate::engine;
use crate::{listener, reader};

pub struct Daemon;

pub struct State {
    pub engine: ActorRef<engine::Message>,
    pub readers: Vec<ActorRef<reader::Message>>,
    pub listener: ActorRef<listener::Message>,
}

pub struct Arguments {
    pub socket_path: PathBuf,
    pub sema_path: PathBuf,
}

pub enum Message {}

#[ractor::async_trait]
impl Actor for Daemon {
    type Msg = Message;
    type State = State;
    type Arguments = Arguments;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        arguments: Arguments,
    ) -> std::result::Result<Self::State, ActorProcessingErr> {
        let sema = Arc::new(Sema::open(&arguments.sema_path)?);
        let reader_count = sema.reader_count()?;

        let (engine_ref, _) = Actor::spawn_linked(
            Some("engine".into()),
            engine::Engine,
            engine::Arguments { sema: Arc::clone(&sema) },
            myself.get_cell(),
        )
        .await?;

        let mut readers = Vec::with_capacity(reader_count as usize);
        for index in 0..reader_count {
            let (reader_ref, _) = Actor::spawn_linked(
                Some(format!("reader-{index}")),
                reader::Reader,
                reader::Arguments { sema: Arc::clone(&sema) },
                myself.get_cell(),
            )
            .await?;
            readers.push(reader_ref);
        }

        let (listener_ref, _) = Actor::spawn_linked(
            Some("listener".into()),
            listener::Listener,
            listener::Arguments {
                socket_path: arguments.socket_path,
                engine: engine_ref.clone(),
                readers: readers.clone(),
            },
            myself.get_cell(),
        )
        .await?;

        Ok(State { engine: engine_ref, readers, listener: listener_ref })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        _message: Message,
        _state: &mut State,
    ) -> std::result::Result<(), ActorProcessingErr> {
        Ok(())
    }
}
