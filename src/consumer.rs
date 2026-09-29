//! Consuming a topic as a member of a consumer group — consumer contract 1.0
//! (`spec/consumer-v1.md` in the protocol repository).
//!
//! A [`Consumer`] reads a topic under a group the store holds, and calls a
//! handler once per message in the order the group hands them out. The group,
//! not this connection, keeps the state — the last position handed out and what
//! is in flight — so a process that crashes loses nothing it had not
//! acknowledged, and another under the same group name carries on.
//!
//! Two ways to settle a message:
//!
//! - [`Consumer::run_auto`] acknowledges each message when the handler returns
//!   `Ok`, and hands it back when it returns `Err`: acknowledge after
//!   processing, at least once.
//! - [`Consumer::run_manual`] lets the handler decide, by returning a
//!   [`Settle`].
//!
//! The group itself is declared in the store (`DEFINE GROUP`), never by this
//! type: declaring it is a schema act that chooses a deadline no client can
//! guess.

use std::future::Future;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::watch;

use crate::client::Client;
use crate::error::{Error, Result};
use crate::query::BuildError;
use crate::value::{Number, Value};
use crate::wire::message::Answer;

mod statements;

use statements::Statements;

/// The first wait after a read that answered nothing (§4.5).
const FIRST_WAIT: Duration = Duration::from_millis(50);
/// The longest wait between reads that answer nothing (§4.5).
const LONGEST_WAIT: Duration = Duration::from_secs(1);
/// Messages asked for per read unless [`Consumer::batch`] says otherwise.
const DEFAULT_BATCH: u64 = 10;

/// One message, as the group handed it out.
#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    /// Its position in the topic, from 1. With the topic and group names it is
    /// a stable key for making an outside effect idempotent.
    pub position: u64,
    /// What it holds.
    pub value: Value,
    /// How many times it has been handed out, 1 the first time.
    pub deliveries: u64,
}

/// What a manual handler decided about a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settle {
    /// Done: never handed out to this group again.
    Ack,
    /// Hand it out again — now, or after the delay.
    Nack(Option<Duration>),
    /// Neither: the group hands it out again when its deadline passes.
    Leave,
}

/// Stops a running [`Consumer`] from another task.
///
/// The handler that is running finishes, and in auto mode its acknowledgement
/// is sent; then the consumer stops reading. What is still in flight returns to
/// the group when its deadline passes.
#[derive(Debug, Clone)]
pub struct Stopper(watch::Sender<bool>);

impl Stopper {
    /// Ask the consumer to stop.
    pub fn stop(&self) {
        // Nobody listening means the consumer is already gone, which is what
        // stopping asked for.
        let _ = self.0.send(true);
    }
}

/// A member of a consumer group, reading one topic over one connection.
#[derive(Debug)]
pub struct Consumer<S = TcpStream> {
    client: Client<S>,
    statements: Statements,
    batch: u64,
    signal: watch::Sender<bool>,
    stopped: watch::Receiver<bool>,
}

fn whole(value: Option<&Value>) -> Option<u64> {
    match value {
        Some(Value::Number(Number::Integer(held))) => u64::try_from(*held).ok(),
        _ => None,
    }
}

impl<S> Consumer<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// A member of `group` reading `topic` in `namespace`/`database`.
    ///
    /// The client should already be signed in when the store is closed: a wire
    /// connection proves who it is once and keeps that identity.
    ///
    /// # Errors
    ///
    /// [`BuildError::NotAName`] when a namespace, database or topic is not a
    /// name, and [`BuildError::NotAGroupName`] when the group is not one — both
    /// refused before anything is sent.
    pub fn new(
        client: Client<S>,
        (namespace, database): (&str, &str),
        topic: &str,
        group: &str,
    ) -> std::result::Result<Self, BuildError> {
        let statements = Statements::new((namespace, database), topic, group)?;
        let (signal, stopped) = watch::channel(false);
        Ok(Self {
            client,
            statements,
            batch: DEFAULT_BATCH,
            signal,
            stopped,
        })
    }

    /// Ask for up to `batch` messages per read (at least one).
    #[must_use]
    pub fn batch(mut self, batch: u64) -> Self {
        self.batch = batch.max(1);
        self
    }

    /// A handle that stops this consumer from elsewhere.
    #[must_use]
    pub fn stopper(&self) -> Stopper {
        Stopper(self.signal.clone())
    }

    /// The connection back, once consuming is over.
    pub fn into_client(self) -> Client<S> {
        self.client
    }

    /// Call `handler` for each message; `Ok` acknowledges it and `Err` hands it
    /// back at once. Returns when stopped, or with the first refusal or
    /// transport failure.
    ///
    /// # Errors
    ///
    /// Whatever the node refused, or the transport failure, as it happened.
    pub async fn run_auto<F, Fut, E>(&mut self, mut handler: F) -> Result<()>
    where
        F: FnMut(Message) -> Fut,
        Fut: Future<Output = std::result::Result<(), E>>,
    {
        while let Some(messages) = self.next_batch().await? {
            for message in messages {
                let position = message.position;
                match handler(message).await {
                    Ok(()) => self.ack(&[position]).await?,
                    Err(_) => self.nack(&[position], None).await?,
                };
                if self.is_stopped() {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Call `handler` for each message and do what it returns.
    ///
    /// # Errors
    ///
    /// Whatever the node refused, or the transport failure, as it happened.
    pub async fn run_manual<F, Fut>(&mut self, mut handler: F) -> Result<()>
    where
        F: FnMut(Message) -> Fut,
        Fut: Future<Output = Settle>,
    {
        while let Some(messages) = self.next_batch().await? {
            for message in messages {
                let position = message.position;
                match handler(message).await {
                    Settle::Ack => {
                        self.ack(&[position]).await?;
                    }
                    Settle::Nack(delay) => {
                        self.nack(&[position], delay).await?;
                    }
                    Settle::Leave => {}
                }
                if self.is_stopped() {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Acknowledge these positions; answers how many were in flight. A position
    /// that was not counts nothing and is not an error.
    ///
    /// # Errors
    ///
    /// Whatever the node refused, or the transport failure.
    pub async fn ack(&mut self, positions: &[u64]) -> Result<u64> {
        if positions.is_empty() {
            return Ok(0);
        }
        let (script, parameters) = self.statements.ack(positions)?;
        self.settle(&script, parameters).await
    }

    /// Hand these positions back, now or after `delay`; answers how many were
    /// in flight.
    ///
    /// # Errors
    ///
    /// Whatever the node refused, or the transport failure.
    pub async fn nack(&mut self, positions: &[u64], delay: Option<Duration>) -> Result<u64> {
        if positions.is_empty() {
            return Ok(0);
        }
        let (script, parameters) = self.statements.nack(positions, delay)?;
        self.settle(&script, parameters).await
    }

    async fn settle(&mut self, script: &str, parameters: Vec<(String, Value)>) -> Result<u64> {
        let answers = self.client.run_with(script, None, parameters).await?;
        match answers.last() {
            Some(Answer::Value { value, .. }) => whole(Some(value)).ok_or(Error::Malformed),
            _ => Err(Error::Malformed),
        }
    }

    fn is_stopped(&self) -> bool {
        *self.stopped.borrow()
    }

    /// The next messages, waiting while there are none (§4.5); `None` once
    /// stopped.
    async fn next_batch(&mut self) -> Result<Option<Vec<Message>>> {
        let mut wait = FIRST_WAIT;
        loop {
            if self.is_stopped() {
                return Ok(None);
            }
            let script = self.statements.read(self.batch);
            let answers = self.client.run(&script, None).await?;
            let messages = match answers.last() {
                Some(Answer::Records { records, .. }) => records
                    .iter()
                    .map(|(_, body)| message(body))
                    .collect::<Result<Vec<_>>>()?,
                _ => return Err(Error::Malformed),
            };
            if !messages.is_empty() {
                return Ok(Some(messages));
            }
            let mut stopped = self.stopped.clone();
            tokio::select! {
                // Stopping first, so a stop that arrives during the wait is not
                // held back by a timer that happens to be ready as well.
                biased;
                _ = stopped.changed() => {}
                () = tokio::time::sleep(wait) => {}
            }
            wait = wait.saturating_mul(2).min(LONGEST_WAIT);
        }
    }
}

/// One answered record as a [`Message`].
fn message(body: &Value) -> Result<Message> {
    let Value::Object(fields) = body else {
        return Err(Error::Malformed);
    };
    Ok(Message {
        position: whole(fields.get("position")).ok_or(Error::Malformed)?,
        value: fields.get("value").cloned().unwrap_or(Value::None),
        deliveries: whole(fields.get("deliveries")).ok_or(Error::Malformed)?,
    })
}
