//! Taking work from a queue — queue contract 1.0 (`spec/queue-v1.md` in the
//! protocol repository).
//!
//! A [`Queue`] claims records from a queue the store holds, as one named
//! worker, and settles each one: **done** deletes it, **release** hands it back,
//! now or not before a delay. A worker that dies says nothing — its hold lapses
//! after the queue's `TIMEOUT` and the next claim takes the record — so the
//! work is at least once, and an outside effect is keyed on the queue name and
//! the record's identity.
//!
//! Two ways to run a handler over the queue:
//!
//! - [`Queue::run_auto`]: `Ok` marks the record done, `Err` hands it back at
//!   once. The attempt was counted at the claim, so a record that keeps failing
//!   reaches the queue's `ATTEMPTS` (and its dead letter, if it declares one).
//! - [`Queue::run_manual`]: the handler returns a [`Handled`].
//!
//! The queue itself is declared in the store (`DEFINE QUEUE`), never by this
//! type: declaring it chooses a timeout no client can guess.

use std::future::Future;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::watch;

use crate::RefusalClass;
use crate::client::Client;
use crate::error::{Error, Result};
use crate::query::{BuildError, Filter};
use crate::value::{Number, Value};
use crate::wire::message::Answer;

mod statements;

use statements::Statements;

/// The first wait after a claim that took nothing (§4.5).
const FIRST_WAIT: Duration = Duration::from_millis(50);
/// The longest wait between claims that take nothing (§4.5).
const LONGEST_WAIT: Duration = Duration::from_secs(1);
/// Records asked for per claim unless [`Queue::batch`] says otherwise.
const DEFAULT_BATCH: u64 = 10;
/// The three fields the engine keeps on a held record, not the work's own.
const ENGINE_FIELDS: [&str; 3] = ["attempts", "claimed_until", "claimed_by"];

/// One record a claim took.
///
/// Settled only through the [`Queue`] that claimed it: the identity is the
/// claim's own spelling, written back unchanged, and there is no way to make
/// one from text (§2).
#[derive(Debug, Clone, PartialEq)]
pub struct Job {
    id: String,
    value: Value,
    attempts: u64,
}

impl Job {
    /// The record's identity as the store spelled it — `7`, `'a7'`, `uuid '…'`.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The record's own fields, without the engine's hold.
    #[must_use]
    pub fn value(&self) -> &Value {
        &self.value
    }

    /// How many times it has been handed out, 1 the first time.
    #[must_use]
    pub fn attempts(&self) -> u64 {
        self.attempts
    }
}

/// What a manual handler decided about a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Handled {
    /// Done: the record is deleted.
    Done,
    /// Hand it back — now, or not before the delay (the queue must declare a
    /// `NOT BEFORE` field for a delay).
    Release(Option<Duration>),
    /// Neither: it is handed out again when the hold lapses.
    Leave,
}

/// Stops a running [`Queue`] worker from another task.
///
/// The handler that is running finishes, and in auto mode its settlement is
/// sent; the rest of the batch is handed back; then the worker stops claiming.
#[derive(Debug, Clone)]
pub struct QueueStopper(watch::Sender<bool>);

impl QueueStopper {
    /// Ask the worker to stop.
    pub fn stop(&self) {
        // Nobody listening means the worker is already gone, which is what
        // stopping asked for.
        let _ = self.0.send(true);
    }
}

/// A worker taking records from one queue over one connection.
#[derive(Debug)]
pub struct Queue<S = TcpStream> {
    client: Client<S>,
    statements: Statements,
    batch: u64,
    narrowed: Option<Filter>,
    signal: watch::Sender<bool>,
    stopped: watch::Receiver<bool>,
}

fn whole(value: Option<&Value>) -> Option<u64> {
    match value {
        Some(Value::Number(Number::Integer(held))) => u64::try_from(*held).ok(),
        _ => None,
    }
}

impl<S> Queue<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// The worker `worker` taking from `queue` in `namespace`/`database`.
    ///
    /// # Errors
    ///
    /// [`BuildError::NotAName`] when a namespace, database or queue is not a
    /// name, and [`BuildError::NotAWorkerName`] when the worker is not one —
    /// both refused before anything is sent.
    pub fn new(
        client: Client<S>,
        (namespace, database): (&str, &str),
        queue: &str,
        worker: &str,
    ) -> std::result::Result<Self, BuildError> {
        let statements = Statements::new((namespace, database), queue, worker)?;
        let (signal, stopped) = watch::channel(false);
        Ok(Self {
            client,
            statements,
            batch: DEFAULT_BATCH,
            narrowed: None,
            signal,
            stopped,
        })
    }

    /// Claim up to `batch` records per claim in the run loops (1 to 500).
    #[must_use]
    pub fn batch(mut self, batch: u64) -> Self {
        self.batch = batch;
        self
    }

    /// Take only records matching `filter` in the run loops.
    #[must_use]
    pub fn only(mut self, filter: Filter) -> Self {
        self.narrowed = Some(filter);
        self
    }

    /// A handle that stops this worker from elsewhere.
    #[must_use]
    pub fn stopper(&self) -> QueueStopper {
        QueueStopper(self.signal.clone())
    }

    /// The connection back, once working is over.
    pub fn into_client(self) -> Client<S> {
        self.client
    }

    /// Claim up to `count` records, narrowed by `filter` when one is given; an
    /// empty answer is an empty queue.
    ///
    /// # Errors
    ///
    /// [`BuildError::NotACount`] outside 1 to 500 or a filter field that is
    /// not a name, before sending; otherwise what the node refused, or the
    /// transport failure.
    pub async fn claim(&mut self, count: u64, filter: Option<&Filter>) -> Result<Vec<Job>> {
        let (script, parameters) = self.statements.claim(count, filter)?;
        let answers = self.client.run_with(&script, None, parameters).await?;
        match answers.last() {
            Some(Answer::Records { records, .. }) => {
                records.iter().map(|(id, body)| job(id, body)).collect()
            }
            _ => Err(Error::Malformed),
        }
    }

    /// The work is done: delete the record.
    ///
    /// # Errors
    ///
    /// What the node refused, or the transport failure.
    pub async fn done(&mut self, job: &Job) -> Result<()> {
        let script = self.statements.done(&job.id);
        self.client.run(&script, None).await.map(|_| ())
    }

    /// Hand the record back, now or not before `after`.
    ///
    /// # Errors
    ///
    /// What the node refused — `NoDelayField` for a delay on a queue declaring
    /// no `NOT BEFORE` field — or the transport failure.
    pub async fn release(&mut self, job: &Job, after: Option<Duration>) -> Result<()> {
        let script = self.statements.release(&job.id, after);
        self.client.run(&script, None).await.map(|_| ())
    }

    /// Call `handler` for each record; `Ok` marks it done and `Err` hands it
    /// back at once. Returns when stopped, or with the first refusal or
    /// transport failure.
    ///
    /// # Errors
    ///
    /// Whatever the node refused, or the transport failure, as it happened.
    pub async fn run_auto<F, Fut, E>(&mut self, mut handler: F) -> Result<()>
    where
        F: FnMut(Job) -> Fut,
        Fut: Future<Output = std::result::Result<(), E>>,
    {
        while let Some(jobs) = self.next_batch().await? {
            let mut jobs = jobs.into_iter();
            while let Some(job) = jobs.next() {
                let held = job.clone();
                match handler(job).await {
                    Ok(()) => self.done(&held).await?,
                    Err(_) => self.release(&held, None).await?,
                }
                if self.is_stopped() {
                    return self.hand_back(jobs).await;
                }
            }
        }
        Ok(())
    }

    /// Call `handler` for each record and do what it returns.
    ///
    /// # Errors
    ///
    /// Whatever the node refused, or the transport failure, as it happened.
    pub async fn run_manual<F, Fut>(&mut self, mut handler: F) -> Result<()>
    where
        F: FnMut(Job) -> Fut,
        Fut: Future<Output = Handled>,
    {
        while let Some(jobs) = self.next_batch().await? {
            let mut jobs = jobs.into_iter();
            while let Some(job) = jobs.next() {
                let held = job.clone();
                match handler(job).await {
                    Handled::Done => self.done(&held).await?,
                    Handled::Release(after) => self.release(&held, after).await?,
                    Handled::Leave => {}
                }
                if self.is_stopped() {
                    return self.hand_back(jobs).await;
                }
            }
        }
        Ok(())
    }

    /// Hand back what the handler never saw, one statement each (§4.6).
    async fn hand_back(&mut self, unseen: impl Iterator<Item = Job>) -> Result<()> {
        for job in unseen {
            self.release(&job, None).await?;
        }
        Ok(())
    }

    fn is_stopped(&self) -> bool {
        *self.stopped.borrow()
    }

    /// The next records, waiting while there are none (§4.5); `None` once
    /// stopped. A claim refused with class `retry` is waited on like an empty
    /// one (§6).
    async fn next_batch(&mut self) -> Result<Option<Vec<Job>>> {
        let mut wait = FIRST_WAIT;
        loop {
            if self.is_stopped() {
                return Ok(None);
            }
            let narrowed = self.narrowed.clone();
            match self.claim(self.batch, narrowed.as_ref()).await {
                Ok(jobs) if !jobs.is_empty() => return Ok(Some(jobs)),
                Ok(_)
                | Err(Error::Refused {
                    class: Some(RefusalClass::Retry),
                    ..
                }) => {}
                Err(other) => return Err(other),
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

/// One answered record as a [`Job`].
fn job(id: &str, body: &Value) -> Result<Job> {
    let Value::Object(fields) = body else {
        return Err(Error::Malformed);
    };
    let attempts = whole(fields.get("attempts")).ok_or(Error::Malformed)?;
    let mut work = fields.clone();
    for engine in ENGINE_FIELDS {
        work.remove(engine);
    }
    Ok(Job {
        id: id.to_owned(),
        value: Value::Object(work),
        attempts,
    })
}
