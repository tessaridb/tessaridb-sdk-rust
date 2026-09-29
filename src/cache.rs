//! A space used as a cache, a counter and a lock — cache contract 1.0
//! (`spec/cache-v1.md` in the protocol repository).
//!
//! A [`Space`] borrows a connection and sends one statement per call, with its
//! own `USE`, so a connection that reconnected underneath it cannot read
//! another database. Every key, value, duration and holder is bound.
//!
//! Two things a cache over this store must know, and that this type makes hard
//! to get wrong:
//!
//! - **A plain [`Space::set`] clears an expiry the key had.** Pass the ttl again
//!   on every write that must keep one.
//! - **A lock is a lease, not a mutex.** Past its ttl another holder may take it
//!   and neither is told. [`Space::release`] is an expiring conditional write,
//!   never a delete: a delete after the lease lapsed would remove the next
//!   holder's lock, and a hand-back with no expiry would make the key permanent.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};

use crate::client::Client;
use crate::error::{Error, Result};
use crate::query::BuildError;
use crate::value::{Number, Value};
use crate::wire::message::Answer;

mod statements;

use statements::{MOST_KEYS, Rendered, Statements, duration, unquoted};

/// How long a key has left.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ttl {
    /// It expires after this long.
    Expires(Duration),
    /// It is there and never expires.
    Never,
    /// There is no such key.
    Absent,
}

/// A lock held by this caller until its ttl passes (§4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    /// The key the lock is.
    pub key: String,
    /// The value that says who holds it.
    pub holder: String,
    /// How long each take or extension holds it for.
    pub ttl: Duration,
}

/// A space in one namespace and database, over a connection this borrows.
#[derive(Debug)]
pub struct Space<'c, S> {
    client: &'c mut Client<S>,
    statements: Statements,
}

impl<'c, S> Space<'c, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// The space `space` in `namespace`/`database`, over `client`.
    ///
    /// The client should already be signed in when the store is closed.
    ///
    /// # Errors
    ///
    /// [`BuildError::NotAName`] when any of the three is not a name — refused
    /// before anything is sent.
    pub fn new(
        client: &'c mut Client<S>,
        (namespace, database): (&str, &str),
        space: &str,
    ) -> std::result::Result<Self, BuildError> {
        Ok(Self {
            client,
            statements: Statements::new((namespace, database), space)?,
        })
    }

    /// The value under `key`, or `None` when there is no such key.
    ///
    /// # Errors
    ///
    /// Whatever the node refused, or the transport failure.
    pub async fn get(&mut self, key: &str) -> Result<Option<Value>> {
        let found = self.value(self.statements.get(key)).await?;
        Ok((found != Value::None).then_some(found))
    }

    /// Store `value` under `key`, expiring after `ttl` if one is given — and
    /// clearing any expiry the key had if not.
    ///
    /// # Errors
    ///
    /// [`Error::NotACacheArgument`] for a zero ttl; whatever the node refused.
    pub async fn set(&mut self, key: &str, value: Value, ttl: Option<Duration>) -> Result<()> {
        let ttl = positive(ttl)?;
        self.value(self.statements.set(key, value, "", None, ttl))
            .await?;
        Ok(())
    }

    /// Store `value` only if there is no `key`; whether it was stored.
    ///
    /// # Errors
    ///
    /// As [`Space::set`].
    pub async fn set_if_absent(
        &mut self,
        key: &str,
        value: Value,
        ttl: Option<Duration>,
    ) -> Result<bool> {
        let ttl = positive(ttl)?;
        self.flag(self.statements.set(key, value, " IF ABSENT", None, ttl))
            .await
    }

    /// Store `value` only if there is a `key`; whether it was stored.
    ///
    /// # Errors
    ///
    /// As [`Space::set`].
    pub async fn set_if_present(
        &mut self,
        key: &str,
        value: Value,
        ttl: Option<Duration>,
    ) -> Result<bool> {
        let ttl = positive(ttl)?;
        self.flag(self.statements.set(key, value, " IF PRESENT", None, ttl))
            .await
    }

    /// Store `value` only if `key` holds `expected`; whether it was stored.
    ///
    /// # Errors
    ///
    /// As [`Space::set`].
    pub async fn compare_and_set(
        &mut self,
        key: &str,
        expected: Value,
        value: Value,
        ttl: Option<Duration>,
    ) -> Result<bool> {
        let ttl = positive(ttl)?;
        self.flag(
            self.statements
                .set(key, value, " IF = $e", Some(expected), ttl),
        )
        .await
    }

    /// Remove `key`; whether there was one. A key holding `NULL` is one.
    ///
    /// # Errors
    ///
    /// Whatever the node refused, or the transport failure.
    pub async fn delete(&mut self, key: &str) -> Result<bool> {
        Ok(self.value(self.statements.delete(key)).await? != Value::None)
    }

    /// Add `by` to the integer under `key` — a missing key counts from zero — and
    /// answer the new value. An expiry the key had is kept.
    ///
    /// # Errors
    ///
    /// Whatever the node refused — a key holding a non-number, an overflow.
    pub async fn incr(&mut self, key: &str, by: i64) -> Result<i64> {
        match self.value(self.statements.incr(key, by)).await? {
            Value::Number(Number::Integer(held)) => Ok(held),
            _ => Err(Error::Malformed),
        }
    }

    /// How long `key` has left: a span, never, or no key at all.
    ///
    /// # Errors
    ///
    /// Whatever the node refused, or the transport failure.
    pub async fn ttl(&mut self, key: &str) -> Result<Ttl> {
        match self.value(self.statements.ttl(key)).await? {
            Value::None => Ok(Ttl::Absent),
            Value::Null => Ok(Ttl::Never),
            Value::Duration { seconds, nanos } => u64::try_from(seconds)
                .map(|seconds| Ttl::Expires(Duration::new(seconds, nanos)))
                .map_err(|_| Error::Malformed),
            _ => Err(Error::Malformed),
        }
    }

    /// Let `key` expire after `ttl`; whether there was a key.
    ///
    /// # Errors
    ///
    /// [`Error::NotACacheArgument`] for a zero ttl, which would remove the key.
    pub async fn expire(&mut self, key: &str, ttl: Duration) -> Result<bool> {
        let ttl = duration(ttl).ok_or(Error::NotACacheArgument {
            reason: "a ttl must be positive: a zero one would remove the key",
        })?;
        self.flag(self.statements.expire(key, ttl)).await
    }

    /// Make `key` never expire; whether there was a key.
    ///
    /// # Errors
    ///
    /// Whatever the node refused, or the transport failure.
    pub async fn persist(&mut self, key: &str) -> Result<bool> {
        self.flag(self.statements.persist(key)).await
    }

    /// Up to `limit` keys (1–1000) in key order, starting with `prefix` (none or
    /// empty: every key) and after `after`.
    ///
    /// # Errors
    ///
    /// [`Error::NotACacheArgument`] for a limit out of range.
    pub async fn keys(
        &mut self,
        prefix: Option<&str>,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<String>> {
        if !(1..=MOST_KEYS).contains(&limit) {
            return Err(Error::NotACacheArgument {
                reason: "a key listing asks for 1 to 1000 keys",
            });
        }
        let answers = self
            .send(self.statements.keys(prefix, after, limit))
            .await?;
        match answers.last() {
            Some(Answer::Keys(keys)) => Ok(keys.iter().map(|key| unquoted(key)).collect()),
            _ => Err(Error::Malformed),
        }
    }

    /// The value under `key`, or — when there is none — what `loader` makes,
    /// stored for `ttl` if nobody stored first (§3).
    ///
    /// Racing callers are not coordinated: each that misses runs its loader, the
    /// first to store wins, and the others answer the winner's value.
    ///
    /// # Errors
    ///
    /// As [`Space::set`].
    pub async fn get_or_set<F, Fut>(&mut self, key: &str, ttl: Duration, loader: F) -> Result<Value>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Value>,
    {
        if let Some(found) = self.get(key).await? {
            return Ok(found);
        }
        let made = loader().await;
        if self.set_if_absent(key, made.clone(), Some(ttl)).await? {
            return Ok(made);
        }
        // Somebody stored first — or stored and it has already expired.
        Ok(self.get(key).await?.unwrap_or(made))
    }

    /// Take the lock `key` for `ttl` as `holder` (a fresh unique one when
    /// `None`); the lease, or `None` when somebody else holds it.
    ///
    /// # Errors
    ///
    /// [`Error::NotACacheArgument`] for a zero ttl or an empty holder.
    pub async fn lock(
        &mut self,
        key: &str,
        ttl: Duration,
        holder: Option<&str>,
    ) -> Result<Option<Lease>> {
        let holder = match holder {
            Some("") => {
                return Err(Error::NotACacheArgument {
                    reason: "a lock's holder is not empty",
                });
            }
            Some(holder) => holder.to_owned(),
            None => fresh_holder(),
        };
        let lasting = required(ttl)?;
        let held = self
            .flag(self.statements.lock(key, &holder, lasting))
            .await?;
        Ok(held.then(|| Lease {
            key: key.to_owned(),
            holder,
            ttl,
        }))
    }

    /// Hold `lease` for another `ttl` (its own when `None`); `false` means the
    /// lease was already lost and the work it guarded must stop.
    ///
    /// # Errors
    ///
    /// [`Error::NotACacheArgument`] for a zero ttl.
    pub async fn extend(&mut self, lease: &Lease, ttl: Option<Duration>) -> Result<bool> {
        let lasting = required(ttl.unwrap_or(lease.ttl))?;
        self.flag(self.statements.extend(&lease.key, &lease.holder, lasting))
            .await
    }

    /// Give `lease` back; whether it was still held.
    ///
    /// # Errors
    ///
    /// Whatever the node refused, or the transport failure.
    pub async fn release(&mut self, lease: Lease) -> Result<bool> {
        self.flag(self.statements.release(&lease.key, &lease.holder))
            .await
    }

    async fn send(&mut self, (script, given): Rendered) -> Result<Vec<Answer>> {
        self.client.run_with(&script, None, given).await
    }

    /// The last answer's value — a statement that answers none (a plain `SET`)
    /// reads as `None`.
    async fn value(&mut self, rendered: Rendered) -> Result<Value> {
        match self.send(rendered).await?.pop() {
            Some(Answer::Value { value, .. }) => Ok(value),
            Some(Answer::Done) => Ok(Value::None),
            _ => Err(Error::Malformed),
        }
    }

    async fn flag(&mut self, rendered: Rendered) -> Result<bool> {
        match self.value(rendered).await? {
            Value::Bool(held) => Ok(held),
            _ => Err(Error::Malformed),
        }
    }
}

fn positive(ttl: Option<Duration>) -> Result<Option<Value>> {
    ttl.map(required).transpose()
}

fn required(ttl: Duration) -> Result<Value> {
    duration(ttl).ok_or(Error::NotACacheArgument {
        reason: "a ttl must be positive",
    })
}

/// 128 bits unique to one lease (§4), without a dependency: the standard
/// library's per-process random hash keys, a counter and the time.
fn fresh_holder() -> String {
    use std::collections::hash_map::RandomState;
    use std::hash::BuildHasher;
    static TAKEN: AtomicU64 = AtomicU64::new(0);
    let count = TAKEN.fetch_add(1, Ordering::Relaxed);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos());
    let high = RandomState::new().hash_one((count, now));
    let low = RandomState::new().hash_one((now, count, high));
    format!("{high:016x}{low:016x}")
}
