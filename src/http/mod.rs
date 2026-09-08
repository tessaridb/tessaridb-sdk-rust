//! The node's other surface.
//!
//! # Why there are two transports and a caller never picks
//!
//! A node serves two surfaces and neither carries everything. Statements and
//! subscriptions go over the wire protocol, because it carries the store's full
//! model of seventeen value types. Objects, files, backup, health, readiness and
//! metrics go over **HTTP**, because nothing else serves them.
//!
//! Routing statements over HTTP would work, reach everything, and silently
//! narrow every result — JSON carries six types — with nothing at the call site
//! to show what was lost. So the choice is forced by the operation rather than
//! offered as a preference (LR-SDK-005).
//!
//! # Why this client is written rather than taken
//!
//! The framing was **measured** against the shipped node before this was
//! written: every route answers with `Content-Length`, and not one uses
//! `Transfer-Encoding: chunked` — including `/backup`, the only plausible
//! streaming candidate. That is what makes a written client tractable instead of
//! reckless.
//!
//! The deciding argument, though, is that the framing is a **closed** grammar:
//! five routes, one way of declaring length, and no case this client has not
//! been shown. Ten crates to read something that small would be a poor trade
//! against a crate that keeps its dependencies few on purpose.
//!
//! The JSON those routes answer with went the other way, and the contrast is the
//! rule rather than an exception to it: its strings carry arbitrary user text,
//! so it is **open**, and `serde_json` is a dependency (ADR-SDK-0004). What is
//! avoided here is an avoidable dependency, not every dependency.
//!
//! What is **not** claimed is that this is a general HTTP client. It speaks to
//! one server, whose framing is known, and it refuses what it has not been shown
//! rather than guessing — the same discipline as the wire half, where an unknown
//! frame kind ends the connection instead of being skipped.
//!
//! # There is no TLS here either
//!
//! As on the wire protocol. A node belongs on a network you protect or behind
//! something that terminates TLS.

mod basic;
mod condition;
mod object;
mod reply;

use serde_json::Value as Json;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::error::{Error, Result};

pub use crate::http::condition::Condition;
pub use crate::http::object::Bucket;
use crate::http::reply::Reply;

/// The node's operational surface.
///
/// Holds an address rather than a connection: these calls are occasional and
/// independent, so a connection per call costs nothing worth keeping a pool for
/// — and a pooled connection to a node that restarted is a failure at the next
/// call rather than at the one that should have had it.
#[derive(Clone)]
pub struct Operations {
    address: String,
    credential: Option<Credential>,
    session: Option<String>,
    attempts: u8,
}

/// Who this handle signs in as, and the header that says so.
///
/// One field rather than two parallel `Option`s. The name is kept because
/// [`change_password`](Operations::change_password) has to build a *new* header
/// afterwards and cannot recover the name from the old one — base64 is
/// reversible, but decoding a credential to reuse half of it is a worse answer
/// than keeping the half that was never a secret.
///
/// Held together so the invariant is structural: there is no state in which a
/// name exists without the header built from it.
#[derive(Clone)]
struct Credential {
    name: String,
    header: String,
}

/// Which credential a request presents.
///
/// Two routes — `POST /session` and `POST /password` — **refuse a token** by
/// design, and both of them are reachable from a handle that is holding one. So
/// the choice cannot be read off the handle's state alone; the route decides it,
/// and says so here rather than in a comment at each call site.
#[derive(Clone, Copy)]
enum Presenting {
    /// The session token when this handle holds one, and the password otherwise.
    ///
    /// This is every route but the two below. Preferring the token is the whole
    /// point of holding one.
    Whatever,
    /// The password, always, even while a session is open.
    ///
    /// A token presented to either of the two routes that require this is
    /// answered `401`, so sending one would turn a working call into a refusal
    /// that reads like a wrong password.
    PasswordOnly,
}

/// How long to wait between attempts.
///
/// Fixed rather than a curve. Three attempts fired in microseconds at a node
/// that is restarting are three failures rather than one, so *some* pause is
/// what makes retrying mean anything; a configurable backoff is flexibility
/// nobody asked for. The number is here so it can be found.
const PAUSE_BETWEEN_ATTEMPTS: std::time::Duration = std::time::Duration::from_millis(50);

/// Written rather than derived, because a derived one prints the credential.
///
/// The header is base64, which is a **spelling** of the password and not a
/// disguise for it — anything that reads the line reads the password. A derived
/// `Debug` therefore puts a working credential into any log, panic message or
/// test failure that formats one of these, and into every [`Bucket`] too, since
/// a bucket holds one of these and derives its own.
///
/// So the presence of a credential is reported and its value never is. Presence
/// is worth reporting: *sent no credential* is the single most likely cause of a
/// `401` a caller is looking at, and hiding the field entirely would remove the
/// one thing the output is being read for.
impl std::fmt::Debug for Operations {
    fn fmt(&self, form: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        form.debug_struct("Operations")
            .field("address", &self.address)
            .field(
                "credential",
                match self.credential {
                    Some(_) => &"<present>",
                    None => &"<none>",
                },
            )
            // Reported for the same reason the credential is, and it answers a
            // different question: *whether a password is being hashed on every
            // request*. A handle with a credential and no session is correct and
            // slow, which is the one state that looks fine from the outside.
            .field(
                "session",
                match self.session {
                    Some(_) => &"<open>",
                    None => &"<none>",
                },
            )
            // Not a secret, and the second thing looked for when a call behaved
            // unexpectedly — a request that took four times as long as expected
            // is explained by this number and by nothing else visible.
            .field("attempts", &self.attempts)
            .finish()
    }
}

impl Operations {
    /// The operational surface of the node at this address.
    ///
    /// This is the node's **HTTP** address, which is not its wire address: a
    /// node serves them on separate ports and may serve only one.
    ///
    /// No credential is sent. That is not a placeholder to be filled in later —
    /// a store with no user runs anything, and against one of those an absent
    /// header is the correct request. Add credentials with
    /// [`as_user`](Self::as_user) when the store has a user.
    pub fn at(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            credential: None,
            session: None,
            attempts: 1,
        }
    }

    /// Try a failed request up to `attempts` times in total.
    ///
    /// The default is **1** — nothing is retried unless it is asked for.
    ///
    /// # What is retried, and why only that
    ///
    /// Only [`Error::Io`] and [`Error::Truncated`]: the exchange did not
    /// complete, and re-asking is safe. Every method this type sends is
    /// **idempotent** — `GET` and `HEAD` read, `PUT` replaces a file's whole
    /// content, `DELETE` answers `204` whether or not the file was there — so
    /// asking twice cannot mean something different from asking once. That
    /// property holds by construction: the `POST` synonym and the ranged write,
    /// the two calls that would break it, are deliberately not offered.
    ///
    /// **Three calls are exempt rather than covered**, and they never reach this
    /// loop. [`change_password`](Self::change_password), because a retry there
    /// re-sends a credential the first attempt may already have invalidated.
    /// [`backup`](Self::backup), because its answer is not a value this client
    /// discards and replaces — it has already been written to the caller's sink,
    /// so a second attempt appends a second copy behind the partial first one.
    /// [`open_session`](Self::open_session), because an answer lost on the way
    /// back leaves a token nobody holds occupying one of the node's bounded
    /// slots, and asking again mints a second.
    ///
    /// The three are worth reading together: retry safety is a property of each
    /// call, never of a transport, and these are the three ways a request on an
    /// otherwise idempotent surface stops having it — a credential the attempt
    /// consumed, an answer already delivered elsewhere, and a side effect the
    /// node keeps whether or not the caller heard about it.
    ///
    /// **Anything the node actually said is not retried.** A `401` retried is a
    /// loop and a `403` retried is a longer one, and `Malformed`, `TooLarge` and
    /// `NotThisProtocol` are statements about what did arrive.
    ///
    /// There is a fixed pause of 50ms between attempts.
    ///
    /// # Why it is off by default
    ///
    /// A silent retry hides a failing node from a caller who wanted to know
    /// quickly, and multiplies against one who has their own loop. The argument
    /// the other way is real — this surface's idempotency is provable, so the
    /// client *could* decide safely — and turning it on later is a change in
    /// behaviour rather than in API, which is the direction that stays open.
    ///
    /// **This does not apply to the wire client**, and cannot. One connection is
    /// one session there, so a reconnect repeats the request in a session where
    /// no `USE` has run — targeting the node's default database rather than the
    /// caller's, which is a wrong answer rather than an error.
    #[must_use]
    pub const fn attempts(mut self, attempts: u8) -> Self {
        // Zero would mean never asking at all, which is not a thing a caller can
        // want from a request they made. Clamped rather than refused: an
        // `Option`-returning builder for a value that has an obvious right
        // reading is ceremony.
        self.attempts = if attempts == 0 { 1 } else { attempts };
        self
    }

    /// Present these credentials on every request this handle makes.
    ///
    /// Taken once and held, rather than passed at each call. A credential per
    /// call is a secret at every call site and one of them eventually goes
    /// without — and a request that silently omits the header does not fail
    /// loudly, it comes back `401` on a closed store and `400` on an open one,
    /// which are two different-looking bugs with one cause.
    ///
    /// The header is built here, so the plaintext password stops existing inside
    /// this crate the moment the handle is made.
    ///
    /// # A closed store and an open one refuse differently
    ///
    /// Measured against the shipped node: on a **closed** store an
    /// uncredentialed call is `401` with `WWW-Authenticate`, a wrong password is
    /// also `401`, and a right credential reaching outside the user's grants is
    /// `403`. On an **open** store the identical call is `400`. The status
    /// follows the store's posture rather than the call, which is why `401` and
    /// `403` are kept apart: a `401` is worth presenting a credential for and a
    /// `403` never is, however many times it is retried.
    #[must_use]
    pub fn as_user(mut self, name: &str, password: &str) -> Self {
        self.credential = Some(Credential {
            name: name.to_owned(),
            header: basic::header(name, password),
        });
        // Any session open here belongs to whoever this handle was before. A
        // token outranks the credential on every ordinary route, so keeping one
        // would make a handle that says it is Ada still act as Grace — and it
        // would keep doing so for twelve hours, with the right credential
        // sitting unused beside it.
        self.session = None;
        self
    }

    /// Spend the password once and hold a token instead.
    ///
    /// Returns the token's lifetime in **seconds from now**, as the node
    /// reported it — read from the answer rather than assumed, because it is the
    /// node's constant and not this client's.
    ///
    /// # Why a caller should almost always do this
    ///
    /// The node verifies a password with Argon2id at the OWASP floor, and HTTP
    /// gives it no connection to remember the answer on — so a handle presenting
    /// `Basic` pays that hash on **every single request**. Measured on a real
    /// deployment: seventeen statements took 2.5 seconds, of which about 99 %
    /// was re-verifying a password the caller already held.
    ///
    /// Nothing about that failure is visible from the outside. The calls all
    /// succeed; they are simply slow, and the cause is in the node's hasher
    /// rather than anywhere a caller would look. That is why this method exists
    /// and why its documentation is longer than its body.
    ///
    /// After it succeeds, every route on this handle presents the token instead
    /// — except the two that refuse one, which continue to present the password
    /// and are named below.
    ///
    /// # The token is not renewed for you
    ///
    /// It lasts twelve hours on the shipped node, and this handle does **not**
    /// notice when it stops working: a `401` arrives and is reported as a `401`.
    /// A long-lived caller therefore reopens the session, either on the returned
    /// lifetime or when a `401` arrives while one is open.
    ///
    /// This is a limit rather than a preference. Renewing inside the client
    /// would mean mutating the handle from the request path, and every read
    /// method here takes `&self` so that a [`Bucket`] can be cloned off it —
    /// changing that is a design decision about the whole type, not a detail of
    /// this call. It is written down here rather than discovered at hour twelve.
    ///
    /// # What ends a token
    ///
    /// Four things, and the node reports all four as `401` without distinction:
    /// it expired; [`close_session`](Self::close_session) gave it back; the user
    /// record changed at all — a password change, a role change, a `DROP USER`;
    /// or the node restarted, because the table is in memory.
    ///
    /// # Errors
    ///
    /// [`Error::NoCredential`] before anything is sent, when this handle
    /// presents no credential — there is no password to spend.
    ///
    /// [`Error::HttpRefused`] at `401` for a wrong password, and also at `401`
    /// on a store with **no users declared**: such a store signs everyone in as
    /// nobody and has no session to open. That one is not worth retrying and not
    /// worth working around — on an open store there is nothing to prove and so
    /// nothing to save.
    ///
    /// [`Error::HttpRefused`] at `503` when the node is already holding as many
    /// sessions as it will. That one **is** retriable, and a caller that treats
    /// every failure here as fatal will give up on a node that is merely busy.
    ///
    /// [`Error::Malformed`] when the answer carries no `token`.
    ///
    /// # This call is not retried
    ///
    /// The third exemption from [`attempts`](Self::attempts), for a reason of
    /// its own: if the request reached the node and the answer was lost on the
    /// way back, a token now exists that nobody holds, and it occupies one of
    /// the node's bounded slots until it expires. Asking again from here would
    /// mint a second. A caller who wants to try again may — the cost is the same
    /// either way — but it is their decision to take rather than one made
    /// silently inside a loop.
    pub async fn open_session(&mut self) -> Result<u64> {
        if self.credential.is_none() {
            return Err(Error::NoCredential);
        }
        let reply = self
            .exchange("POST", "/session", None, Presenting::PasswordOnly)
            .await?;
        if reply.status != 200 {
            return Err(refusal(&reply));
        }
        let answer: Json = serde_json::from_slice(&reply.body).map_err(|_| Error::Malformed)?;
        let token = answer
            .get("token")
            .and_then(Json::as_str)
            .ok_or(Error::Malformed)?;
        // Absent rather than fatal: the lifetime is worth reporting and is not
        // worth refusing a working token over. A caller reading zero learns the
        // node did not say, which is the truth.
        let lifetime = answer
            .get("expires_in")
            .and_then(Json::as_u64)
            .unwrap_or_default();
        // The header is built once and held, the same way the password's is, so
        // the token is formatted in one place rather than at each request.
        self.session = Some(format!("Bearer {token}"));
        Ok(lifetime)
    }

    /// Give the token back and go back to presenting the password.
    ///
    /// A handle with no session open does nothing and succeeds — there is
    /// nothing to give back, and nothing a caller would do differently.
    ///
    /// **The session is dropped locally whatever the node answers.** A token
    /// this client has stopped holding is a token it will never present again,
    /// so keeping it after a failed request would leave the handle carrying a
    /// credential it has already decided not to use.
    ///
    /// The node answers the same whether or not it was holding the token —
    /// whether one it never issued *exists* is not something the presenter is
    /// entitled to learn.
    ///
    /// # Errors
    ///
    /// [`Error::HttpRefused`] at any status other than `200`, and the transport
    /// errors of any other call. The session is already gone in every case.
    pub async fn close_session(&mut self) -> Result<()> {
        if self.session.is_none() {
            return Ok(());
        }
        // The token has to still be held while this is sent — it travels in the
        // header and it is the only thing the node has to identify what to
        // forget. Clearing it first would send the password instead, and the
        // node would answer `200` for a token it still holds.
        let outcome = self
            .exchange("DELETE", "/session", None, Presenting::Whatever)
            .await;
        // Dropped before the answer is judged, so every path below leaves the
        // handle in the same state: this client has decided not to present that
        // token again, and whether the node agreed does not change that.
        self.session = None;
        let reply = outcome?;
        if reply.status != 200 {
            return Err(refusal(&reply));
        }
        Ok(())
    }

    /// Change this user's own password, and keep the handle usable.
    ///
    /// The one call on this surface whose **success invalidates the credential
    /// that authorised it**. Every other method here leaves the handle as it
    /// found it; after this one the stored header is a credential for a password
    /// that no longer exists, and the next call would come back `401` while the
    /// caller's own reading is that the change worked. Nothing raises an error
    /// at the moment the handle goes stale, which is why this takes `&mut self`
    /// and rebuilds the header rather than documenting the trap.
    ///
    /// # What the node does with it
    ///
    /// Measured, not assumed. The body is the new password as plain text, and
    /// the node **trims trailing newlines** from it — a password ending in one
    /// cannot be set through this route, and this client does not pretend
    /// otherwise by escaping it. The current password is re-verified before the
    /// change, so a wrong one is refused rather than quietly accepted. On
    /// success **every token this user held stops working**, including one this
    /// handle is holding — so the session is discarded here rather than left to
    /// fail on the next call. Reopen it with
    /// [`open_session`](Self::open_session) if the handle is long-lived.
    ///
    /// # A clone made earlier is not updated
    ///
    /// [`bucket`](Self::bucket) clones the handle, so a [`Bucket`] built before
    /// the change keeps the old credential and starts failing. That follows from
    /// `Clone` and is stated rather than defended against: a handle that reached
    /// back into its own clones to correct them would be a larger surprise than
    /// the one it fixed. Build buckets after the change, or rebuild them.
    ///
    /// # Errors
    ///
    /// [`Error::NoCredential`] — **before anything is sent** — when this handle
    /// presents no credential. The body of this request *is* the new password,
    /// and the route needs a current one, so a handle without a credential would
    /// be putting a secret on the wire for a request that cannot succeed.
    ///
    /// [`Error::HttpRefused`] otherwise: `401` for a wrong current password, and
    /// note that a token would be refused here too — this route takes Basic
    /// credentials only, deliberately, because a token is not proof of a
    /// password. The handle is left untouched on every failure.
    ///
    /// # This one call is never retried
    ///
    /// [`attempts`](Self::attempts) does not reach it, and the exemption is the
    /// point rather than an oversight. Retrying is safe on this surface because
    /// every other method it carries is idempotent; this one is the exception
    /// the invariant was waiting for. If the first attempt reached the store and
    /// its answer was lost on the way back, the password has already changed —
    /// so the second attempt presents a credential the first one invalidated and
    /// comes back `401`, reporting a failure for a change that succeeded.
    ///
    /// A transport error here therefore means *the outcome is unknown*, which is
    /// the honest answer and is one a caller can act on: try the new password.
    pub async fn change_password(&mut self, new: &str) -> Result<()> {
        let Some(held) = self.credential.as_ref() else {
            return Err(Error::NoCredential);
        };
        // Built before the exchange but installed only after it, so a refusal
        // cannot leave the handle holding a password the store never took.
        let next = Credential {
            name: held.name.clone(),
            header: basic::header(&held.name, new),
        };

        // `exchange` rather than `send`: one attempt, deliberately, for the
        // reason given above. This is the only caller on the type that reaches
        // past the retry loop, and it is the only one that may.
        let reply = self
            .exchange(
                "POST",
                "/password",
                Some(new.as_bytes()),
                Presenting::PasswordOnly,
            )
            .await?;
        if reply.status != 200 {
            return Err(refusal(&reply));
        }

        self.credential = Some(next);
        // The node ended it the moment the record changed; dropping it here is
        // what keeps this handle's state and the node's the same. Left in place
        // it would be presented on the next request and refused, and the `401`
        // would read as a password change that did not take.
        self.session = None;
        Ok(())
    }

    /// Take a backup of the whole store, writing it to `sink`.
    ///
    /// Answers the number of bytes written.
    ///
    /// # Why this one takes a writer when nothing else here does
    ///
    /// Every other route on this surface answers something whose size is known
    /// before it is asked for — a health object, a metrics page, a refusal, or a
    /// file the caller wrote themselves. This one answers the store's whole log,
    /// and on any store worth backing up that is larger than the 16 MiB this
    /// client will hold in memory. Returning `Vec<u8>` would have produced a
    /// method that works in its own tests and refuses every real store; raising
    /// the limit for one route would have traded that refusal for an
    /// out-of-memory kill. Streaming needs no limit, because nothing is allocated
    /// in proportion to the answer.
    ///
    /// A caller who does want the bytes in memory has lost nothing: `Vec<u8>` is
    /// itself a sink.
    ///
    /// # What the node decides, and this client does not
    ///
    /// `BACKUP` is a statement, and this route is a surface over it rather than a
    /// second implementation — so **who may take a backup is the language's
    /// answer**, given once. The statement needs an owner, and a grant-governed
    /// user is refused by name. A backup is every table at once, which is exactly
    /// why an endpoint deciding that for itself would be a second answer to a
    /// question already settled.
    ///
    /// Note the posture rule that applies here as everywhere: on a store with no
    /// user declared, this call **succeeds without a credential**, because there
    /// is no one for the node to refuse.
    ///
    /// # A refusal is never written to the sink
    ///
    /// The status decides before any byte moves. A `401` writes nothing, leaving
    /// the sink exactly as it was found — which matters more here than anywhere
    /// else on this surface, since a refusal copied into a backup file produces
    /// sixty plausible bytes that fail at restore rather than at the call.
    ///
    /// # This call is never retried
    ///
    /// [`attempts`](Self::attempts) does not reach it, and the reason is not
    /// [`change_password`](Self::change_password)'s. Retrying is safe elsewhere
    /// on this surface because a repeated request produces a fresh answer that
    /// replaces the first. Here the first answer is **already in the caller's
    /// writer**: a retry after a transport failure at three megabytes appends a
    /// second copy behind the partial one and reports success over a corrupt
    /// file.
    ///
    /// So a failure mid-copy is [`Error::Truncated`] **with bytes already
    /// written**, and that is the honest report rather than an oversight —
    /// nothing here can un-write another party's sink. A caller writing to a file
    /// discards it and asks again.
    ///
    /// # Errors
    ///
    /// [`Error::HttpRefused`] carrying the node's status and sentence, with the
    /// sink untouched. [`Error::Io`] from **either side** — a dropped connection
    /// and a full disk arrive as the same variant, which is worth knowing before
    /// treating one as the other.
    pub async fn backup<W>(&self, sink: &mut W) -> Result<u64>
    where
        W: AsyncWrite + Unpin,
    {
        self.backed_up("/backup", sink).await
    }

    /// Take a backup of everything committed **after** `from`, writing it to `sink`.
    ///
    /// The node's incremental: `BACKUP FROM <sequence>`, where the sequence is a
    /// commit position rather than a byte offset. It is not a resume — a copy
    /// that failed halfway is restarted, not continued — and this client offers
    /// no resume because the node offers nothing to build one from.
    ///
    /// # The route and the language disagree about how large a sequence can be
    ///
    /// Measured, and stated because it is surprising. The route reads this query
    /// as a `u64`, so this parameter is one; the statement it is folded into is
    /// then read by the language, whose integers are narrower. A sequence past
    /// that comes back `400` — *"18446744073709551615 … is not a number this
    /// store can hold"* — which is a refusal about the **value** rather than
    /// about the caller or the store.
    ///
    /// This client does not narrow the type to hide it. A sequence comes from a
    /// previous backup rather than from arithmetic, so the range is not one a
    /// caller reaches by accident, and clamping here would replace a clear
    /// sentence from the node with a silent adjustment nobody asked for.
    ///
    /// # Errors
    ///
    /// As [`backup`](Self::backup), plus `400` for a sequence the store cannot
    /// hold.
    pub async fn backup_from<W>(&self, from: u64, sink: &mut W) -> Result<u64>
    where
        W: AsyncWrite + Unpin,
    {
        self.backed_up(&format!("/backup?from={from}"), sink).await
    }

    /// Both backup routes, which differ only in their query.
    async fn backed_up<W>(&self, path: &str, sink: &mut W) -> Result<u64>
    where
        W: AsyncWrite + Unpin,
    {
        // `stream_into` rather than `send`: one attempt, for the reason given on
        // `backup`. This and `change_password` are the only callers on the type
        // that reach past the retry loop.
        let reply = self.stream_into(path, sink).await?;
        if reply.status != 200 {
            return Err(refusal(&reply));
        }
        // Present whenever a body was copied — the streaming reader requires a
        // declared length before it moves a byte.
        reply.length.ok_or(Error::Malformed)
    }

    /// The bucket of this name, in this database, in this namespace.
    ///
    /// Nothing is checked here and nothing is reached: the three names are
    /// carried to the node, which is the only party that knows whether they
    /// exist. A bucket must have been declared with `DEFINE BUCKET` — a table of
    /// the same name is refused, and refused by the node rather than guessed at
    /// here (LR-SDK-004).
    pub fn bucket(
        &self,
        namespace: impl Into<String>,
        database: impl Into<String>,
        name: impl Into<String>,
    ) -> Bucket {
        Bucket::new(self.clone(), namespace, database, name)
    }

    /// What the node reports about itself, in the Prometheus text format.
    ///
    /// Returned as the node wrote it. Parsing it here would mean this crate
    /// deciding which metrics matter and re-shaping them, and every metric the
    /// node adds would be one this client hides until it is taught about it.
    ///
    /// # Errors
    ///
    /// [`Error::HttpRefused`] carrying the status when the node answers with one
    /// that is not a success, and whatever the transport reports otherwise.
    pub async fn metrics(&self) -> Result<String> {
        let reply = self.get("/metrics").await?;
        String::from_utf8(reply.body).map_err(|_| Error::Malformed)
    }

    /// Whether the store behind this node is well.
    ///
    /// A failing liveness probe means *restart this node*. That is a different
    /// instruction from a failing readiness probe, which is why this is not
    /// [`ready`](Self::ready) under another name: on a healthy node the two
    /// agree, and they are worth separating precisely where they stop agreeing.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] if the node reports a condition this build does not
    /// know, and whatever the transport reports otherwise. A node that says it
    /// is **unwell** has answered the question, and arrives as
    /// [`Condition::Unwell`] rather than as an error.
    pub async fn health(&self) -> Result<Condition> {
        self.condition("/health").await
    }

    /// Whether this node will take new work now.
    ///
    /// A failing readiness probe means *stop sending traffic here* — the node is
    /// still running and may be perfectly well. During a staged shutdown it
    /// answers [`Condition::Leaving`] while [`health`](Self::health) still
    /// answers [`Condition::Ok`], which is the moment the two routes exist for.
    ///
    /// # Errors
    ///
    /// As [`health`](Self::health).
    pub async fn ready(&self) -> Result<Condition> {
        self.condition("/ready").await
    }

    /// Ask one of the condition routes, where 503 is an answer.
    ///
    /// These two routes report a refusal to serve *as their content*, so the
    /// usual "any non-2xx is a failure" rule is wrong here: a caller handed
    /// `Err` for a node that said "I am not ready" would have to read the
    /// message to tell it apart from a wrong address. Every other status is
    /// still a refusal.
    async fn condition(&self, path: &str) -> Result<Condition> {
        let reply = self.send("GET", path, None).await?;
        if reply.status != 200 && reply.status != 503 {
            return Err(refusal(&reply));
        }
        Condition::read(&reply.body)
    }

    /// Issue a GET and insist on a successful status.
    async fn get(&self, path: &str) -> Result<Reply> {
        let reply = self.send("GET", path, None).await?;
        if !(200..300).contains(&reply.status) {
            return Err(refusal(&reply));
        }
        Ok(reply)
    }

    /// Connect, write one request, read one response, drop the connection.
    ///
    /// `body` is `None` for a request that carries none, which is not the same
    /// as `Some(&[])`: an empty file is written by declaring a length of zero,
    /// and a request with no `Content-Length` at all is a different request.
    async fn send(&self, method: &str, path: &str, body: Option<&[u8]>) -> Result<Reply> {
        let mut made = 1_u8;
        loop {
            let outcome = self
                .exchange(method, path, body, Presenting::Whatever)
                .await;
            match &outcome {
                Err(failure) if retryable(failure) && made < self.attempts => {
                    made = made.saturating_add(1);
                    tokio::time::sleep(PAUSE_BETWEEN_ATTEMPTS).await;
                }
                _ => return outcome,
            }
        }
    }

    /// One connection, one request, one response — the attempt itself.
    ///
    /// Split from [`send`](Self::send) so that the retry decision is made in one
    /// place over a whole exchange. A retry woven into the steps below would have
    /// to decide what a half-written request means, and the answer is that this
    /// connection is finished either way.
    async fn exchange(
        &self,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
        presenting: Presenting,
    ) -> Result<Reply> {
        let mut reader = self.open(method, path, body, presenting).await?;
        // `expects_body` is the method's property, not the response's: a HEAD
        // answers with the `Content-Length` a GET would carry and sends nothing
        // after the headers, so a reader that trusts the header is reading bytes
        // that are never coming.
        //
        // **Measured, 2026-09-01**, because this comment used to say "waits
        // forever" and that is not what happens here: forcing this argument to
        // `true` fails in milliseconds with `Error::Truncated`, not in a hang.
        // The reason is `Connection: close` below — the node shuts the socket
        // after the headers, so `read_exact` hits the end of the stream instead
        // of blocking on it. The hang is real on a keep-alive connection and
        // this client has none, which is worth saying plainly rather than
        // leaving a scarier and wrong claim in place: the next person to weigh
        // connection reuse should know the failure gets *worse*, not that it is
        // already the worst.
        reply::read(&mut reader, method != "HEAD").await
    }

    /// One connection, one request, and a successful body written straight out.
    ///
    /// The streaming twin of [`exchange`](Self::exchange), and separate from it
    /// because the difference is only in how the answer is read: everything up to
    /// the first response byte is the same request, built in one place so the two
    /// paths cannot drift on a header.
    async fn stream_into<W>(&self, path: &str, sink: &mut W) -> Result<Reply>
    where
        W: AsyncWrite + Unpin,
    {
        let mut reader = self.open("GET", path, None, Presenting::Whatever).await?;
        reply::read_into(&mut reader, sink).await
    }

    /// Connect and write one request; the caller reads the answer.
    async fn open(
        &self,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
        presenting: Presenting,
    ) -> Result<BufReader<TcpStream>> {
        let mut stream = TcpStream::connect(&self.address).await?;
        stream.set_nodelay(true)?;

        // `Host` is required of an HTTP/1.1 request, and `Connection: close`
        // says this connection carries one exchange — which is true, and saying
        // so lets the node release it rather than hold it open for a reuse that
        // is not coming.
        let length = match body {
            Some(bytes) => format!("Content-Length: {}\r\n", bytes.len()),
            None => String::new(),
        };
        // Absent rather than empty when there is no credential. An
        // `Authorization:` with nothing after it is a header the node must then
        // decide what to make of, and the answer it gives to that is not one
        // this client has measured.
        let authorization = match (presenting, &self.session, &self.credential) {
            // The token first, because it is the reason it was asked for: the
            // node verifies a password with Argon2id at the OWASP floor on every
            // request that carries one, and looks a token up in a map.
            (Presenting::Whatever, Some(bearer), _) => {
                format!("Authorization: {bearer}\r\n")
            }
            (_, _, Some(held)) => format!("Authorization: {}\r\n", held.header),
            (_, _, None) => String::new(),
        };
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\
             {authorization}{length}\r\n",
            self.address
        );
        stream.write_all(request.as_bytes()).await?;
        if let Some(bytes) = body {
            stream.write_all(bytes).await?;
        }
        stream.flush().await?;

        Ok(BufReader::new(stream))
    }
}

/// Whether a failure means the exchange did not complete.
///
/// The two cases are *the socket failed* and *the stream ended inside the
/// answer*, and they are one thing from a caller's side: nothing usable came
/// back, and on an idempotent request asking again is safe. A connection dropped
/// mid-answer is indistinguishable from one dropped before it, which is why
/// `Truncated` is here — and a node genuinely breaking its framing exhausts the
/// attempts and reports it anyway.
///
/// Everything else is excluded because it is something that **did** arrive:
/// `HttpRefused` is the node's own answer at a status, and `Malformed`,
/// `TooLarge` and `NotThisProtocol` are judgements about bytes that were
/// received. Repeating the request cannot change any of them.
const fn retryable(failure: &Error) -> bool {
    matches!(failure, Error::Io(_) | Error::Truncated)
}

/// The node said no, over HTTP, with a status.
///
/// The status is carried as a field rather than folded into the sentence. The
/// protocol enumerates its refusals by code and says a client branches on the
/// code, so a client that offered only prose would be handing the caller the one
/// thing it was told not to parse.
fn refusal(reply: &Reply) -> Error {
    Error::HttpRefused {
        status: reply.status,
        message: sentence(&reply.body),
    }
}

/// The node's sentence, unwrapped from the JSON that carried it.
///
/// Every refusal the protocol answers as JSON is the same single-field object,
/// `{"error": "…"}`. Handing that object to a caller as the error message leaves
/// them reading punctuation, or parsing it a second time — so it is unwrapped
/// here, once.
///
/// The fallback is the body verbatim, for a refusal that is not that shape at
/// all. Something between the caller and the node — a proxy, a gateway — answers
/// in its own words, and those words are worth showing rather than replacing
/// with a sentence this crate invented about a node it never reached.
fn sentence(body: &[u8]) -> String {
    serde_json::from_slice::<Json>(body)
        .ok()
        .and_then(|json| json.get("error").and_then(Json::as_str).map(str::to_owned))
        .unwrap_or_else(|| String::from_utf8_lossy(body).trim().to_owned())
}

#[cfg(test)]
mod tests {
    // A test is the one place a panic is the correct outcome; these lints exist
    // to keep panics out of the paths a caller runs.
    #![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    use super::Operations;
    use crate::error::Error;

    /// A listener that hangs up on its first `hangups` connections and then
    /// answers `status` — counting every connection it accepts.
    ///
    /// The count is the point. An assertion on the *answer* passes whether the
    /// client asked once or five times, so a retry loop that retried refusals
    /// too would look identical from outside. Counting connections is the only
    /// observation that can tell them apart.
    async fn listener(hangups: usize, status: u16) -> (String, Arc<AtomicUsize>) {
        let socket = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a free port for the mock");
        let address = socket
            .local_addr()
            .expect("the mock's own address")
            .to_string();
        let seen = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&seen);

        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = socket.accept().await else {
                    return;
                };
                let so_far = counted.fetch_add(1, Ordering::SeqCst);
                if so_far < hangups {
                    // Dropped without answering: the client sees the stream end
                    // where a status line should be.
                    drop(stream);
                    continue;
                }
                let body = "ok";
                let answer = format!(
                    "HTTP/1.1 {status} .\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(answer.as_bytes()).await;
                let _ = stream.flush().await;
            }
        });

        (address, seen)
    }

    /// A listener that answers `status` and keeps every request it was handed,
    /// headers and body together.
    ///
    /// The counting listener above cannot see what was *sent*, and the whole
    /// claim of [`Operations::change_password`] is about a header changing. A
    /// test that only checked the returned `Result` would pass on a method that
    /// updated nothing, which is the failure being guarded against.
    async fn recorder(status: u16, body: &'static str) -> (String, Arc<Mutex<Vec<String>>>) {
        let socket = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a free port for the mock");
        let address = socket
            .local_addr()
            .expect("the mock's own address")
            .to_string();
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let kept = Arc::clone(&seen);

        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = socket.accept().await else {
                    return;
                };
                let mut request = Vec::new();
                let mut byte = [0_u8; 1];
                // One byte at a time, which is slow and exactly right for a
                // mock: it stops at the end of the body instead of blocking on
                // a read that will never fill.
                while stream.read_exact(&mut byte).await.is_ok() {
                    request.push(byte[0]);
                    if finished(&request) {
                        break;
                    }
                }
                kept.lock()
                    .expect("the mock's record")
                    .push(String::from_utf8_lossy(&request).into_owned());

                let answer = format!(
                    "HTTP/1.1 {status} .\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(answer.as_bytes()).await;
                let _ = stream.flush().await;
            }
        });

        (address, seen)
    }

    /// Whether a request is complete: headers, then exactly the declared body.
    fn finished(request: &[u8]) -> bool {
        let text = String::from_utf8_lossy(request);
        let Some(head) = text.find("\r\n\r\n") else {
            return false;
        };
        let declared = text
            .lines()
            .find_map(|line| line.strip_prefix("Content-Length: "))
            .and_then(|written| written.trim().parse::<usize>().ok())
            .unwrap_or(0);
        // Saturating because this crate denies bare arithmetic everywhere,
        // tests included: a mock that overflowed its way to `true` would report
        // a complete request where the bytes had not arrived.
        request.len() >= head.saturating_add(4).saturating_add(declared)
    }

    /// The header a handle presents, as it appears on the wire.
    const OLD: &str = "Basic YWRtaW46b2xk";
    /// The same, after the password below has been changed to `new`.
    const NEW: &str = "Basic YWRtaW46bmV3";

    #[tokio::test]
    async fn changing_a_password_without_one_opens_no_connection() {
        // C4, and the criterion that cannot be checked from the return value:
        // the refusal looks identical whether or not the new password crossed
        // the network, so the connection count is the whole assertion. The body
        // of this request *is* a secret, and this client terminates no TLS.
        let (address, seen) = listener(0, 200).await;
        let mut handle = Operations::at(&address);

        let refused = handle
            .change_password("new")
            .await
            .expect_err("a handle with no credential cannot change a password");

        assert!(
            matches!(refused, Error::NoCredential),
            "reported as itself, not as a 401 this client invented; got {refused:?}"
        );
        assert_eq!(
            seen.load(Ordering::SeqCst),
            0,
            "nothing may be sent — the body would be the new password, on an \
             exchange that cannot succeed"
        );
    }

    #[tokio::test]
    async fn the_new_password_is_the_body_and_the_old_credential_signs_for_it() {
        // C1. Two claims about one request, and both are on the wire rather
        // than in the return value.
        let (address, seen) = recorder(200, "ok").await;
        let mut handle = Operations::at(&address).as_user("admin", "old");

        handle
            .change_password("new")
            .await
            .expect("the mock says 200");

        let sent = seen.lock().expect("the mock's record");
        let request = sent.first().expect("exactly one request was made");
        assert!(
            request.starts_with("POST /password HTTP/1.1\r\n"),
            "the route and method are the node's, not invented; got {request}"
        );
        assert!(
            request.contains(&format!("Authorization: {OLD}\r\n")),
            "the *current* password authorises the change; got {request}"
        );
        assert!(
            request.ends_with("\r\n\r\nnew"),
            "the body is the new password with nothing wrapped around it; got \
             {request}"
        );
    }

    #[tokio::test]
    async fn a_successful_change_leaves_the_handle_holding_the_new_password() {
        // C2, the wave's reason for existing. Without this the call succeeds
        // and every later call on the same handle returns 401, with nothing
        // anywhere in an error state at the moment the handle went stale.
        let (address, seen) = recorder(200, "ok").await;
        let mut handle = Operations::at(&address).as_user("admin", "old");

        handle
            .change_password("new")
            .await
            .expect("the mock says 200");
        let _ = handle.metrics().await;

        let sent = seen.lock().expect("the mock's record");
        let after = sent.get(1).expect("a second request followed the change");
        assert!(
            after.contains(&format!("Authorization: {NEW}\r\n")),
            "the handle must sign the next call with the password it just set; \
             got {after}"
        );
    }

    #[tokio::test]
    async fn a_refused_change_leaves_the_handle_exactly_as_it_was() {
        // C5. The other half of C2 and the one that would go unnoticed: a
        // handle that adopted a password the store rejected would fail every
        // later call, and the failure would name the later call rather than
        // this one.
        let (address, seen) = recorder(401, "ok").await;
        let mut handle = Operations::at(&address).as_user("admin", "old");

        let refused = handle
            .change_password("new")
            .await
            .expect_err("the mock refuses");
        assert!(
            matches!(refused, Error::HttpRefused { status: 401, .. }),
            "the node's own answer reaches the caller; got {refused:?}"
        );

        let _ = handle.metrics().await;
        let sent = seen.lock().expect("the mock's record");
        let after = sent.get(1).expect("a second request followed the refusal");
        assert!(
            after.contains(&format!("Authorization: {OLD}\r\n")),
            "a refused change must not be adopted; got {after}"
        );
    }

    #[tokio::test]
    async fn a_password_change_is_never_retried_however_many_attempts_are_set() {
        // The exemption, pinned. Routing this call back through `send` would
        // pass every other test in this file and break only under a truncated
        // answer, where the second attempt presents the credential the first
        // one invalidated and a successful change reports 401.
        let (address, seen) = listener(1, 200).await;
        let mut handle = Operations::at(&address).as_user("admin", "old").attempts(5);

        let failed = handle
            .change_password("new")
            .await
            .expect_err("the first attempt is hung up on and there is no second");

        assert!(
            matches!(failed, Error::Truncated | Error::Io(_)),
            "the transport failure is reported as itself; got {failed:?}"
        );
        assert_eq!(
            seen.load(Ordering::SeqCst),
            1,
            "exactly one attempt, even with five allowed — the outcome of a lost \
             answer is unknown, and asking again cannot resolve it"
        );
    }

    #[tokio::test]
    async fn a_transport_failure_that_heals_is_survived() {
        // F1. The first connection is hung up on; the second is answered.
        let (address, seen) = listener(1, 200).await;

        let answered = Operations::at(&address)
            .attempts(3)
            .metrics()
            .await
            .expect("the second attempt should be answered");

        assert_eq!(answered, "ok");
        assert_eq!(
            seen.load(Ordering::SeqCst),
            2,
            "one failed attempt and one that worked — no more and no fewer"
        );
    }

    #[tokio::test]
    async fn the_default_does_not_retry() {
        // F2. The same listener, the same failure, and no second attempt —
        // because retrying is something a caller asks for.
        let (address, seen) = listener(1, 200).await;

        let refused = Operations::at(&address)
            .metrics()
            .await
            .expect_err("a hung-up connection with one attempt must not succeed");

        assert!(
            matches!(refused, Error::Truncated | Error::Io(_)),
            "the failure must be the transport's, not something invented; got {refused:?}"
        );
        assert_eq!(
            seen.load(Ordering::SeqCst),
            1,
            "the default is one attempt, so exactly one connection is made"
        );
    }

    #[tokio::test]
    async fn a_refusal_is_answered_once_however_many_attempts_are_allowed() {
        // F3, and the criterion most able to pass vacuously. A 401 comes back
        // either way; only the connection count shows whether the client
        // hammered the node with a credential it already knows is wrong.
        let (address, seen) = listener(0, 401).await;

        let refused = Operations::at(&address)
            .attempts(5)
            .metrics()
            .await
            .expect_err("a 401 is a refusal, not a success");

        assert!(
            matches!(refused, Error::HttpRefused { status: 401, .. }),
            "the node's own answer must survive the retry layer; got {refused:?}"
        );
        assert_eq!(
            seen.load(Ordering::SeqCst),
            1,
            "a refusal is the node answering, so asking again is a loop"
        );
    }

    #[tokio::test]
    async fn attempts_are_bounded() {
        // F4. A listener that never answers, and a client that gives up after
        // exactly the number it was given.
        let (address, seen) = listener(usize::MAX, 200).await;

        let gave_up = Operations::at(&address)
            .attempts(2)
            .metrics()
            .await
            .expect_err("nothing ever answers, so this must fail");

        assert!(
            matches!(gave_up, Error::Truncated | Error::Io(_)),
            "the last failure is reported as itself; got {gave_up:?}"
        );
        assert_eq!(
            seen.load(Ordering::SeqCst),
            2,
            "two attempts were allowed, so two connections were made"
        );
    }

    #[tokio::test]
    async fn asking_for_no_attempts_still_asks_once() {
        // A request nobody sends is not something a caller can want. Clamped
        // rather than refused, and pinned so the clamp is not quietly dropped.
        let (address, seen) = listener(0, 200).await;

        Operations::at(&address)
            .attempts(0)
            .metrics()
            .await
            .expect("zero attempts must still mean one");

        assert_eq!(seen.load(Ordering::SeqCst), 1);
    }

    /// The password, and the base64 that is only a spelling of it.
    ///
    /// Both are asserted absent: checking for the plaintext alone would pass on
    /// a `Debug` that printed the header, which is the exact thing being
    /// prevented — the header is a working credential, decodable by anything.
    #[test]
    fn debugging_a_handle_does_not_print_the_credential() {
        let handle = Operations::at("127.0.0.1:1").as_user("admin", "s3cr3t pw");
        let shown = format!("{handle:?}");

        assert!(
            !shown.contains("s3cr3t"),
            "the plaintext password must not be formatted; got {shown}"
        );
        assert!(
            !shown.contains("YWRtaW46czNjcjN0IHB3"),
            "nor the header, which decodes back to it; got {shown}"
        );
        assert!(
            shown.contains("<present>"),
            "that a credential is set is worth saying — it is the first thing \
             looked for when a 401 arrives; got {shown}"
        );
        assert!(
            shown.contains("127.0.0.1:1"),
            "the address is not a secret and is the other half of the question; \
             got {shown}"
        );
    }

    /// A bucket derives its `Debug` and holds one of these, so it is the second
    /// way the credential reaches a log and is worth pinning separately.
    #[test]
    fn debugging_a_bucket_does_not_print_the_credential_either() {
        let bucket = Operations::at("127.0.0.1:1")
            .as_user("admin", "s3cr3t pw")
            .bucket("app", "main", "files");
        let shown = format!("{bucket:?}");

        assert!(
            !shown.contains("s3cr3t") && !shown.contains("YWRtaW46czNjcjN0IHB3"),
            "a bucket carries the handle, and formatting it must not carry the \
             credential out with it; got {shown}"
        );
    }

    /// A node whose answer can be larger than this client would ever buffer, and
    /// which can stop short of what it declared.
    ///
    /// One helper for the whole backup surface, because the four things worth
    /// asserting there are four settings of the same two numbers: `declared` is
    /// the `Content-Length` it writes and `sent` is how many bytes actually
    /// follow. Equal, it is an ordinary large answer; `sent` smaller, it is a
    /// connection that dies mid-body, which is the only way to observe whether
    /// the call was retried.
    ///
    /// Both the requests and the connection count are returned, because C3 and
    /// C5 cannot be seen from a return value: a refusal is `Err` whether or not
    /// it scribbled in the sink, and a truncated read is `Err` whether the client
    /// asked once or three times.
    async fn serves(
        status: u16,
        declared: usize,
        sent: usize,
    ) -> (String, Arc<Mutex<Vec<String>>>, Arc<AtomicUsize>) {
        let socket = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a free port for the mock");
        let address = socket
            .local_addr()
            .expect("the mock's own address")
            .to_string();
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let kept = Arc::clone(&seen);
        let count = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&count);

        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = socket.accept().await else {
                    return;
                };
                counted.fetch_add(1, Ordering::SeqCst);
                let mut request = Vec::new();
                let mut byte = [0_u8; 1];
                while stream.read_exact(&mut byte).await.is_ok() {
                    request.push(byte[0]);
                    if finished(&request) {
                        break;
                    }
                }
                kept.lock()
                    .expect("the mock's record")
                    .push(String::from_utf8_lossy(&request).into_owned());

                let head = format!(
                    "HTTP/1.1 {status} .\r\nContent-Length: {declared}\r\n\
                     Connection: close\r\n\r\n"
                );
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.write_all(&pattern(sent)).await;
                let _ = stream.flush().await;
            }
        });

        (address, seen, count)
    }

    /// `size` bytes that vary along their length.
    ///
    /// A constant byte would pass a copy that duplicated or dropped a chunk as
    /// long as the total came out right; a repeating word does not.
    fn pattern(size: usize) -> Vec<u8> {
        const WORD: &[u8] = b"tessaridb-backup";
        let mut made = Vec::with_capacity(size);
        while made.len() < size {
            made.extend_from_slice(WORD);
        }
        made.truncate(size);
        made
    }

    #[tokio::test]
    async fn a_backup_is_asked_for_with_the_credential_and_lands_in_the_sink() {
        // C1. Both halves matter: a method that opened the right route and wrote
        // nothing would pass an assertion on the request alone.
        let (address, seen, _) = serves(200, 4096, 4096).await;
        let mut sink = Vec::new();

        let written = Operations::at(&address)
            .as_user("admin", "old")
            .backup(&mut sink)
            .await
            .expect("the node answered a backup");

        let request = seen
            .lock()
            .expect("the mock's record")
            .first()
            .expect("the mock was asked exactly once")
            .clone();
        assert!(
            request.starts_with("GET /backup HTTP/1.1\r\n"),
            "the whole-store backup takes no query; got {request:?}"
        );
        assert!(
            request.contains(&format!("Authorization: {OLD}\r\n")),
            "the handle's credential has to reach a route the node authorises; \
             got {request:?}"
        );
        assert_eq!(written, 4096, "the count is what was written");
        assert_eq!(
            sink,
            pattern(4096),
            "and the bytes are the node's, in order"
        );
    }

    #[tokio::test]
    async fn an_incremental_backup_names_the_sequence_in_the_query() {
        // C2. The node refuses any query but this one, so the spelling is the
        // assertion — `from=7` and nothing else.
        let (address, seen, _) = serves(200, 32, 32).await;
        let mut sink = Vec::new();

        Operations::at(&address)
            .backup_from(7, &mut sink)
            .await
            .expect("the node answered an incremental backup");

        let request = seen
            .lock()
            .expect("the mock's record")
            .first()
            .expect("the mock was asked exactly once")
            .clone();
        assert!(
            request.starts_with("GET /backup?from=7 HTTP/1.1\r\n"),
            "the sequence is the one query this route takes; got {request:?}"
        );
    }

    #[tokio::test]
    async fn a_refused_backup_writes_nothing_at_all_into_the_sink() {
        // C3, and the criterion that would fail silently in production: the call
        // returns `Err` whether or not the refusal was copied, so the assertion
        // is on the sink. A `{"error":…}` written into a backup file is sixty
        // plausible bytes that fail at restore rather than at the call.
        let (address, _, _) = serves(401, 46, 46).await;
        let mut sink = Vec::new();

        let refused = Operations::at(&address)
            .backup(&mut sink)
            .await
            .expect_err("a 401 is a refusal");

        assert!(
            matches!(refused, Error::HttpRefused { status: 401, .. }),
            "the node's own status, carried as a field; got {refused:?}"
        );
        assert!(
            sink.is_empty(),
            "a refusal body must not reach the caller's writer; got {} bytes",
            sink.len()
        );
    }

    #[tokio::test]
    async fn a_backup_larger_than_this_client_would_buffer_is_delivered() {
        // C4, and the reason the wave exists. 17 MiB is over the 16 MiB ceiling
        // every other route on this surface is held to, so a buffering
        // implementation fails here with `TooLarge` — which is exactly what
        // returning `Vec<u8>` would have done to every real store.
        const OVER: usize = 17 * 1024 * 1024;
        let (address, _, _) = serves(200, OVER, OVER).await;
        let mut sink = Vec::new();

        let written = Operations::at(&address)
            .backup(&mut sink)
            .await
            .expect("a backup is not held to a ceiling it would always exceed");

        assert_eq!(
            written,
            u64::try_from(OVER).expect("a test constant fits"),
            "every declared byte was copied"
        );
        assert_eq!(sink.len(), OVER, "and every one of them reached the sink");
        assert_eq!(
            sink.get(..16),
            Some(&b"tessaridb-backup"[..]),
            "in order, from the first chunk"
        );
    }

    #[tokio::test]
    async fn a_backup_is_never_retried_however_many_attempts_are_set() {
        // C5. The server declares more than it sends, and the numbers are chosen
        // to straddle a chunk boundary so the failure lands with bytes ALREADY
        // in the sink — which is the whole hazard. A retry would append a second
        // copy behind that partial one and report success over a corrupt file,
        // so the connection count is the assertion: the returned error is
        // identical either way.
        let (address, _, connections) = serves(200, 200_000, 100_000).await;
        let mut sink = Vec::new();

        let failed = Operations::at(&address)
            .attempts(3)
            .backup(&mut sink)
            .await
            .expect_err("a body that ends early is a truncated answer");

        assert!(
            matches!(failed, Error::Truncated),
            "the stream ended inside the body; got {failed:?}"
        );
        assert!(
            !sink.is_empty(),
            "the partial write is the premise of this test, not an accident — \
             without it a retry would be harmless and the exemption pointless"
        );
        assert_eq!(
            connections.load(Ordering::SeqCst),
            1,
            "asked exactly once: the first answer is already in the caller's \
             sink, so a second attempt would append to it"
        );
    }

    /// Absence is reported as absence rather than as nothing at all.
    #[test]
    fn a_handle_with_no_credential_says_which_it_is() {
        let shown = format!("{:?}", Operations::at("127.0.0.1:1"));
        assert!(
            shown.contains("<none>"),
            "sending no credential is the most likely cause of a 401, so the \
             output has to distinguish it from a credential it declined to \
             print; got {shown}"
        );
    }

    /// The `n`th request the recorder kept.
    ///
    /// `sent[n]` would be the obvious spelling and this crate denies indexing
    /// everywhere, tests included — a panic inside an assertion reports a
    /// missing request as a crash rather than as the failure it is.
    fn nth(sent: &[String], n: usize) -> &str {
        sent.get(n).map_or_else(
            || panic!("the mock never recorded request {n}"),
            String::as_str,
        )
    }

    /// The answer `POST /session` gives, as the node writes it.
    const MINTED: &str = r#"{"token":"3f1c","expires_in":43200}"#;

    #[tokio::test]
    async fn opening_a_session_spends_the_password_and_then_stops_sending_it() {
        // The whole reason the method exists, and neither half is visible in the
        // return value: the *first* request must carry the password, and every
        // request after it must carry the token instead. A method that stored
        // the token and went on presenting Basic would return `Ok(43200)` and
        // change nothing a caller could measure except the latency.
        let (address, seen) = recorder(200, MINTED).await;
        let mut handle = Operations::at(&address).as_user("admin", "old");

        let lifetime = handle
            .open_session()
            .await
            .expect("the mock answers the shape the node answers");
        assert_eq!(lifetime, 43_200, "read from the answer, not assumed");

        let _ = handle.health().await;

        let sent = seen.lock().expect("the mock's record");
        assert_eq!(sent.len(), 2, "one exchange to open, one to use it");
        assert!(
            nth(&sent, 0).starts_with("POST /session "),
            "the password is spent on /session; got {}",
            nth(&sent, 0).lines().next().unwrap_or_default()
        );
        assert!(
            nth(&sent, 0).contains(&format!("Authorization: {OLD}\r\n")),
            "the route refuses a token, so this one carries the password"
        );
        assert!(
            nth(&sent, 1).contains("Authorization: Bearer 3f1c\r\n"),
            "every later request carries the token; got {}",
            nth(&sent, 1)
        );
        assert!(
            !nth(&sent, 1).contains("Basic"),
            "and stops carrying the password — that saving is the point"
        );
    }

    #[tokio::test]
    async fn a_session_is_never_offered_to_the_two_routes_that_refuse_one() {
        // `POST /session` and `POST /password` answer `401` to a token by
        // design. A handle that presented the token it is holding would turn a
        // working call into a refusal that reads exactly like a wrong password,
        // which is the most expensive way to be wrong here.
        let (address, seen) = recorder(200, MINTED).await;
        let mut handle = Operations::at(&address).as_user("admin", "old");

        handle.open_session().await.expect("a token to hold");
        // A second open, now while one is held, and a password change after it.
        handle.open_session().await.expect("another token");
        handle
            .change_password("new")
            .await
            .expect("the mock answers 200");

        let sent = seen.lock().expect("the mock's record");
        for request in sent.iter() {
            let line = request.lines().next().unwrap_or_default();
            assert!(
                request.contains("Authorization: Basic "),
                "both refusing routes take the password; {line} did not"
            );
            assert!(
                !request.contains("Bearer"),
                "and must not be handed a token; {line} was"
            );
        }
    }

    #[tokio::test]
    async fn a_password_change_drops_the_session_the_node_has_already_ended() {
        // The node ends every token the user held the moment the record
        // changes. A handle that kept its own would present it on the next call
        // and read the `401` as a password change that did not take — the
        // failure is a mis-diagnosis rather than a lost request.
        let (address, seen) = recorder(200, MINTED).await;
        let mut handle = Operations::at(&address).as_user("admin", "old");

        handle.open_session().await.expect("a token to hold");
        handle
            .change_password("new")
            .await
            .expect("the mock answers 200");
        let _ = handle.health().await;

        let sent = seen.lock().expect("the mock's record");
        let last = sent.last().expect("three requests were made");
        assert!(
            last.contains(&format!("Authorization: {NEW}\r\n")),
            "back to the new password, with no token in sight; got {last}"
        );
    }

    #[tokio::test]
    async fn closing_a_session_hands_the_token_back_before_forgetting_it() {
        // The token travels in the header and is the only thing naming what the
        // node should forget. Clearing it locally first would send the password
        // instead, and the node would answer `200` for a token it still holds —
        // a leak that looks exactly like a success.
        let (address, seen) = recorder(200, MINTED).await;
        let mut handle = Operations::at(&address).as_user("admin", "old");

        handle.open_session().await.expect("a token to hold");
        handle.close_session().await.expect("the mock answers 200");
        let _ = handle.health().await;

        let sent = seen.lock().expect("the mock's record");
        assert!(
            nth(&sent, 1).starts_with("DELETE /session "),
            "given back on the route that takes it"
        );
        assert!(
            nth(&sent, 1).contains("Authorization: Bearer 3f1c\r\n"),
            "carrying the token, which is what the node forgets by; got {}",
            nth(&sent, 1)
        );
        assert!(
            nth(&sent, 2).contains(&format!("Authorization: {OLD}\r\n")),
            "and afterwards the handle is back on the password"
        );
    }

    #[tokio::test]
    async fn closing_a_session_nobody_opened_opens_no_connection() {
        // There is nothing to give back and nothing a caller would do
        // differently, so this succeeds without a request. Counted rather than
        // asserted on the result, because `Ok(())` says nothing about whether a
        // pointless exchange happened.
        let (address, seen) = listener(0, 200).await;
        let mut handle = Operations::at(&address).as_user("admin", "old");

        handle
            .close_session()
            .await
            .expect("no session is not a failure");

        assert_eq!(seen.load(Ordering::SeqCst), 0, "nothing to send");
    }

    #[tokio::test]
    async fn opening_a_session_without_a_password_opens_no_connection() {
        // The same shape as the password-change refusal: the route exists to
        // spend a password, a handle with none has nothing to spend, and the
        // refusal is reported as itself rather than as a `401` this client
        // invented about a node it never reached.
        let (address, seen) = listener(0, 200).await;
        let mut handle = Operations::at(&address);

        let refused = handle
            .open_session()
            .await
            .expect_err("there is no password to spend");

        assert!(
            matches!(refused, Error::NoCredential),
            "reported as itself; got {refused:?}"
        );
        assert_eq!(seen.load(Ordering::SeqCst), 0, "nothing may be sent");
    }

    #[tokio::test]
    async fn an_answer_with_no_token_in_it_is_malformed_and_leaves_no_session() {
        // A `200` whose body is not the documented shape must not leave the
        // handle believing it holds something. The failure to guard against is
        // storing `Bearer null` and presenting it for twelve hours.
        let (address, seen) = recorder(200, r#"{"expires_in":43200}"#).await;
        let mut handle = Operations::at(&address).as_user("admin", "old");

        let refused = handle
            .open_session()
            .await
            .expect_err("no token field, no session");
        assert!(
            matches!(refused, Error::Malformed),
            "the body is not the shape the route promises; got {refused:?}"
        );

        let _ = handle.health().await;
        let sent = seen.lock().expect("the mock's record");
        assert!(
            nth(&sent, 1).contains(&format!("Authorization: {OLD}\r\n")),
            "still on the password, holding nothing; got {}",
            nth(&sent, 1)
        );
    }

    #[tokio::test]
    async fn signing_in_as_somebody_else_does_not_keep_the_first_ones_token() {
        // A token outranks the credential on every ordinary route, so a handle
        // that kept one across `as_user` would say it is one user and act as
        // another — for twelve hours, with the right credential sitting unused
        // beside it.
        let (address, seen) = recorder(200, MINTED).await;
        let mut handle = Operations::at(&address).as_user("admin", "old");
        handle.open_session().await.expect("a token to hold");

        let handle = handle.as_user("admin", "new");
        let _ = handle.health().await;

        let sent = seen.lock().expect("the mock's record");
        let last = sent.last().expect("two requests were made");
        assert!(
            last.contains(&format!("Authorization: {NEW}\r\n")),
            "the new identity's password, not the old identity's token; got {last}"
        );
    }
}
