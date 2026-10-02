//! An async client for TessariDB.
//!
//! # What this crate is written against
//!
//! The **protocol specification**, and nothing else. This client links no crate
//! from the database's own repository, in this language or any other. What it
//! depends on is the frame layout, the tag numbers, the version byte and the
//! value codec — all of which are specified, and the specification is the
//! interface.
//!
//! That is not tidiness. A client that links the server's crates cannot be
//! written in another language at all, and couples every release of every client
//! to the server's internal refactorings.
//!
//! # Two transports, and the choice is forced
//!
//! A node serves two surfaces and neither carries everything. Statements and
//! subscriptions go over the **wire protocol**, because it carries the store's
//! full model of seventeen value types. Objects, files, backup and the operational
//! routes go over **HTTP**, because nothing else serves them.
//!
//! A caller never picks a transport per call. Routing statements over HTTP would
//! work, reach everything, and silently narrow every result — JSON carries six
//! types — and nothing at the call site would show what was lost.
//!
//! The wire half is [`Client`]. The HTTP half begins at [`Operations`], which
//! reaches the operational routes, the backup route, and — through
//! [`Bucket`] — the object and file surface.
//!
//! # Open a session before making many HTTP calls
//!
//! HTTP has no connection for a node to remember an identity on, so a handle
//! presenting a password presents it on **every request**, and the node verifies
//! it with Argon2id at the OWASP floor every time.
//! [`Operations::open_session`] spends the password once and holds a token
//! instead. Skipping it is correct and slow, and nothing about the slowness
//! points at its cause — which is the only reason it is mentioned this early.
//!
//! # TLS, or a network you trust
//!
//! A node given a certificate speaks TLS 1.3 on both ports and nothing else, and
//! a cluster node serves clients in the clear only when its operator chose to.
//! [`Client::connect_tls`] and [`Operations::with_tls`] take a [`Tls`] saying
//! whom to trust, and check the node's certificate and name on every
//! connection — there is no switch that skips either. [`Client::connect`] and a
//! plain [`Operations`] send credentials as they are given, which belongs on a
//! network you protect.
//!
//! # Example
//!
//! This is the README's example, kept here so the compiler checks it. A usage
//! example that lives only in prose is one that drifts.
//!
//! ```no_run
//! # async fn run() -> Result<(), tessaridb_client::Error> {
//! use tessaridb_client::{Client, Follow, Value};
//!
//! let mut client = Client::connect("127.0.0.1:9080").await?;
//!
//! let answers = client
//!     .run_with(
//!         "SELECT * FROM users WHERE age > $min;",
//!         None,
//!         [("min", Value::from(21_i64))],
//!     )
//!     .await?;
//!
//! // A subscription consumes the connection: a socket delivering changes is
//! // not also answering scripts. Two jobs, two connections.
//! let mut feed = Client::connect("127.0.0.1:9080")
//!     .await?
//!     .follow(&Follow::everything().to_table("users"))
//!     .await?;
//!
//! while let Some(change) = feed.next().await? {
//!     println!("{} {} at {}", change.table, change.id, change.sequence);
//! }
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]

pub mod codec;
pub mod geometry;
pub mod http;
pub mod mapping;
pub mod query;
pub mod value;
pub mod wire;

mod cache;
mod client;
mod consumer;
mod error;
mod feed;
#[cfg(feature = "tls")]
mod tls;
mod vault;

pub use crate::cache::{Lease, Space, Ttl};
pub use crate::client::Client;
pub use crate::consumer::{Consumer, Message, Settle, Stopper};
pub use crate::error::{EncodingFault, Error, Result};
pub use crate::feed::Feed;
#[cfg(feature = "tls")]
pub use crate::tls::Tls;
/// A connection to a node over TLS, as [`Client::connect_tls`] holds it.
#[cfg(feature = "tls")]
pub type Secured = tokio_rustls::client::TlsStream<tokio::net::TcpStream>;
pub use crate::geometry::{Geometry, Polygon, Position, Ring};
pub use crate::http::{Bucket, Condition, Operations, Series};
pub use crate::mapping::{FromRecord, FromValue, MappingFault, Row};
pub use crate::value::{Number, RecordId, RecordRef, Value, ValueRange};
pub use crate::vault::{Custody, Page, SealState, Vault, VaultStatus};
pub use crate::wire::message::{
    Answer, Correction, Exact, Names, Note, Parameters, Request, Suggested,
};
pub use crate::wire::push::{Became, Change, Follow};
pub use crate::wire::redirect::{Redirect, Settlement};

/// The protocol constants.
///
/// # The name lives here and only here
///
/// [`GREETING`] is four bytes that happen to spell the product's short name.
/// They are **not required to**: they are an arbitrary magic number whose only
/// job is to make "this is not one of ours" a clear answer at the first read.
///
/// Keeping them in one place is deliberate. A rename of the product must not
/// become a protocol break, and it only stays cheap while these bytes are a
/// constant rather than a string typed in several files.
pub mod protocol {
    /// What every connection says first, in both directions.
    pub const GREETING: &[u8; 4] = b"TESS";

    /// The protocol major this client speaks.
    ///
    /// Checked at the greeting so a mismatch is one clear refusal at the start
    /// rather than a decode failure somewhere in the middle that reads like
    /// corruption. A differing **major** means the bodies are not the same
    /// protocol and no conversation is possible.
    pub const MAJOR: u8 = 1;

    /// The protocol minor this client speaks.
    ///
    /// A differing minor is **not** a refusal. Each side learns the other's and
    /// uses it for one thing only: declining to send what an older peer cannot
    /// read. Decoding is already safe without it — an unknown outcome is stepped
    /// over by its length, and an unknown frame kind closes the connection.
    ///
    /// `2`: this client reads the redirect frame minor 1 introduced and sends
    /// the vault frame minor 2 introduced.
    pub const MINOR: u8 = 2;

    /// The largest frame this client will read — or send.
    ///
    /// A declared length above this is refused *before anything is allocated*,
    /// because a length from a stranger is not a promise. The ceiling applies
    /// outbound as well: a peer that emits what it would refuse to read is
    /// running two protocols.
    pub const CEILING: u32 = 16 * 1024 * 1024;

    /// The fixed size of a frame header: one kind byte and a `u32` length.
    pub const HEADER: usize = 5;

    /// The size of the greeting: the magic, then major, then minor.
    ///
    /// Its own constant rather than a reuse of [`HEADER`]. The two were the same
    /// number until the version became two bytes, and a shared constant would
    /// have made one of them silently wrong.
    pub const GREETING_BYTES: usize = GREETING.len() + 2;
}
