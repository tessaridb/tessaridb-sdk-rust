//! What can go wrong, kept apart rather than collapsed.
//!
//! The protocol distinguishes ten failures and a caller acts on them
//! differently. A client that flattens them into one transport error has thrown
//! away the part the caller needed — most sharply with [`Error::NoWritablePeer`],
//! whose remedy is a statement nobody ran rather than anything on the network.

use std::io;

/// The result of talking to a node.
pub type Result<T> = std::result::Result<T, Error>;

/// A failure talking to a node.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The socket failed. Retry the transport.
    #[error(transparent)]
    Io(#[from] io::Error),

    /// TLS with the node failed: the handshake, its name, its chain.
    ///
    /// The protocol's transport class (§1.1, §6) and deliberately not
    /// [`Error::Io`]: nothing about the next attempt at the same node would
    /// differ, so it is never retried — a retry loop matching `Io` must not see
    /// it. The TLS library's own words are carried through.
    #[error("TLS with that node failed: {0}")]
    Tls(String),

    /// Whatever answered is not one of these nodes.
    ///
    /// The greeting did not begin with the expected magic, so nothing further is
    /// attempted — the alternative is decoding arbitrary bytes as a frame.
    #[error("that is not a TessariDB node")]
    NotThisProtocol,

    /// It is a node, of a version this build does not speak.
    ///
    /// Refused at the greeting rather than discovered mid-conversation.
    #[error("that node speaks version {found}; this client speaks {supported}")]
    WrongVersion {
        /// What the node said.
        found: u8,
        /// What this client implements.
        supported: u8,
    },

    /// A frame kind this build does not have.
    ///
    /// The connection is finished rather than the frame skipped: a protocol that
    /// ignores what it does not understand is one where a version mismatch looks
    /// like silence.
    #[error("frame kind {tag} is not one this client knows")]
    UnknownFrame {
        /// The tag that arrived.
        tag: u8,
    },

    /// A declared length above what this build will read.
    ///
    /// Raised *before* the allocation, which is the whole point of the ceiling.
    #[error("a frame declared {length} bytes, which is more than this client will read")]
    TooLarge {
        /// What was declared.
        length: u32,
    },

    /// The stream ended inside something.
    #[error("the connection ended mid-frame")]
    Truncated,

    /// A body that does not hold what its own header claims.
    #[error("a frame's body is not the shape its header says")]
    Malformed,

    /// A value could not be decoded.
    ///
    /// Distinct from [`Error::Malformed`]: the frame was well formed and the
    /// value inside it was not.
    #[error("a value could not be decoded: {reason}")]
    Encoding {
        /// What the codec objected to.
        reason: EncodingFault,
    },

    /// This node accepts no writes and knows of no peer that does.
    ///
    /// **Not a network failure.** The remedy is a `DEFINE REPLICA … ROLES
    /// writable` nobody ran, and reporting this as a failed connection sends an
    /// operator to look at the network, where there is nothing to find.
    #[error("that node does not accept writes, and no peer is declared writable")]
    NoWritablePeer,

    /// The store said no, in its own words.
    ///
    /// Carried through verbatim. The node already writes messages that name the
    /// place in the script, and a client rewording them becomes a second author
    /// for one error.
    #[error("{message}")]
    Refused {
        /// The node's own words.
        message: String,
    },

    /// An HTTP route refused, and the status is how a caller tells why.
    ///
    /// Separate from [`Error::Refused`] because it carries something that one
    /// cannot: a status code. The protocol enumerates thirteen refusals by
    /// status and says in as many words that a client branches on the code and
    /// never on the sentence — the sentence is written for a person and embeds
    /// the caller's own input. A variant that offered only the message would
    /// leave a caller parsing prose to do what the protocol says to do with an
    /// integer.
    ///
    /// The distinctions that cost the most to miss: **401** means sign in,
    /// **403** means the grants do not cover this and signing in again never
    /// will, and **409** means a store-level conflict that is worth retrying
    /// after a change.
    #[error("the node answered {status}: {message}")]
    HttpRefused {
        /// The status the node sent.
        status: u16,
        /// The node's sentence, unwrapped from the JSON that carried it.
        message: String,
    },

    /// A batch for [`Series::append`](crate::Series::append) holds something an
    /// event cannot carry, found before anything was sent (protocol §5.9).
    #[error("not an event: {reason}")]
    NotAnEvent {
        /// What was wrong with it.
        reason: &'static str,
    },

    /// The node is older than the call: its greeting carried a minor below the
    /// one the vault frame needs. Refused before sending, because an older node
    /// closes the connection on a frame it does not know, which would read as a
    /// network fault rather than as the version gap it is.
    #[error("that node speaks protocol minor {found}; this call needs {needed} or later")]
    NodeTooOld {
        /// The minor the node said.
        found: u8,
        /// The minor the call needs.
        needed: u8,
    },

    /// The node sent the request elsewhere (protocol §3.12) and this client
    /// cannot follow: it was handed a stream it does not know how to dial
    /// again. Nothing ran; the redirect is the answer.
    #[error("the node sent this request to {} and this client cannot dial there", .0.endpoint)]
    Redirected(crate::wire::redirect::Redirect),

    /// Three redirects in a row and still no answer. Following further would
    /// not tell a loop from progress.
    #[error("still redirected after {hops} hops; stopping rather than going round")]
    RedirectLoop {
        /// How many were followed.
        hops: u8,
    },

    /// A redirect dated by an older leadership than one this request already
    /// followed: it was decided before that one and points at the past.
    #[error("redirected under epoch {epoch} after following epoch {floor}")]
    StaleRedirect {
        /// The epoch the redirect carried.
        epoch: u64,
        /// The newest epoch already followed.
        floor: u64,
    },

    /// The address a redirect named answered as a different node, so the
    /// request was not sent there.
    #[error("the redirect named another node than the one that answered there")]
    WrongNode {
        /// The node the redirect named.
        expected: [u8; 16],
    },

    /// The session's namespace or database is not a plain name, so it is not
    /// selected again on the node a redirect named — a name is grammar, and
    /// this client does not quote one into a script.
    #[error("cannot follow: `{name}` is not a plain name to select on the other node")]
    NotFollowable {
        /// The name refused.
        name: String,
    },

    /// A name refused before sending — a vault call's field or actor that is not
    /// a name (vault contract §4).
    #[error(transparent)]
    Build(#[from] crate::query::BuildError),

    /// An argument to a [`Vault`](crate::Vault) call the vault contract refuses
    /// before sending — a listing limit out of range, a write with no fields
    /// (vault contract §6).
    #[error("not a vault call: {reason}")]
    NotAVaultArgument {
        /// What was wrong with it.
        reason: &'static str,
    },

    /// An argument to a [`Space`](crate::Space) call the cache contract refuses
    /// before sending — a zero ttl, which would remove a key, a key listing out
    /// of range, an empty lock holder (cache contract §2).
    #[error("not a cache call: {reason}")]
    NotACacheArgument {
        /// What was wrong with it.
        reason: &'static str,
    },

    /// The call needs a credential and this handle holds none.
    ///
    /// The only failure here the node never saw, and deliberately so. It is
    /// raised by the two calls that need a password rather than merely an
    /// identity.
    ///
    /// [`Operations::change_password`](crate::Operations::change_password),
    /// whose request body **is** the new password: sending it to a route that
    /// will certainly answer `401` would put a secret on the wire — in the
    /// clear, since this client terminates no TLS — on an exchange that cannot
    /// succeed.
    ///
    /// [`Operations::open_session`](crate::Operations::open_session), which
    /// exists to spend a password once and hold something cheaper instead.
    /// A handle with no password has nothing to spend, and the route refuses a
    /// token by design.
    ///
    /// So the refusal happens before the socket opens. It is reported as itself
    /// rather than as a fabricated `401`, because a client inventing an answer
    /// from a node it never reached is a worse habit than an extra variant.
    #[error("this handle presents no credential, and that call needs the current one")]
    NoCredential,
}

/// Why a value would not decode.
///
/// Separate from [`Error`] so that a codec fault names the byte-level cause
/// without the transport enum growing a variant per tag.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EncodingFault {
    /// The bytes ran out before the value did.
    #[error("the value is truncated")]
    Truncated,

    /// A type tag this build has no type for.
    ///
    /// Refused rather than guessed at: a codec that infers a type from what
    /// follows reads a newer format as a plausible wrong value, and nothing
    /// downstream can tell.
    #[error("value tag {tag:#04x} is not one this client knows")]
    UnknownTag {
        /// The tag that arrived.
        tag: u8,
    },

    /// Text that is not valid UTF-8.
    #[error("a string is not valid UTF-8")]
    InvalidUtf8,

    /// A sub-second component outside the representable range.
    #[error("{nanos} is not a representable sub-second value")]
    InvalidSubSecond {
        /// What arrived.
        nanos: u32,
    },

    /// A variable-length component that never terminated.
    #[error("a variable-length component is unterminated")]
    Unterminated,

    /// An escape sequence that is not one.
    #[error("{found:#04x} is not a valid escape")]
    InvalidEscape {
        /// The byte that followed the escape.
        found: u8,
    },

    /// Bytes left over after a complete value.
    ///
    /// An error rather than something to ignore: trailing bytes mean this build
    /// and the sender disagree about the value's shape, and continuing would
    /// hand the caller a value that is right by luck.
    #[error("{count} bytes remain after a complete value")]
    TrailingBytes {
        /// How many were left.
        count: usize,
    },
}

impl From<EncodingFault> for Error {
    fn from(reason: EncodingFault) -> Self {
        Self::Encoding { reason }
    }
}
