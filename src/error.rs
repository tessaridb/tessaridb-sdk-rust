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
